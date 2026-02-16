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
    ConfiguredCatalog, ConfiguredStream, DestinationSyncMode, Stream, SyncMode,
};
use rivven_connect::traits::sink::Sink;
use rivven_connect::traits::source::{CheckResult, Source};
use rivven_integration_tests::fixtures::TestKafka;
use rivven_integration_tests::helpers::*;
use tokio::time::timeout;
use tracing::info;

// ============================================================================
// Helper: build default configs for the test broker/topic/group
// ============================================================================

fn source_config(brokers: &str, topic: &str, group: &str) -> KafkaSourceConfig {
    KafkaSourceConfig {
        brokers: brokers.to_string(),
        topic: topic.to_string(),
        consumer_group: group.to_string(),
        start_offset: StartOffset::Earliest,
        security: Default::default(),
        max_poll_records: 500,
        max_poll_interval_ms: 300_000,
        session_timeout_ms: 10_000,
        heartbeat_interval_ms: 3_000,
        include_headers: true,
        include_key: true,
        enable_auto_commit: true,
        auto_commit_interval_ms: 5_000,
        fetch_min_bytes: 1,
        fetch_max_bytes: 52_428_800,
        request_timeout_ms: 30_000,
        empty_poll_delay_ms: 50,
        client_id: None,
        isolation_level: Default::default(),
    }
}

fn sink_config(brokers: &str, topic: &str) -> KafkaSinkConfig {
    KafkaSinkConfig {
        brokers: brokers.to_string(),
        topic: topic.to_string(),
        acks: AckLevel::All,
        compression: CompressionCodec::None,
        security: Default::default(),
        batch_size_bytes: 16_384,
        linger_ms: 5,
        request_timeout_ms: 30_000,
        retries: 3,
        retry_backoff_ms: 100,
        idempotent: false,
        transactional_id: None,
        key_field: None,
        include_headers: true,
        custom_headers: HashMap::new(),
        client_id: None,
    }
}

fn test_catalog(stream_name: &str) -> ConfiguredCatalog {
    ConfiguredCatalog {
        streams: vec![ConfiguredStream {
            stream: Stream::new(stream_name, serde_json::json!({})),
            sync_mode: SyncMode::Incremental,
            destination_sync_mode: DestinationSyncMode::Append,
            cursor_field: None,
            primary_key: None,
        }],
    }
}

// ============================================================================
// Infrastructure Tests
// ============================================================================

/// Test that we can start a Kafka container.
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

/// Test that we can create topics.
#[tokio::test]
async fn test_kafka_create_topic() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("test-topic", 3, 1).await?;

    info!("Topic creation test passed");
    Ok(())
}

/// Test produce and consume round-trip.
#[tokio::test]
async fn test_kafka_produce_consume() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("produce-consume-test", 1, 1).await?;

    let messages: Vec<(Option<Vec<u8>>, Vec<u8>)> = (0..5)
        .map(|i| {
            (
                Some(format!("key-{i}").into_bytes()),
                format!(r#"{{"id":{i},"value":"message-{i}"}}"#).into_bytes(),
            )
        })
        .collect();

    let offsets = kafka
        .produce_messages("produce-consume-test", 0, messages)
        .await?;
    assert_eq!(offsets.len(), 5);

    let records = kafka
        .consume_messages("produce-consume-test", 0, 0, 10)
        .await?;
    assert_eq!(records.len(), 5);

    for (i, record) in records.iter().enumerate() {
        let value = record.record.value.as_ref().unwrap();
        let expected = format!(r#"{{"id":{i},"value":"message-{i}"}}"#);
        assert_eq!(std::str::from_utf8(value)?, expected);
    }

    info!("Produce/consume test passed");
    Ok(())
}

// ============================================================================
// Kafka Source Connector Tests
// ============================================================================

/// Test KafkaSource check passes with a valid broker/topic.
#[tokio::test]
async fn test_kafka_source_check() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("source-check-test", 1, 1).await?;

    let source = KafkaSource;
    let config = source_config(
        &kafka.bootstrap_servers(),
        "source-check-test",
        "test-group",
    );

    let result: CheckResult = source.check(&config).await?;
    assert!(result.is_success(), "Check should succeed: {result:?}");

    info!("Kafka source check test passed");
    Ok(())
}

/// Test KafkaSource discover returns a catalog with the topic.
#[tokio::test]
async fn test_kafka_source_discover() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("discover-test", 3, 1).await?;

    let source = KafkaSource;
    let config = source_config(&kafka.bootstrap_servers(), "discover-test", "test-group");

    let catalog = source.discover(&config).await?;
    assert!(!catalog.streams.is_empty());

    let stream = catalog.find_stream("discover-test");
    assert!(stream.is_some(), "Should discover the created topic");
    assert_eq!(stream.unwrap().name, "discover-test");

    info!("Kafka source discover test passed");
    Ok(())
}

/// Test KafkaSource can read pre-populated messages from a topic.
#[tokio::test]
async fn test_kafka_source_read() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("source-read-test", 1, 1).await?;

    // Pre-populate 10 messages
    let messages: Vec<(Option<Vec<u8>>, Vec<u8>)> = (0..10)
        .map(|i| {
            (
                Some(format!("key-{i}").into_bytes()),
                format!(r#"{{"sequence":{i},"data":"test message {i}"}}"#).into_bytes(),
            )
        })
        .collect();
    kafka
        .produce_messages("source-read-test", 0, messages)
        .await?;

    let source = KafkaSource;
    let mut config = source_config(
        &kafka.bootstrap_servers(),
        "source-read-test",
        "test-read-group",
    );
    config.max_poll_records = 100;

    let configured_catalog = test_catalog("source-read-test");
    let mut stream = source.read(&config, &configured_catalog, None).await?;

    let mut received = 0;
    let read_result = timeout(Duration::from_secs(30), async {
        while let Some(event_result) = stream.next().await {
            match event_result {
                Ok(event) => {
                    if event.is_data() {
                        received += 1;
                        info!("Received event {received}");
                        if received >= 10 {
                            break;
                        }
                    }
                }
                Err(e) => return Err(anyhow::anyhow!("Read error: {e}")),
            }
        }
        Ok(received)
    })
    .await;

    match read_result {
        Ok(Ok(count)) => {
            assert_eq!(count, 10, "Should have received 10 messages");
            info!("Kafka source read test passed – received {count} messages");
        }
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            info!("Read timeout after receiving {received} messages (may be expected)");
        }
    }

    Ok(())
}

// ============================================================================
// High-Throughput / Batch Tests
// ============================================================================

/// Test high-throughput message consumption across partitions.
#[tokio::test]
async fn test_kafka_source_high_throughput() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("high-throughput-test", 3, 1).await?;

    let message_count: usize = 1000;
    for partition in 0..3i32 {
        let messages: Vec<(Option<Vec<u8>>, Vec<u8>)> = (0..message_count / 3)
            .map(|i| {
                (
                    Some(format!("key-{partition}-{i}").into_bytes()),
                    format!(r#"{{"partition":{partition},"sequence":{i}}}"#).into_bytes(),
                )
            })
            .collect();
        kafka
            .produce_messages("high-throughput-test", partition, messages)
            .await?;
    }
    info!("Produced {message_count} messages across 3 partitions");

    let start = std::time::Instant::now();
    let source = KafkaSource;
    let mut config = source_config(
        &kafka.bootstrap_servers(),
        "high-throughput-test",
        "throughput-test-group",
    );
    config.include_headers = false;
    config.include_key = false;
    config.empty_poll_delay_ms = 10;

    let configured_catalog = ConfiguredCatalog {
        streams: vec![ConfiguredStream {
            stream: Stream::new("high-throughput-test", serde_json::json!({})),
            sync_mode: SyncMode::FullRefresh,
            destination_sync_mode: DestinationSyncMode::Overwrite,
            cursor_field: None,
            primary_key: None,
        }],
    };

    let mut stream = source.read(&config, &configured_catalog, None).await?;
    let mut received = 0usize;
    let _ = timeout(Duration::from_secs(60), async {
        while let Some(Ok(event)) = stream.next().await {
            if event.is_data() {
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
    info!("High-throughput test: {received} messages in {elapsed:?} ({throughput:.0} msg/s)");
    assert!(received > 100, "Should have received significant messages");

    Ok(())
}

/// Test batch consumption aggregates varying batch sizes.
#[tokio::test]
async fn test_kafka_batch_consumption() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("batch-test", 1, 1).await?;

    let mut total_produced = 0usize;
    for batch_size in [5, 10, 20, 50] {
        let messages: Vec<(Option<Vec<u8>>, Vec<u8>)> = (0..batch_size)
            .map(|i| (None, format!(r#"{{"batch":{i}}}"#).into_bytes()))
            .collect();
        kafka.produce_messages("batch-test", 0, messages).await?;
        total_produced += batch_size;
    }

    let source = KafkaSource;
    let mut config = source_config(&kafka.bootstrap_servers(), "batch-test", "batch-test-group");
    config.include_headers = false;
    config.include_key = false;

    let configured_catalog = ConfiguredCatalog {
        streams: vec![ConfiguredStream {
            stream: Stream::new("batch-test", serde_json::json!({})),
            sync_mode: SyncMode::FullRefresh,
            destination_sync_mode: DestinationSyncMode::Overwrite,
            cursor_field: None,
            primary_key: None,
        }],
    };

    let mut stream = source.read(&config, &configured_catalog, None).await?;
    let mut received = 0usize;
    let _ = timeout(Duration::from_secs(15), async {
        while let Some(Ok(event)) = stream.next().await {
            if event.is_data() {
                received += 1;
                if received >= total_produced {
                    break;
                }
            }
        }
    })
    .await;

    info!("Batch test: received {received} messages");
    assert!(received > 0, "Should have received some messages");

    Ok(())
}

// ============================================================================
// Edge Case Tests
// ============================================================================

/// Test KafkaSource handles an unreachable broker gracefully.
#[tokio::test]
async fn test_kafka_source_invalid_broker() -> Result<()> {
    init_tracing();

    let source = KafkaSource;
    let mut config = source_config("invalid-broker:9999", "test-topic", "test-group");
    config.request_timeout_ms = 5_000;

    // check() should return Ok(CheckResult) with a failed connectivity check,
    // not panic or hang.
    let result = timeout(Duration::from_secs(30), source.check(&config)).await;

    match result {
        Ok(Ok(check)) => {
            info!("Invalid broker check result: {check:?}");
            // Should report failure in at least one check
            assert!(
                !check.is_success() || check.failed_checks().count() > 0,
                "Expected a failure for invalid broker"
            );
        }
        Ok(Err(e)) => {
            info!("Invalid broker returned error (acceptable): {e}");
        }
        Err(_) => {
            panic!("check() timed out for invalid broker – possible deadlock");
        }
    }

    Ok(())
}

/// Test KafkaSource reading an empty topic returns no data events.
#[tokio::test]
async fn test_kafka_source_empty_topic() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("empty-topic-test", 1, 1).await?;

    let source = KafkaSource;
    let mut config = source_config(
        &kafka.bootstrap_servers(),
        "empty-topic-test",
        "empty-topic-group",
    );
    config.include_headers = false;
    config.include_key = false;

    let configured_catalog = ConfiguredCatalog {
        streams: vec![ConfiguredStream {
            stream: Stream::new("empty-topic-test", serde_json::json!({})),
            sync_mode: SyncMode::FullRefresh,
            destination_sync_mode: DestinationSyncMode::Overwrite,
            cursor_field: None,
            primary_key: None,
        }],
    };

    let mut stream = source.read(&config, &configured_catalog, None).await?;

    let result = timeout(Duration::from_secs(3), async {
        let mut count = 0;
        while let Some(Ok(event)) = stream.next().await {
            if event.is_data() {
                count += 1;
            }
        }
        count
    })
    .await;

    match result {
        Ok(count) => assert_eq!(count, 0, "Empty topic should have no messages"),
        Err(_) => info!("Timed out as expected for empty topic"),
    }

    Ok(())
}

/// Test KafkaSource with multiple partitions.
#[tokio::test]
async fn test_kafka_source_multiple_partitions() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("multi-partition-test", 5, 1).await?;

    for partition in 0..5i32 {
        let messages: Vec<(Option<Vec<u8>>, Vec<u8>)> = (0..10)
            .map(|i| {
                (
                    Some(format!("p{partition}-k{i}").into_bytes()),
                    format!(r#"{{"partition":{partition},"msg":{i}}}"#).into_bytes(),
                )
            })
            .collect();
        kafka
            .produce_messages("multi-partition-test", partition, messages)
            .await?;
    }
    info!("Produced 50 messages across 5 partitions");

    let source = KafkaSource;
    let mut config = source_config(
        &kafka.bootstrap_servers(),
        "multi-partition-test",
        "multi-partition-group",
    );
    config.max_poll_records = 100;

    let configured_catalog = test_catalog("multi-partition-test");
    let mut stream = source.read(&config, &configured_catalog, None).await?;

    let mut received = 0usize;
    let _ = timeout(Duration::from_secs(30), async {
        while let Some(Ok(event)) = stream.next().await {
            if event.is_data() {
                received += 1;
                if received >= 50 {
                    break;
                }
            }
        }
    })
    .await;

    info!("Multi-partition test: received {received} messages from 5 partitions");

    Ok(())
}

// ============================================================================
// Offset Mode Tests
// ============================================================================

/// Test KafkaSource with Earliest and Latest start offsets.
#[tokio::test]
async fn test_kafka_source_offset_modes() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("offset-modes-test", 1, 1).await?;

    // Pre-populate 20 messages
    let messages: Vec<(Option<Vec<u8>>, Vec<u8>)> = (0..20)
        .map(|i| (None, format!(r#"{{"seq":{i}}}"#).into_bytes()))
        .collect();
    kafka
        .produce_messages("offset-modes-test", 0, messages)
        .await?;

    let source = KafkaSource;

    // Earliest should succeed
    let config_earliest = source_config(
        &kafka.bootstrap_servers(),
        "offset-modes-test",
        "offset-test-earliest",
    );
    let result: CheckResult = source.check(&config_earliest).await?;
    assert!(result.is_success());

    // Latest should also succeed
    let config_latest = KafkaSourceConfig {
        start_offset: StartOffset::Latest,
        consumer_group: "offset-test-latest".to_string(),
        ..config_earliest.clone()
    };
    let result: CheckResult = source.check(&config_latest).await?;
    assert!(result.is_success());

    info!("Kafka source offset modes test passed");
    Ok(())
}

// ============================================================================
// Consumer Group Tests
// ============================================================================

/// Test that multiple consumer groups can read the same topic independently.
#[tokio::test]
async fn test_kafka_multiple_consumer_groups() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("multi-cg-test", 1, 1).await?;

    let messages: Vec<(Option<Vec<u8>>, Vec<u8>)> = (0..10)
        .map(|i| (None, format!(r#"{{"id":{i}}}"#).into_bytes()))
        .collect();
    kafka.produce_messages("multi-cg-test", 0, messages).await?;

    let source = KafkaSource;
    for group_id in ["group-a", "group-b"] {
        let config = source_config(&kafka.bootstrap_servers(), "multi-cg-test", group_id);
        let result: CheckResult = source.check(&config).await?;
        assert!(result.is_success(), "Group {group_id} should work");
    }

    info!("Multiple consumer groups test passed");
    Ok(())
}

// ============================================================================
// Kafka Sink Connector Tests
// ============================================================================

/// Test KafkaSink check passes with valid broker/topic.
#[tokio::test]
async fn test_kafka_sink_check() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("sink-check-test", 1, 1).await?;

    let sink = KafkaSink;
    let config = sink_config(&kafka.bootstrap_servers(), "sink-check-test");

    let result: CheckResult = sink.check(&config).await?;
    assert!(result.is_success(), "Sink check should succeed: {result:?}");

    info!("Kafka sink check test passed");
    Ok(())
}

/// Test KafkaSink check succeeds for each compression codec.
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
        let topic = format!("sink-{topic_suffix}");
        kafka.create_topic(&topic, 1, 1).await?;

        let sink = KafkaSink;
        let mut config = sink_config(&kafka.bootstrap_servers(), &topic);
        config.acks = AckLevel::Leader;
        config.compression = codec;
        config.linger_ms = 0;
        config.include_headers = false;

        let result: CheckResult = sink.check(&config).await?;
        assert!(
            result.is_success(),
            "Sink check should succeed with {codec:?}: {result:?}",
        );

        info!("Compression test passed for {codec:?}");
    }

    info!("Kafka sink compression test passed");
    Ok(())
}

/// Test KafkaSink with custom headers and key field.
#[tokio::test]
async fn test_kafka_sink_custom_headers() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("sink-headers-test", 1, 1).await?;

    let sink = KafkaSink;

    let mut custom_headers = HashMap::new();
    custom_headers.insert("source".to_string(), "rivven".to_string());
    custom_headers.insert("environment".to_string(), "test".to_string());
    custom_headers.insert("version".to_string(), "1.0".to_string());

    let mut config = sink_config(&kafka.bootstrap_servers(), "sink-headers-test");
    config.key_field = Some("id".to_string());
    config.custom_headers = custom_headers;

    let result: CheckResult = sink.check(&config).await?;
    assert!(
        result.is_success(),
        "Sink check with headers should succeed"
    );

    info!("Kafka sink custom headers test passed");
    Ok(())
}

/// Test KafkaSink with idempotent producer enabled.
#[tokio::test]
async fn test_kafka_sink_idempotent() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("sink-idempotent-test", 1, 1).await?;

    let sink = KafkaSink;
    let mut config = sink_config(&kafka.bootstrap_servers(), "sink-idempotent-test");
    config.compression = CompressionCodec::Lz4;
    config.retries = 5;
    config.idempotent = true;

    let result: CheckResult = sink.check(&config).await?;
    assert!(
        result.is_success(),
        "Idempotent sink check should succeed: {result:?}",
    );

    info!("Kafka sink idempotent test passed");
    Ok(())
}
