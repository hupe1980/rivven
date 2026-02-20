//! Protobuf wire protocol integration tests
//!
//! Tests for the protobuf wire format support. These tests verify that the server
//! correctly handles protobuf-encoded requests and responds with protobuf-encoded
//! responses.
//!
//! The protobuf format enables cross-language client support (Go, Java, Python, etc.)
//!
//! Run with: cargo test -p rivven-integration-tests --test protobuf_protocol -- --nocapture

use anyhow::Result;
use bytes::Bytes;
use rivven_integration_tests::fixtures::TestBroker;
use rivven_integration_tests::helpers::*;
use rivven_protocol::{Request, Response, WireFormat};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::info;

// ============================================================================
// Protobuf Test Client
// ============================================================================

/// A simple protobuf client for testing wire format
struct ProtobufClient {
    stream: TcpStream,
}

impl ProtobufClient {
    /// Connect to a broker using raw TCP
    async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self { stream })
    }

    /// Send a request using protobuf wire format
    async fn send_request(&mut self, request: Request) -> Result<Response> {
        // Serialize with protobuf format byte (0x01)
        let request_bytes = request.to_wire(WireFormat::Protobuf, 0)?;

        // Send length-prefixed message
        let len = request_bytes.len() as u32;
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(&request_bytes).await?;
        self.stream.flush().await?;

        // Read response length
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let response_len = u32::from_be_bytes(len_buf) as usize;

        // Read response
        let mut response_buf = vec![0u8; response_len];
        self.stream.read_exact(&mut response_buf).await?;

        // Verify format byte is protobuf
        assert_eq!(
            response_buf[0], 0x01,
            "Expected protobuf format byte (0x01), got 0x{:02x}",
            response_buf[0]
        );

        // Parse response
        let (response, format, _correlation_id) = Response::from_wire(&response_buf)?;
        assert_eq!(format, WireFormat::Protobuf);

        Ok(response)
    }

    /// Send a request using postcard wire format
    async fn send_request_postcard(&mut self, request: Request) -> Result<(Response, WireFormat)> {
        let request_bytes = request.to_wire(WireFormat::Postcard, 0)?;

        let len = request_bytes.len() as u32;
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(&request_bytes).await?;
        self.stream.flush().await?;

        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let response_len = u32::from_be_bytes(len_buf) as usize;

        let mut response_buf = vec![0u8; response_len];
        self.stream.read_exact(&mut response_buf).await?;

        let (response, format, _correlation_id) = Response::from_wire(&response_buf)?;
        Ok((response, format))
    }
}

// ============================================================================
// Basic Protocol Tests
// ============================================================================

/// Test that server responds to protobuf Ping with protobuf Pong
#[tokio::test]
async fn test_protobuf_ping_pong() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    info!("Broker started at {}", broker.connection_string());

    let mut client = ProtobufClient::connect(&broker.connection_string()).await?;
    info!("Protobuf client connected");

    let response = client.send_request(Request::Ping).await?;
    assert!(matches!(response, Response::Pong));
    info!("Received Pong response via protobuf");

    broker.shutdown().await?;
    Ok(())
}

/// Test that server echoes the wire format back
#[tokio::test]
async fn test_format_echo() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = ProtobufClient::connect(&broker.connection_string()).await?;

    // Test postcard format
    let (response, format) = client.send_request_postcard(Request::Ping).await?;
    assert_eq!(format, WireFormat::Postcard);
    assert!(matches!(response, Response::Pong));
    info!("Postcard format echoed correctly");

    // Test protobuf format
    let response = client.send_request(Request::Ping).await?;
    assert!(matches!(response, Response::Pong));
    info!("Protobuf format echoed correctly");

    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// Topic Operations via Protobuf
// ============================================================================

/// Test creating a topic via protobuf
#[tokio::test]
async fn test_protobuf_create_topic() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = ProtobufClient::connect(&broker.connection_string()).await?;

    let topic_name = unique_topic_name("proto-create");

    let response = client
        .send_request(Request::CreateTopic {
            name: topic_name.clone(),
            partitions: Some(4),
        })
        .await?;

    if let Response::TopicCreated { name, partitions } = response {
        assert_eq!(name, topic_name);
        assert_eq!(partitions, 4);
        info!(
            "Created topic '{}' with {} partitions via protobuf",
            name, partitions
        );
    } else {
        panic!("Expected TopicCreated response, got {:?}", response);
    }

    broker.shutdown().await?;
    Ok(())
}

/// Test listing topics via protobuf
#[tokio::test]
async fn test_protobuf_list_topics() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = ProtobufClient::connect(&broker.connection_string()).await?;

    // Create topics
    let topic1 = unique_topic_name("proto-list-1");
    let topic2 = unique_topic_name("proto-list-2");

    client
        .send_request(Request::CreateTopic {
            name: topic1.clone(),
            partitions: Some(1),
        })
        .await?;

    client
        .send_request(Request::CreateTopic {
            name: topic2.clone(),
            partitions: Some(1),
        })
        .await?;

    // List topics
    let response = client.send_request(Request::ListTopics).await?;

    if let Response::Topics { topics } = response {
        assert!(topics.contains(&topic1), "Should contain topic1");
        assert!(topics.contains(&topic2), "Should contain topic2");
        info!("Listed {} topics via protobuf", topics.len());
    } else {
        panic!("Expected Topics response, got {:?}", response);
    }

    broker.shutdown().await?;
    Ok(())
}

/// Test deleting a topic via protobuf
#[tokio::test]
async fn test_protobuf_delete_topic() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = ProtobufClient::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("proto-delete");

    // Create topic
    client
        .send_request(Request::CreateTopic {
            name: topic.clone(),
            partitions: Some(1),
        })
        .await?;

    // Delete topic
    let response = client
        .send_request(Request::DeleteTopic {
            name: topic.clone(),
        })
        .await?;

    assert!(
        matches!(response, Response::TopicDeleted),
        "Expected TopicDeleted, got {:?}",
        response
    );
    info!("Deleted topic '{}' via protobuf", topic);

    // Verify topic is gone
    let response = client.send_request(Request::ListTopics).await?;
    if let Response::Topics { topics } = response {
        assert!(!topics.contains(&topic), "Topic should be deleted");
    }

    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// Publish/Consume via Protobuf
// ============================================================================

/// Test publishing a message via protobuf
#[tokio::test]
async fn test_protobuf_publish() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = ProtobufClient::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("proto-publish");

    // Create topic
    client
        .send_request(Request::CreateTopic {
            name: topic.clone(),
            partitions: Some(1),
        })
        .await?;

    // Publish message
    let response = client
        .send_request(Request::Publish {
            topic: topic.clone(),
            partition: Some(0),
            key: Some(Bytes::from("key-1")),
            value: Bytes::from("Hello via protobuf!"),
            leader_epoch: None,
        })
        .await?;

    if let Response::Published { offset, partition } = response {
        assert_eq!(partition, 0);
        info!("Published message at offset {} via protobuf", offset);
    } else {
        panic!("Expected Published response, got {:?}", response);
    }

    broker.shutdown().await?;
    Ok(())
}

/// Test full publish/consume cycle via protobuf
#[tokio::test]
async fn test_protobuf_publish_consume() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = ProtobufClient::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("proto-pubsub");

    // Create topic
    client
        .send_request(Request::CreateTopic {
            name: topic.clone(),
            partitions: Some(1),
        })
        .await?;

    // Publish multiple messages
    let messages = [
        ("key-1", "Message 1"),
        ("key-2", "Message 2"),
        ("key-3", "Message 3"),
    ];

    let mut first_offset = 0u64;
    for (i, (key, value)) in messages.iter().enumerate() {
        let response = client
            .send_request(Request::Publish {
                topic: topic.clone(),
                partition: Some(0),
                key: Some(Bytes::from(*key)),
                value: Bytes::from(*value),
                leader_epoch: None,
            })
            .await?;

        if let Response::Published { offset, .. } = response {
            if i == 0 {
                first_offset = offset;
            }
        }
    }
    info!("Published {} messages via protobuf", messages.len());

    // Consume messages
    let response = client
        .send_request(Request::Consume {
            topic: topic.clone(),
            partition: 0,
            offset: first_offset,
            max_messages: 10,
            isolation_level: None,
            max_wait_ms: None,
        })
        .await?;

    if let Response::Messages { messages: consumed } = response {
        assert_eq!(consumed.len(), 3);
        assert_eq!(consumed[0].value, Bytes::from("Message 1"));
        assert_eq!(consumed[1].value, Bytes::from("Message 2"));
        assert_eq!(consumed[2].value, Bytes::from("Message 3"));
        info!("Consumed {} messages via protobuf", consumed.len());
    } else {
        panic!("Expected Messages response, got {:?}", response);
    }

    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// Consumer Offset Management via Protobuf
// ============================================================================

/// Test commit and get offset via protobuf
#[tokio::test]
async fn test_protobuf_offset_management() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = ProtobufClient::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("proto-offset");
    let group = format!("group-{}", uuid::Uuid::new_v4());

    // Create topic
    client
        .send_request(Request::CreateTopic {
            name: topic.clone(),
            partitions: Some(1),
        })
        .await?;

    // Publish messages so that offset 3 is valid (offsets 0..4 → next_offset = 5)
    for i in 0..5u32 {
        client
            .send_request(Request::Publish {
                topic: topic.clone(),
                partition: Some(0),
                key: None,
                value: Bytes::from(format!("offset-msg-{}", i)),
                leader_epoch: None,
            })
            .await?;
    }

    // Commit offset
    let response = client
        .send_request(Request::CommitOffset {
            consumer_group: group.clone(),
            topic: topic.clone(),
            partition: 0,
            offset: 3,
        })
        .await?;

    assert!(
        matches!(response, Response::OffsetCommitted),
        "Expected OffsetCommitted, got {:?}",
        response
    );
    info!("Committed offset 3 for group '{}' via protobuf", group);

    // Get offset
    let response = client
        .send_request(Request::GetOffset {
            consumer_group: group.clone(),
            topic: topic.clone(),
            partition: 0,
        })
        .await?;

    if let Response::Offset { offset } = response {
        assert_eq!(offset, Some(3));
        info!("Retrieved offset {:?} via protobuf", offset);
    } else {
        panic!("Expected Offset response, got {:?}", response);
    }

    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// Metadata via Protobuf
// ============================================================================

/// Test getting topic metadata via protobuf
#[tokio::test]
async fn test_protobuf_metadata() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = ProtobufClient::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("proto-meta");

    // Create topic with 4 partitions
    client
        .send_request(Request::CreateTopic {
            name: topic.clone(),
            partitions: Some(4),
        })
        .await?;

    // Get metadata
    let response = client
        .send_request(Request::GetMetadata {
            topic: topic.clone(),
        })
        .await?;

    if let Response::Metadata { name, partitions } = response {
        assert_eq!(name, topic);
        assert_eq!(partitions, 4);
        info!("Got metadata: topic='{}', partitions={}", name, partitions);
    } else {
        panic!("Expected Metadata response, got {:?}", response);
    }

    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// Error Handling via Protobuf
// ============================================================================

/// Test error response via protobuf (topic not found)
#[tokio::test]
async fn test_protobuf_error_topic_not_found() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = ProtobufClient::connect(&broker.connection_string()).await?;

    // Try to consume from non-existent topic
    let response = client
        .send_request(Request::Consume {
            topic: "non-existent-topic".to_string(),
            partition: 0,
            offset: 0,
            max_messages: 10,
            isolation_level: None,
            max_wait_ms: None,
        })
        .await?;

    assert!(
        matches!(response, Response::Error { .. }),
        "Expected Error response for non-existent topic, got {:?}",
        response
    );
    info!("Received expected error for non-existent topic");

    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// Mixed Format Stress Test
// ============================================================================

/// Test alternating between postcard and protobuf formats
#[tokio::test]
async fn test_mixed_format_operations() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = ProtobufClient::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("proto-mixed");

    // Create topic (postcard)
    let (response, format) = client
        .send_request_postcard(Request::CreateTopic {
            name: topic.clone(),
            partitions: Some(1),
        })
        .await?;
    assert_eq!(format, WireFormat::Postcard);
    assert!(matches!(response, Response::TopicCreated { .. }));
    info!("Created topic via postcard");

    // Publish (protobuf)
    let response = client
        .send_request(Request::Publish {
            topic: topic.clone(),
            partition: Some(0),
            key: None,
            value: Bytes::from("mixed-format-message"),
            leader_epoch: None,
        })
        .await?;
    assert!(matches!(response, Response::Published { .. }));
    info!("Published via protobuf");

    // Consume (postcard)
    let (response, format) = client
        .send_request_postcard(Request::Consume {
            topic: topic.clone(),
            partition: 0,
            offset: 0,
            max_messages: 10,
            isolation_level: None,
            max_wait_ms: None,
        })
        .await?;
    assert_eq!(format, WireFormat::Postcard);
    if let Response::Messages { messages } = response {
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, Bytes::from("mixed-format-message"));
    }
    info!("Consumed via postcard");

    // List topics (protobuf)
    let response = client.send_request(Request::ListTopics).await?;
    if let Response::Topics { topics } = response {
        assert!(topics.contains(&topic));
    }
    info!("Listed topics via protobuf");

    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// Performance Comparison
// ============================================================================

/// Compare request/response times between postcard and protobuf
#[tokio::test]
async fn test_format_performance_comparison() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = ProtobufClient::connect(&broker.connection_string()).await?;

    const ITERATIONS: usize = 100;

    // Warm up
    for _ in 0..10 {
        client.send_request(Request::Ping).await?;
    }

    // Benchmark postcard
    let start = std::time::Instant::now();
    for _ in 0..ITERATIONS {
        client.send_request_postcard(Request::Ping).await?;
    }
    let postcard_duration = start.elapsed();

    // Benchmark protobuf
    let start = std::time::Instant::now();
    for _ in 0..ITERATIONS {
        client.send_request(Request::Ping).await?;
    }
    let protobuf_duration = start.elapsed();

    info!(
        "Performance ({} iterations): postcard={:?}, protobuf={:?}",
        ITERATIONS, postcard_duration, protobuf_duration
    );
    info!(
        "Per-request: postcard={:?}, protobuf={:?}",
        postcard_duration / ITERATIONS as u32,
        protobuf_duration / ITERATIONS as u32
    );

    // Both should complete in reasonable time
    assert!(postcard_duration < Duration::from_secs(5));
    assert!(protobuf_duration < Duration::from_secs(5));

    broker.shutdown().await?;
    Ok(())
}
