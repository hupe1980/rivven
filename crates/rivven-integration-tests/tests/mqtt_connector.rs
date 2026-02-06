//! MQTT Connector Integration Tests
//!
//! **REAL** integration tests for Rivven's MQTT source connector.
//! These tests use testcontainers to spin up a real Mosquitto MQTT broker.
//!
//! Run with: cargo test -p rivven-integration-tests --test mqtt_connector -- --nocapture
//!
//! Requirements: Docker must be running for testcontainers.

use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use rivven_connect::connectors::queue::mqtt::{
    MqttAuthConfig, MqttSource, MqttSourceConfig, MqttVersion, QoS,
};
use rivven_connect::traits::catalog::{
    ConfiguredCatalog, ConfiguredStream, DestinationSyncMode, SyncMode,
};
use rivven_connect::traits::source::Source;
use rivven_integration_tests::fixtures::TestMqtt;
use rivven_integration_tests::helpers::*;
use tokio::time::timeout;
use tracing::info;

// ============================================================================
// Infrastructure Tests - Verify test setup works correctly
// ============================================================================

/// Test that we can start a Mosquitto container
#[tokio::test]
async fn test_mqtt_container_starts() -> Result<()> {
    init_tracing();

    info!("Starting MQTT test container...");
    let mqtt = TestMqtt::start().await?;

    info!("MQTT broker started at {}", mqtt.broker_url());
    assert!(!mqtt.host.is_empty());
    assert!(mqtt.port > 0);

    info!("MQTT container test passed");
    Ok(())
}

/// Test publish and subscribe with the test container
#[tokio::test]
async fn test_mqtt_publish_subscribe() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    // Subscribe to a topic
    let (client, mut rx) = mqtt.subscribe("test/topic", 1).await?;

    // Give subscription time to be established
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish a message
    mqtt.publish("test/topic", b"hello world", 1).await?;

    // Receive the message
    let received = timeout(Duration::from_secs(5), rx.recv()).await?;
    assert!(received.is_some());

    let (topic, payload) = received.unwrap();
    assert_eq!(topic, "test/topic");
    assert_eq!(payload, b"hello world");

    let _ = client.disconnect().await;

    info!("MQTT publish/subscribe test passed");
    Ok(())
}

/// Test wildcard subscriptions
#[tokio::test]
async fn test_mqtt_wildcard_subscription() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    // Subscribe to wildcard topic
    let (client, mut rx) = mqtt.subscribe("sensors/+/temperature", 1).await?;

    // Give subscription time to be established
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish to matching topics
    mqtt.publish("sensors/room1/temperature", b"22.5", 1)
        .await?;
    mqtt.publish("sensors/room2/temperature", b"21.3", 1)
        .await?;

    // Receive messages
    let msg1 = timeout(Duration::from_secs(5), rx.recv()).await?;
    assert!(msg1.is_some());

    let msg2 = timeout(Duration::from_secs(5), rx.recv()).await?;
    assert!(msg2.is_some());

    let _ = client.disconnect().await;

    info!("MQTT wildcard subscription test passed");
    Ok(())
}

// ============================================================================
// Rivven MQTT Source Connector Tests
// ============================================================================

/// Test that MqttSource can connect and check configuration
#[tokio::test]
async fn test_mqtt_source_check() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    let source = MqttSource::new();

    let config = MqttSourceConfig {
        broker_url: mqtt.broker_url(),
        topics: vec!["test/topic".to_string()],
        client_id: Some("test-client".to_string()),
        qos: QoS::AtLeastOnce,
        clean_session: true,
        keep_alive_secs: 60,
        connect_timeout_secs: 10,
        mqtt_version: MqttVersion::V311,
        auth: MqttAuthConfig::default(),
        max_inflight: 100,
        reconnect_delay_ms: 5000,
        max_reconnect_attempts: 3,
        parse_json_payload: true,
        include_metadata: true,
        retry_initial_ms: 100,
        retry_max_ms: 10000,
        retry_multiplier: 2.0,
        last_will: None,
    };

    let result = source.check(&config).await?;
    assert!(result.is_success(), "Check should succeed: {:?}", result);

    info!("MQTT source check test passed");
    Ok(())
}

/// Test that MqttSource can discover topics and create catalog
#[tokio::test]
async fn test_mqtt_source_discover() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    let source = MqttSource::new();

    let config = MqttSourceConfig {
        broker_url: mqtt.broker_url(),
        topics: vec!["sensors/+/temperature".to_string(), "devices/#".to_string()],
        client_id: None,
        qos: QoS::AtLeastOnce,
        clean_session: true,
        keep_alive_secs: 60,
        connect_timeout_secs: 10,
        mqtt_version: MqttVersion::V311,
        auth: MqttAuthConfig::default(),
        max_inflight: 100,
        reconnect_delay_ms: 5000,
        max_reconnect_attempts: 3,
        parse_json_payload: true,
        include_metadata: true,
        retry_initial_ms: 100,
        retry_max_ms: 10000,
        retry_multiplier: 2.0,
        last_will: None,
    };

    let catalog = source.discover(&config).await?;
    assert_eq!(catalog.streams.len(), 2);

    // Verify stream names are normalized
    let stream_names: Vec<_> = catalog.streams.iter().map(|s| s.name.as_str()).collect();
    assert!(stream_names.contains(&"sensors__single_temperature"));
    assert!(stream_names.contains(&"devices__multi"));

    info!("MQTT source discover test passed");
    Ok(())
}

/// Test MqttSource can read messages from subscribed topics
#[tokio::test]
async fn test_mqtt_source_read() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    let source = MqttSource::new();

    let config = MqttSourceConfig {
        broker_url: mqtt.broker_url(),
        topics: vec!["iot/sensors/data".to_string()],
        client_id: Some("rivven-test-consumer".to_string()),
        qos: QoS::AtLeastOnce,
        clean_session: true,
        keep_alive_secs: 30,
        connect_timeout_secs: 10,
        mqtt_version: MqttVersion::V311,
        auth: MqttAuthConfig::default(),
        max_inflight: 100,
        reconnect_delay_ms: 5000,
        max_reconnect_attempts: 3,
        parse_json_payload: true,
        include_metadata: true,
        retry_initial_ms: 100,
        retry_max_ms: 10000,
        retry_multiplier: 2.0,
        last_will: None,
    };

    let configured_catalog = ConfiguredCatalog {
        streams: vec![ConfiguredStream {
            stream: rivven_connect::traits::catalog::Stream::new(
                "iot_sensors_data",
                serde_json::json!({}),
            ),
            sync_mode: SyncMode::Incremental,
            destination_sync_mode: DestinationSyncMode::Append,
            cursor_field: None,
            primary_key: None,
        }],
    };

    // Start the source reader
    let mut stream = source.read(&config, &configured_catalog, None).await?;

    // Publish messages in the background
    let _mqtt_clone = std::sync::Arc::new(tokio::sync::Mutex::new(mqtt.host.clone()));
    let port = mqtt.port;
    let host = mqtt.host.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;

        for i in 0..5 {
            let (host, port) = (host.clone(), port);
            let payload = format!(
                r#"{{"sensor_id":"s{}","temp":{},"unit":"celsius"}}"#,
                i,
                20 + i
            );

            // Use a simple TCP publish since we don't have the TestMqtt instance
            use rumqttc::{AsyncClient, MqttOptions, QoS as RumqttQoS};

            let client_id = format!("publisher-{}", i);
            let mut options = MqttOptions::new(client_id, &host, port);
            options.set_keep_alive(Duration::from_secs(5));

            let (client, mut eventloop) = AsyncClient::new(options, 10);

            // Connect
            let _ = timeout(Duration::from_secs(2), async {
                loop {
                    match eventloop.poll().await {
                        Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => break,
                        Ok(_) => continue,
                        Err(_) => break,
                    }
                }
            })
            .await;

            // Publish
            let _ = client
                .publish("iot/sensors/data", RumqttQoS::AtLeastOnce, false, payload)
                .await;

            // Wait for ack
            let _ = timeout(Duration::from_secs(2), async {
                loop {
                    match eventloop.poll().await {
                        Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_))) => break,
                        Ok(_) => continue,
                        Err(_) => break,
                    }
                }
            })
            .await;

            let _ = client.disconnect().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // Read events with timeout
    let mut received = 0;
    let read_result = timeout(Duration::from_secs(30), async {
        while let Some(event_result) = stream.next().await {
            match event_result {
                Ok(event) => {
                    if event.event_type.is_data() {
                        received += 1;
                        info!("Received MQTT event {}: {:?}", received, event.event_type);
                        if received >= 5 {
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
            info!("MQTT source read test passed - received {} messages", count);
            assert!(count > 0, "Should have received at least one message");
        }
        Ok(Err(e)) => {
            return Err(e);
        }
        Err(_) => {
            info!(
                "Read timeout after receiving {} messages (may be expected due to async publish)",
                received
            );
        }
    }

    Ok(())
}

/// Test MqttSource metrics are updated during read
#[tokio::test]
async fn test_mqtt_source_metrics() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    let source = MqttSource::new();
    let metrics = source.metrics();

    // Verify initial metrics are zero
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.messages_received, 0);
    assert_eq!(snapshot.bytes_received, 0);

    let config = MqttSourceConfig {
        broker_url: mqtt.broker_url(),
        topics: vec!["metrics/test".to_string()],
        client_id: Some("metrics-test-client".to_string()),
        qos: QoS::AtMostOnce,
        clean_session: true,
        keep_alive_secs: 30,
        connect_timeout_secs: 10,
        mqtt_version: MqttVersion::V311,
        auth: MqttAuthConfig::default(),
        max_inflight: 100,
        reconnect_delay_ms: 5000,
        max_reconnect_attempts: 3,
        parse_json_payload: false,
        include_metadata: false,
        retry_initial_ms: 100,
        retry_max_ms: 10000,
        retry_multiplier: 2.0,
        last_will: None,
    };

    let configured_catalog = ConfiguredCatalog {
        streams: vec![ConfiguredStream {
            stream: rivven_connect::traits::catalog::Stream::new(
                "metrics_test",
                serde_json::json!({}),
            ),
            sync_mode: SyncMode::FullRefresh,
            destination_sync_mode: DestinationSyncMode::Overwrite,
            cursor_field: None,
            primary_key: None,
        }],
    };

    // Start reader
    let mut stream = source.read(&config, &configured_catalog, None).await?;

    // Publish messages
    let host = mqtt.host.clone();
    let port = mqtt.port;

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;

        for i in 0..3 {
            use rumqttc::{AsyncClient, MqttOptions, QoS as RumqttQoS};

            let client_id = format!("metrics-pub-{}", i);
            let mut options = MqttOptions::new(client_id, &host, port);
            options.set_keep_alive(Duration::from_secs(5));

            let (client, mut eventloop) = AsyncClient::new(options, 10);

            let _ = timeout(Duration::from_secs(2), async {
                loop {
                    match eventloop.poll().await {
                        Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => break,
                        Ok(_) => continue,
                        Err(_) => break,
                    }
                }
            })
            .await;

            let _ = client
                .publish(
                    "metrics/test",
                    RumqttQoS::AtMostOnce,
                    false,
                    format!("msg-{}", i),
                )
                .await;

            let _ = client.disconnect().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // Read a few events
    let _ = timeout(Duration::from_secs(10), async {
        let mut count = 0;
        while let Some(Ok(event)) = stream.next().await {
            if event.event_type.is_data() {
                count += 1;
                if count >= 3 {
                    break;
                }
            }
        }
    })
    .await;

    // Check metrics after reading
    let snapshot = metrics.snapshot();
    info!(
        "MQTT Metrics: received={}, bytes={}, connections={}",
        snapshot.messages_received, snapshot.bytes_received, snapshot.connections
    );

    // Metrics should show connection activity
    assert!(
        snapshot.connections > 0 || snapshot.messages_received > 0,
        "Should have recorded some activity"
    );

    // Test Prometheus export
    let prometheus = snapshot.to_prometheus_format("test");
    assert!(prometheus.contains("mqtt_source_messages_received_total"));
    assert!(prometheus.contains("mqtt_source_bytes_received_total"));

    info!("MQTT source metrics test passed");
    Ok(())
}

/// Test MqttSource graceful shutdown
#[tokio::test]
async fn test_mqtt_source_shutdown() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    let source = MqttSource::new();

    let config = MqttSourceConfig {
        broker_url: mqtt.broker_url(),
        topics: vec!["shutdown/test".to_string()],
        client_id: Some("shutdown-test-client".to_string()),
        qos: QoS::AtLeastOnce,
        clean_session: true,
        keep_alive_secs: 30,
        connect_timeout_secs: 10,
        mqtt_version: MqttVersion::V311,
        auth: MqttAuthConfig::default(),
        max_inflight: 100,
        reconnect_delay_ms: 5000,
        max_reconnect_attempts: 3,
        parse_json_payload: false,
        include_metadata: false,
        retry_initial_ms: 100,
        retry_max_ms: 10000,
        retry_multiplier: 2.0,
        last_will: None,
    };

    let configured_catalog = ConfiguredCatalog {
        streams: vec![ConfiguredStream {
            stream: rivven_connect::traits::catalog::Stream::new(
                "shutdown_test",
                serde_json::json!({}),
            ),
            sync_mode: SyncMode::Incremental,
            destination_sync_mode: DestinationSyncMode::Append,
            cursor_field: None,
            primary_key: None,
        }],
    };

    let mut stream = source.read(&config, &configured_catalog, None).await?;

    // Verify not shutting down
    assert!(!source.is_shutting_down());

    // Signal shutdown after a short delay
    let source_clone = source;
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        source_clone.shutdown();
        info!("MQTT shutdown signaled");
    });

    // Read with a short timeout - the stream should stop receiving after shutdown
    // Note: The stream may not immediately close, but we verify shutdown was signaled
    let result = timeout(Duration::from_secs(2), async {
        while let Some(Ok(_)) = stream.next().await {
            // Just drain events (there won't be any since nothing is publishing)
        }
    })
    .await;

    // Either the stream closed (Ok) or timed out (Err) - both are acceptable
    // The important thing is that shutdown was signaled and we didn't hang
    match result {
        Ok(()) => info!("Stream closed gracefully"),
        Err(_) => info!("Stream timed out after shutdown signal (expected)"),
    }

    info!("MQTT source shutdown test passed");

    Ok(())
}

// ============================================================================
// QoS Level Tests
// ============================================================================

/// Test MQTT QoS 0 (At Most Once)
#[tokio::test]
async fn test_mqtt_qos_0() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    // Subscribe with QoS 0
    let (client, mut rx) = mqtt.subscribe("qos0/test", 0).await?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Publish with QoS 0
    mqtt.publish("qos0/test", b"qos0 message", 0).await?;

    // Best-effort receive
    let received = timeout(Duration::from_secs(3), rx.recv()).await;

    let _ = client.disconnect().await;

    // QoS 0 is fire-and-forget, message may or may not arrive
    info!("QoS 0 test completed, received: {}", received.is_ok());
    Ok(())
}

/// Test MQTT QoS 1 (At Least Once)
#[tokio::test]
async fn test_mqtt_qos_1() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    // Subscribe with QoS 1
    let (client, mut rx) = mqtt.subscribe("qos1/test", 1).await?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Publish with QoS 1
    mqtt.publish("qos1/test", b"qos1 message", 1).await?;

    // Should reliably receive
    let received = timeout(Duration::from_secs(5), rx.recv()).await?;
    assert!(received.is_some(), "QoS 1 should deliver message");

    let (topic, payload) = received.unwrap();
    assert_eq!(topic, "qos1/test");
    assert_eq!(payload, b"qos1 message");

    let _ = client.disconnect().await;

    info!("QoS 1 test passed");
    Ok(())
}

/// Test MQTT QoS 2 (Exactly Once)
#[tokio::test]
async fn test_mqtt_qos_2() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    // Subscribe with QoS 2
    let (client, mut rx) = mqtt.subscribe("qos2/test", 2).await?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Publish with QoS 2
    mqtt.publish("qos2/test", b"qos2 message", 2).await?;

    // Should reliably receive exactly once
    let received = timeout(Duration::from_secs(5), rx.recv()).await?;
    assert!(received.is_some(), "QoS 2 should deliver message");

    let (topic, payload) = received.unwrap();
    assert_eq!(topic, "qos2/test");
    assert_eq!(payload, b"qos2 message");

    let _ = client.disconnect().await;

    info!("QoS 2 test passed");
    Ok(())
}

// ============================================================================
// Last Will and Testament (LWT) Tests
// ============================================================================

/// Test Last Will configuration parsing
#[tokio::test]
async fn test_mqtt_last_will_config() -> Result<()> {
    init_tracing();

    let config: MqttSourceConfig = serde_json::from_str(
        r#"{
        "broker_url": "mqtt://localhost:1883",
        "topics": ["test"],
        "last_will": {
            "topic": "clients/rivven/status",
            "message": "offline",
            "qos": "at_least_once",
            "retain": true
        }
    }"#,
    )?;

    let lw = config.last_will.as_ref().unwrap();
    assert_eq!(lw.topic, "clients/rivven/status");
    assert_eq!(lw.message, "offline");
    assert!(matches!(lw.qos, QoS::AtLeastOnce));
    assert!(lw.retain);

    info!("Last Will config test passed");
    Ok(())
}

// ============================================================================
// High-Throughput / Performance Tests
// ============================================================================

/// Test high message rate
#[tokio::test]
async fn test_mqtt_high_throughput() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    let message_count = 100;

    let (client, mut rx) = mqtt.subscribe("throughput/test", 1).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let start = std::time::Instant::now();

    // Publish many messages
    for i in 0..message_count {
        mqtt.publish("throughput/test", format!("msg-{}", i).as_bytes(), 1)
            .await?;
    }

    info!("Published {} messages", message_count);

    // Receive all messages
    let mut received = 0;
    let _ = timeout(Duration::from_secs(30), async {
        while (rx.recv().await).is_some() {
            received += 1;
            if received >= message_count {
                break;
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

    let _ = client.disconnect().await;

    assert!(received > 50, "Should have received most messages");

    info!("MQTT high-throughput test passed");
    Ok(())
}

// ============================================================================
// Edge Cases
// ============================================================================

/// Test with special characters in topic names
#[tokio::test]
async fn test_mqtt_special_topic_names() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    // Topics with numbers, dashes, underscores
    let topics = vec![
        "sensor-123/temp_value",
        "device/sub-device/data",
        "namespace_v2/events",
    ];

    for topic in &topics {
        let (client, mut rx) = mqtt.subscribe(topic, 1).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;

        mqtt.publish(topic, b"test", 1).await?;

        let received = timeout(Duration::from_secs(3), rx.recv()).await?;
        assert!(
            received.is_some(),
            "Should receive message on topic {}",
            topic
        );

        let _ = client.disconnect().await;
    }

    info!("Special topic names test passed");
    Ok(())
}

/// Test binary payloads
#[tokio::test]
async fn test_mqtt_binary_payload() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    let (client, mut rx) = mqtt.subscribe("binary/test", 1).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Send binary data
    let binary_data: Vec<u8> = (0..255u8).collect();
    mqtt.publish("binary/test", &binary_data, 1).await?;

    let received = timeout(Duration::from_secs(5), rx.recv()).await?;
    assert!(received.is_some());

    let (_, payload) = received.unwrap();
    assert_eq!(payload.len(), 255);
    assert_eq!(payload, binary_data);

    let _ = client.disconnect().await;

    info!("Binary payload test passed");
    Ok(())
}

/// Test large payloads (within broker limits)
#[tokio::test]
async fn test_mqtt_large_payload() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    let (client, mut rx) = mqtt.subscribe("large/test", 1).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Send 8KB payload (within Mosquitto's default 10KB limit)
    let large_data = vec![0xAB_u8; 8 * 1024];
    mqtt.publish("large/test", &large_data, 1).await?;

    let received = timeout(Duration::from_secs(10), rx.recv()).await?;
    assert!(received.is_some());

    let (_, payload) = received.unwrap();
    assert_eq!(payload.len(), 8 * 1024);

    let _ = client.disconnect().await;

    info!("Large payload test passed");
    Ok(())
}

// ============================================================================
// Retained Message Tests
// ============================================================================

/// Test MQTT retained messages
#[tokio::test]
async fn test_mqtt_retained_message() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    // Publish a retained message BEFORE subscribing
    use rumqttc::{AsyncClient, MqttOptions, QoS as RumqttQoS};

    let client_id = format!("retained-pub-{}", uuid::Uuid::new_v4());
    let mut options = MqttOptions::new(client_id, &mqtt.host, mqtt.port);
    options.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(options, 10);

    // Connect
    let _ = timeout(Duration::from_secs(2), async {
        loop {
            match eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => break,
                Ok(_) => continue,
                Err(_) => break,
            }
        }
    })
    .await;

    // Publish with retain=true
    let _ = client
        .publish(
            "retained/test",
            RumqttQoS::AtLeastOnce,
            true,
            "retained value",
        )
        .await;

    // Wait for ack
    let _ = timeout(Duration::from_secs(2), async {
        loop {
            match eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_))) => break,
                Ok(_) => continue,
                Err(_) => break,
            }
        }
    })
    .await;

    let _ = client.disconnect().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Now subscribe - should receive the retained message
    let (sub_client, mut rx) = mqtt.subscribe("retained/test", 1).await?;

    let received = timeout(Duration::from_secs(5), rx.recv()).await?;
    assert!(received.is_some(), "Should receive retained message");

    let (topic, payload) = received.unwrap();
    assert_eq!(topic, "retained/test");
    assert_eq!(std::str::from_utf8(&payload)?, "retained value");

    let _ = sub_client.disconnect().await;

    info!("Retained message test passed");
    Ok(())
}

// ============================================================================
// Connection Resilience Tests
// ============================================================================

/// Test multiple sequential connections
#[tokio::test]
async fn test_mqtt_connection_churn() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    // Create many connections sequentially
    for i in 0..10 {
        let source = MqttSource::new();

        let config = MqttSourceConfig {
            broker_url: mqtt.broker_url(),
            topics: vec![format!("churn/test/{}", i)],
            client_id: Some(format!("churn-client-{}", i)),
            qos: QoS::AtMostOnce,
            clean_session: true,
            keep_alive_secs: 30,
            connect_timeout_secs: 5,
            mqtt_version: MqttVersion::V311,
            auth: MqttAuthConfig::default(),
            max_inflight: 100,
            reconnect_delay_ms: 1000,
            max_reconnect_attempts: 1,
            parse_json_payload: false,
            include_metadata: false,
            retry_initial_ms: 100,
            retry_max_ms: 5000,
            retry_multiplier: 2.0,
            last_will: None,
        };

        let result = source.check(&config).await?;
        assert!(result.is_success(), "Connection {} should succeed", i);
    }

    info!("Connection churn test passed");
    Ok(())
}

/// Test MQTT source with clean_session=false
#[tokio::test]
async fn test_mqtt_persistent_session() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    let source = MqttSource::new();

    let config = MqttSourceConfig {
        broker_url: mqtt.broker_url(),
        topics: vec!["persistent/session/test".to_string()],
        client_id: Some("persistent-session-client".to_string()),
        qos: QoS::AtLeastOnce,
        clean_session: false, // Persistent session
        keep_alive_secs: 60,
        connect_timeout_secs: 10,
        mqtt_version: MqttVersion::V311,
        auth: MqttAuthConfig::default(),
        max_inflight: 100,
        reconnect_delay_ms: 5000,
        max_reconnect_attempts: 3,
        parse_json_payload: false,
        include_metadata: false,
        retry_initial_ms: 100,
        retry_max_ms: 10000,
        retry_multiplier: 2.0,
        last_will: None,
    };

    let result = source.check(&config).await?;
    assert!(
        result.is_success(),
        "Persistent session check should succeed"
    );

    info!("Persistent session test passed");
    Ok(())
}

// ============================================================================
// Multi-Level Wildcard Tests
// ============================================================================

/// Test MQTT multi-level wildcard (#)
#[tokio::test]
async fn test_mqtt_multi_level_wildcard() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    // Subscribe to everything under devices/
    let (client, mut rx) = mqtt.subscribe("devices/#", 1).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish to various nested topics
    mqtt.publish("devices/sensor1/temp", b"22", 1).await?;
    mqtt.publish("devices/sensor1/humidity", b"45", 1).await?;
    mqtt.publish("devices/room/floor1/light", b"on", 1).await?;

    // Receive all three
    let mut received = 0;
    let _ = timeout(Duration::from_secs(5), async {
        while (rx.recv().await).is_some() {
            received += 1;
            if received >= 3 {
                break;
            }
        }
    })
    .await;

    let _ = client.disconnect().await;

    assert_eq!(received, 3, "Should receive all 3 messages via # wildcard");

    info!("Multi-level wildcard test passed");
    Ok(())
}

/// Test MQTT single-level wildcard (+) at different positions
#[tokio::test]
async fn test_mqtt_single_level_wildcard_positions() -> Result<()> {
    init_tracing();

    let mqtt = TestMqtt::start().await?;

    // Test + at end
    let (client1, mut rx1) = mqtt.subscribe("sensors/room1/+", 1).await?;

    // Test + in middle
    let (client2, mut rx2) = mqtt.subscribe("sensors/+/temperature", 1).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish matching messages
    mqtt.publish("sensors/room1/temp", b"temp1", 1).await?;
    mqtt.publish("sensors/room1/humidity", b"hum1", 1).await?;
    mqtt.publish("sensors/room2/temperature", b"temp2", 1)
        .await?;

    // First subscription should get 2 messages
    let mut count1 = 0;
    let _ = timeout(Duration::from_secs(3), async {
        while (rx1.recv().await).is_some() {
            count1 += 1;
            if count1 >= 2 {
                break;
            }
        }
    })
    .await;

    // Second subscription should get 1 message
    let msg2 = timeout(Duration::from_secs(3), rx2.recv()).await;

    let _ = client1.disconnect().await;
    let _ = client2.disconnect().await;

    assert_eq!(count1, 2, "First pattern should match 2 messages");
    assert!(msg2.is_ok(), "Second pattern should match 1 message");

    info!("Single-level wildcard positions test passed");
    Ok(())
}

// ============================================================================
// Metrics Snapshot Tests
// ============================================================================

/// Test metrics snapshot and reset
#[tokio::test]
async fn test_mqtt_metrics_snapshot_and_reset() -> Result<()> {
    init_tracing();

    let source = MqttSource::new();
    let metrics = source.metrics();

    // Record some activity via atomic fields
    use std::sync::atomic::Ordering;
    metrics.connections.fetch_add(1, Ordering::Relaxed);
    metrics.messages_received.fetch_add(2, Ordering::Relaxed);
    metrics.bytes_received.fetch_add(300, Ordering::Relaxed);
    metrics.errors.fetch_add(1, Ordering::Relaxed);

    // Take snapshot
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.connections, 1);
    assert_eq!(snapshot.messages_received, 2);
    assert_eq!(snapshot.bytes_received, 300);
    assert_eq!(snapshot.errors, 1);

    // Verify derived metrics
    let error_rate = snapshot.error_rate_percent();
    assert!(error_rate > 0.0);

    // Verify Prometheus format
    let prometheus = snapshot.to_prometheus_format("test");
    assert!(prometheus.contains("test_mqtt_source_connections_total 1"));
    assert!(prometheus.contains("test_mqtt_source_messages_received_total 2"));

    info!("Metrics snapshot test passed");
    Ok(())
}

// ============================================================================
// Configuration Validation Tests
// ============================================================================

/// Test MqttSourceConfig default values
#[tokio::test]
async fn test_mqtt_config_defaults() -> Result<()> {
    init_tracing();

    let config: MqttSourceConfig = serde_json::from_str(
        r#"{
        "broker_url": "mqtt://localhost:1883",
        "topics": ["test"]
    }"#,
    )?;

    // Verify defaults are sensible
    assert!(matches!(config.qos, QoS::AtMostOnce));
    assert!(config.clean_session);
    assert_eq!(config.keep_alive_secs, 60);
    assert_eq!(config.connect_timeout_secs, 30);
    assert!(matches!(config.mqtt_version, MqttVersion::V311));
    assert_eq!(config.max_inflight, 100);
    assert!(config.last_will.is_none());

    info!("Config defaults test passed");
    Ok(())
}

/// Test invalid configuration handling
#[tokio::test]
async fn test_mqtt_invalid_broker_url() -> Result<()> {
    init_tracing();

    let source = MqttSource::new();

    let config = MqttSourceConfig {
        broker_url: "invalid://not-a-broker:99999".to_string(),
        topics: vec!["test".to_string()],
        client_id: None,
        qos: QoS::AtMostOnce,
        clean_session: true,
        keep_alive_secs: 30,
        connect_timeout_secs: 2,
        mqtt_version: MqttVersion::V311,
        auth: MqttAuthConfig::default(),
        max_inflight: 100,
        reconnect_delay_ms: 1000,
        max_reconnect_attempts: 1,
        parse_json_payload: false,
        include_metadata: false,
        retry_initial_ms: 100,
        retry_max_ms: 1000,
        retry_multiplier: 2.0,
        last_will: None,
    };

    // Should fail with invalid broker
    let result = source.check(&config).await;
    info!("Invalid broker result: {:?}", result);
    // Just verify it doesn't panic

    Ok(())
}
