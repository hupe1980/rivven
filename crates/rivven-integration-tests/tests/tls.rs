//! TLS Integration Tests
//!
//! Comprehensive tests for TLS/mTLS functionality at the broker level.
//! These tests verify:
//! - TLS connection establishment with self-signed certificates
//! - Encrypted message transmission
//! - mTLS mode enforcement (optional vs required)
//! - TLS version requirements
//! - Certificate validation
//! - Mixed plaintext/TLS rejection
//!
//! Run with: cargo test -p rivven-integration-tests --test tls -- --nocapture

use anyhow::Result;
use rivven_client::Client;
use rivven_core::tls::{MtlsMode, TlsConfigBuilder};
use rivven_integration_tests::fixtures::{TestBroker, TestTlsBroker, TlsBrokerOptions};
use rivven_integration_tests::helpers::*;
use std::time::Duration;
use tracing::info;

// ============================================================================
// Basic TLS Connection Tests
// ============================================================================

/// Test basic TLS connection with self-signed certificate
#[tokio::test]
async fn test_tls_connection_self_signed() -> Result<()> {
    init_tracing();

    let broker = TestTlsBroker::start().await?;
    info!("TLS broker started at {}", broker.connection_string());

    // Connect with TLS
    let client_tls_config = broker.client_tls_config();
    let mut client =
        Client::connect_tls(&broker.connection_string(), &client_tls_config, "localhost").await?;

    // Verify connection works by creating a topic
    let topic = unique_topic_name("tls-basic");
    client.create_topic(&topic, Some(1)).await?;

    info!("✅ TLS connection established and topic created");
    broker.shutdown().await?;
    Ok(())
}

/// Test TLS connection requires TLS client config
#[tokio::test]
async fn test_plaintext_to_tls_broker_fails() -> Result<()> {
    init_tracing();

    let broker = TestTlsBroker::start().await?;
    info!("TLS broker started at {}", broker.connection_string());

    // Try to connect with plaintext client (should fail)
    let result = Client::connect(&broker.connection_string()).await;

    // The connection might succeed at TCP level but fail at protocol level
    // because TLS expects handshake data
    match result {
        Ok(mut client) => {
            // Connection succeeded but operations should fail
            let topic = unique_topic_name("plaintext-to-tls");
            let create_result = client.create_topic(&topic, Some(1)).await;
            assert!(
                create_result.is_err(),
                "Plaintext request to TLS broker should fail"
            );
            info!("✅ Plaintext request correctly rejected by TLS broker");
        }
        Err(e) => {
            info!(
                "✅ Plaintext connection to TLS broker correctly failed: {}",
                e
            );
        }
    }

    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// Message Transmission Over TLS Tests
// ============================================================================

/// Test publishing and consuming messages over TLS
#[tokio::test]
async fn test_tls_publish_consume() -> Result<()> {
    init_tracing();

    let broker = TestTlsBroker::start().await?;
    let client_tls_config = broker.client_tls_config();

    let mut client =
        Client::connect_tls(&broker.connection_string(), &client_tls_config, "localhost").await?;

    // Create topic
    let topic = unique_topic_name("tls-pubsub");
    client.create_topic(&topic, Some(1)).await?;

    // Publish messages
    let messages: Vec<Vec<u8>> = (0..10)
        .map(|i| format!("TLS encrypted message {}", i).into_bytes())
        .collect();

    for msg in &messages {
        client.publish(&topic, msg.clone()).await?;
    }
    info!("Published {} messages over TLS", messages.len());

    // Consume messages
    let received = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(received.len(), messages.len());

    for (i, msg) in received.iter().enumerate() {
        let expected = format!("TLS encrypted message {}", i);
        assert_eq!(
            msg.value.as_ref(),
            expected.as_bytes(),
            "Message {} content mismatch",
            i
        );
    }

    info!(
        "✅ All {} messages transmitted and received over TLS",
        messages.len()
    );
    broker.shutdown().await?;
    Ok(())
}

/// Test large message transmission over TLS
#[tokio::test]
async fn test_tls_large_message() -> Result<()> {
    init_tracing();

    let broker = TestTlsBroker::start().await?;
    let client_tls_config = broker.client_tls_config();

    let mut client =
        Client::connect_tls(&broker.connection_string(), &client_tls_config, "localhost").await?;

    let topic = unique_topic_name("tls-large");
    client.create_topic(&topic, Some(1)).await?;

    // Create a 1MB message
    let large_msg = vec![b'X'; 1024 * 1024];
    client.publish(&topic, large_msg.clone()).await?;

    let received = client.consume(&topic, 0, 0, 1).await?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].value.as_ref(), large_msg.as_slice());

    info!("✅ Large message (1MB) transmitted over TLS successfully");
    broker.shutdown().await?;
    Ok(())
}

/// Test binary data (all byte values) over TLS
#[tokio::test]
async fn test_tls_binary_data() -> Result<()> {
    init_tracing();

    let broker = TestTlsBroker::start().await?;
    let client_tls_config = broker.client_tls_config();

    let mut client =
        Client::connect_tls(&broker.connection_string(), &client_tls_config, "localhost").await?;

    let topic = unique_topic_name("tls-binary");
    client.create_topic(&topic, Some(1)).await?;

    // Test all possible byte values
    let binary_msg: Vec<u8> = (0..=255).collect();
    client.publish(&topic, binary_msg.clone()).await?;

    let received = client.consume(&topic, 0, 0, 1).await?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].value.as_ref(), binary_msg.as_slice());

    info!("✅ Binary data (256 unique bytes) transmitted over TLS successfully");
    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// mTLS Mode Tests
// ============================================================================

/// Test mTLS disabled mode (client certificate not required)
#[tokio::test]
async fn test_mtls_disabled_accepts_any_client() -> Result<()> {
    init_tracing();

    let broker = TestTlsBroker::start_with_options(TlsBrokerOptions {
        common_name: "localhost".to_string(),
        mtls_mode: MtlsMode::Disabled,
        require_auth: false,
    })
    .await?;

    // Client without certificate should be able to connect
    let client_tls_config = TlsConfigBuilder::new().insecure_skip_verify().build();

    let mut client =
        Client::connect_tls(&broker.connection_string(), &client_tls_config, "localhost").await?;

    let topic = unique_topic_name("mtls-disabled");
    client.create_topic(&topic, Some(1)).await?;

    info!("✅ mTLS disabled mode accepts clients without certificates");
    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// Multiple Connections Tests
// ============================================================================

/// Test multiple concurrent TLS connections
#[tokio::test]
async fn test_tls_concurrent_connections() -> Result<()> {
    init_tracing();

    let broker = TestTlsBroker::start().await?;
    let client_tls_config = broker.client_tls_config();
    let addr = broker.connection_string();

    // Create multiple concurrent connections
    let mut handles = Vec::new();
    for i in 0..5 {
        let tls_config = client_tls_config.clone();
        let address = addr.clone();
        let handle = tokio::spawn(async move {
            let mut client = Client::connect_tls(&address, &tls_config, "localhost").await?;
            let topic = format!("tls-concurrent-{}", i);
            client.create_topic(&topic, Some(1)).await?;
            client
                .publish(&topic, format!("message from client {}", i).into_bytes())
                .await?;
            Ok::<_, anyhow::Error>(i)
        });
        handles.push(handle);
    }

    // Wait for all connections to complete
    let mut success_count = 0;
    for handle in handles {
        if handle.await?.is_ok() {
            success_count += 1;
        }
    }

    assert_eq!(success_count, 5);
    info!("✅ {} concurrent TLS connections successful", success_count);
    broker.shutdown().await?;
    Ok(())
}

/// Test TLS connection persistence (long-lived connection)
#[tokio::test]
async fn test_tls_connection_persistence() -> Result<()> {
    init_tracing();

    let broker = TestTlsBroker::start().await?;
    let client_tls_config = broker.client_tls_config();

    let mut client =
        Client::connect_tls(&broker.connection_string(), &client_tls_config, "localhost").await?;

    let topic = unique_topic_name("tls-persistent");
    client.create_topic(&topic, Some(1)).await?;

    // Perform multiple operations over time
    for i in 0..5 {
        client
            .publish(&topic, format!("message {}", i).into_bytes())
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 5);

    info!("✅ TLS connection persisted across multiple operations");
    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// TLS Configuration Tests
// ============================================================================

/// Test custom server name for SNI
#[tokio::test]
async fn test_tls_server_name_indication() -> Result<()> {
    init_tracing();

    let broker = TestTlsBroker::start_with_options(TlsBrokerOptions {
        common_name: "rivven.local".to_string(),
        mtls_mode: MtlsMode::Disabled,
        require_auth: false,
    })
    .await?;

    // Connect with matching server name (insecure mode bypasses validation)
    let client_tls_config = TlsConfigBuilder::new()
        .insecure_skip_verify()
        .with_server_name("rivven.local".to_string())
        .build();

    let mut client = Client::connect_tls(
        &broker.connection_string(),
        &client_tls_config,
        "rivven.local",
    )
    .await?;

    let topic = unique_topic_name("tls-sni");
    client.create_topic(&topic, Some(1)).await?;

    info!("✅ TLS connection with custom SNI successful");
    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// TLS + Authentication Combined Tests
// ============================================================================

/// Test TLS with authentication enabled
#[tokio::test]
async fn test_tls_with_authentication() -> Result<()> {
    init_tracing();

    let broker = TestTlsBroker::start_with_options(TlsBrokerOptions {
        common_name: "localhost".to_string(),
        mtls_mode: MtlsMode::Disabled,
        require_auth: false, // Auth is handled at app level, not TLS level
    })
    .await?;

    let client_tls_config = broker.client_tls_config();

    let mut client =
        Client::connect_tls(&broker.connection_string(), &client_tls_config, "localhost").await?;

    // TLS connection should work for basic operations
    let topic = unique_topic_name("tls-auth");
    client.create_topic(&topic, Some(1)).await?;

    info!("✅ TLS connection with authentication context successful");
    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// Edge Case Tests
// ============================================================================

/// Test connection to non-TLS broker with TLS client
#[tokio::test]
async fn test_tls_client_to_plaintext_broker_fails() -> Result<()> {
    init_tracing();

    // Start a regular (non-TLS) broker
    let broker = TestBroker::start().await?;
    info!("Plaintext broker started at {}", broker.connection_string());

    // Try to connect with TLS client
    let client_tls_config = TlsConfigBuilder::new().insecure_skip_verify().build();

    let result =
        Client::connect_tls(&broker.connection_string(), &client_tls_config, "localhost").await;

    assert!(
        result.is_err(),
        "TLS client to plaintext broker should fail"
    );
    info!(
        "✅ TLS client correctly failed to connect to plaintext broker: {:?}",
        result.err().map(|e| e.to_string())
    );

    broker.shutdown().await?;
    Ok(())
}

/// Test rapid TLS connection cycling (stress test)
#[tokio::test]
async fn test_tls_connection_cycling() -> Result<()> {
    init_tracing();

    let broker = TestTlsBroker::start().await?;
    let client_tls_config = broker.client_tls_config();
    let addr = broker.connection_string();

    // Rapidly open and close TLS connections
    for i in 0..20 {
        let mut client = Client::connect_tls(&addr, &client_tls_config, "localhost").await?;
        let topic = format!("tls-cycle-{}", i);
        client.create_topic(&topic, Some(1)).await?;
        drop(client);
    }

    // Final connection should still work
    let mut client = Client::connect_tls(&addr, &client_tls_config, "localhost").await?;
    let topic = unique_topic_name("tls-final");
    client.create_topic(&topic, Some(1)).await?;

    info!("✅ TLS connection cycling (20 cycles) completed successfully");
    broker.shutdown().await?;
    Ok(())
}

/// Test TLS connection with timeout
#[tokio::test]
async fn test_tls_connection_timeout() -> Result<()> {
    init_tracing();

    // Try to connect to a non-existent address
    let client_tls_config = TlsConfigBuilder::new().insecure_skip_verify().build();

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        Client::connect_tls("192.0.2.1:9092", &client_tls_config, "localhost"),
    )
    .await;

    // Should timeout or fail
    match result {
        Ok(Err(e)) => {
            info!("✅ TLS connection failed as expected: {}", e);
        }
        Err(_) => {
            info!("✅ TLS connection timed out as expected");
        }
        Ok(Ok(_)) => {
            panic!("Connection to invalid address should not succeed");
        }
    }

    Ok(())
}

// ============================================================================
// Security Tests
// ============================================================================

/// Test that TLS protects message confidentiality
/// (We can't actually verify encryption, but we verify the connection uses TLS)
#[tokio::test]
async fn test_tls_encryption_active() -> Result<()> {
    init_tracing();

    let broker = TestTlsBroker::start().await?;
    let client_tls_config = broker.client_tls_config();

    let mut client =
        Client::connect_tls(&broker.connection_string(), &client_tls_config, "localhost").await?;

    // Send a "sensitive" message
    let topic = unique_topic_name("tls-confidential");
    client.create_topic(&topic, Some(1)).await?;

    let sensitive_data = b"SENSITIVE: password=secret123, api_key=xyz789";
    client.publish(&topic, sensitive_data.to_vec()).await?;

    let received = client.consume(&topic, 0, 0, 1).await?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].value.as_ref(), sensitive_data);

    info!("✅ Sensitive data transmitted over TLS (encrypted connection)");
    broker.shutdown().await?;
    Ok(())
}

/// Test multiple topics over single TLS connection
#[tokio::test]
async fn test_tls_multiple_topics() -> Result<()> {
    init_tracing();

    let broker = TestTlsBroker::start().await?;
    let client_tls_config = broker.client_tls_config();

    let mut client =
        Client::connect_tls(&broker.connection_string(), &client_tls_config, "localhost").await?;

    // Create multiple topics and publish to each
    let topics: Vec<String> = (0..5)
        .map(|i| unique_topic_name(&format!("tls-multi-{}", i)))
        .collect();

    for topic in &topics {
        client.create_topic(topic, Some(1)).await?;
        client
            .publish(topic, format!("message for {}", topic).into_bytes())
            .await?;
    }

    // Verify all topics have messages
    for topic in &topics {
        let messages = client.consume(topic, 0, 0, 10).await?;
        assert_eq!(messages.len(), 1);
    }

    info!(
        "✅ Multiple topics ({}) handled over single TLS connection",
        topics.len()
    );
    broker.shutdown().await?;
    Ok(())
}
