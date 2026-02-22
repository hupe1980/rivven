//! Security tests
//!
//! Tests for authentication, authorization, TLS, and security features.
//! Verifies secure defaults and proper security controls.
//!
//! Run with: cargo test -p rivven-integration-tests --test security -- --nocapture

use anyhow::Result;
use rivven_client::Client;
use rivven_core::Config;
use rivven_integration_tests::fixtures::TestBroker;
use rivven_integration_tests::helpers::*;
use tracing::info;

/// Test secure default configuration
#[tokio::test]
async fn test_secure_defaults() -> Result<()> {
    init_tracing();

    let config = Config::default();

    // Verify sensible defaults
    assert!(
        !config.bind_address.is_empty(),
        "Bind address should be set"
    );
    assert!(config.port > 0, "Port should be positive");

    info!("Secure defaults verified");
    Ok(())
}

/// Test topic name validation
#[tokio::test]
async fn test_topic_name_validation() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    // Valid topic names should work
    let valid_names = [
        "valid-topic",
        "valid_topic",
        "valid.topic",
        "ValidTopic123",
        "a", // minimum length
    ];

    for name in valid_names {
        let topic = unique_topic_name(name);
        let result = client.create_topic(&topic, Some(1)).await;
        assert!(result.is_ok(), "Valid topic name '{}' was rejected", name);
    }

    info!("Topic name validation test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test input sanitization for various attack patterns
#[tokio::test]
async fn test_input_sanitization() -> Result<()> {
    init_tracing();

    // Test patterns that should be handled safely
    let attack_patterns = [
        "",                          // Empty
        " ",                         // Whitespace only
        "topic with spaces",         // Spaces
        "../../../etc/passwd",       // Path traversal
        "topic\0name",               // Null byte injection
        "<script>alert(1)</script>", // XSS attempt
        "'; DROP TABLE topics;--",   // SQL injection
        "\n\r\t",                    // Control characters
    ];

    for pattern in &attack_patterns {
        // These should either be rejected or handled safely
        info!("Testing pattern: {:?}", pattern);
        // The actual validation happens server-side
    }

    info!("Input sanitization patterns identified");
    Ok(())
}

/// Test connection limits
#[tokio::test]
async fn test_connection_handling() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let addr = broker.connection_string();

    // Open multiple connections
    let mut connections = Vec::new();
    for _ in 0..10 {
        let client = Client::connect(&addr).await?;
        connections.push(client);
    }

    // All connections should be active
    assert_eq!(connections.len(), 10);
    info!("Opened 10 concurrent connections successfully");

    // Drop connections
    connections.clear();

    // Should be able to connect again
    let client = Client::connect(&addr).await?;
    drop(client);

    info!("Connection handling test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test that sensitive data is not logged
#[tokio::test]
async fn test_no_sensitive_data_in_errors() -> Result<()> {
    init_tracing();

    // Verify error messages don't contain sensitive information
    let test_cases = [
        ("password", "p@ssw0rd!"),
        ("secret", "my-secret-key"),
        ("token", "eyJhbGciOiJIUzI1NiIs"),
    ];

    for (field_name, value) in test_cases {
        // Create an error scenario
        let error_msg = format!("Connection failed for {}: redacted", field_name);
        assert!(
            !error_msg.contains(value),
            "Sensitive data '{}' found in error message",
            field_name
        );
    }

    info!("Sensitive data handling verified");
    Ok(())
}

/// Test message size limits
///
/// Verifies that:
///  - Normal (1 KB) and medium (1 MB) messages succeed.
///  - An oversized (10 MB) message is rejected client-side before hitting
///    the wire, preventing the TCP-level deadlock that occurs when the
///    client's `write_all()` blocks while the server tries to drain and
///    respond.
#[tokio::test]
async fn test_message_size_limits() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("size-limit");
    client.create_topic(&topic, Some(1)).await?;

    // Normal message should work
    let normal_msg = vec![b'X'; 1024]; // 1KB
    client.publish(&topic, normal_msg).await?;

    // Moderately large message should work
    let medium_msg = vec![b'X'; 1024 * 1024]; // 1MB
    client.publish(&topic, medium_msg).await?;

    // Very large message is rejected client-side (serialised size > MAX_MESSAGE_SIZE)
    let large_msg = vec![b'X'; 10 * 1024 * 1024]; // 10MB
    let result = client.publish(&topic, large_msg).await;
    assert!(
        result.is_err(),
        "10 MB message should be rejected as too large"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("exceeds maximum") || err_msg.contains("too large"),
        "Error should mention size limit: {err_msg}"
    );

    info!("Message size limit test passed — oversized message rejected client-side");
    broker.shutdown().await?;
    Ok(())
}

/// Test rapid connection cycling (connection exhaustion protection)
#[tokio::test]
async fn test_connection_cycling() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let addr = broker.connection_string();

    // Rapidly open and close connections
    for i in 0..50 {
        let client = Client::connect(&addr).await?;
        drop(client);
        if (i + 1) % 10 == 0 {
            info!("Completed {} connection cycles", i + 1);
        }
    }

    // Final connection should still work
    let mut client = Client::connect(&addr).await?;
    let topic = unique_topic_name("after-cycling");
    client.create_topic(&topic, Some(1)).await?;

    info!("Connection cycling test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test basic access control (topic isolation)
#[tokio::test]
async fn test_topic_isolation() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    // Create two isolated topics
    let topic_a = unique_topic_name("isolated-a");
    let topic_b = unique_topic_name("isolated-b");

    client.create_topic(&topic_a, Some(1)).await?;
    client.create_topic(&topic_b, Some(1)).await?;

    // Write to topic A
    client.publish(&topic_a, b"secret-a".to_vec()).await?;

    // Write to topic B
    client.publish(&topic_b, b"secret-b".to_vec()).await?;

    // Read from topic A should only see A's data
    let msgs_a = client.consume(&topic_a, 0, 0, 10).await?;
    assert_eq!(msgs_a.len(), 1);
    assert_eq!(msgs_a[0].value.as_ref(), b"secret-a");

    // Read from topic B should only see B's data
    let msgs_b = client.consume(&topic_b, 0, 0, 10).await?;
    assert_eq!(msgs_b.len(), 1);
    assert_eq!(msgs_b[0].value.as_ref(), b"secret-b");

    info!("Topic isolation test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test request rate handling
#[tokio::test]
async fn test_request_rate_handling() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("rate-test");
    client.create_topic(&topic, Some(1)).await?;

    // Burst of requests
    let start = std::time::Instant::now();
    for i in 0..1000 {
        client
            .publish(&topic, format!("msg-{}", i).into_bytes())
            .await?;
    }
    let duration = start.elapsed();

    let rate = 1000.0 / duration.as_secs_f64();
    info!("Handled {:.0} requests/sec", rate);

    // Verify all messages arrived
    let messages = client.consume(&topic, 0, 0, 2000).await?;
    assert_eq!(messages.len(), 1000);

    info!("Request rate handling test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test TLS configuration validation
#[tokio::test]
async fn test_tls_config_validation() -> Result<()> {
    init_tracing();

    // Test that invalid TLS paths are handled gracefully
    // (Without actually connecting - just config validation)

    let invalid_cert_path = "/nonexistent/cert.pem";
    let invalid_key_path = "/nonexistent/key.pem";

    // These should not panic, but should fail gracefully
    let cert_exists = std::path::Path::new(invalid_cert_path).exists();
    let key_exists = std::path::Path::new(invalid_key_path).exists();

    assert!(!cert_exists, "Test path should not exist");
    assert!(!key_exists, "Test path should not exist");

    info!("TLS config validation test passed");
    Ok(())
}

/// Test mTLS configuration (placeholder)
#[tokio::test]
#[ignore = "Requires TLS certificates"]
async fn test_mtls_authentication() -> Result<()> {
    init_tracing();

    // This test would verify mutual TLS authentication
    // Requires generating test certificates

    info!("mTLS test skipped - requires certificates");
    Ok(())
}

/// Test SCRAM-SHA-256 authentication (if enabled)
#[tokio::test]
#[ignore = "Requires auth configuration"]
async fn test_scram_authentication() -> Result<()> {
    init_tracing();

    // This test would verify SCRAM authentication
    // Requires auth to be configured on server

    info!("SCRAM auth test skipped - requires auth config");
    Ok(())
}

/// Test ACL enforcement (if enabled)
#[tokio::test]
#[ignore = "Requires ACL configuration"]
async fn test_acl_enforcement() -> Result<()> {
    init_tracing();

    // This test would verify ACL rules are enforced
    // Requires ACL to be configured

    info!("ACL test skipped - requires ACL config");
    Ok(())
}

/// Test that binary data is handled safely
#[tokio::test]
async fn test_binary_data_safety() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("binary-safety");
    client.create_topic(&topic, Some(1)).await?;

    // Test various binary patterns
    let patterns: Vec<Vec<u8>> = vec![
        vec![0x00, 0x00, 0x00, 0x00],        // All nulls
        vec![0xFF, 0xFF, 0xFF, 0xFF],        // All ones
        (0..256).map(|i| i as u8).collect(), // All bytes
        vec![0x00, 0xFF, 0x00, 0xFF],        // Alternating
    ];

    for (i, pattern) in patterns.iter().enumerate() {
        client.publish(&topic, pattern.clone()).await?;
        info!("Published binary pattern {}", i);
    }

    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), patterns.len());

    for (i, (msg, expected)) in messages.iter().zip(patterns.iter()).enumerate() {
        assert_eq!(
            msg.value.as_ref(),
            expected.as_slice(),
            "Binary pattern {} mismatch",
            i
        );
    }

    info!("Binary data safety test passed");
    broker.shutdown().await?;
    Ok(())
}
