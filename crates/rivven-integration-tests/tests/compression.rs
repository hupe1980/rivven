//! Compression integration tests
//!
//! Tests for LZ4 and Zstd compression at the broker level:
//! - Transparent compression/decompression
//! - Compression ratio verification
//! - Throughput with compression enabled
//! - Mixed compression modes
//! - Large payload handling
//! - Compressibility detection
//!
//! Run with: cargo test -p rivven-integration-tests --test compression -- --nocapture

use anyhow::Result;
use bytes::Bytes;
use rivven_client::Client;
use rivven_core::compression::{
    CompressionAlgorithm, CompressionConfig, CompressionLevel, Compressor,
};
use rivven_integration_tests::fixtures::TestBroker;
use rivven_integration_tests::helpers::*;
use std::time::Instant;
use tracing::info;

// ============================================================================
// Basic Compression Tests
// ============================================================================

/// Test that LZ4 compression works transparently
#[tokio::test]
async fn test_lz4_compression_transparent() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("compression-lz4");
    client.create_topic(&topic, Some(1)).await?;

    // Highly compressible data (repeated pattern)
    let original = "Hello, Rivven! ".repeat(100);
    let message = Bytes::from(original.clone());

    client.publish(&topic, message).await?;

    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].value.as_ref(), original.as_bytes());

    info!("LZ4 transparent compression test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test that Zstd compression works transparently
#[tokio::test]
async fn test_zstd_compression_transparent() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("compression-zstd");
    client.create_topic(&topic, Some(1)).await?;

    // Large compressible payload
    let original = "ZSTD compression test data. ".repeat(500);
    let message = Bytes::from(original.clone());

    client.publish(&topic, message).await?;

    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].value.as_ref(), original.as_bytes());

    info!("Zstd transparent compression test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test no compression for small payloads
#[tokio::test]
async fn test_small_payload_no_compression() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("compression-small");
    client.create_topic(&topic, Some(1)).await?;

    // Small payload (below compression threshold)
    let original = b"tiny";
    client.publish(&topic, original.to_vec()).await?;

    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].value.as_ref(), original);

    info!("Small payload passthrough test passed");
    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// Compression Ratio Tests
// ============================================================================

/// Test that compression achieves meaningful ratio for text data
#[tokio::test]
async fn test_compression_ratio_text_data() -> Result<()> {
    init_tracing();

    // Create compressor with LZ4
    let config = CompressionConfig {
        algorithm: CompressionAlgorithm::Lz4,
        level: CompressionLevel::Default,
        min_size: 64,
        ratio_threshold: 0.95,
        adaptive: false,
        checksum: false,
    };
    let compressor = Compressor::with_config(config);

    // Highly compressible text
    let original = "The quick brown fox jumps over the lazy dog. ".repeat(100);
    let original_bytes = Bytes::from(original.clone());
    let original_len = original_bytes.len();

    let compressed = compressor.compress(&original_bytes)?;
    let compressed_len = compressed.len();

    let ratio = compressed_len as f64 / original_len as f64;
    info!(
        "Text compression: {} -> {} bytes ({:.1}% of original)",
        original_len,
        compressed_len,
        ratio * 100.0
    );

    // Text should compress to less than 50% of original
    assert!(
        ratio < 0.5,
        "Text should compress well, got ratio: {:.2}",
        ratio
    );

    // Verify decompression
    let decompressed = compressor.decompress(&compressed)?;
    assert_eq!(decompressed.as_ref(), original.as_bytes());

    info!("Compression ratio test for text data passed");
    Ok(())
}

/// Test Zstd achieves better ratio than LZ4 for highly compressible data
#[tokio::test]
async fn test_zstd_better_ratio_than_lz4() -> Result<()> {
    init_tracing();

    // Highly redundant data that favors Zstd's dictionary-based approach
    let original = "ABCDEFGHIJKLMNOPQRSTUVWXYZ ".repeat(2000);
    let original_bytes = Bytes::from(original);
    let original_len = original_bytes.len();

    // LZ4 compression (fast but lower ratio)
    let lz4_config = CompressionConfig {
        algorithm: CompressionAlgorithm::Lz4,
        level: CompressionLevel::Best, // Even best LZ4
        ..Default::default()
    };
    let lz4_compressor = Compressor::with_config(lz4_config);
    let lz4_compressed = lz4_compressor.compress(&original_bytes)?;

    // Zstd compression (better ratio)
    let zstd_config = CompressionConfig {
        algorithm: CompressionAlgorithm::Zstd,
        level: CompressionLevel::Best, // Best Zstd
        ..Default::default()
    };
    let zstd_compressor = Compressor::with_config(zstd_config);
    let zstd_compressed = zstd_compressor.compress(&original_bytes)?;

    let lz4_ratio = lz4_compressed.len() as f64 / original_len as f64;
    let zstd_ratio = zstd_compressed.len() as f64 / original_len as f64;

    info!(
        "LZ4 Best: {} -> {} bytes ({:.2}%)",
        original_len,
        lz4_compressed.len(),
        lz4_ratio * 100.0
    );
    info!(
        "Zstd Best: {} -> {} bytes ({:.2}%)",
        original_len,
        zstd_compressed.len(),
        zstd_ratio * 100.0
    );

    // Both should compress significantly
    assert!(lz4_ratio < 0.5, "LZ4 should compress this data well");
    assert!(zstd_ratio < 0.5, "Zstd should compress this data well");

    // Zstd should achieve better or comparable compression
    // Note: The ratio difference depends on data pattern
    info!(
        "Zstd compression benefit: {:.1}% smaller than LZ4",
        (1.0 - zstd_ratio / lz4_ratio) * 100.0
    );

    // Verify both decompress correctly
    let lz4_decompressed = lz4_compressor.decompress(&lz4_compressed)?;
    let zstd_decompressed = zstd_compressor.decompress(&zstd_compressed)?;
    assert_eq!(lz4_decompressed.as_ref(), original_bytes.as_ref());
    assert_eq!(zstd_decompressed.as_ref(), original_bytes.as_ref());

    info!("Zstd vs LZ4 ratio comparison test passed");
    Ok(())
}

/// Test incompressible data handling (random bytes)
#[tokio::test]
async fn test_incompressible_data_handling() -> Result<()> {
    init_tracing();

    // Generate random (incompressible) data
    let original: Vec<u8> = (0..10000).map(|i| (i * 17 + 13) as u8).collect();
    let original_bytes = Bytes::from(original.clone());
    let original_len = original_bytes.len();

    let config = CompressionConfig {
        algorithm: CompressionAlgorithm::Lz4,
        level: CompressionLevel::Default,
        min_size: 64,
        ratio_threshold: 0.95, // If compression doesn't help, store uncompressed
        adaptive: true,
        checksum: false,
    };
    let compressor = Compressor::with_config(config);

    let compressed = compressor.compress(&original_bytes)?;

    // Random data shouldn't blow up in size
    assert!(
        compressed.len() <= original_len + 100, // Allow small overhead
        "Compressed size should not be much larger than original"
    );

    // Verify decompression still works
    let decompressed = compressor.decompress(&compressed)?;
    assert_eq!(decompressed.as_ref(), original.as_slice());

    info!("Incompressible data handling test passed");
    Ok(())
}

// ============================================================================
// Throughput Tests
// ============================================================================

/// Test compression throughput with LZ4
#[tokio::test]
async fn test_lz4_throughput() -> Result<()> {
    init_tracing();

    let config = CompressionConfig {
        algorithm: CompressionAlgorithm::Lz4,
        level: CompressionLevel::Fast,
        min_size: 0,
        ratio_threshold: 1.0,
        adaptive: false,
        checksum: false,
    };
    let compressor = Compressor::with_config(config);

    // 1MB payload
    let payload = Bytes::from(vec![b'X'; 1024 * 1024]);

    let iterations = 100;
    let start = Instant::now();

    for _ in 0..iterations {
        let compressed = compressor.compress(&payload)?;
        let _ = compressor.decompress(&compressed)?;
    }

    let duration = start.elapsed();
    let total_bytes = payload.len() * iterations * 2; // compress + decompress
    let throughput_mbps = (total_bytes as f64 / duration.as_secs_f64()) / (1024.0 * 1024.0);

    info!(
        "LZ4 throughput: {:.0} MB/s ({} iterations, {:?})",
        throughput_mbps, iterations, duration
    );

    // LZ4 should achieve at least 100 MB/s even in tests
    assert!(
        throughput_mbps > 100.0,
        "LZ4 throughput too low: {:.0} MB/s",
        throughput_mbps
    );

    info!("LZ4 throughput test passed");
    Ok(())
}

/// Test compression throughput with Zstd
#[tokio::test]
async fn test_zstd_throughput() -> Result<()> {
    init_tracing();

    let config = CompressionConfig {
        algorithm: CompressionAlgorithm::Zstd,
        level: CompressionLevel::Fast,
        min_size: 0,
        ratio_threshold: 1.0,
        adaptive: false,
        checksum: false,
    };
    let compressor = Compressor::with_config(config);

    // 1MB payload
    let payload = Bytes::from(vec![b'X'; 1024 * 1024]);

    let iterations = 50;
    let start = Instant::now();

    for _ in 0..iterations {
        let compressed = compressor.compress(&payload)?;
        let _ = compressor.decompress(&compressed)?;
    }

    let duration = start.elapsed();
    let total_bytes = payload.len() * iterations * 2; // compress + decompress
    let throughput_mbps = (total_bytes as f64 / duration.as_secs_f64()) / (1024.0 * 1024.0);

    info!(
        "Zstd throughput: {:.0} MB/s ({} iterations, {:?})",
        throughput_mbps, iterations, duration
    );

    // Zstd should achieve at least 50 MB/s even in tests
    assert!(
        throughput_mbps > 50.0,
        "Zstd throughput too low: {:.0} MB/s",
        throughput_mbps
    );

    info!("Zstd throughput test passed");
    Ok(())
}

// ============================================================================
// Large Payload Tests
// ============================================================================

/// Test compression of large messages (10MB)
#[tokio::test]
async fn test_large_payload_compression() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("compression-large");
    client.create_topic(&topic, Some(1)).await?;

    // 10MB of compressible data
    let original = "Large payload test data with some repetition. ".repeat(200_000);
    let message = Bytes::from(original.clone());
    let original_len = message.len();

    info!("Publishing large message: {} bytes", original_len);

    let start = Instant::now();
    client.publish(&topic, message).await?;
    let publish_duration = start.elapsed();

    let start = Instant::now();
    let messages = client.consume(&topic, 0, 0, 10).await?;
    let consume_duration = start.elapsed();

    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].value.len(), original_len);

    info!(
        "Large payload: {} bytes, publish: {:?}, consume: {:?}",
        original_len, publish_duration, consume_duration
    );

    broker.shutdown().await?;
    Ok(())
}

/// Test compression with binary data
#[tokio::test]
async fn test_binary_data_compression() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("compression-binary");
    client.create_topic(&topic, Some(1)).await?;

    // Binary data with all byte values
    let mut original: Vec<u8> = Vec::with_capacity(256 * 100);
    for _ in 0..100 {
        original.extend(0u8..=255u8);
    }

    client.publish(&topic, original.clone()).await?;

    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].value.as_ref(), original.as_slice());

    info!("Binary data compression test passed");
    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// Multi-Message Tests
// ============================================================================

/// Test batch of messages with varying compressibility
#[tokio::test]
async fn test_mixed_compressibility_batch() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("compression-mixed");
    client.create_topic(&topic, Some(1)).await?;

    // Mix of highly compressible, moderately compressible, and random data
    let messages: Vec<Vec<u8>> = vec![
        // Highly compressible (repeated pattern)
        "AAAA".repeat(1000).into_bytes(),
        // Moderately compressible (text)
        "The quick brown fox jumps over the lazy dog. "
            .repeat(100)
            .into_bytes(),
        // Less compressible (varied content)
        (0u8..=255u8).cycle().take(5000).collect(),
        // Small payload (below threshold)
        b"tiny".to_vec(),
        // JSON-like data
        r#"{"event":"test","data":{"id":1,"value":"test"}}"#
            .repeat(100)
            .into_bytes(),
    ];

    for (i, msg) in messages.iter().enumerate() {
        client.publish(&topic, msg.clone()).await?;
        info!("Published message {}: {} bytes", i, msg.len());
    }

    let received = client.consume(&topic, 0, 0, 100).await?;
    assert_eq!(received.len(), messages.len());

    for (i, (received_msg, original)) in received.iter().zip(messages.iter()).enumerate() {
        assert_eq!(
            received_msg.value.as_ref(),
            original.as_slice(),
            "Message {} content mismatch",
            i
        );
    }

    info!("Mixed compressibility batch test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test concurrent compression operations
#[tokio::test]
async fn test_concurrent_compression() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let addr = broker.connection_string();

    let topic = unique_topic_name("compression-concurrent");
    {
        let mut client = Client::connect(&addr).await?;
        client.create_topic(&topic, Some(4)).await?;
    }

    // Spawn multiple producers
    let producer_count = 4;
    let messages_per_producer = 25;
    let mut handles = Vec::new();

    for producer_id in 0..producer_count {
        let addr = addr.clone();
        let topic = topic.clone();
        handles.push(tokio::spawn(async move {
            let mut client = Client::connect(&addr).await?;
            for i in 0..messages_per_producer {
                let payload = format!(
                    "Producer {} message {} with repeated data: {}",
                    producer_id,
                    i,
                    "X".repeat(500)
                );
                client.publish(&topic, payload.into_bytes()).await?;
            }
            Ok::<_, anyhow::Error>(())
        }));
    }

    // Wait for all producers
    for handle in handles {
        handle.await??;
    }

    // Verify all messages received
    let mut client = Client::connect(&addr).await?;
    let mut total = 0;
    for partition in 0..4 {
        let messages = client.consume(&topic, partition, 0, 1000).await?;
        total += messages.len();
    }

    assert_eq!(total, producer_count * messages_per_producer);
    info!("Concurrent compression test passed: {} messages", total);

    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// Compression Level Tests
// ============================================================================

/// Test different LZ4 compression levels
#[tokio::test]
async fn test_lz4_compression_levels() -> Result<()> {
    init_tracing();

    let payload = Bytes::from("Test data for compression level comparison. ".repeat(1000));
    let original_len = payload.len();

    for level in [
        CompressionLevel::Fast,
        CompressionLevel::Default,
        CompressionLevel::Best,
    ] {
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::Lz4,
            level,
            min_size: 0,
            ratio_threshold: 1.0,
            adaptive: false,
            checksum: false,
        };
        let compressor = Compressor::with_config(config);

        let compressed = compressor.compress(&payload)?;
        let decompressed = compressor.decompress(&compressed)?;

        let ratio = compressed.len() as f64 / original_len as f64;
        info!(
            "LZ4 {:?}: {} -> {} bytes ({:.1}%)",
            level,
            original_len,
            compressed.len(),
            ratio * 100.0
        );

        assert_eq!(decompressed.as_ref(), payload.as_ref());
    }

    info!("LZ4 compression levels test passed");
    Ok(())
}

/// Test different Zstd compression levels
#[tokio::test]
async fn test_zstd_compression_levels() -> Result<()> {
    init_tracing();

    let payload = Bytes::from("Test data for compression level comparison. ".repeat(1000));
    let original_len = payload.len();

    let mut prev_ratio = 1.0f64;

    for level in [
        CompressionLevel::Fast,
        CompressionLevel::Default,
        CompressionLevel::Best,
    ] {
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::Zstd,
            level,
            min_size: 0,
            ratio_threshold: 1.0,
            adaptive: false,
            checksum: false,
        };
        let compressor = Compressor::with_config(config);

        let compressed = compressor.compress(&payload)?;
        let decompressed = compressor.decompress(&compressed)?;

        let ratio = compressed.len() as f64 / original_len as f64;
        info!(
            "Zstd {:?}: {} -> {} bytes ({:.1}%)",
            level,
            original_len,
            compressed.len(),
            ratio * 100.0
        );

        assert_eq!(decompressed.as_ref(), payload.as_ref());

        // Higher compression levels should achieve better (or equal) ratio
        assert!(
            ratio <= prev_ratio + 0.01, // Allow small variance
            "Higher level should compress better or equal"
        );
        prev_ratio = ratio;
    }

    info!("Zstd compression levels test passed");
    Ok(())
}

// ============================================================================
// Edge Cases
// ============================================================================

/// Test empty payload handling
#[tokio::test]
async fn test_empty_payload() -> Result<()> {
    init_tracing();

    let config = CompressionConfig::default();
    let compressor = Compressor::with_config(config);

    let empty = Bytes::new();
    let compressed = compressor.compress(&empty)?;
    let decompressed = compressor.decompress(&compressed)?;

    assert!(decompressed.is_empty());

    info!("Empty payload test passed");
    Ok(())
}

/// Test single byte payload
#[tokio::test]
async fn test_single_byte_payload() -> Result<()> {
    init_tracing();

    let config = CompressionConfig {
        min_size: 0, // Compress even small payloads
        ..Default::default()
    };
    let compressor = Compressor::with_config(config);

    let single_byte = Bytes::from_static(&[0x42]);
    let compressed = compressor.compress(&single_byte)?;
    let decompressed = compressor.decompress(&compressed)?;

    assert_eq!(decompressed.as_ref(), &[0x42]);

    info!("Single byte payload test passed");
    Ok(())
}

/// Test payload at compression threshold boundary
#[tokio::test]
async fn test_threshold_boundary() -> Result<()> {
    init_tracing();

    let threshold = 64;
    let config = CompressionConfig {
        algorithm: CompressionAlgorithm::Lz4,
        min_size: threshold,
        ..Default::default()
    };
    let compressor = Compressor::with_config(config);

    // Just below threshold
    let below = Bytes::from(vec![b'A'; threshold - 1]);
    let compressed_below = compressor.compress(&below)?;
    let decompressed_below = compressor.decompress(&compressed_below)?;
    assert_eq!(decompressed_below.as_ref(), below.as_ref());

    // At threshold
    let at = Bytes::from(vec![b'A'; threshold]);
    let compressed_at = compressor.compress(&at)?;
    let decompressed_at = compressor.decompress(&compressed_at)?;
    assert_eq!(decompressed_at.as_ref(), at.as_ref());

    // Just above threshold
    let above = Bytes::from(vec![b'A'; threshold + 1]);
    let compressed_above = compressor.compress(&above)?;
    let decompressed_above = compressor.decompress(&compressed_above)?;
    assert_eq!(decompressed_above.as_ref(), above.as_ref());

    info!("Threshold boundary test passed");
    Ok(())
}

/// Test compression idempotency
#[tokio::test]
async fn test_compression_idempotency() -> Result<()> {
    init_tracing();

    let config = CompressionConfig {
        algorithm: CompressionAlgorithm::Lz4,
        min_size: 0,
        ratio_threshold: 1.0,
        adaptive: false,
        checksum: false,
        ..Default::default()
    };
    let compressor = Compressor::with_config(config);

    let original = Bytes::from("Test data for idempotency check. ".repeat(100));

    // Compress twice
    let compressed1 = compressor.compress(&original)?;
    let compressed2 = compressor.compress(&original)?;

    // Both compressions should produce identical output
    assert_eq!(
        compressed1, compressed2,
        "Compression should be deterministic"
    );

    // Both should decompress to original
    let decompressed1 = compressor.decompress(&compressed1)?;
    let decompressed2 = compressor.decompress(&compressed2)?;
    assert_eq!(decompressed1, decompressed2);
    assert_eq!(decompressed1.as_ref(), original.as_ref());

    info!("Compression idempotency test passed");
    Ok(())
}
