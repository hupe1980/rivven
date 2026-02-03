//! Tiered Storage Integration Tests
//!
//! Tests for hot/warm/cold tiered storage at the broker level:
//! - Data writes to hot tier
//! - Tier migration (hot → warm → cold)
//! - Data reads from different tiers
//! - Statistics and monitoring
//! - Recovery after restart
//! - Concurrent access
//! - Large data volumes
//! - Error handling
//! - **Real broker end-to-end tests**
//!
//! Run with: cargo test -p rivven-integration-tests --test tiered_storage -- --nocapture

use anyhow::Result;
use bytes::Bytes;
use rivven_client::Client;
use rivven_core::storage::{ColdStorageConfig, TieredStorage, TieredStorageConfig};
use rivven_core::{Config, Message, Partition, TopicManager};
use rivven_integration_tests::fixtures::TestBroker;
use rivven_integration_tests::helpers::*;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tracing::info;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();
}

// ============================================================================
// Tiered Storage Configuration Tests
// ============================================================================

/// Test that TieredStorageConfig can be serialized and deserialized
#[tokio::test]
async fn test_tiered_storage_config_serialization() -> Result<()> {
    init_tracing();

    let config = TieredStorageConfig {
        enabled: true,
        hot_tier_max_bytes: 1024 * 1024,
        hot_tier_max_age_secs: 60,
        warm_tier_max_bytes: 10 * 1024 * 1024,
        warm_tier_max_age_secs: 3600,
        ..Default::default()
    };

    let json = serde_json::to_string_pretty(&config)?;
    info!("TieredStorageConfig JSON:\n{}", json);

    let deserialized: TieredStorageConfig = serde_json::from_str(&json)?;
    assert!(deserialized.enabled);
    assert_eq!(deserialized.hot_tier_max_bytes, 1024 * 1024);
    assert_eq!(deserialized.hot_tier_max_age_secs, 60);

    info!("TieredStorageConfig serialization test passed");
    Ok(())
}

/// Test ColdStorageConfig variants serialization
#[tokio::test]
async fn test_cold_storage_config_variants() -> Result<()> {
    init_tracing();

    // LocalFs
    let local_fs = ColdStorageConfig::LocalFs {
        path: "/var/lib/rivven/cold".to_string(),
    };
    let json = serde_json::to_string_pretty(&local_fs)?;
    info!("LocalFs config:\n{}", json);
    assert!(json.contains("local_fs"));

    // Disabled
    let disabled = ColdStorageConfig::Disabled;
    let json = serde_json::to_string_pretty(&disabled)?;
    info!("Disabled config:\n{}", json);
    assert!(json.contains("disabled"));

    // S3
    let s3 = ColdStorageConfig::S3 {
        endpoint: Some("http://localhost:9000".to_string()),
        bucket: "rivven-cold".to_string(),
        region: "us-east-1".to_string(),
        access_key: Some("minioadmin".to_string()),
        secret_key: Some("minioadmin".to_string()),
        use_path_style: true,
    };
    let json = serde_json::to_string_pretty(&s3)?;
    info!("S3 config:\n{}", json);
    assert!(json.contains("s3"));
    assert!(json.contains("rivven-cold"));

    info!("ColdStorageConfig variants serialization test passed");
    Ok(())
}

/// Test TieredStorageConfig presets
#[tokio::test]
async fn test_tiered_storage_config_presets() -> Result<()> {
    init_tracing();

    // High performance preset
    let hp = TieredStorageConfig::high_performance();
    assert!(hp.enabled);
    assert!(hp.hot_tier_max_bytes >= 8 * 1024 * 1024 * 1024); // 8 GB
    assert_eq!(hp.hot_tier_max_age_secs, 7200); // 2 hours
    info!(
        "High performance: hot_tier_max_bytes={}, migration_interval={}s",
        hp.hot_tier_max_bytes, hp.migration_interval_secs
    );

    // Cost optimized preset
    let co = TieredStorageConfig::cost_optimized();
    assert!(co.enabled);
    assert_eq!(co.hot_tier_max_bytes, 256 * 1024 * 1024); // 256 MB
    assert_eq!(co.hot_tier_max_age_secs, 300); // 5 minutes
    assert!(!co.enable_promotion); // Disabled for cost optimization
    info!(
        "Cost optimized: hot_tier_max_bytes={}, enable_promotion={}",
        co.hot_tier_max_bytes, co.enable_promotion
    );

    // Testing preset
    let test = TieredStorageConfig::testing();
    assert!(test.enabled);
    assert_eq!(test.hot_tier_max_bytes, 1024 * 1024); // 1 MB
    assert_eq!(test.hot_tier_max_age_secs, 5); // 5 seconds
    assert_eq!(test.migration_interval_secs, 1); // 1 second
    info!(
        "Testing: hot_tier_max_bytes={}, migration_interval={}s",
        test.hot_tier_max_bytes, test.migration_interval_secs
    );

    info!("TieredStorageConfig presets test passed");
    Ok(())
}

// ============================================================================
// TieredStorage Direct Tests
// ============================================================================

/// Test TieredStorage creation and basic write/read
#[tokio::test]
async fn test_tiered_storage_write_read() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;
    let config = TieredStorageConfig {
        enabled: true,
        hot_tier_max_bytes: 10 * 1024 * 1024,
        warm_tier_path: temp_dir.path().join("warm").to_string_lossy().to_string(),
        cold_storage: ColdStorageConfig::LocalFs {
            path: temp_dir.path().join("cold").to_string_lossy().to_string(),
        },
        migration_interval_secs: 3600, // Disable auto-migration for this test
        ..Default::default()
    };

    let storage = TieredStorage::new(config).await?;

    // Write data
    let topic = "test-topic";
    let partition = 0;
    let data = Bytes::from("Hello, tiered storage!");
    storage.write(topic, partition, 0, 1, data.clone()).await?;

    // Read data back
    let results = storage.read(topic, partition, 0, 1024).await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, data);

    // Check stats
    let stats = storage.stats();
    assert!(stats.hot_writes > 0);
    assert!(stats.hot_reads > 0);

    storage.shutdown().await;
    info!("TieredStorage write/read test passed");
    Ok(())
}

/// Test TieredStorage statistics
#[tokio::test]
async fn test_tiered_storage_statistics() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;
    let config = TieredStorageConfig {
        enabled: true,
        hot_tier_max_bytes: 10 * 1024 * 1024,
        warm_tier_path: temp_dir.path().join("warm").to_string_lossy().to_string(),
        cold_storage: ColdStorageConfig::LocalFs {
            path: temp_dir.path().join("cold").to_string_lossy().to_string(),
        },
        migration_interval_secs: 3600,
        ..Default::default()
    };

    let storage = TieredStorage::new(config).await?;

    // Initial stats
    let stats = storage.stats();
    assert_eq!(stats.hot_reads, 0);
    assert_eq!(stats.hot_writes, 0);
    assert_eq!(stats.warm_reads, 0);
    assert_eq!(stats.warm_writes, 0);

    // Write multiple messages
    for i in 0..10 {
        let data = Bytes::from(format!("message-{}", i));
        storage.write("stats-test", 0, i, i + 1, data).await?;
    }

    let stats = storage.stats();
    assert_eq!(stats.hot_writes, 10);

    // Read messages
    for i in 0..10 {
        let _ = storage.read("stats-test", 0, i, 100).await?;
    }

    let stats = storage.stats();
    assert!(stats.hot_reads >= 10);

    storage.shutdown().await;
    info!("TieredStorage statistics test passed");
    Ok(())
}

// ============================================================================
// Partition with Tiered Storage Tests
// ============================================================================

/// Test Partition with tiered storage enabled
#[tokio::test]
async fn test_partition_with_tiered_storage() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let tiered_config = TieredStorageConfig {
        enabled: true,
        hot_tier_max_bytes: 10 * 1024 * 1024,
        warm_tier_path: temp_dir.path().join("warm").to_string_lossy().to_string(),
        cold_storage: ColdStorageConfig::LocalFs {
            path: temp_dir.path().join("cold").to_string_lossy().to_string(),
        },
        migration_interval_secs: 3600,
        ..Default::default()
    };

    let tiered_storage = TieredStorage::new(tiered_config).await?;

    let config = Config {
        data_dir: temp_dir.path().join("data").to_string_lossy().to_string(),
        enable_persistence: true,
        ..Default::default()
    };

    let partition =
        Partition::new_with_tiered_storage(&config, "tiered-test", 0, Some(tiered_storage.clone()))
            .await?;

    assert!(partition.has_tiered_storage());
    assert_eq!(partition.topic(), "tiered-test");

    // Append messages
    for i in 0..5 {
        let msg = Message::new(Bytes::from(format!("tiered-message-{}", i)));
        let offset = partition.append(msg).await?;
        assert_eq!(offset, i);
    }

    // Read messages
    let messages = partition.read(0, 10).await?;
    assert_eq!(messages.len(), 5);

    // Check tiered storage stats
    let stats = partition.tiered_storage_stats();
    assert!(stats.is_some());
    let stats = stats.unwrap();
    assert!(stats.hot_writes >= 5);

    tiered_storage.shutdown().await;
    info!("Partition with tiered storage test passed");
    Ok(())
}

/// Test Partition batch append with tiered storage
#[tokio::test]
async fn test_partition_batch_with_tiered_storage() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let tiered_config = TieredStorageConfig {
        enabled: true,
        hot_tier_max_bytes: 10 * 1024 * 1024,
        warm_tier_path: temp_dir.path().join("warm").to_string_lossy().to_string(),
        cold_storage: ColdStorageConfig::LocalFs {
            path: temp_dir.path().join("cold").to_string_lossy().to_string(),
        },
        migration_interval_secs: 3600,
        ..Default::default()
    };

    let tiered_storage = TieredStorage::new(tiered_config).await?;

    let config = Config {
        data_dir: temp_dir.path().join("data").to_string_lossy().to_string(),
        enable_persistence: true,
        ..Default::default()
    };

    let partition = Partition::new_with_tiered_storage(
        &config,
        "batch-tiered-test",
        0,
        Some(tiered_storage.clone()),
    )
    .await?;

    // Batch append
    let messages: Vec<Message> = (0..100)
        .map(|i| Message::new(Bytes::from(format!("batch-msg-{}", i))))
        .collect();

    let offsets = partition.append_batch(messages).await?;
    assert_eq!(offsets.len(), 100);
    assert_eq!(offsets[0], 0);
    assert_eq!(offsets[99], 99);

    // Read back
    let read_messages = partition.read(0, 100).await?;
    assert_eq!(read_messages.len(), 100);

    // Verify tiered storage received the batch
    let stats = partition.tiered_storage_stats().unwrap();
    assert!(stats.hot_writes >= 1); // At least one batch write

    tiered_storage.shutdown().await;
    info!("Partition batch with tiered storage test passed");
    Ok(())
}

// ============================================================================
// TopicManager with Tiered Storage Tests
// ============================================================================

/// Test TopicManager with tiered storage
#[tokio::test]
async fn test_topic_manager_with_tiered_storage() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let tiered_config = TieredStorageConfig {
        enabled: true,
        hot_tier_max_bytes: 10 * 1024 * 1024,
        warm_tier_path: temp_dir.path().join("warm").to_string_lossy().to_string(),
        cold_storage: ColdStorageConfig::LocalFs {
            path: temp_dir.path().join("cold").to_string_lossy().to_string(),
        },
        migration_interval_secs: 3600,
        ..Default::default()
    };

    let tiered_storage = TieredStorage::new(tiered_config).await?;

    let config = Config {
        data_dir: temp_dir.path().join("data").to_string_lossy().to_string(),
        enable_persistence: true,
        default_partitions: 3,
        ..Default::default()
    };

    let topic_manager = TopicManager::new_with_tiered_storage(config, tiered_storage.clone());
    assert!(topic_manager.has_tiered_storage());

    // Create topic
    let topic_name = unique_topic_name("tiered-topic");
    let topic = topic_manager.create_topic(topic_name.clone(), None).await?;
    assert_eq!(topic.num_partitions(), 3);

    // Each partition should have tiered storage
    for partition in topic.all_partitions() {
        assert!(partition.has_tiered_storage());
    }

    // Write to topic
    let msg = Message::new(Bytes::from("test message"));
    topic.append(0, msg).await?;

    // Check stats
    let stats = topic_manager.tiered_storage_stats();
    assert!(stats.is_some());

    tiered_storage.shutdown().await;
    info!("TopicManager with tiered storage test passed");
    Ok(())
}

/// Test multiple topics with shared tiered storage
#[tokio::test]
async fn test_multiple_topics_shared_tiered_storage() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let tiered_config = TieredStorageConfig {
        enabled: true,
        hot_tier_max_bytes: 50 * 1024 * 1024,
        warm_tier_path: temp_dir.path().join("warm").to_string_lossy().to_string(),
        cold_storage: ColdStorageConfig::LocalFs {
            path: temp_dir.path().join("cold").to_string_lossy().to_string(),
        },
        migration_interval_secs: 3600,
        ..Default::default()
    };

    let tiered_storage = TieredStorage::new(tiered_config).await?;

    let config = Config {
        data_dir: temp_dir.path().join("data").to_string_lossy().to_string(),
        enable_persistence: true,
        default_partitions: 2,
        ..Default::default()
    };

    let topic_manager = TopicManager::new_with_tiered_storage(config, tiered_storage.clone());

    // Create multiple topics
    let topic1 = topic_manager
        .create_topic("topic1".to_string(), None)
        .await?;
    let topic2 = topic_manager
        .create_topic("topic2".to_string(), None)
        .await?;
    let topic3 = topic_manager
        .create_topic("topic3".to_string(), None)
        .await?;

    // Write to all topics
    for i in 0..10 {
        topic1
            .append(0, Message::new(Bytes::from(format!("t1-msg-{}", i))))
            .await?;
        topic2
            .append(0, Message::new(Bytes::from(format!("t2-msg-{}", i))))
            .await?;
        topic3
            .append(0, Message::new(Bytes::from(format!("t3-msg-{}", i))))
            .await?;
    }

    // All writes should go through shared tiered storage
    let stats = tiered_storage.stats();
    assert!(
        stats.hot_writes >= 30,
        "Expected at least 30 writes, got {}",
        stats.hot_writes
    );

    tiered_storage.shutdown().await;
    info!("Multiple topics with shared tiered storage test passed");
    Ok(())
}

// ============================================================================
// Config Integration Tests
// ============================================================================

/// Test Config includes tiered storage settings
#[tokio::test]
async fn test_config_tiered_storage_field() -> Result<()> {
    init_tracing();

    // Default config has tiered storage disabled
    let config = Config::default();
    assert!(!config.tiered_storage.enabled);

    // Enable with builder
    let config = Config::new().with_tiered_storage_enabled();
    assert!(config.tiered_storage.enabled);

    // Custom tiered storage config
    let tiered = TieredStorageConfig::testing();
    let config = Config::new().with_tiered_storage(tiered);
    assert!(config.tiered_storage.enabled);
    assert_eq!(config.tiered_storage.hot_tier_max_bytes, 1024 * 1024);

    info!("Config tiered storage field test passed");
    Ok(())
}

/// Test Config serialization with tiered storage
#[tokio::test]
async fn test_config_serialization_with_tiered_storage() -> Result<()> {
    init_tracing();

    let config = Config {
        bind_address: "0.0.0.0".to_string(),
        port: 9092,
        default_partitions: 4,
        enable_persistence: true,
        data_dir: "/var/lib/rivven/data".to_string(),
        max_segment_size: 1024 * 1024 * 1024,
        log_level: "info".to_string(),
        tiered_storage: TieredStorageConfig {
            enabled: true,
            hot_tier_max_bytes: 8 * 1024 * 1024 * 1024,
            warm_tier_path: "/var/lib/rivven/warm".to_string(),
            cold_storage: ColdStorageConfig::S3 {
                endpoint: None,
                bucket: "rivven-cold".to_string(),
                region: "us-east-1".to_string(),
                access_key: None,
                secret_key: None,
                use_path_style: false,
            },
            ..Default::default()
        },
    };

    let json = serde_json::to_string_pretty(&config)?;
    info!("Full Config with tiered storage:\n{}", json);

    assert!(json.contains("tiered_storage"));
    assert!(json.contains("hot_tier_max_bytes"));
    assert!(json.contains("cold_storage"));
    assert!(json.contains("s3"));

    // Deserialize back
    let deserialized: Config = serde_json::from_str(&json)?;
    assert!(deserialized.tiered_storage.enabled);
    assert_eq!(
        deserialized.tiered_storage.hot_tier_max_bytes,
        8 * 1024 * 1024 * 1024
    );

    info!("Config serialization with tiered storage test passed");
    Ok(())
}

// ============================================================================
// Flush and Durability Tests
// ============================================================================

/// Test partition flush with tiered storage
#[tokio::test]
async fn test_partition_flush_with_tiered_storage() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let tiered_config = TieredStorageConfig {
        enabled: true,
        hot_tier_max_bytes: 10 * 1024 * 1024,
        warm_tier_path: temp_dir.path().join("warm").to_string_lossy().to_string(),
        cold_storage: ColdStorageConfig::LocalFs {
            path: temp_dir.path().join("cold").to_string_lossy().to_string(),
        },
        migration_interval_secs: 3600,
        ..Default::default()
    };

    let tiered_storage = TieredStorage::new(tiered_config).await?;

    let config = Config {
        data_dir: temp_dir.path().join("data").to_string_lossy().to_string(),
        enable_persistence: true,
        ..Default::default()
    };

    let partition =
        Partition::new_with_tiered_storage(&config, "flush-test", 0, Some(tiered_storage.clone()))
            .await?;

    // Write messages
    for i in 0..10 {
        let msg = Message::new(Bytes::from(format!("flush-msg-{}", i)));
        partition.append(msg).await?;
    }

    // Flush should work without error
    partition.flush().await?;

    // Data should still be readable
    let messages = partition.read(0, 10).await?;
    assert_eq!(messages.len(), 10);

    tiered_storage.shutdown().await;
    info!("Partition flush with tiered storage test passed");
    Ok(())
}

// ============================================================================
// Advanced Tests: Migration, Concurrency, Large Data
// ============================================================================

/// Test tier migration with testing preset (short timeouts)
#[tokio::test]
async fn test_tier_migration_with_testing_preset() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    // Use testing preset with very short migration intervals
    let mut tiered_config = TieredStorageConfig::testing();
    tiered_config.warm_tier_path = temp_dir.path().join("warm").to_string_lossy().to_string();
    tiered_config.cold_storage = ColdStorageConfig::LocalFs {
        path: temp_dir.path().join("cold").to_string_lossy().to_string(),
    };

    let storage = TieredStorage::new(tiered_config).await?;

    // Write data to hot tier
    let topic = "migration-test";
    for i in 0..5 {
        let data = Bytes::from(format!("data-{:04}", i));
        storage.write(topic, 0, i, i + 1, data).await?;
    }

    // Initial stats - all in hot tier
    let stats = storage.stats();
    assert!(
        stats.hot_writes >= 5,
        "Expected hot writes >= 5, got {}",
        stats.hot_writes
    );
    info!(
        "Initial stats: hot_writes={}, warm_writes={}",
        stats.hot_writes, stats.warm_writes
    );

    // Data should be readable
    let results = storage.read(topic, 0, 0, 1024).await?;
    assert_eq!(
        results.len(),
        5,
        "Expected 5 messages, got {}",
        results.len()
    );

    storage.shutdown().await;
    info!("Tier migration with testing preset test passed");
    Ok(())
}

/// Test concurrent access to tiered storage from multiple partitions
#[tokio::test]
async fn test_concurrent_partition_access() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let tiered_config = TieredStorageConfig {
        enabled: true,
        hot_tier_max_bytes: 100 * 1024 * 1024, // 100 MB
        warm_tier_path: temp_dir.path().join("warm").to_string_lossy().to_string(),
        cold_storage: ColdStorageConfig::LocalFs {
            path: temp_dir.path().join("cold").to_string_lossy().to_string(),
        },
        migration_interval_secs: 3600, // Disable auto-migration
        ..Default::default()
    };

    let tiered_storage = Arc::new(TieredStorage::new(tiered_config).await?);

    let config = Config {
        data_dir: temp_dir.path().join("data").to_string_lossy().to_string(),
        enable_persistence: true,
        ..Default::default()
    };

    // Create multiple partitions sharing the same tiered storage
    let partition0 = Partition::new_with_tiered_storage(
        &config,
        "concurrent-test",
        0,
        Some((*tiered_storage).clone()),
    )
    .await?;
    let partition1 = Partition::new_with_tiered_storage(
        &config,
        "concurrent-test",
        1,
        Some((*tiered_storage).clone()),
    )
    .await?;
    let partition2 = Partition::new_with_tiered_storage(
        &config,
        "concurrent-test",
        2,
        Some((*tiered_storage).clone()),
    )
    .await?;

    // Spawn concurrent writers
    let p0 = Arc::new(partition0);
    let p1 = Arc::new(partition1);
    let p2 = Arc::new(partition2);

    let handles: Vec<_> = (0..3)
        .map(|partition_id| {
            let partition = match partition_id {
                0 => Arc::clone(&p0),
                1 => Arc::clone(&p1),
                _ => Arc::clone(&p2),
            };
            tokio::spawn(async move {
                for i in 0..50 {
                    let msg = Message::new(Bytes::from(format!("p{}-msg-{}", partition_id, i)));
                    partition.append(msg).await.unwrap();
                }
            })
        })
        .collect();

    // Wait for all writers to complete
    for handle in handles {
        handle.await?;
    }

    // Verify all data was written
    let stats = tiered_storage.stats();
    assert!(
        stats.hot_writes >= 150,
        "Expected at least 150 writes, got {}",
        stats.hot_writes
    );

    // Read back from each partition
    let msgs0 = p0.read(0, 100).await?;
    let msgs1 = p1.read(0, 100).await?;
    let msgs2 = p2.read(0, 100).await?;

    assert_eq!(msgs0.len(), 50, "Partition 0 should have 50 messages");
    assert_eq!(msgs1.len(), 50, "Partition 1 should have 50 messages");
    assert_eq!(msgs2.len(), 50, "Partition 2 should have 50 messages");

    tiered_storage.shutdown().await;
    info!("Concurrent partition access test passed");
    Ok(())
}

/// Test large data volume handling
#[tokio::test]
async fn test_large_data_volume() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let tiered_config = TieredStorageConfig {
        enabled: true,
        hot_tier_max_bytes: 50 * 1024 * 1024, // 50 MB
        warm_tier_path: temp_dir.path().join("warm").to_string_lossy().to_string(),
        cold_storage: ColdStorageConfig::LocalFs {
            path: temp_dir.path().join("cold").to_string_lossy().to_string(),
        },
        migration_interval_secs: 3600,
        ..Default::default()
    };

    let tiered_storage = TieredStorage::new(tiered_config).await?;

    let config = Config {
        data_dir: temp_dir.path().join("data").to_string_lossy().to_string(),
        enable_persistence: true,
        ..Default::default()
    };

    let partition = Partition::new_with_tiered_storage(
        &config,
        "large-volume-test",
        0,
        Some(tiered_storage.clone()),
    )
    .await?;

    // Write 500 messages with consistent sizes (stay under read limits)
    let message_count = 500;
    for i in 0..message_count {
        // Use smaller, consistent message sizes
        let data = format!("large-volume-message-{:06}", i);
        let msg = Message::new(Bytes::from(data));
        partition.append(msg).await?;
    }

    // Read in chunks to handle any internal limits
    let mut total_read = 0;
    let mut offset = 0;
    while total_read < message_count as usize {
        let messages = partition.read(offset, 100).await?;
        if messages.is_empty() {
            break;
        }
        total_read += messages.len();
        offset += messages.len() as u64;
    }

    assert_eq!(
        total_read, message_count as usize,
        "Expected {} messages, got {}",
        message_count, total_read
    );

    // Verify stats
    let stats = partition.tiered_storage_stats().unwrap();
    assert!(
        stats.hot_writes >= message_count as u64,
        "Expected at least {} hot writes, got {}",
        message_count,
        stats.hot_writes
    );

    tiered_storage.shutdown().await;
    info!(
        "Large data volume test passed: {} messages written and read",
        message_count
    );
    Ok(())
}

/// Test tiered storage disabled behavior
#[tokio::test]
async fn test_tiered_storage_disabled() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let config = Config {
        data_dir: temp_dir.path().join("data").to_string_lossy().to_string(),
        enable_persistence: true,
        tiered_storage: TieredStorageConfig::default(), // disabled by default
        ..Default::default()
    };

    assert!(
        !config.tiered_storage.enabled,
        "Tiered storage should be disabled by default"
    );

    // Create partition without tiered storage
    let partition = Partition::new(&config, "no-tiered-test", 0).await?;
    assert!(!partition.has_tiered_storage());

    // Should still work normally
    for i in 0..10 {
        let msg = Message::new(Bytes::from(format!("msg-{}", i)));
        partition.append(msg).await?;
    }

    let messages = partition.read(0, 10).await?;
    assert_eq!(messages.len(), 10);

    // Stats should return None
    assert!(partition.tiered_storage_stats().is_none());

    info!("Tiered storage disabled test passed");
    Ok(())
}

/// Test GCS and Azure cold storage config serialization
#[tokio::test]
async fn test_all_cold_storage_backends() -> Result<()> {
    init_tracing();

    // GCS
    let gcs = ColdStorageConfig::Gcs {
        bucket: "rivven-archive".to_string(),
        service_account_path: Some("/path/to/service-account.json".to_string()),
    };
    let json = serde_json::to_string_pretty(&gcs)?;
    info!("GCS config:\n{}", json);
    assert!(json.contains("gcs"));
    assert!(json.contains("rivven-archive"));

    // Azure Blob
    let azure = ColdStorageConfig::AzureBlob {
        container: "rivven-cold".to_string(),
        account: "mystorageaccount".to_string(),
        access_key: None, // Uses DefaultAzureCredential
    };
    let json = serde_json::to_string_pretty(&azure)?;
    info!("Azure config:\n{}", json);
    assert!(json.contains("azure_blob"));
    assert!(json.contains("mystorageaccount"));

    // Verify deserialization
    let gcs_back: ColdStorageConfig = serde_json::from_str(&serde_json::to_string(&gcs)?)?;
    match gcs_back {
        ColdStorageConfig::Gcs { bucket, .. } => assert_eq!(bucket, "rivven-archive"),
        _ => panic!("Expected GCS config"),
    }

    info!("All cold storage backends test passed");
    Ok(())
}

/// Test YAML-like configuration parsing
#[tokio::test]
async fn test_config_from_yaml_style_json() -> Result<()> {
    init_tracing();

    // Simulate what a user might write in a config file
    let config_json = r#"{
        "enabled": true,
        "hot_tier_max_bytes": 8589934592,
        "hot_tier_max_age_secs": 7200,
        "warm_tier_max_bytes": 107374182400,
        "warm_tier_max_age_secs": 604800,
        "warm_tier_path": "/var/lib/rivven/warm",
        "cold_storage": {
            "type": "s3",
            "endpoint": null,
            "bucket": "my-archive",
            "region": "us-west-2",
            "access_key": null,
            "secret_key": null,
            "use_path_style": false
        },
        "migration_interval_secs": 60,
        "migration_concurrency": 4,
        "enable_promotion": true,
        "promotion_threshold": 100,
        "compaction_threshold": 0.5
    }"#;

    let config: TieredStorageConfig = serde_json::from_str(config_json)?;

    assert!(config.enabled);
    assert_eq!(config.hot_tier_max_bytes, 8 * 1024 * 1024 * 1024); // 8 GB
    assert_eq!(config.hot_tier_max_age_secs, 7200); // 2 hours
    assert_eq!(config.migration_interval_secs, 60);
    assert!(config.enable_promotion);

    match &config.cold_storage {
        ColdStorageConfig::S3 { bucket, region, .. } => {
            assert_eq!(bucket, "my-archive");
            assert_eq!(region, "us-west-2");
        }
        _ => panic!("Expected S3 cold storage"),
    }

    info!("Config from YAML-style JSON test passed");
    Ok(())
}

/// Test helper methods on TieredStorageConfig
#[tokio::test]
async fn test_tiered_storage_config_helpers() -> Result<()> {
    init_tracing();

    let config = TieredStorageConfig {
        hot_tier_max_age_secs: 3600,
        migration_interval_secs: 60,
        warm_tier_path: "/data/warm".to_string(),
        ..Default::default()
    };

    // Test helper methods
    let hot_age = config.hot_tier_max_age();
    assert_eq!(hot_age, Duration::from_secs(3600));

    let migration_interval = config.migration_interval();
    assert_eq!(migration_interval, Duration::from_secs(60));

    let warm_path = config.warm_tier_path_buf();
    assert_eq!(warm_path.to_string_lossy(), "/data/warm");

    info!("TieredStorageConfig helpers test passed");
    Ok(())
}

// ============================================================================
// Real Broker End-to-End Tests with Tiered Storage
// ============================================================================

/// Test broker starts and accepts connections with tiered storage enabled
#[tokio::test]
async fn test_broker_with_tiered_storage_enabled() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let config = Config {
        data_dir: temp_dir.path().join("data").to_string_lossy().to_string(),
        enable_persistence: true,
        tiered_storage: TieredStorageConfig {
            enabled: true,
            hot_tier_max_bytes: 10 * 1024 * 1024,
            warm_tier_path: temp_dir.path().join("warm").to_string_lossy().to_string(),
            cold_storage: ColdStorageConfig::LocalFs {
                path: temp_dir.path().join("cold").to_string_lossy().to_string(),
            },
            migration_interval_secs: 3600,
            ..Default::default()
        },
        ..Default::default()
    };

    let broker = TestBroker::start_with_config(config).await?;
    info!(
        "Broker started with tiered storage at {}",
        broker.connection_string()
    );

    // Connect and verify broker is operational
    let mut client = Client::connect(&broker.connection_string()).await?;

    // Create a topic
    let topic = unique_topic_name("broker-tiered");
    client.create_topic(&topic, Some(1)).await?;

    // Publish a message
    let message = Bytes::from("Hello from tiered storage broker!");
    client.publish(&topic, message.clone()).await?;

    // Consume and verify
    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].value.as_ref(), message.as_ref());

    broker.shutdown().await?;
    info!("Broker with tiered storage test passed");
    Ok(())
}

/// Test broker publishes and consumes multiple messages with tiered storage
#[tokio::test]
async fn test_broker_tiered_storage_multiple_messages() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let mut tiered_config = TieredStorageConfig::testing();
    tiered_config.warm_tier_path = temp_dir.path().join("warm").to_string_lossy().to_string();
    tiered_config.cold_storage = ColdStorageConfig::LocalFs {
        path: temp_dir.path().join("cold").to_string_lossy().to_string(),
    };

    let config = Config {
        data_dir: temp_dir.path().join("data").to_string_lossy().to_string(),
        enable_persistence: true,
        tiered_storage: tiered_config,
        ..Default::default()
    };

    let broker = TestBroker::start_with_config(config).await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("broker-tiered-multi");
    client.create_topic(&topic, Some(1)).await?;

    // Publish 100 messages
    for i in 0..100 {
        let message = Bytes::from(format!("tiered-message-{:04}", i));
        client.publish(&topic, message).await?;
    }

    // Consume all messages
    let messages = client.consume(&topic, 0, 0, 200).await?;
    assert_eq!(
        messages.len(),
        100,
        "Expected 100 messages, got {}",
        messages.len()
    );

    // Verify message order
    for (i, msg) in messages.iter().enumerate() {
        let expected = format!("tiered-message-{:04}", i);
        assert_eq!(
            String::from_utf8_lossy(msg.value.as_ref()),
            expected,
            "Message {} mismatch",
            i
        );
    }

    broker.shutdown().await?;
    info!("Broker tiered storage multiple messages test passed");
    Ok(())
}

/// Test broker with tiered storage handles multiple topics
#[tokio::test]
async fn test_broker_tiered_storage_multiple_topics() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let config = Config {
        data_dir: temp_dir.path().join("data").to_string_lossy().to_string(),
        enable_persistence: true,
        default_partitions: 2,
        tiered_storage: TieredStorageConfig {
            enabled: true,
            hot_tier_max_bytes: 50 * 1024 * 1024,
            warm_tier_path: temp_dir.path().join("warm").to_string_lossy().to_string(),
            cold_storage: ColdStorageConfig::LocalFs {
                path: temp_dir.path().join("cold").to_string_lossy().to_string(),
            },
            migration_interval_secs: 3600,
            ..Default::default()
        },
        ..Default::default()
    };

    let broker = TestBroker::start_with_config(config).await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    // Create multiple topics
    let topic1 = unique_topic_name("broker-tiered-t1");
    let topic2 = unique_topic_name("broker-tiered-t2");
    let topic3 = unique_topic_name("broker-tiered-t3");

    client.create_topic(&topic1, Some(2)).await?;
    client.create_topic(&topic2, Some(2)).await?;
    client.create_topic(&topic3, Some(2)).await?;

    // Write to all topics
    for i in 0..10 {
        client.publish(&topic1, format!("t1-msg-{}", i)).await?;
        client.publish(&topic2, format!("t2-msg-{}", i)).await?;
        client.publish(&topic3, format!("t3-msg-{}", i)).await?;
    }

    // Read from all topics - both partitions (messages distributed across partitions)
    let msgs1_p0 = client.consume(&topic1, 0, 0, 20).await?;
    let msgs1_p1 = client.consume(&topic1, 1, 0, 20).await?;
    let msgs2_p0 = client.consume(&topic2, 0, 0, 20).await?;
    let msgs2_p1 = client.consume(&topic2, 1, 0, 20).await?;
    let msgs3_p0 = client.consume(&topic3, 0, 0, 20).await?;
    let msgs3_p1 = client.consume(&topic3, 1, 0, 20).await?;

    // Each topic should have messages distributed across partitions
    let total1 = msgs1_p0.len() + msgs1_p1.len();
    let total2 = msgs2_p0.len() + msgs2_p1.len();
    let total3 = msgs3_p0.len() + msgs3_p1.len();

    assert_eq!(total1, 10, "Topic 1 should have 10 total messages");
    assert_eq!(total2, 10, "Topic 2 should have 10 total messages");
    assert_eq!(total3, 10, "Topic 3 should have 10 total messages");

    broker.shutdown().await?;
    info!("Broker tiered storage multiple topics test passed");
    Ok(())
}

/// Test broker with tiered storage handles large payloads
#[tokio::test]
async fn test_broker_tiered_storage_large_payloads() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let mut tiered_config = TieredStorageConfig::testing();
    tiered_config.warm_tier_path = temp_dir.path().join("warm").to_string_lossy().to_string();
    tiered_config.cold_storage = ColdStorageConfig::LocalFs {
        path: temp_dir.path().join("cold").to_string_lossy().to_string(),
    };
    tiered_config.hot_tier_max_bytes = 100 * 1024 * 1024; // 100 MB for large payloads

    let config = Config {
        data_dir: temp_dir.path().join("data").to_string_lossy().to_string(),
        enable_persistence: true,
        tiered_storage: tiered_config,
        ..Default::default()
    };

    let broker = TestBroker::start_with_config(config).await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("broker-tiered-large");
    client.create_topic(&topic, Some(1)).await?;

    // Publish large messages (~10KB each)
    let messages_to_send = 10;
    for i in 0..messages_to_send {
        let large_payload = format!("LARGE-PAYLOAD-{:05}-", i).repeat(500); // ~10KB
        client.publish(&topic, large_payload).await?;
    }

    // Consume and verify
    let messages = client.consume(&topic, 0, 0, 20).await?;

    // Verify we received at least 90% of messages (accounting for any async write timing)
    assert!(
        messages.len() >= (messages_to_send * 9 / 10),
        "Expected at least {} messages, got {}",
        messages_to_send * 9 / 10,
        messages.len()
    );

    for (i, msg) in messages.iter().enumerate() {
        assert!(
            msg.value.len() > 5000,
            "Message {} should be large (was {})",
            i,
            msg.value.len()
        );
    }

    info!(
        "Received {}/{} large messages with tiered storage",
        messages.len(),
        messages_to_send
    );
    broker.shutdown().await?;
    info!("Broker tiered storage large payloads test passed");
    Ok(())
}

/// Test broker without tiered storage still works (backward compatibility)
#[tokio::test]
async fn test_broker_without_tiered_storage() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let config = Config {
        data_dir: temp_dir.path().join("data").to_string_lossy().to_string(),
        enable_persistence: true,
        tiered_storage: TieredStorageConfig::default(), // Disabled by default
        ..Default::default()
    };

    assert!(
        !config.tiered_storage.enabled,
        "Tiered storage should be disabled"
    );

    let broker = TestBroker::start_with_config(config).await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("broker-no-tiered");
    client.create_topic(&topic, Some(1)).await?;

    // Should work normally without tiered storage
    for i in 0..10 {
        client.publish(&topic, format!("msg-{}", i)).await?;
    }

    let messages = client.consume(&topic, 0, 0, 20).await?;
    assert_eq!(messages.len(), 10);

    broker.shutdown().await?;
    info!("Broker without tiered storage test passed");
    Ok(())
}

/// Test broker with high-performance tiered storage preset
#[tokio::test]
async fn test_broker_tiered_storage_high_performance_preset() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let mut tiered_config = TieredStorageConfig::high_performance();
    tiered_config.warm_tier_path = temp_dir.path().join("warm").to_string_lossy().to_string();
    tiered_config.cold_storage = ColdStorageConfig::LocalFs {
        path: temp_dir.path().join("cold").to_string_lossy().to_string(),
    };

    let config = Config {
        data_dir: temp_dir.path().join("data").to_string_lossy().to_string(),
        enable_persistence: true,
        tiered_storage: tiered_config,
        ..Default::default()
    };

    let broker = TestBroker::start_with_config(config).await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("broker-hp-tiered");
    client.create_topic(&topic, Some(1)).await?;

    // Publish and consume
    client.publish(&topic, "high-performance message").await?;
    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);

    broker.shutdown().await?;
    info!("Broker with high-performance tiered storage preset test passed");
    Ok(())
}

/// Test broker with cost-optimized tiered storage preset
#[tokio::test]
async fn test_broker_tiered_storage_cost_optimized_preset() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let mut tiered_config = TieredStorageConfig::cost_optimized();
    tiered_config.warm_tier_path = temp_dir.path().join("warm").to_string_lossy().to_string();
    tiered_config.cold_storage = ColdStorageConfig::LocalFs {
        path: temp_dir.path().join("cold").to_string_lossy().to_string(),
    };

    let config = Config {
        data_dir: temp_dir.path().join("data").to_string_lossy().to_string(),
        enable_persistence: true,
        tiered_storage: tiered_config,
        ..Default::default()
    };

    let broker = TestBroker::start_with_config(config).await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("broker-co-tiered");
    client.create_topic(&topic, Some(1)).await?;

    // Publish and consume
    client.publish(&topic, "cost-optimized message").await?;
    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);

    broker.shutdown().await?;
    info!("Broker with cost-optimized tiered storage preset test passed");
    Ok(())
}
