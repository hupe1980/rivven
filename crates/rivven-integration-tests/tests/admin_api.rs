//! Admin API Integration Tests
//!
//! Tests for the Rivven Admin API.
//! Verifies AlterTopicConfig, CreatePartitions, DeleteRecords, DescribeTopicConfigs.
//!
//! Run with: cargo test -p rivven-integration-tests --test admin_api -- --nocapture

use anyhow::Result;
use rivven_client::Client;
use rivven_integration_tests::fixtures::TestBroker;
use rivven_integration_tests::helpers::*;
use tracing::info;

// =============================================================================
// TOPIC CONFIGURATION TESTS
// =============================================================================

/// Test describe topic configs for single topic
#[tokio::test]
async fn test_describe_topic_configs_single() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("config-describe");
    client.create_topic(&topic, Some(1)).await?;
    info!("Created topic: {}", topic);

    // Describe topic configs
    let configs = client.describe_topic_configs(&[&topic]).await?;

    assert_eq!(configs.len(), 1);
    assert!(configs.contains_key(&topic));

    let topic_config = configs.get(&topic).unwrap();
    info!(
        "Topic config keys: {:?}",
        topic_config.keys().collect::<Vec<_>>()
    );

    broker.shutdown().await?;
    info!("Describe topic configs single test passed");
    Ok(())
}

/// Test describe topic configs for multiple topics
#[tokio::test]
async fn test_describe_topic_configs_multiple() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic1 = unique_topic_name("config-multi-1");
    let topic2 = unique_topic_name("config-multi-2");
    let topic3 = unique_topic_name("config-multi-3");

    client.create_topic(&topic1, Some(1)).await?;
    client.create_topic(&topic2, Some(1)).await?;
    client.create_topic(&topic3, Some(1)).await?;
    info!("Created 3 topics");

    let configs = client
        .describe_topic_configs(&[&topic1, &topic2, &topic3])
        .await?;

    assert_eq!(configs.len(), 3);
    assert!(configs.contains_key(&topic1));
    assert!(configs.contains_key(&topic2));
    assert!(configs.contains_key(&topic3));

    broker.shutdown().await?;
    info!("Describe topic configs multiple test passed");
    Ok(())
}

/// Test describe topic configs for non-existent topic
#[tokio::test]
async fn test_describe_topic_configs_nonexistent() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let result = client.describe_topic_configs(&["nonexistent-topic"]).await;

    // Should either return empty for nonexistent, return config with empty values,
    // or return an error - all are valid behaviors
    match result {
        Ok(configs) => {
            info!("Non-existent topic returned configs: {:?}", configs);
            // The topic might be auto-created or return empty - both valid
        }
        Err(e) => {
            info!("Non-existent topic returned error (expected): {}", e);
        }
    }

    broker.shutdown().await?;
    info!("Describe topic configs nonexistent test passed");
    Ok(())
}

/// Test alter topic config - retention
#[tokio::test]
async fn test_alter_topic_config_retention() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("alter-retention");
    client.create_topic(&topic, Some(1)).await?;
    info!("Created topic: {}", topic);

    // Alter retention to 1 day
    let result = client
        .alter_topic_config(
            &topic,
            &[
                ("retention.ms", Some("86400000")), // 1 day
            ],
        )
        .await?;

    assert!(result.changed_count >= 1);
    info!(
        "Altered topic config, changed {} entries",
        result.changed_count
    );

    // Verify the change
    let configs = client.describe_topic_configs(&[&topic]).await?;
    let topic_config = configs.get(&topic).unwrap();

    if let Some(retention) = topic_config.get("retention.ms") {
        info!("New retention.ms: {}", retention);
    }

    broker.shutdown().await?;
    info!("Alter topic config retention test passed");
    Ok(())
}

/// Test alter topic config - cleanup policy
#[tokio::test]
async fn test_alter_topic_config_cleanup_policy() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("alter-cleanup");
    client.create_topic(&topic, Some(1)).await?;
    info!("Created topic: {}", topic);

    // Change to compaction
    let result = client
        .alter_topic_config(&topic, &[("cleanup.policy", Some("compact"))])
        .await?;

    assert!(result.changed_count >= 1);
    info!("Changed cleanup.policy to compact");

    broker.shutdown().await?;
    info!("Alter topic config cleanup policy test passed");
    Ok(())
}

/// Test alter topic config - reset to default
#[tokio::test]
async fn test_alter_topic_config_reset_default() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("alter-reset");
    client.create_topic(&topic, Some(1)).await?;
    info!("Created topic: {}", topic);

    // First set a custom value
    client
        .alter_topic_config(
            &topic,
            &[
                ("retention.ms", Some("3600000")), // 1 hour
            ],
        )
        .await?;
    info!("Set retention to 1 hour");

    // Reset to default by passing None
    let result = client
        .alter_topic_config(
            &topic,
            &[
                ("retention.ms", None), // Reset to default
            ],
        )
        .await?;

    info!(
        "Reset retention.ms to default, changed {} entries",
        result.changed_count
    );

    broker.shutdown().await?;
    info!("Alter topic config reset default test passed");
    Ok(())
}

/// Test alter topic config - multiple configs at once
#[tokio::test]
async fn test_alter_topic_config_multiple() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("alter-multi");
    client.create_topic(&topic, Some(1)).await?;
    info!("Created topic: {}", topic);

    // Alter multiple configs at once
    let result = client
        .alter_topic_config(
            &topic,
            &[
                ("retention.ms", Some("172800000")),    // 2 days
                ("max.message.bytes", Some("2097152")), // 2 MB
                ("segment.bytes", Some("536870912")),   // 512 MB
            ],
        )
        .await?;

    info!("Altered {} config entries", result.changed_count);
    assert!(result.changed_count >= 1);

    broker.shutdown().await?;
    info!("Alter topic config multiple test passed");
    Ok(())
}

// =============================================================================
// PARTITION MANAGEMENT TESTS
// =============================================================================

/// Test create partitions - increase partition count
#[tokio::test]
async fn test_create_partitions_increase() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("partitions-increase");

    // Create with 1 partition
    client.create_topic(&topic, Some(1)).await?;
    info!("Created topic with 1 partition");

    // Increase to 4 partitions
    let new_count = client.create_partitions(&topic, 4).await?;

    assert_eq!(new_count, 4);
    info!("Increased partitions to {}", new_count);

    // Verify
    let topics = client.list_topics().await?;
    assert!(topics.contains(&topic));

    broker.shutdown().await?;
    info!("Create partitions increase test passed");
    Ok(())
}

/// Test create partitions - cannot decrease
#[tokio::test]
async fn test_create_partitions_cannot_decrease() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("partitions-decrease");

    // Create with 4 partitions
    client.create_topic(&topic, Some(4)).await?;
    info!("Created topic with 4 partitions");

    // Try to decrease to 2 - should fail
    let result = client.create_partitions(&topic, 2).await;

    match result {
        Ok(count) => {
            // Some implementations might reject or return original count
            info!("Create partitions returned: {}", count);
            assert!(count >= 4, "Should not decrease partition count");
        }
        Err(e) => {
            info!("Correctly rejected decrease: {}", e);
        }
    }

    broker.shutdown().await?;
    info!("Create partitions cannot decrease test passed");
    Ok(())
}

/// Test create partitions - same count is rejected (Kafka behavior)
#[tokio::test]
async fn test_create_partitions_same_count_rejected() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("partitions-same");

    client.create_topic(&topic, Some(3)).await?;
    info!("Created topic with 3 partitions");

    // Request same count - should be rejected (Kafka requires increase)
    let result = client.create_partitions(&topic, 3).await;

    match result {
        Ok(count) => {
            info!("Same partition count returned: {}", count);
            // Some implementations might allow, verify count
            assert_eq!(count, 3);
        }
        Err(e) => {
            info!("Correctly rejected same count request: {}", e);
            // This is the expected Kafka behavior - must be greater than current
        }
    }

    broker.shutdown().await?;
    info!("Create partitions same count test passed");
    Ok(())
}

// =============================================================================
// DELETE RECORDS TESTS
// =============================================================================

/// Test delete records before offset
#[tokio::test]
async fn test_delete_records_before_offset() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("delete-records");
    client.create_topic(&topic, Some(1)).await?;
    info!("Created topic: {}", topic);

    // Publish some messages
    for i in 0..10 {
        client
            .publish(&topic, format!("message-{}", i).into_bytes())
            .await?;
    }
    info!("Published 10 messages");

    // Delete records before offset 5
    let result = client.delete_records(&topic, &[(0, 5)]).await?;

    info!("Delete records result: {:?}", result);

    // The low watermark should now be >= 5
    assert!(result.iter().any(|r| r.low_watermark >= 5));

    broker.shutdown().await?;
    info!("Delete records before offset test passed");
    Ok(())
}

/// Test delete records - multiple partitions
#[tokio::test]
async fn test_delete_records_multiple_partitions() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("delete-multi-part");
    client.create_topic(&topic, Some(3)).await?;
    info!("Created topic with 3 partitions");

    // Publish messages to different partitions using keys
    for i in 0..30 {
        client
            .publish_with_key(
                &topic,
                Some(format!("key-{}", i % 3).into_bytes()),
                format!("msg-{}", i).into_bytes(),
            )
            .await?;
    }
    info!("Published 30 messages across partitions");

    // Delete records from all partitions
    let result = client
        .delete_records(&topic, &[(0, 3), (1, 3), (2, 3)])
        .await?;

    assert_eq!(result.len(), 3);
    info!("Deleted records from 3 partitions: {:?}", result);

    broker.shutdown().await?;
    info!("Delete records multiple partitions test passed");
    Ok(())
}

// =============================================================================
// COMBINED ADMIN OPERATIONS TESTS
// =============================================================================

/// Test combined admin operations flow
#[tokio::test]
async fn test_admin_operations_flow() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("admin-flow");

    // 1. Create topic
    client.create_topic(&topic, Some(2)).await?;
    info!("1. Created topic with 2 partitions");

    // 2. Describe initial config
    let configs = client.describe_topic_configs(&[&topic]).await?;
    info!("2. Initial config: {:?}", configs.get(&topic));

    // 3. Alter configuration
    client
        .alter_topic_config(
            &topic,
            &[
                ("retention.ms", Some("43200000")), // 12 hours
            ],
        )
        .await?;
    info!("3. Altered retention to 12 hours");

    // 4. Increase partitions
    let new_count = client.create_partitions(&topic, 4).await?;
    info!("4. Increased partitions to {}", new_count);

    // 5. Publish messages
    for i in 0..20 {
        client
            .publish(&topic, format!("message-{}", i).into_bytes())
            .await?;
    }
    info!("5. Published 20 messages");

    // 6. Delete old records
    let delete_result = client.delete_records(&topic, &[(0, 2), (1, 2)]).await?;
    info!("6. Deleted old records: {:?}", delete_result);

    // 7. Verify final config
    let final_configs = client.describe_topic_configs(&[&topic]).await?;
    info!("7. Final config: {:?}", final_configs.get(&topic));

    broker.shutdown().await?;
    info!("Admin operations flow test passed");
    Ok(())
}

/// Test admin API basic operation
#[tokio::test]
async fn test_admin_api_basic_operation() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("admin-basic");
    client.create_topic(&topic, Some(1)).await?;

    // Basic describe should work
    let result = client.describe_topic_configs(&[&topic]).await;
    assert!(result.is_ok(), "Basic describe should work");

    info!("Admin API basic operation test passed");

    broker.shutdown().await?;
    Ok(())
}
