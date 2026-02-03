//! Quota Integration Tests
//!
//! Tests for per-principal quotas (Kafka KIP-55 parity).
//! Verifies throughput limiting on a per-user, per-client-id, or per-consumer-group basis.
//!
//! Run with: cargo test -p rivven-integration-tests --test quotas -- --nocapture

use anyhow::Result;
use rivven_core::quota::{
    QuotaConfig, QuotaEntity, QuotaManager, QuotaResult, QuotaType, DEFAULT_CONSUME_BYTES_RATE,
    DEFAULT_PRODUCE_BYTES_RATE, DEFAULT_REQUEST_RATE,
};
use rivven_integration_tests::helpers::*;
use std::time::Duration;
use tracing::info;

// =============================================================================
// QUOTA MANAGER INTEGRATION TESTS
// =============================================================================

/// Test default quota configuration
#[tokio::test]
async fn test_default_quota_configuration() -> Result<()> {
    init_tracing();

    let manager = QuotaManager::new();
    let defaults = manager.get_defaults();

    // Check default values
    assert_eq!(
        defaults.produce_bytes_rate,
        Some(DEFAULT_PRODUCE_BYTES_RATE)
    );
    info!(
        "Default produce bytes rate: {} bytes/sec",
        DEFAULT_PRODUCE_BYTES_RATE
    );

    assert_eq!(
        defaults.consume_bytes_rate,
        Some(DEFAULT_CONSUME_BYTES_RATE)
    );
    info!(
        "Default consume bytes rate: {} bytes/sec",
        DEFAULT_CONSUME_BYTES_RATE
    );

    assert_eq!(defaults.request_rate, Some(DEFAULT_REQUEST_RATE));
    info!(
        "Default request rate: {} requests/sec",
        DEFAULT_REQUEST_RATE
    );

    info!("Default quota configuration test passed");
    Ok(())
}

/// Test user-specific quota configuration
#[tokio::test]
async fn test_user_specific_quota() -> Result<()> {
    init_tracing();

    let manager = QuotaManager::new();

    // Set Alice's quota
    manager.set_quota(
        QuotaEntity::user("alice"),
        QuotaConfig::new().with_produce_rate(10_000_000), // 10 MB/s
    );

    // Alice should have custom quota
    let alice_quota = manager.get_effective_quota(Some("alice"), None);
    assert_eq!(alice_quota.produce_bytes_rate, Some(10_000_000));
    info!(
        "Alice's produce quota: {:?}",
        alice_quota.produce_bytes_rate
    );

    // Bob should have default quota
    let bob_quota = manager.get_effective_quota(Some("bob"), None);
    assert_eq!(
        bob_quota.produce_bytes_rate,
        Some(DEFAULT_PRODUCE_BYTES_RATE)
    );
    info!(
        "Bob's produce quota (default): {:?}",
        bob_quota.produce_bytes_rate
    );

    info!("User-specific quota test passed");
    Ok(())
}

/// Test client-id specific quota
#[tokio::test]
async fn test_client_id_quota() -> Result<()> {
    init_tracing();

    let manager = QuotaManager::new();
    manager.set_quota(
        QuotaEntity::client_id("batch-processor"),
        QuotaConfig::new().with_consume_rate(200_000_000), // 200 MB/s for batch jobs
    );

    let batch_quota = manager.get_effective_quota(None, Some("batch-processor"));
    assert_eq!(batch_quota.consume_bytes_rate, Some(200_000_000));
    info!(
        "Batch processor consume quota: {:?}",
        batch_quota.consume_bytes_rate
    );

    let normal_quota = manager.get_effective_quota(None, Some("web-app"));
    assert_eq!(
        normal_quota.consume_bytes_rate,
        Some(DEFAULT_CONSUME_BYTES_RATE)
    );
    info!(
        "Web app consume quota (default): {:?}",
        normal_quota.consume_bytes_rate
    );

    info!("Client-id quota test passed");
    Ok(())
}

/// Test consumer group quota
#[tokio::test]
async fn test_consumer_group_quota() -> Result<()> {
    init_tracing();

    let manager = QuotaManager::new();
    manager.set_quota(
        QuotaEntity::consumer_group("order-processors"),
        QuotaConfig::new().with_consume_rate(500_000_000), // 500 MB/s for critical consumer group
    );

    let quotas = manager.list_quotas();
    assert_eq!(quotas.len(), 1);
    assert_eq!(quotas[0].0, QuotaEntity::consumer_group("order-processors"));
    assert_eq!(quotas[0].1.consume_bytes_rate, Some(500_000_000));

    info!("Order processors group quota set successfully");
    info!("Consumer group quota test passed");
    Ok(())
}

/// Test quota resolution order (specific > user default > global default)
#[tokio::test]
async fn test_quota_resolution_order() -> Result<()> {
    init_tracing();

    let manager = QuotaManager::with_defaults(QuotaConfig::new().with_produce_rate(1000));

    // Set user default
    manager.set_quota(
        QuotaEntity::default_user(),
        QuotaConfig::new().with_produce_rate(2000),
    );

    // Set specific user
    manager.set_quota(
        QuotaEntity::user("alice"),
        QuotaConfig::new().with_produce_rate(3000),
    );

    // Alice gets specific quota
    let alice = manager.get_effective_quota(Some("alice"), None);
    assert_eq!(alice.produce_bytes_rate, Some(3000));
    info!("Alice specific quota: {:?}", alice.produce_bytes_rate);

    // Bob gets user default
    let bob = manager.get_effective_quota(Some("bob"), None);
    assert_eq!(bob.produce_bytes_rate, Some(2000));
    info!("Bob (user default) quota: {:?}", bob.produce_bytes_rate);

    // Anonymous gets global default
    let anon = manager.get_effective_quota(None, None);
    assert_eq!(anon.produce_bytes_rate, Some(1000));
    info!(
        "Anonymous (global default) quota: {:?}",
        anon.produce_bytes_rate
    );

    info!("Quota resolution order test passed");
    Ok(())
}

/// Test unlimited quota setting
#[tokio::test]
async fn test_unlimited_quota() -> Result<()> {
    init_tracing();

    let manager = QuotaManager::new();
    manager.set_quota(QuotaEntity::user("superuser"), QuotaConfig::unlimited());

    // Superuser can produce massive amounts without throttling
    let result = manager.record_produce(Some("superuser"), None, 1_000_000_000);
    assert!(result.is_allowed());
    info!("Superuser has unlimited quota - 1GB produce allowed");

    info!("Unlimited quota test passed");
    Ok(())
}

/// Test quota enforcement - throttle when exceeded
#[tokio::test]
async fn test_throttle_when_quota_exceeded() -> Result<()> {
    init_tracing();

    let manager = QuotaManager::with_defaults(
        QuotaConfig::new().with_produce_rate(1000), // 1 KB/s
    );

    // First request within quota
    let result = manager.record_produce(Some("limited-user"), None, 500);
    assert!(result.is_allowed());
    info!("First 500 bytes allowed");

    // Second request exceeds quota
    let result = manager.record_produce(Some("limited-user"), None, 1000);
    assert!(!result.is_allowed());

    match result {
        QuotaResult::Throttled {
            throttle_time,
            quota_type,
            entity,
        } => {
            assert!(throttle_time.as_millis() > 0);
            assert_eq!(quota_type, QuotaType::ProduceBytes);
            assert!(entity.contains("limited-user"));
            info!(
                "Throttled for {:?} - quota type: {:?}",
                throttle_time, quota_type
            );
        }
        _ => panic!("Expected throttled result"),
    }

    info!("Throttle when quota exceeded test passed");
    Ok(())
}

/// Test quota statistics tracking
#[tokio::test]
async fn test_quota_statistics() -> Result<()> {
    init_tracing();

    let manager = QuotaManager::with_defaults(QuotaConfig::new().with_produce_rate(100));

    // Exceed quota to generate violation
    manager.record_produce(Some("stats-user"), None, 200);

    let stats = manager.stats();
    assert_eq!(stats.produce_violations(), 1);
    assert!(stats.total_throttle_time_ms() > 0);

    info!("Produce violations: {}", stats.produce_violations());
    info!("Total throttle time: {}ms", stats.total_throttle_time_ms());

    info!("Quota statistics test passed");
    Ok(())
}

/// Test quota configuration with builder pattern
#[tokio::test]
async fn test_quota_config_builder() -> Result<()> {
    init_tracing();

    let config = QuotaConfig::new()
        .with_produce_rate(10_000_000)
        .with_consume_rate(20_000_000)
        .with_request_rate(500);

    assert_eq!(config.produce_bytes_rate, Some(10_000_000));
    assert_eq!(config.consume_bytes_rate, Some(20_000_000));
    assert_eq!(config.request_rate, Some(500));

    info!(
        "Config - produce: {:?}, consume: {:?}, request: {:?}",
        config.produce_bytes_rate, config.consume_bytes_rate, config.request_rate
    );

    info!("Quota config builder test passed");
    Ok(())
}

/// Test request rate limiting
#[tokio::test]
async fn test_request_rate_limiting() -> Result<()> {
    init_tracing();

    let manager = QuotaManager::with_defaults(
        QuotaConfig::new().with_request_rate(10), // 10 requests/s
    );

    // First 10 requests should be allowed
    for i in 0..10 {
        let result = manager.record_request(Some("rate-limited-user"), None);
        assert!(result.is_allowed(), "Request {} should be allowed", i);
    }
    info!("First 10 requests allowed");

    // 11th request should be throttled
    let result = manager.record_request(Some("rate-limited-user"), None);
    assert!(!result.is_allowed());
    info!("11th request throttled as expected");

    info!("Request rate limiting test passed");
    Ok(())
}

/// Test consume quota enforcement
#[tokio::test]
async fn test_consume_quota_enforcement() -> Result<()> {
    init_tracing();

    let manager = QuotaManager::with_defaults(QuotaConfig::new().with_consume_rate(1000));

    let result = manager.record_consume(Some("consumer"), None, 500);
    assert!(result.is_allowed());
    info!("First 500 bytes consume allowed");

    let result = manager.record_consume(Some("consumer"), None, 1000);
    assert!(!result.is_allowed());
    assert_eq!(manager.stats().consume_violations(), 1);
    info!("Consume quota exceeded - violation recorded");

    info!("Consume quota enforcement test passed");
    Ok(())
}

/// Test quota removal
#[tokio::test]
async fn test_quota_removal() -> Result<()> {
    init_tracing();

    let manager = QuotaManager::new();
    let entity = QuotaEntity::user("temp-user");

    // Set custom quota
    manager.set_quota(
        entity.clone(),
        QuotaConfig::new().with_produce_rate(5_000_000),
    );

    // Verify custom quota
    let quota = manager.get_effective_quota(Some("temp-user"), None);
    assert_eq!(quota.produce_bytes_rate, Some(5_000_000));
    info!("Custom quota set: {:?}", quota.produce_bytes_rate);

    // Remove quota
    manager.remove_quota(&entity);

    // Should fall back to default
    let quota = manager.get_effective_quota(Some("temp-user"), None);
    assert_eq!(quota.produce_bytes_rate, Some(DEFAULT_PRODUCE_BYTES_RATE));
    info!(
        "After removal, using default: {:?}",
        quota.produce_bytes_rate
    );

    info!("Quota removal test passed");
    Ok(())
}

/// Test list quotas
#[tokio::test]
async fn test_list_quotas() -> Result<()> {
    init_tracing();

    let manager = QuotaManager::new();

    manager.set_quota(
        QuotaEntity::user("alice"),
        QuotaConfig::new().with_produce_rate(1000),
    );
    manager.set_quota(
        QuotaEntity::user("bob"),
        QuotaConfig::new().with_produce_rate(2000),
    );
    manager.set_quota(
        QuotaEntity::client_id("app-1"),
        QuotaConfig::new().with_request_rate(100),
    );

    let quotas = manager.list_quotas();
    assert_eq!(quotas.len(), 3);
    info!("Listed {} quota configurations", quotas.len());

    info!("List quotas test passed");
    Ok(())
}

/// Test active entity count tracking
#[tokio::test]
async fn test_active_entity_count() -> Result<()> {
    init_tracing();

    let manager = QuotaManager::new();
    assert_eq!(manager.active_entity_count(), 0);
    info!("Initial active entities: 0");

    manager.record_produce(Some("alice"), None, 100);
    assert_eq!(manager.active_entity_count(), 1);
    info!("After alice produces: 1 active entity");

    manager.record_produce(Some("bob"), None, 100);
    assert_eq!(manager.active_entity_count(), 2);
    info!("After bob produces: 2 active entities");

    manager.record_consume(Some("charlie"), None, 100);
    assert_eq!(manager.active_entity_count(), 3);
    info!("After charlie consumes: 3 active entities");

    info!("Active entity count test passed");
    Ok(())
}

/// Test entity statistics
#[tokio::test]
async fn test_entity_statistics() -> Result<()> {
    init_tracing();

    let manager = QuotaManager::new();

    // Record activity for alice
    manager.record_produce(Some("alice"), None, 1024);
    manager.record_consume(Some("alice"), None, 2048);
    manager.record_request(Some("alice"), None);

    let stats = manager.get_entity_stats(Some("alice"), None);
    assert!(stats.is_some());
    let stats = stats.unwrap();
    info!(
        "Alice stats - produce_rate: {}, consume_rate: {}, request_rate: {}",
        stats.produce_bytes_rate, stats.consume_bytes_rate, stats.request_rate
    );

    info!("Entity statistics test passed");
    Ok(())
}

/// Test quota entity display formatting
#[tokio::test]
async fn test_quota_entity_display() -> Result<()> {
    init_tracing();

    let user = QuotaEntity::user("alice");
    assert_eq!(user.to_string(), "user:alice");
    info!("User entity: {}", user);

    let client = QuotaEntity::client_id("my-app");
    assert_eq!(client.to_string(), "client-id:my-app");
    info!("Client entity: {}", client);

    let group = QuotaEntity::consumer_group("processors");
    assert_eq!(group.to_string(), "consumer-group:processors");
    info!("Group entity: {}", group);

    let default = QuotaEntity::default_entity();
    assert_eq!(default.to_string(), "default:<default>");
    info!("Default entity: {}", default);

    info!("Quota entity display test passed");
    Ok(())
}

/// Test concurrent quota operations
#[tokio::test]
async fn test_concurrent_quota_operations() -> Result<()> {
    init_tracing();

    use std::sync::Arc;
    use tokio::task::JoinSet;

    let manager = Arc::new(QuotaManager::new());
    let mut tasks = JoinSet::new();

    // Spawn multiple tasks recording usage
    for i in 0..10 {
        let manager_clone = Arc::clone(&manager);
        tasks.spawn(async move {
            for _ in 0..100 {
                manager_clone.record_produce(Some(&format!("user-{}", i)), None, 1000);
                manager_clone.record_request(Some(&format!("user-{}", i)), None);
            }
        });
    }

    // Wait for all tasks
    while let Some(result) = tasks.join_next().await {
        result?;
    }

    // Verify all entities tracked
    assert_eq!(manager.active_entity_count(), 10);
    info!(
        "Concurrent operations completed - {} active entities",
        manager.active_entity_count()
    );

    info!("Concurrent quota operations test passed");
    Ok(())
}

/// Test quota result methods
#[tokio::test]
async fn test_quota_result_methods() -> Result<()> {
    init_tracing();

    // Allowed result
    let allowed = QuotaResult::Allowed;
    assert!(allowed.is_allowed());
    assert_eq!(allowed.throttle_time_ms(), 0);
    info!("Allowed result - is_allowed: true, throttle_time: 0ms");

    // Throttled result
    let throttled = QuotaResult::Throttled {
        throttle_time: Duration::from_millis(500),
        quota_type: QuotaType::ProduceBytes,
        entity: "user:test".to_string(),
    };
    assert!(!throttled.is_allowed());
    assert_eq!(throttled.throttle_time_ms(), 500);
    info!("Throttled result - is_allowed: false, throttle_time: 500ms");

    info!("Quota result methods test passed");
    Ok(())
}
