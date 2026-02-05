//! Tests for rivven-rdbc pool module

use rivven_rdbc::prelude::*;
use std::time::Duration;

// ==================== PoolConfig Tests ====================

#[test]
fn test_pool_config_default() {
    let config = PoolConfig::default();

    assert_eq!(config.min_size, 1);
    assert_eq!(config.max_size, 10);
    assert_eq!(config.acquire_timeout, Duration::from_secs(30));
    assert_eq!(config.max_lifetime, Duration::from_secs(1800));
    assert_eq!(config.idle_timeout, Duration::from_secs(600));
    assert_eq!(config.health_check_interval, Duration::from_secs(30));
    assert!(config.test_on_borrow);
    assert!(!config.test_on_return);
}

#[test]
fn test_pool_config_new() {
    let config = PoolConfig::new("postgres://localhost/test");

    assert_eq!(config.connection.url, "postgres://localhost/test");
    assert_eq!(config.min_size, 1);
    assert_eq!(config.max_size, 10);
}

#[test]
fn test_pool_config_builder() {
    let config = PoolConfig::new("postgres://localhost/test")
        .with_min_size(5)
        .with_max_size(20)
        .with_acquire_timeout(Duration::from_secs(60))
        .with_max_lifetime(Duration::from_secs(3600))
        .with_idle_timeout(Duration::from_secs(300))
        .with_test_on_borrow(false)
        .with_test_on_return(true);

    assert_eq!(config.min_size, 5);
    assert_eq!(config.max_size, 20);
    assert_eq!(config.acquire_timeout, Duration::from_secs(60));
    assert_eq!(config.max_lifetime, Duration::from_secs(3600));
    assert_eq!(config.idle_timeout, Duration::from_secs(300));
    assert!(!config.test_on_borrow);
    assert!(config.test_on_return);
}

// ==================== PoolStats Tests ====================

#[test]
fn test_pool_stats_default() {
    let stats = PoolStats::default();

    assert_eq!(stats.connections_created, 0);
    assert_eq!(stats.connections_closed, 0);
    assert_eq!(stats.acquisitions, 0);
    assert_eq!(stats.exhausted_count, 0);
    assert_eq!(stats.total_wait_time_ms, 0);
}

#[test]
fn test_atomic_pool_stats() {
    let stats = AtomicPoolStats::default();

    // Record connection creation
    stats.record_created();
    let snapshot = stats.snapshot();
    assert_eq!(snapshot.connections_created, 1);

    // Record acquisition
    stats.record_acquisition(10);
    let snapshot = stats.snapshot();
    assert_eq!(snapshot.acquisitions, 1);
    assert_eq!(snapshot.total_wait_time_ms, 10);

    // Record exhaustion
    stats.record_exhausted();
    let snapshot = stats.snapshot();
    assert_eq!(snapshot.exhausted_count, 1);

    // Record connection close
    stats.record_closed();
    let snapshot = stats.snapshot();
    assert_eq!(snapshot.connections_closed, 1);
}

#[test]
fn test_atomic_pool_stats_multiple() {
    let stats = AtomicPoolStats::default();

    // Simulate pool activity
    for _ in 0..5 {
        stats.record_created();
    }

    for _ in 0..100 {
        stats.record_acquisition(5);
    }

    for _ in 0..3 {
        stats.record_exhausted();
    }

    for _ in 0..2 {
        stats.record_closed();
    }

    let snapshot = stats.snapshot();
    assert_eq!(snapshot.connections_created, 5);
    assert_eq!(snapshot.connections_closed, 2);
    assert_eq!(snapshot.acquisitions, 100);
    assert_eq!(snapshot.exhausted_count, 3);
    assert_eq!(snapshot.total_wait_time_ms, 500); // 100 * 5
}

// ==================== PoolBuilder Tests ====================

#[test]
fn test_pool_builder_new() {
    let builder = PoolBuilder::new("postgres://localhost/test");
    let config = builder.config();

    assert_eq!(config.connection.url, "postgres://localhost/test");
}

#[test]
fn test_pool_builder_chain() {
    let config = PoolBuilder::new("mysql://localhost/test")
        .min_size(2)
        .max_size(15)
        .acquire_timeout(Duration::from_secs(45))
        .max_lifetime(Duration::from_secs(1200))
        .idle_timeout(Duration::from_secs(120))
        .test_on_borrow(true)
        .config();

    assert_eq!(config.min_size, 2);
    assert_eq!(config.max_size, 15);
    assert_eq!(config.acquire_timeout, Duration::from_secs(45));
    assert_eq!(config.max_lifetime, Duration::from_secs(1200));
    assert_eq!(config.idle_timeout, Duration::from_secs(120));
    assert!(config.test_on_borrow);
}

// ==================== ConnectionConfig Integration with Pool ====================

#[test]
fn test_pool_with_connection_config() {
    let conn_config = ConnectionConfig::new("postgres://localhost/test")
        .with_connect_timeout(10_000)
        .with_query_timeout(60_000);

    let pool_config = PoolConfig {
        connection: conn_config,
        min_size: 2,
        max_size: 10,
        acquire_timeout: Duration::from_secs(30),
        max_lifetime: Duration::from_secs(1800),
        idle_timeout: Duration::from_secs(600),
        health_check_interval: Duration::from_secs(30),
        test_on_borrow: true,
        test_on_return: false,
    };

    assert_eq!(pool_config.connection.connect_timeout_ms, 10_000);
    assert_eq!(pool_config.connection.query_timeout_ms, 60_000);
}

// ==================== Pool Size Validation Tests ====================

#[test]
fn test_pool_config_size_validation() {
    // min_size can equal max_size
    let config = PoolConfig::new("postgres://localhost/test")
        .with_min_size(5)
        .with_max_size(5);

    assert_eq!(config.min_size, 5);
    assert_eq!(config.max_size, 5);
}

#[test]
fn test_pool_config_zero_min_size() {
    // min_size can be 0
    let config = PoolConfig::new("postgres://localhost/test")
        .with_min_size(0)
        .with_max_size(10);

    assert_eq!(config.min_size, 0);
    assert_eq!(config.max_size, 10);
}

// ==================== AtomicPoolStats Additional Tests ====================

#[test]
fn test_atomic_pool_stats_avg_wait_time() {
    let stats = AtomicPoolStats::default();

    // Initially no stats
    assert_eq!(stats.avg_wait_time_ms(), 0.0);

    // Record acquisitions with different wait times
    stats.record_acquisition(100);
    stats.record_acquisition(200);
    stats.record_acquisition(300);

    // Average should be (100 + 200 + 300) / 3 = 200
    assert!((stats.avg_wait_time_ms() - 200.0).abs() < 0.01);
}

#[test]
fn test_atomic_pool_stats_health_check_failure() {
    let stats = AtomicPoolStats::default();

    stats.record_health_check_failure();
    stats.record_health_check_failure();

    let snapshot = stats.snapshot();
    assert_eq!(snapshot.health_check_failures, 2);
}
