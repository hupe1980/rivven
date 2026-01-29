//! MariaDB CDC Integration Tests
//!
//! **REAL** integration tests for Rivven's MariaDB CDC connector.
//! MariaDB uses the same MySQL binary log protocol, so we use the same MySqlCdc
//! connector with MariaDB-specific configuration.
//!
//! Run with: cargo test -p rivven-integration-tests --test cdc_mariadb -- --nocapture
//!
//! Requirements: Docker must be running for testcontainers.

use std::time::Duration;

use anyhow::Result;
use mysql_async::prelude::Queryable;
use rivven_cdc::common::{CdcOp, CdcSource};
use rivven_cdc::mysql::{MySqlCdc, MySqlCdcConfig};
use rivven_integration_tests::fixtures::TestMariadb;
use rivven_integration_tests::helpers::*;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::info;

// ============================================================================
// Infrastructure Tests - Verify test setup works correctly
// ============================================================================

/// Test that we can start a MariaDB container with binary logging enabled
#[tokio::test]
async fn test_mariadb_container_starts() -> Result<()> {
    init_tracing();

    info!("Starting MariaDB test container...");
    let mariadb = TestMariadb::start().await?;

    info!("MariaDB started on {}:{}", mariadb.host, mariadb.port);
    assert!(!mariadb.host.is_empty());
    assert!(mariadb.port > 0);

    // Verify connection works
    let pool = mariadb.pool();
    let mut conn = pool.get_conn().await?;
    let result: Option<i32> = conn.query_first("SELECT 1").await?;
    assert_eq!(result, Some(1));

    info!("MariaDB container test passed");
    Ok(())
}

/// Test MariaDB container creates test schema successfully
#[tokio::test]
async fn test_mariadb_creates_test_schema() -> Result<()> {
    init_tracing();

    let mariadb = TestMariadb::start().await?;
    mariadb.setup_test_schema().await?;

    // Verify tables exist
    let pool = mariadb.pool();
    let mut conn = pool.get_conn().await?;

    let tables: Vec<String> = conn.query("SHOW TABLES").await?;

    assert!(tables.iter().any(|t| t == "users"));
    assert!(tables.iter().any(|t| t == "orders"));

    info!("MariaDB schema test passed");
    Ok(())
}

/// Test MariaDB binary log is enabled and configured correctly
#[tokio::test]
async fn test_mariadb_binlog_enabled() -> Result<()> {
    init_tracing();

    let mariadb = TestMariadb::start().await?;

    let pool = mariadb.pool();
    let mut conn = pool.get_conn().await?;

    // Check binlog format
    let format: Option<String> = conn.query_first("SELECT @@binlog_format").await?;
    assert_eq!(format, Some("ROW".to_string()));

    // Check binlog_row_image
    let row_image: Option<String> = conn.query_first("SELECT @@binlog_row_image").await?;
    assert_eq!(row_image, Some("FULL".to_string()));

    // Check log_bin is ON
    let log_bin: Option<String> = conn.query_first("SELECT @@log_bin").await?;
    assert_eq!(log_bin, Some("1".to_string()));

    info!("MariaDB binary log configuration test passed");
    Ok(())
}

/// Test MariaDB GTID mode is enabled
#[tokio::test]
async fn test_mariadb_gtid_enabled() -> Result<()> {
    init_tracing();

    let mariadb = TestMariadb::start().await?;

    let pool = mariadb.pool();
    let mut conn = pool.get_conn().await?;

    // MariaDB uses a different GTID system than MySQL
    // Check that GTID is being tracked
    let gtid_mode: Option<String> = conn.query_first("SELECT @@gtid_strict_mode").await?;
    assert!(gtid_mode.is_some());

    info!("MariaDB GTID configuration test passed");
    Ok(())
}

// ============================================================================
// Rivven MariaDB CDC Connector Tests - Actually testing rivven-cdc code!
// ============================================================================

/// Test that MySqlCdc connector can be created for MariaDB
#[tokio::test]
async fn test_mariadb_cdc_connector_creation() -> Result<()> {
    init_tracing();

    let mariadb = TestMariadb::start().await?;
    mariadb.setup_test_schema().await?;

    // MariaDB uses the same MySqlCdc connector
    let config = MySqlCdcConfig::new(&mariadb.host, "testuser")
        .with_password("testpass")
        .with_port(mariadb.port)
        .with_database("testdb")
        .with_server_id(22345);

    // Create the CDC connector
    let cdc = MySqlCdc::new(config);

    // Verify connector is not active yet
    assert!(!cdc.is_healthy().await);

    info!("MariaDB CDC connector creation test passed");
    Ok(())
}

/// Test that MySqlCdc connector can start and stop with MariaDB
#[tokio::test]
async fn test_mariadb_cdc_connector_lifecycle() -> Result<()> {
    init_tracing();

    let mariadb = TestMariadb::start().await?;
    mariadb.setup_test_schema().await?;

    let (binlog_file, binlog_pos) = mariadb.get_binlog_position().await?;

    let config = MySqlCdcConfig::new(&mariadb.host, "testuser")
        .with_password("testpass")
        .with_port(mariadb.port)
        .with_database("testdb")
        .with_server_id(22346)
        .with_binlog_position(&binlog_file, binlog_pos as u32);

    let mut cdc = MySqlCdc::new(config);

    // Start the connector
    cdc.start().await?;
    assert!(cdc.is_healthy().await);
    info!("MariaDB CDC connector started");

    // Give it a moment to initialize
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Stop the connector
    cdc.stop().await?;
    assert!(!cdc.is_healthy().await);
    info!("MariaDB CDC connector stopped");

    info!("MariaDB CDC connector lifecycle test passed");
    Ok(())
}

/// **Core Test**: MySqlCdc captures INSERT events from MariaDB
#[tokio::test]
async fn test_mariadb_cdc_captures_insert_events() -> Result<()> {
    init_tracing();

    let mariadb = TestMariadb::start().await?;
    mariadb.setup_test_schema().await?;

    let (binlog_file, binlog_pos) = mariadb.get_binlog_position().await?;
    info!(
        "Starting CDC from binlog position: {}:{}",
        binlog_file, binlog_pos
    );

    // Create event channel
    let (tx, mut rx) = mpsc::channel(100);

    let config = MySqlCdcConfig::new(&mariadb.host, "testuser")
        .with_password("testpass")
        .with_port(mariadb.port)
        .with_database("testdb")
        .with_server_id(22347)
        .with_binlog_position(&binlog_file, binlog_pos as u32);

    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    // Start CDC
    cdc.start().await?;

    // Give CDC time to connect
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Insert a user - this should trigger a CDC event
    mariadb.insert_test_users(1).await?;
    info!("Inserted test user, waiting for CDC event...");

    // Wait for and verify the INSERT event
    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive event");

    info!("Received CDC event: {:?}", event);

    // MariaDB uses the same protocol as MySQL
    assert_eq!(event.source_type, "mysql");
    assert_eq!(event.op, CdcOp::Insert);
    assert_eq!(event.table, "users");
    assert!(event.after.is_some());

    // Verify the captured data
    let after = event.after.as_ref().unwrap();
    assert!(after.get("username").is_some());
    assert!(after.get("email").is_some());

    cdc.stop().await?;
    info!("MariaDB CDC captures INSERT events test passed");
    Ok(())
}

/// **Core Test**: MySqlCdc captures UPDATE events from MariaDB with before/after states
#[tokio::test]
async fn test_mariadb_cdc_captures_update_events() -> Result<()> {
    init_tracing();

    let mariadb = TestMariadb::start().await?;
    mariadb.setup_test_schema().await?;

    // Insert a user before starting CDC
    let pool = mariadb.pool();
    let mut conn = pool.get_conn().await?;
    conn.exec_drop(
        "INSERT INTO users (username, email) VALUES (?, ?)",
        ("original_name", "original@example.com"),
    )
    .await?;

    let user_id: Option<u64> = conn.query_first("SELECT LAST_INSERT_ID()").await?;
    let user_id = user_id.unwrap();

    // Now get binlog position AFTER the insert
    let (binlog_file, binlog_pos) = mariadb.get_binlog_position().await?;

    let (tx, mut rx) = mpsc::channel(100);

    let config = MySqlCdcConfig::new(&mariadb.host, "testuser")
        .with_password("testpass")
        .with_port(mariadb.port)
        .with_database("testdb")
        .with_server_id(22348)
        .with_binlog_position(&binlog_file, binlog_pos as u32);

    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Update the user - this should trigger an UPDATE event
    conn.exec_drop(
        "UPDATE users SET username = ? WHERE id = ?",
        ("updated_name", user_id),
    )
    .await?;
    info!("Updated user, waiting for CDC event...");

    // Wait for the UPDATE event
    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive event");

    info!("Received CDC event: {:?}", event);

    assert_eq!(event.op, CdcOp::Update);
    assert_eq!(event.table, "users");

    // With binlog_row_image=FULL, UPDATE should have both before and after states
    assert!(event.before.is_some(), "UPDATE should have before state");
    assert!(event.after.is_some(), "UPDATE should have after state");

    let before = event.before.as_ref().unwrap();
    let after = event.after.as_ref().unwrap();

    // Verify the change
    assert_eq!(
        before.get("username").and_then(|v| v.as_str()),
        Some("original_name")
    );
    assert_eq!(
        after.get("username").and_then(|v| v.as_str()),
        Some("updated_name")
    );

    cdc.stop().await?;
    info!("MariaDB CDC captures UPDATE events test passed");
    Ok(())
}

/// **Core Test**: MySqlCdc captures DELETE events from MariaDB
#[tokio::test]
async fn test_mariadb_cdc_captures_delete_events() -> Result<()> {
    init_tracing();

    let mariadb = TestMariadb::start().await?;
    mariadb.setup_test_schema().await?;

    // Insert a user before starting CDC
    let pool = mariadb.pool();
    let mut conn = pool.get_conn().await?;
    conn.exec_drop(
        "INSERT INTO users (username, email) VALUES (?, ?)",
        ("to_delete", "delete@example.com"),
    )
    .await?;

    let user_id: Option<u64> = conn.query_first("SELECT LAST_INSERT_ID()").await?;
    let user_id = user_id.unwrap();

    let (binlog_file, binlog_pos) = mariadb.get_binlog_position().await?;

    let (tx, mut rx) = mpsc::channel(100);

    let config = MySqlCdcConfig::new(&mariadb.host, "testuser")
        .with_password("testpass")
        .with_port(mariadb.port)
        .with_database("testdb")
        .with_server_id(22349)
        .with_binlog_position(&binlog_file, binlog_pos as u32);

    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Delete the user - this should trigger a DELETE event
    conn.exec_drop("DELETE FROM users WHERE id = ?", (user_id,))
        .await?;
    info!("Deleted user, waiting for CDC event...");

    // Wait for the DELETE event
    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive event");

    info!("Received CDC event: {:?}", event);

    assert_eq!(event.op, CdcOp::Delete);
    assert_eq!(event.table, "users");

    // DELETE should have before state (the deleted row)
    assert!(event.before.is_some(), "DELETE should have before state");

    let before = event.before.as_ref().unwrap();
    assert_eq!(
        before.get("username").and_then(|v| v.as_str()),
        Some("to_delete")
    );

    cdc.stop().await?;
    info!("MariaDB CDC captures DELETE events test passed");
    Ok(())
}

/// **Core Test**: MySqlCdc captures multiple events in order from MariaDB
#[tokio::test]
async fn test_mariadb_cdc_captures_multiple_events_in_order() -> Result<()> {
    init_tracing();

    let mariadb = TestMariadb::start().await?;
    mariadb.setup_test_schema().await?;

    let (binlog_file, binlog_pos) = mariadb.get_binlog_position().await?;

    let (tx, mut rx) = mpsc::channel(100);

    let config = MySqlCdcConfig::new(&mariadb.host, "testuser")
        .with_password("testpass")
        .with_port(mariadb.port)
        .with_database("testdb")
        .with_server_id(22350)
        .with_binlog_position(&binlog_file, binlog_pos as u32);

    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Perform a series of operations
    let pool = mariadb.pool();
    let mut conn = pool.get_conn().await?;

    // 1. INSERT
    conn.exec_drop(
        "INSERT INTO users (username, email) VALUES (?, ?)",
        ("multi_user", "multi@example.com"),
    )
    .await?;

    let user_id: Option<u64> = conn.query_first("SELECT LAST_INSERT_ID()").await?;
    let user_id = user_id.unwrap();

    // 2. UPDATE
    conn.exec_drop(
        "UPDATE users SET username = ? WHERE id = ?",
        ("multi_user_updated", user_id),
    )
    .await?;

    // 3. DELETE
    conn.exec_drop("DELETE FROM users WHERE id = ?", (user_id,))
        .await?;

    info!("Performed INSERT, UPDATE, DELETE sequence");

    // Collect events
    let mut events = Vec::new();
    for _ in 0..3 {
        let event = timeout(Duration::from_secs(10), rx.recv())
            .await?
            .expect("should receive event");
        events.push(event);
    }

    // Verify order and types
    assert_eq!(events[0].op, CdcOp::Insert, "First event should be INSERT");
    assert_eq!(events[1].op, CdcOp::Update, "Second event should be UPDATE");
    assert_eq!(events[2].op, CdcOp::Delete, "Third event should be DELETE");

    info!("Received {} events in correct order", events.len());

    cdc.stop().await?;
    info!("MariaDB CDC captures multiple events test passed");
    Ok(())
}

/// Test CDC with multiple tables from MariaDB
#[tokio::test]
async fn test_mariadb_cdc_multiple_tables() -> Result<()> {
    init_tracing();

    let mariadb = TestMariadb::start().await?;
    mariadb.setup_test_schema().await?;

    let (binlog_file, binlog_pos) = mariadb.get_binlog_position().await?;

    let (tx, mut rx) = mpsc::channel(100);

    let config = MySqlCdcConfig::new(&mariadb.host, "testuser")
        .with_password("testpass")
        .with_port(mariadb.port)
        .with_database("testdb")
        .with_server_id(22351)
        .with_binlog_position(&binlog_file, binlog_pos as u32);

    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Insert into multiple tables
    let pool = mariadb.pool();
    let mut conn = pool.get_conn().await?;

    conn.exec_drop(
        "INSERT INTO users (username, email) VALUES (?, ?)",
        ("multi_table_user", "mt@example.com"),
    )
    .await?;

    let user_id: Option<u64> = conn.query_first("SELECT LAST_INSERT_ID()").await?;
    let user_id = user_id.unwrap();

    conn.exec_drop(
        "INSERT INTO orders (user_id, total_amount, status) VALUES (?, ?, ?)",
        (user_id, 99.99f64, "pending"),
    )
    .await?;

    info!("Inserted into users and orders tables");

    // Collect events from both tables
    let event1 = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive first event");
    let event2 = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive second event");

    let tables: Vec<&str> = vec![&event1.table, &event2.table];
    assert!(
        tables.contains(&"users"),
        "Should capture users table event"
    );
    assert!(
        tables.contains(&"orders"),
        "Should capture orders table event"
    );

    info!("Captured events from tables: {:?}", tables);

    cdc.stop().await?;
    info!("MariaDB CDC multiple tables test passed");
    Ok(())
}

/// Test that CDC connector can restart and resume with MariaDB
#[tokio::test]
async fn test_mariadb_cdc_connector_can_restart() -> Result<()> {
    init_tracing();

    let mariadb = TestMariadb::start().await?;
    mariadb.setup_test_schema().await?;

    let (binlog_file, binlog_pos) = mariadb.get_binlog_position().await?;

    // First run
    let config = MySqlCdcConfig::new(&mariadb.host, "testuser")
        .with_password("testpass")
        .with_port(mariadb.port)
        .with_database("testdb")
        .with_server_id(22353)
        .with_binlog_position(&binlog_file, binlog_pos as u32);

    let mut cdc = MySqlCdc::new(config.clone());
    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    cdc.stop().await?;
    info!("First CDC session completed");

    // Get new binlog position after first session
    let (new_binlog_file, new_binlog_pos) = mariadb.get_binlog_position().await?;

    // Second run from new position
    let (tx, mut rx) = mpsc::channel(100);
    let config2 = MySqlCdcConfig::new(&mariadb.host, "testuser")
        .with_password("testpass")
        .with_port(mariadb.port)
        .with_database("testdb")
        .with_server_id(22354)
        .with_binlog_position(&new_binlog_file, new_binlog_pos as u32);

    let mut cdc2 = MySqlCdc::new(config2).with_event_channel(tx);
    cdc2.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Insert data after reconnection
    mariadb.insert_test_users(1).await?;

    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive event after restart");

    assert_eq!(event.op, CdcOp::Insert);
    info!("CDC connector successfully restarted and captured events");

    cdc2.stop().await?;
    Ok(())
}

// ============================================================================
// Infrastructure Helper Tests
// ============================================================================

/// Test MariaDB GTID tracking
#[tokio::test]
async fn test_mariadb_gtid_tracking() -> Result<()> {
    init_tracing();

    let mariadb = TestMariadb::start().await?;
    mariadb.setup_test_schema().await?;

    // Get initial GTID
    let initial_gtid = mariadb.get_gtid_current_pos().await?;
    info!("Initial GTID current_pos: {}", initial_gtid);

    // Insert data
    mariadb.insert_test_users(5).await?;

    // Get new GTID
    let new_gtid = mariadb.get_gtid_current_pos().await?;
    info!("New GTID current_pos: {}", new_gtid);

    // GTID should have changed (more transactions)
    assert_ne!(initial_gtid, new_gtid);

    info!("MariaDB GTID tracking test passed");
    Ok(())
}

/// Test MariaDB concurrent connections
#[tokio::test]
async fn test_mariadb_concurrent_connections() -> Result<()> {
    init_tracing();

    let mariadb = TestMariadb::start().await?;
    mariadb.setup_test_schema().await?;

    let pool = mariadb.pool();

    // Spawn multiple concurrent operations
    let mut handles = Vec::new();
    for i in 0..10 {
        let pool = pool.clone();
        let handle = tokio::spawn(async move {
            let mut conn = pool.get_conn().await.unwrap();
            conn.exec_drop(
                "INSERT INTO users (username, email) VALUES (?, ?)",
                (
                    format!("concurrent_user_{}", i),
                    format!("concurrent{}@example.com", i),
                ),
            )
            .await
            .unwrap();
        });
        handles.push(handle);
    }

    // Wait for all
    for handle in handles {
        handle.await?;
    }

    // Verify count
    let mut conn = pool.get_conn().await?;
    let count: Option<i64> = conn
        .query_first("SELECT COUNT(*) FROM users WHERE username LIKE 'concurrent_user_%'")
        .await?;
    assert_eq!(count, Some(10));

    info!("MariaDB concurrent connections test passed");
    Ok(())
}

// ============================================================================
// Stress Tests
// ============================================================================

/// Stress test: High-throughput CDC capture from MariaDB
#[tokio::test]
#[ignore = "Long-running stress test"]
async fn stress_test_mariadb_high_throughput_cdc() -> Result<()> {
    init_tracing();

    let mariadb = TestMariadb::start().await?;
    mariadb.setup_test_schema().await?;

    let (binlog_file, binlog_pos) = mariadb.get_binlog_position().await?;

    let (tx, mut rx) = mpsc::channel(1000);

    let config = MySqlCdcConfig::new(&mariadb.host, "testuser")
        .with_password("testpass")
        .with_port(mariadb.port)
        .with_database("testdb")
        .with_server_id(22355)
        .with_binlog_position(&binlog_file, binlog_pos as u32);

    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let num_inserts = 100;
    let start = std::time::Instant::now();

    // Bulk insert
    mariadb.insert_test_users(num_inserts).await?;
    info!("Inserted {} users", num_inserts);

    // Collect all events
    let mut events = Vec::new();
    for _ in 0..num_inserts {
        match timeout(Duration::from_secs(30), rx.recv()).await {
            Ok(Some(event)) => events.push(event),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let duration = start.elapsed();
    let throughput = events.len() as f64 / duration.as_secs_f64();

    info!(
        "Captured {} events in {:?} ({:.1} events/sec)",
        events.len(),
        duration,
        throughput
    );

    assert_eq!(
        events.len(),
        num_inserts,
        "Should capture all insert events"
    );

    cdc.stop().await?;
    Ok(())
}
