//! MySQL CDC Integration Tests
//!
//! **REAL** integration tests for Rivven's MySQL CDC connector.
//! These tests verify that the rivven-cdc crate correctly captures database changes
//! via MySQL binary logging.
//!
//! Run with: cargo test -p rivven-integration-tests --test cdc_mysql -- --nocapture
//!
//! Requirements: Docker must be running for testcontainers.

use std::time::Duration;

use anyhow::Result;
use mysql_async::prelude::Queryable;
use rivven_cdc::common::{CdcOp, CdcSource};
use rivven_cdc::mysql::{MySqlCdc, MySqlCdcConfig};
use rivven_integration_tests::fixtures::TestMysql;
use rivven_integration_tests::helpers::*;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::info;

// ============================================================================
// Infrastructure Tests - Verify test setup works correctly
// ============================================================================

/// Test that we can start a MySQL container with binary logging enabled
#[tokio::test]
async fn test_mysql_container_starts() -> Result<()> {
    init_tracing();

    info!("Starting MySQL test container...");
    let mysql = TestMysql::start().await?;

    info!("MySQL started on {}:{}", mysql.host, mysql.port);
    assert!(!mysql.host.is_empty());
    assert!(mysql.port > 0);

    // Verify connection works
    let pool = mysql.pool();
    let mut conn = pool.get_conn().await?;
    let result: Option<i32> = conn.query_first("SELECT 1").await?;
    assert_eq!(result, Some(1));

    info!("MySQL container test passed");
    Ok(())
}

/// Test MySQL container creates test schema successfully
#[tokio::test]
async fn test_mysql_creates_test_schema() -> Result<()> {
    init_tracing();

    let mysql = TestMysql::start().await?;
    mysql.setup_test_schema().await?;

    // Verify tables exist
    let pool = mysql.pool();
    let mut conn = pool.get_conn().await?;

    let tables: Vec<String> = conn.query("SHOW TABLES").await?;

    assert!(tables.iter().any(|t| t == "users"));
    assert!(tables.iter().any(|t| t == "orders"));

    info!("MySQL schema test passed");
    Ok(())
}

/// Test MySQL binary log is enabled and configured correctly
#[tokio::test]
async fn test_mysql_binlog_enabled() -> Result<()> {
    init_tracing();

    let mysql = TestMysql::start().await?;

    let pool = mysql.pool();
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

    info!("MySQL binary log configuration test passed");
    Ok(())
}

/// Test MySQL GTID mode is enabled
#[tokio::test]
async fn test_mysql_gtid_enabled() -> Result<()> {
    init_tracing();

    let mysql = TestMysql::start().await?;

    let pool = mysql.pool();
    let mut conn = pool.get_conn().await?;

    // Check GTID mode
    let gtid_mode: Option<String> = conn.query_first("SELECT @@gtid_mode").await?;
    assert_eq!(gtid_mode, Some("ON".to_string()));

    // Check enforce_gtid_consistency
    let enforce: Option<String> = conn
        .query_first("SELECT @@enforce_gtid_consistency")
        .await?;
    assert_eq!(enforce, Some("ON".to_string()));

    info!("MySQL GTID configuration test passed");
    Ok(())
}

// ============================================================================
// Rivven MySQL CDC Connector Tests - Actually testing rivven-cdc code!
// ============================================================================

/// Test that MySqlCdc connector can be created and configured
#[tokio::test]
async fn test_mysql_cdc_connector_creation() -> Result<()> {
    init_tracing();

    let mysql = TestMysql::start().await?;
    mysql.setup_test_schema().await?;

    // Create CDC configuration using Rivven's MySqlCdcConfig
    let config = MySqlCdcConfig::new(&mysql.host, "testuser")
        .with_password("testpass")
        .with_port(mysql.port)
        .with_database("testdb")
        .with_server_id(12345);

    // Create the CDC connector
    let cdc = MySqlCdc::new(config);

    // Verify connector is not active yet
    assert!(!cdc.is_healthy().await);

    info!("MySQL CDC connector creation test passed");
    Ok(())
}

/// Test that MySqlCdc connector can start and stop
#[tokio::test]
async fn test_mysql_cdc_connector_lifecycle() -> Result<()> {
    init_tracing();

    let mysql = TestMysql::start().await?;
    mysql.setup_test_schema().await?;

    let (binlog_file, binlog_pos) = mysql.get_binlog_position().await?;

    let config = MySqlCdcConfig::new(&mysql.host, "testuser")
        .with_password("testpass")
        .with_port(mysql.port)
        .with_database("testdb")
        .with_server_id(12346)
        .with_binlog_position(&binlog_file, binlog_pos as u32);

    let mut cdc = MySqlCdc::new(config);

    // Start the connector
    cdc.start().await?;
    assert!(cdc.is_healthy().await);
    info!("MySQL CDC connector started");

    // Give it a moment to initialize
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Stop the connector
    cdc.stop().await?;
    assert!(!cdc.is_healthy().await);
    info!("MySQL CDC connector stopped");

    info!("MySQL CDC connector lifecycle test passed");
    Ok(())
}

/// **Core Test**: MySqlCdc captures INSERT events
#[tokio::test]
async fn test_mysql_cdc_captures_insert_events() -> Result<()> {
    init_tracing();

    let mysql = TestMysql::start().await?;
    mysql.setup_test_schema().await?;

    let (binlog_file, binlog_pos) = mysql.get_binlog_position().await?;
    info!(
        "Starting CDC from binlog position: {}:{}",
        binlog_file, binlog_pos
    );

    // Create event channel
    let (tx, mut rx) = mpsc::channel(100);

    let config = MySqlCdcConfig::new(&mysql.host, "testuser")
        .with_password("testpass")
        .with_port(mysql.port)
        .with_database("testdb")
        .with_server_id(12347)
        .with_binlog_position(&binlog_file, binlog_pos as u32);

    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    // Start CDC
    cdc.start().await?;

    // Give CDC time to connect
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Insert a user - this should trigger a CDC event
    mysql.insert_test_users(1).await?;
    info!("Inserted test user, waiting for CDC event...");

    // Wait for and verify the INSERT event
    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive event");

    info!("Received CDC event: {:?}", event);

    assert_eq!(event.source_type, "mysql");
    assert_eq!(event.op, CdcOp::Insert);
    assert_eq!(event.table, "users");
    assert!(event.after.is_some());

    // Verify the captured data
    let after = event.after.as_ref().unwrap();
    assert!(after.get("username").is_some());
    assert!(after.get("email").is_some());

    cdc.stop().await?;
    info!("MySQL CDC captures INSERT events test passed");
    Ok(())
}

/// **Core Test**: MySqlCdc captures UPDATE events with before/after states
#[tokio::test]
async fn test_mysql_cdc_captures_update_events() -> Result<()> {
    init_tracing();

    let mysql = TestMysql::start().await?;
    mysql.setup_test_schema().await?;

    // Insert a user before starting CDC
    let pool = mysql.pool();
    let mut conn = pool.get_conn().await?;
    conn.exec_drop(
        "INSERT INTO users (username, email) VALUES (?, ?)",
        ("original_name", "original@example.com"),
    )
    .await?;

    let user_id: Option<u64> = conn.query_first("SELECT LAST_INSERT_ID()").await?;
    let user_id = user_id.unwrap();

    // Now get binlog position AFTER the insert
    let (binlog_file, binlog_pos) = mysql.get_binlog_position().await?;

    let (tx, mut rx) = mpsc::channel(100);

    let config = MySqlCdcConfig::new(&mysql.host, "testuser")
        .with_password("testpass")
        .with_port(mysql.port)
        .with_database("testdb")
        .with_server_id(12348)
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
    info!("MySQL CDC captures UPDATE events test passed");
    Ok(())
}

/// **Core Test**: MySqlCdc captures DELETE events
#[tokio::test]
async fn test_mysql_cdc_captures_delete_events() -> Result<()> {
    init_tracing();

    let mysql = TestMysql::start().await?;
    mysql.setup_test_schema().await?;

    // Insert a user before starting CDC
    let pool = mysql.pool();
    let mut conn = pool.get_conn().await?;
    conn.exec_drop(
        "INSERT INTO users (username, email) VALUES (?, ?)",
        ("to_delete", "delete@example.com"),
    )
    .await?;

    let user_id: Option<u64> = conn.query_first("SELECT LAST_INSERT_ID()").await?;
    let user_id = user_id.unwrap();

    let (binlog_file, binlog_pos) = mysql.get_binlog_position().await?;

    let (tx, mut rx) = mpsc::channel(100);

    let config = MySqlCdcConfig::new(&mysql.host, "testuser")
        .with_password("testpass")
        .with_port(mysql.port)
        .with_database("testdb")
        .with_server_id(12349)
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
    info!("MySQL CDC captures DELETE events test passed");
    Ok(())
}

/// **Core Test**: MySqlCdc captures multiple events in order
#[tokio::test]
async fn test_mysql_cdc_captures_multiple_events_in_order() -> Result<()> {
    init_tracing();

    let mysql = TestMysql::start().await?;
    mysql.setup_test_schema().await?;

    let (binlog_file, binlog_pos) = mysql.get_binlog_position().await?;

    let (tx, mut rx) = mpsc::channel(100);

    let config = MySqlCdcConfig::new(&mysql.host, "testuser")
        .with_password("testpass")
        .with_port(mysql.port)
        .with_database("testdb")
        .with_server_id(12350)
        .with_binlog_position(&binlog_file, binlog_pos as u32);

    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Perform a series of operations
    let pool = mysql.pool();
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
    info!("MySQL CDC captures multiple events test passed");
    Ok(())
}

/// Test CDC with multiple tables
#[tokio::test]
async fn test_mysql_cdc_multiple_tables() -> Result<()> {
    init_tracing();

    let mysql = TestMysql::start().await?;
    mysql.setup_test_schema().await?;

    let (binlog_file, binlog_pos) = mysql.get_binlog_position().await?;

    let (tx, mut rx) = mpsc::channel(100);

    let config = MySqlCdcConfig::new(&mysql.host, "testuser")
        .with_password("testpass")
        .with_port(mysql.port)
        .with_database("testdb")
        .with_server_id(12351)
        .with_binlog_position(&binlog_file, binlog_pos as u32);

    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Insert into multiple tables
    let pool = mysql.pool();
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
    info!("MySQL CDC multiple tables test passed");
    Ok(())
}

/// Test CDC with include table filter
#[tokio::test]
async fn test_mysql_cdc_table_filtering() -> Result<()> {
    init_tracing();

    let mysql = TestMysql::start().await?;
    mysql.setup_test_schema().await?;

    let (binlog_file, binlog_pos) = mysql.get_binlog_position().await?;

    let (tx, mut rx) = mpsc::channel(100);

    // Only capture users table, exclude orders
    let config = MySqlCdcConfig::new(&mysql.host, "testuser")
        .with_password("testpass")
        .with_port(mysql.port)
        .with_database("testdb")
        .with_server_id(12352)
        .with_binlog_position(&binlog_file, binlog_pos as u32)
        .include_table("testdb.users");

    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Insert into both tables
    let pool = mysql.pool();
    let mut conn = pool.get_conn().await?;

    conn.exec_drop(
        "INSERT INTO users (username, email) VALUES (?, ?)",
        ("filter_test_user", "filter@example.com"),
    )
    .await?;

    let user_id: Option<u64> = conn.query_first("SELECT LAST_INSERT_ID()").await?;
    let user_id = user_id.unwrap();

    conn.exec_drop(
        "INSERT INTO orders (user_id, total_amount, status) VALUES (?, ?, ?)",
        (user_id, 50.00f64, "pending"),
    )
    .await?;

    info!("Inserted into both tables, but only users should be captured");

    // Should only receive the users insert (orders is filtered out)
    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive event from users table");

    assert_eq!(event.table, "users", "Should only capture users table");

    // Try to get another event - should timeout (orders filtered)
    let result = timeout(Duration::from_secs(2), rx.recv()).await;
    assert!(result.is_err(), "Should not receive orders table event");

    cdc.stop().await?;
    info!("MySQL CDC table filtering test passed");
    Ok(())
}

/// Test that CDC connector can restart and resume
#[tokio::test]
async fn test_mysql_cdc_connector_can_restart() -> Result<()> {
    init_tracing();

    let mysql = TestMysql::start().await?;
    mysql.setup_test_schema().await?;

    let (binlog_file, binlog_pos) = mysql.get_binlog_position().await?;

    // First run
    let config = MySqlCdcConfig::new(&mysql.host, "testuser")
        .with_password("testpass")
        .with_port(mysql.port)
        .with_database("testdb")
        .with_server_id(12353)
        .with_binlog_position(&binlog_file, binlog_pos as u32);

    let mut cdc = MySqlCdc::new(config.clone());
    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    cdc.stop().await?;
    info!("First CDC session completed");

    // Get new binlog position after first session
    let (new_binlog_file, new_binlog_pos) = mysql.get_binlog_position().await?;

    // Second run from new position
    let (tx, mut rx) = mpsc::channel(100);
    let config2 = MySqlCdcConfig::new(&mysql.host, "testuser")
        .with_password("testpass")
        .with_port(mysql.port)
        .with_database("testdb")
        .with_server_id(12354)
        .with_binlog_position(&new_binlog_file, new_binlog_pos as u32);

    let mut cdc2 = MySqlCdc::new(config2).with_event_channel(tx);
    cdc2.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Insert data after reconnection
    mysql.insert_test_users(1).await?;

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

/// Test MySQL GTID tracking
#[tokio::test]
async fn test_mysql_gtid_tracking() -> Result<()> {
    init_tracing();

    let mysql = TestMysql::start().await?;
    mysql.setup_test_schema().await?;

    // Get initial GTID
    let initial_gtid = mysql.get_gtid_executed().await?;
    info!("Initial GTID executed: {}", initial_gtid);

    // Insert data
    mysql.insert_test_users(5).await?;

    // Get new GTID
    let new_gtid = mysql.get_gtid_executed().await?;
    info!("New GTID executed: {}", new_gtid);

    // GTID should have changed (more transactions)
    assert_ne!(initial_gtid, new_gtid);

    info!("MySQL GTID tracking test passed");
    Ok(())
}

/// Test MySQL concurrent connections
#[tokio::test]
async fn test_mysql_concurrent_connections() -> Result<()> {
    init_tracing();

    let mysql = TestMysql::start().await?;
    mysql.setup_test_schema().await?;

    let pool = mysql.pool();

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

    info!("MySQL concurrent connections test passed");
    Ok(())
}

// ============================================================================
// Stress Tests
// ============================================================================

/// Stress test: High-throughput CDC capture
#[tokio::test]
#[ignore = "Long-running stress test"]
async fn stress_test_mysql_high_throughput_cdc() -> Result<()> {
    init_tracing();

    let mysql = TestMysql::start().await?;
    mysql.setup_test_schema().await?;

    let (binlog_file, binlog_pos) = mysql.get_binlog_position().await?;

    let (tx, mut rx) = mpsc::channel(1000);

    let config = MySqlCdcConfig::new(&mysql.host, "testuser")
        .with_password("testpass")
        .with_port(mysql.port)
        .with_database("testdb")
        .with_server_id(12355)
        .with_binlog_position(&binlog_file, binlog_pos as u32);

    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let num_inserts = 100;
    let start = std::time::Instant::now();

    // Bulk insert
    mysql.insert_test_users(num_inserts).await?;
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
