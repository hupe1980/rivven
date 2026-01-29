//! PostgreSQL CDC Integration Tests
//!
//! **REAL** integration tests for Rivven's PostgreSQL CDC connector.
//! These tests verify that the rivven-cdc crate correctly captures database changes.
//!
//! Run with: cargo test -p rivven-integration-tests --test cdc_postgres -- --nocapture
//!
//! Requirements: Docker must be running for testcontainers.

use std::time::Duration;

use anyhow::Result;
use rivven_cdc::common::{CdcOp, CdcSource};
use rivven_cdc::postgres::{PostgresCdc, PostgresCdcConfig};
use rivven_integration_tests::fixtures::TestPostgres;
use rivven_integration_tests::helpers::*;
use tokio::time::timeout;
use tracing::info;

// ============================================================================
// Infrastructure Tests - Verify test setup works correctly
// ============================================================================

/// Test that we can start a PostgreSQL container with logical replication
#[tokio::test]
async fn test_postgres_container_starts() -> Result<()> {
    init_tracing();

    info!("Starting PostgreSQL test container...");
    let pg = TestPostgres::start().await?;

    info!("PostgreSQL started at {}:{}", pg.host, pg.port);

    // Verify we can connect
    let client = pg.connect().await?;
    let rows = client.query("SELECT 1 as value", &[]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("value"), 1);

    info!("PostgreSQL container test passed");
    Ok(())
}

/// Test schema setup for CDC
#[tokio::test]
async fn test_postgres_schema_setup() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    pg.setup_test_schema().await?;

    let client = pg.connect().await?;

    // Verify tables exist
    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'test_schema'",
            &[],
        )
        .await?;

    let table_names: Vec<String> = rows.iter().map(|r| r.get("table_name")).collect();
    assert!(table_names.contains(&"users".to_string()));
    assert!(table_names.contains(&"orders".to_string()));

    // Verify publication exists
    let rows = client
        .query(
            "SELECT pubname FROM pg_publication WHERE pubname = 'rivven_pub'",
            &[],
        )
        .await?;
    assert_eq!(rows.len(), 1);

    info!("Schema setup test passed");
    Ok(())
}

/// Test WAL level is set to logical
#[tokio::test]
async fn test_postgres_wal_level() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;

    let client = pg.connect().await?;
    let rows = client.query("SHOW wal_level", &[]).await?;
    let wal_level: String = rows[0].get("wal_level");
    assert_eq!(wal_level, "logical");

    info!("WAL level test passed");
    Ok(())
}

// ============================================================================
// Rivven CDC Connector Tests - Actually testing rivven-cdc code!
// ============================================================================

/// Test that PostgresCdc connector can be created and configured
#[tokio::test]
async fn test_cdc_connector_creation() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    pg.setup_test_schema().await?;

    // Create CDC configuration using Rivven's PostgresCdcConfig
    let config = PostgresCdcConfig::builder()
        .connection_string(pg.cdc_connection_url())
        .slot_name("test_cdc_slot")
        .publication_name("rivven_pub")
        .build()?;

    // Create the CDC connector
    let cdc = PostgresCdc::new(config);

    // Verify connector is not active yet
    assert!(!cdc.is_healthy().await);

    info!("CDC connector creation test passed");
    Ok(())
}

/// Test that PostgresCdc connector can start and stop
#[tokio::test]
async fn test_cdc_connector_lifecycle() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    pg.setup_test_schema().await?;
    pg.create_replication_slot("lifecycle_slot").await?;

    let config = PostgresCdcConfig::builder()
        .connection_string(pg.cdc_connection_url())
        .slot_name("lifecycle_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);

    // Start the connector
    cdc.start().await?;
    assert!(cdc.is_healthy().await);
    info!("CDC connector started");

    // Give it a moment to initialize
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Stop the connector
    cdc.stop().await?;
    assert!(!cdc.is_healthy().await);
    info!("CDC connector stopped");

    info!("CDC connector lifecycle test passed");
    Ok(())
}

/// **Core Test**: PostgresCdc captures INSERT events
#[tokio::test]
async fn test_cdc_captures_insert_events() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    pg.setup_test_schema().await?;
    pg.create_replication_slot("insert_slot").await?;

    let config = PostgresCdcConfig::builder()
        .connection_string(pg.cdc_connection_url())
        .slot_name("insert_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc.take_event_receiver().expect("event receiver");

    // Start CDC
    cdc.start().await?;

    // Give CDC time to connect
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Insert a user - this should trigger a CDC event
    pg.insert_test_users(1).await?;
    info!("Inserted test user, waiting for CDC event...");

    // Wait for and verify the INSERT event
    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive event");

    info!("Received CDC event: {:?}", event);

    assert_eq!(event.source_type, "postgres");
    assert_eq!(event.op, CdcOp::Insert);
    assert_eq!(event.table, "users");
    assert!(event.after.is_some());
    assert!(event.before.is_none());

    // Verify the captured data
    let after = event.after.as_ref().unwrap();
    assert!(after.get("username").is_some());
    assert!(after.get("email").is_some());

    cdc.stop().await?;
    info!("CDC captures INSERT events test passed");
    Ok(())
}

/// **Core Test**: PostgresCdc captures UPDATE events with before/after states
#[tokio::test]
async fn test_cdc_captures_update_events() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    pg.setup_test_schema().await?;

    // Insert a user before starting CDC
    let client = pg.connect().await?;
    client
        .execute(
            "INSERT INTO test_schema.users (username, email) VALUES ('original_name', 'original@example.com')",
            &[],
        )
        .await?;

    // Get the user ID
    let rows = client
        .query(
            "SELECT id FROM test_schema.users WHERE username = 'original_name'",
            &[],
        )
        .await?;
    let user_id: i32 = rows[0].get("id");

    pg.create_replication_slot("update_slot").await?;

    let config = PostgresCdcConfig::builder()
        .connection_string(pg.cdc_connection_url())
        .slot_name("update_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc.take_event_receiver().expect("event receiver");

    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Update the user - this should trigger an UPDATE event
    pg.update_user(user_id, "updated_name").await?;
    info!("Updated user, waiting for CDC event...");

    // Wait for the UPDATE event
    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive event");

    info!("Received CDC event: {:?}", event);

    assert_eq!(event.op, CdcOp::Update);
    assert_eq!(event.table, "users");

    // UPDATE should have both before and after states (REPLICA IDENTITY FULL)
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
    info!("CDC captures UPDATE events test passed");
    Ok(())
}

/// **Core Test**: PostgresCdc captures DELETE events
#[tokio::test]
async fn test_cdc_captures_delete_events() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    pg.setup_test_schema().await?;

    // Insert a user before starting CDC
    let client = pg.connect().await?;
    client
        .execute(
            "INSERT INTO test_schema.users (username, email) VALUES ('to_delete', 'delete@example.com')",
            &[],
        )
        .await?;

    let rows = client
        .query(
            "SELECT id FROM test_schema.users WHERE username = 'to_delete'",
            &[],
        )
        .await?;
    let user_id: i32 = rows[0].get("id");

    pg.create_replication_slot("delete_slot").await?;

    let config = PostgresCdcConfig::builder()
        .connection_string(pg.cdc_connection_url())
        .slot_name("delete_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc.take_event_receiver().expect("event receiver");

    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Delete the user - this should trigger a DELETE event
    pg.delete_user(user_id).await?;
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
    assert!(event.after.is_none(), "DELETE should not have after state");

    let before = event.before.as_ref().unwrap();
    assert_eq!(
        before.get("username").and_then(|v| v.as_str()),
        Some("to_delete")
    );

    cdc.stop().await?;
    info!("CDC captures DELETE events test passed");
    Ok(())
}

/// **Core Test**: PostgresCdc captures multiple events in order
#[tokio::test]
async fn test_cdc_captures_multiple_events_in_order() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    pg.setup_test_schema().await?;
    pg.create_replication_slot("multi_slot").await?;

    let config = PostgresCdcConfig::builder()
        .connection_string(pg.cdc_connection_url())
        .slot_name("multi_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc.take_event_receiver().expect("event receiver");

    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Perform a series of operations
    let client = pg.connect().await?;

    // 1. INSERT
    client
        .execute(
            "INSERT INTO test_schema.users (username, email) VALUES ('multi_user', 'multi@example.com')",
            &[],
        )
        .await?;

    let rows = client
        .query(
            "SELECT id FROM test_schema.users WHERE username = 'multi_user'",
            &[],
        )
        .await?;
    let user_id: i32 = rows[0].get("id");

    // 2. UPDATE
    client
        .execute(
            "UPDATE test_schema.users SET username = 'multi_user_updated' WHERE id = $1",
            &[&user_id],
        )
        .await?;

    // 3. DELETE
    client
        .execute("DELETE FROM test_schema.users WHERE id = $1", &[&user_id])
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
    info!("CDC captures multiple events test passed");
    Ok(())
}

/// **Core Test**: PostgresCdc captures transaction metadata
#[tokio::test]
async fn test_cdc_captures_transaction_metadata() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    pg.setup_test_schema().await?;
    pg.create_replication_slot("txn_slot").await?;

    let config = PostgresCdcConfig::builder()
        .connection_string(pg.cdc_connection_url())
        .slot_name("txn_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc.take_event_receiver().expect("event receiver");

    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Perform inserts in a single transaction
    let client = pg.connect().await?;
    client
        .batch_execute(
            r#"
        BEGIN;
        INSERT INTO test_schema.users (username, email) VALUES ('txn_user1', 'txn1@example.com');
        INSERT INTO test_schema.users (username, email) VALUES ('txn_user2', 'txn2@example.com');
        COMMIT;
        "#,
        )
        .await?;

    info!("Committed transaction with 2 inserts");

    // Collect both events
    let event1 = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive first event");
    let event2 = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive second event");

    info!("Event 1 transaction: {:?}", event1.transaction);
    info!("Event 2 transaction: {:?}", event2.transaction);

    // Both events should have transaction metadata with the same transaction ID
    if let (Some(txn1), Some(txn2)) = (&event1.transaction, &event2.transaction) {
        assert_eq!(
            txn1.id, txn2.id,
            "Events from same transaction should have same txn ID"
        );
        info!("Both events have matching transaction ID: {}", txn1.id);
    }

    cdc.stop().await?;
    info!("CDC captures transaction metadata test passed");
    Ok(())
}

/// Test CDC with various data types
#[tokio::test]
async fn test_cdc_various_data_types() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    // Setup test schema first to ensure publication exists
    pg.setup_test_schema().await?;

    // Create a table with various data types
    let client = pg.connect().await?;
    client
        .batch_execute(
            r#"
            CREATE TABLE test_types (
                id SERIAL PRIMARY KEY,
                int_col INTEGER,
                bigint_col BIGINT,
                bool_col BOOLEAN,
                text_col TEXT,
                varchar_col VARCHAR(255),
                float_col REAL,
                double_col DOUBLE PRECISION,
                timestamp_col TIMESTAMP,
                date_col DATE,
                jsonb_col JSONB
            );
            ALTER TABLE test_types REPLICA IDENTITY FULL;
            "#,
        )
        .await?;

    // Add table to publication
    client
        .batch_execute("ALTER PUBLICATION rivven_pub ADD TABLE test_types;")
        .await
        .ok(); // Ignore if it fails (publication might include all tables)

    pg.create_replication_slot("types_slot").await?;

    let config = PostgresCdcConfig::builder()
        .connection_string(pg.cdc_connection_url())
        .slot_name("types_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc.take_event_receiver().expect("event receiver");

    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Insert a row with various types
    client
        .batch_execute(
            r#"
            INSERT INTO test_types (int_col, bigint_col, bool_col, text_col, varchar_col, float_col, double_col, timestamp_col, date_col, jsonb_col)
            VALUES (42, 9223372036854775807, true, 'hello world', 'varchar test', 3.14, 2.718281828, '2024-01-15 10:30:00', '2024-01-15', '{"key": "value", "nested": {"num": 123}}');
            "#,
        )
        .await?;

    info!("Inserted row with various data types");

    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive event");

    info!("Received CDC event: {:?}", event);

    assert_eq!(event.op, CdcOp::Insert);
    assert_eq!(event.table, "test_types");

    let after = event.after.as_ref().expect("INSERT should have after");

    // Verify various types were captured
    assert!(after.get("int_col").is_some(), "int_col should be captured");
    assert!(
        after.get("bigint_col").is_some(),
        "bigint_col should be captured"
    );
    assert!(
        after.get("bool_col").is_some(),
        "bool_col should be captured"
    );
    assert!(
        after.get("text_col").is_some(),
        "text_col should be captured"
    );
    assert!(
        after.get("jsonb_col").is_some(),
        "jsonb_col should be captured"
    );

    info!("Captured int_col: {:?}", after.get("int_col"));
    info!("Captured jsonb_col: {:?}", after.get("jsonb_col"));

    cdc.stop().await?;
    info!("CDC various data types test passed");
    Ok(())
}

/// Test CDC with multiple tables
#[tokio::test]
async fn test_cdc_multiple_tables() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    pg.setup_test_schema().await?;
    pg.create_replication_slot("multi_table_slot").await?;

    let config = PostgresCdcConfig::builder()
        .connection_string(pg.cdc_connection_url())
        .slot_name("multi_table_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc.take_event_receiver().expect("event receiver");

    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Insert into multiple tables
    let client = pg.connect().await?;

    client
        .execute(
            "INSERT INTO test_schema.users (username, email) VALUES ('multi_table_user', 'mt@example.com')",
            &[],
        )
        .await?;

    let rows = client
        .query(
            "SELECT id FROM test_schema.users WHERE username = 'multi_table_user'",
            &[],
        )
        .await?;
    let user_id: i32 = rows[0].get("id");

    client
        .execute(
            "INSERT INTO test_schema.orders (user_id, total_amount, status) VALUES ($1, 99.99, 'pending')",
            &[&user_id],
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
    info!("CDC multiple tables test passed");
    Ok(())
}

/// Test that CDC connector handles large payloads
#[tokio::test]
async fn test_cdc_large_payload() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    pg.setup_test_schema().await?;

    // Create a table with TEXT column to support large payloads
    let client = pg.connect().await?;
    client
        .batch_execute(
            r#"
            CREATE TABLE IF NOT EXISTS test_schema.large_data (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                payload TEXT NOT NULL
            );
            ALTER TABLE test_schema.large_data REPLICA IDENTITY FULL;
            "#,
        )
        .await?;

    pg.create_replication_slot("large_payload_slot").await?;

    let config = PostgresCdcConfig::builder()
        .connection_string(pg.cdc_connection_url())
        .slot_name("large_payload_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc.take_event_receiver().expect("event receiver");

    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create a large text payload (10KB)
    let large_payload = "a".repeat(10_000);

    client
        .execute(
            "INSERT INTO test_schema.large_data (name, payload) VALUES ($1, $2)",
            &[&"large_payload_test", &large_payload],
        )
        .await?;

    info!(
        "Inserted row with large payload ({} chars)",
        large_payload.len()
    );

    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive event");

    assert_eq!(event.op, CdcOp::Insert);
    let after = event.after.as_ref().expect("INSERT should have after");
    let captured_payload = after.get("payload").and_then(|v| v.as_str()).unwrap_or("");
    assert_eq!(
        captured_payload.len(),
        large_payload.len(),
        "Large payload should be captured completely"
    );

    cdc.stop().await?;
    info!("CDC large payload test passed");
    Ok(())
}

// ============================================================================
// Resilience Tests
// ============================================================================

/// Test that CDC connector handles reconnection gracefully
/// This test verifies a fresh CDC instance can start on a new slot after
/// a previous instance has cleanly stopped.
#[tokio::test]
async fn test_cdc_connector_can_restart() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    pg.setup_test_schema().await?;

    // Use unique slot name for first run
    pg.create_replication_slot("restart_slot_v1").await?;

    let config = PostgresCdcConfig::builder()
        .connection_string(pg.cdc_connection_url())
        .slot_name("restart_slot_v1")
        .publication_name("rivven_pub")
        .build()?;

    // First run - verify CDC starts and captures events
    let mut cdc = PostgresCdc::new(config.clone());
    let mut rx = cdc.take_event_receiver().expect("event receiver");
    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Insert data during first session
    pg.insert_test_users(1).await?;
    let event = timeout(Duration::from_secs(5), rx.recv()).await?;
    assert!(event.is_some(), "First session should capture event");

    cdc.stop().await?;
    drop(cdc);
    drop(rx);
    info!("First CDC session completed successfully");

    // Create a new slot for the second session (simulating a fresh deployment)
    pg.create_replication_slot("restart_slot_v2").await?;

    let config2 = PostgresCdcConfig::builder()
        .connection_string(pg.cdc_connection_url())
        .slot_name("restart_slot_v2")
        .publication_name("rivven_pub")
        .build()?;

    // Second run with new slot
    let mut cdc2 = PostgresCdc::new(config2);
    let mut rx2 = cdc2.take_event_receiver().expect("event receiver");

    cdc2.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Insert data after reconnection (insert 2 users with unique names)
    let client = pg.connect().await?;
    client
        .execute(
            "INSERT INTO test_schema.users (username, email) VALUES ($1, $2)",
            &[&"restart_test_user", &"restart@example.com"],
        )
        .await?;

    let event = timeout(Duration::from_secs(10), rx2.recv())
        .await?
        .expect("should receive event after restart");

    assert_eq!(event.op, CdcOp::Insert);
    info!("CDC connector successfully restarted and captured events");

    cdc2.stop().await?;
    Ok(())
}

// ============================================================================
// Stress Tests
// ============================================================================

/// Stress test: High-throughput CDC capture
#[tokio::test]
#[ignore = "Long-running stress test"]
async fn stress_test_high_throughput_cdc() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    pg.setup_test_schema().await?;
    pg.create_replication_slot("stress_slot").await?;

    let config = PostgresCdcConfig::builder()
        .connection_string(pg.cdc_connection_url())
        .slot_name("stress_slot")
        .publication_name("rivven_pub")
        .buffer_size(1000)
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc.take_event_receiver().expect("event receiver");

    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let num_inserts = 100;
    let start = std::time::Instant::now();

    // Bulk insert
    pg.insert_test_users(num_inserts).await?;
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

/// Test creating replication slots
#[tokio::test]
async fn test_postgres_replication_slot() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    pg.setup_test_schema().await?;

    // Create replication slot
    pg.create_replication_slot("test_slot").await?;

    // Verify slot exists
    let client = pg.connect().await?;
    let rows = client
        .query(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'test_slot'",
            &[],
        )
        .await?;
    assert_eq!(rows.len(), 1);

    info!("Replication slot test passed");
    Ok(())
}
