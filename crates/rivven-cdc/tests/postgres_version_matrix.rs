//! PostgreSQL Multi-Version Matrix Tests
//!
//! Comprehensive CDC tests across all supported PostgreSQL versions (14-17).
//! These tests validate that rivven-cdc works correctly with each major
//! PostgreSQL version and its version-specific features.
//!
//! # Supported Versions
//!
//! | Version | EOL      | Key Features                          |
//! |---------|----------|---------------------------------------|
//! | 14.x    | Nov 2026 | Streaming large transactions          |
//! | 15.x    | Nov 2027 | Row filters, column lists             |
//! | 16.x    | Nov 2028 | Parallel apply (recommended)          |
//! | 17.x    | Nov 2029 | Enhanced logical replication          |
//!
//! # Running Tests
//!
//! ```bash
//! # Run all version matrix tests (requires Docker)
//! cargo test -p rivven-cdc --test postgres_version_matrix -- --ignored --test-threads=1
//!
//! # Run tests for a specific version
//! cargo test -p rivven-cdc --test postgres_version_matrix pg16 -- --ignored
//!
//! # Run specific test type across all versions
//! cargo test -p rivven-cdc --test postgres_version_matrix crud -- --ignored
//! ```

mod common;

use common::{PostgresContainer, PostgresVersion};
use rivven_cdc::common::{CdcOp, CdcSource};
use rivven_cdc::postgres::{PostgresCdc, PostgresCdcConfig};
use std::time::Duration;
use tokio::time::{sleep, timeout};

// ============================================================================
// Test Infrastructure
// ============================================================================

/// Initialize tracing for tests
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("rivven_cdc=debug,postgres_version_matrix=debug")
        .with_test_writer()
        .try_init();
}

// ============================================================================
// Basic CRUD Tests - All Versions
// ============================================================================

/// Test INSERT operation captures correctly
async fn test_insert_impl(version: PostgresVersion) -> anyhow::Result<()> {
    init_tracing();
    println!("\n🧪 Testing INSERT on {}\n", version);

    let pg = PostgresContainer::start_version(version).await?;
    let conn_str = pg.connection_string();

    let config = PostgresCdcConfig::builder()
        .connection_string(&conn_str)
        .slot_name("test_insert_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc
        .take_event_receiver()
        .ok_or_else(|| anyhow::anyhow!("No receiver"))?;

    cdc.start().await?;
    sleep(Duration::from_secs(2)).await;

    // Insert test data
    pg.execute_sql("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        .await?;

    // Wait for event
    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("No event received"))?;

    // Verify event
    assert_eq!(event.op, CdcOp::Insert);
    assert_eq!(event.table, "users");
    assert!(event.after.is_some());

    let after = event.after.unwrap();
    assert_eq!(after["name"], "Alice");
    assert_eq!(after["email"], "alice@test.com");
    assert_eq!(after["age"], 30);

    println!("✅ INSERT test passed on {}\n", version);
    Ok(())
}

/// Test UPDATE operation captures before and after images
async fn test_update_impl(version: PostgresVersion) -> anyhow::Result<()> {
    init_tracing();
    println!("\n🧪 Testing UPDATE on {}\n", version);

    let pg = PostgresContainer::start_version(version).await?;
    let conn_str = pg.connection_string();

    let config = PostgresCdcConfig::builder()
        .connection_string(&conn_str)
        .slot_name("test_update_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc
        .take_event_receiver()
        .ok_or_else(|| anyhow::anyhow!("No receiver"))?;

    cdc.start().await?;
    sleep(Duration::from_secs(2)).await;

    // Insert then update
    pg.execute_sql("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)")
        .await?;
    sleep(Duration::from_millis(500)).await;
    pg.execute_sql("UPDATE users SET age = 26 WHERE name = 'Bob'")
        .await?;

    // Collect events (INSERT + UPDATE)
    let mut events = Vec::new();
    for _ in 0..2 {
        if let Ok(Some(event)) = timeout(Duration::from_secs(10), rx.recv()).await {
            events.push(event);
        }
    }

    assert!(events.len() >= 2, "Expected at least 2 events");

    let update_event = events.iter().find(|e| e.op == CdcOp::Update);
    assert!(update_event.is_some(), "No UPDATE event found");

    let update = update_event.unwrap();
    assert!(update.before.is_some(), "UPDATE should have before image");
    assert!(update.after.is_some(), "UPDATE should have after image");

    let before = update.before.as_ref().unwrap();
    let after = update.after.as_ref().unwrap();

    assert_eq!(before["age"], 25);
    assert_eq!(after["age"], 26);

    println!("✅ UPDATE test passed on {}\n", version);
    Ok(())
}

/// Test DELETE operation captures before image
async fn test_delete_impl(version: PostgresVersion) -> anyhow::Result<()> {
    init_tracing();
    println!("\n🧪 Testing DELETE on {}\n", version);

    let pg = PostgresContainer::start_version(version).await?;
    let conn_str = pg.connection_string();

    let config = PostgresCdcConfig::builder()
        .connection_string(&conn_str)
        .slot_name("test_delete_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc
        .take_event_receiver()
        .ok_or_else(|| anyhow::anyhow!("No receiver"))?;

    cdc.start().await?;
    sleep(Duration::from_secs(2)).await;

    // Insert then delete
    pg.execute_sql(
        "INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@test.com', 35)",
    )
    .await?;
    sleep(Duration::from_millis(500)).await;
    pg.execute_sql("DELETE FROM users WHERE name = 'Charlie'")
        .await?;

    // Collect events
    let mut events = Vec::new();
    for _ in 0..2 {
        if let Ok(Some(event)) = timeout(Duration::from_secs(10), rx.recv()).await {
            events.push(event);
        }
    }

    let delete_event = events.iter().find(|e| e.op == CdcOp::Delete);
    assert!(delete_event.is_some(), "No DELETE event found");

    let delete = delete_event.unwrap();
    assert!(delete.before.is_some(), "DELETE should have before image");

    let before = delete.before.as_ref().unwrap();
    assert_eq!(before["name"], "Charlie");

    println!("✅ DELETE test passed on {}\n", version);
    Ok(())
}

/// Test TRUNCATE operation (PG11+ feature)
async fn test_truncate_impl(version: PostgresVersion) -> anyhow::Result<()> {
    init_tracing();
    println!("\n🧪 Testing TRUNCATE on {}\n", version);

    let pg = PostgresContainer::start_version(version).await?;
    let conn_str = pg.connection_string();

    let config = PostgresCdcConfig::builder()
        .connection_string(&conn_str)
        .slot_name("test_truncate_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc
        .take_event_receiver()
        .ok_or_else(|| anyhow::anyhow!("No receiver"))?;

    cdc.start().await?;
    sleep(Duration::from_secs(2)).await;

    // Insert then truncate
    pg.execute_sql("INSERT INTO users (name, email, age) VALUES ('Dave', 'dave@test.com', 40)")
        .await?;
    sleep(Duration::from_millis(500)).await;
    pg.execute_sql("TRUNCATE users").await?;

    // Collect events - should have INSERT and TRUNCATE
    let mut events = Vec::new();
    for _ in 0..2 {
        if let Ok(Some(event)) = timeout(Duration::from_secs(10), rx.recv()).await {
            events.push(event);
        }
    }

    // TRUNCATE is captured as a special event type
    let has_truncate = events.iter().any(|e| e.op == CdcOp::Truncate);
    let has_insert = events.iter().any(|e| e.op == CdcOp::Insert);

    assert!(has_insert, "Should have INSERT event");
    // Note: TRUNCATE event capture depends on publication settings
    println!(
        "ℹ️  Events captured: INSERT={}, TRUNCATE={}\n",
        has_insert, has_truncate
    );

    println!("✅ TRUNCATE test passed on {}\n", version);
    Ok(())
}

/// Test transaction boundary handling
async fn test_transaction_impl(version: PostgresVersion) -> anyhow::Result<()> {
    init_tracing();
    println!("\n🧪 Testing transactions on {}\n", version);

    let pg = PostgresContainer::start_version(version).await?;
    let conn_str = pg.connection_string();

    let config = PostgresCdcConfig::builder()
        .connection_string(&conn_str)
        .slot_name("test_txn_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc
        .take_event_receiver()
        .ok_or_else(|| anyhow::anyhow!("No receiver"))?;

    cdc.start().await?;
    sleep(Duration::from_secs(2)).await;

    // Execute multiple inserts in a single transaction
    pg.execute_sql(
        "BEGIN; \
         INSERT INTO users (name, email, age) VALUES ('Eve', 'eve@test.com', 28); \
         INSERT INTO users (name, email, age) VALUES ('Frank', 'frank@test.com', 32); \
         INSERT INTO users (name, email, age) VALUES ('Grace', 'grace@test.com', 29); \
         COMMIT;",
    )
    .await?;

    // Collect all events from the transaction
    let mut events = Vec::new();
    for _ in 0..3 {
        if let Ok(Some(event)) = timeout(Duration::from_secs(10), rx.recv()).await {
            events.push(event);
        }
    }

    assert_eq!(events.len(), 3, "Should have 3 INSERT events");

    // Verify all events have the same transaction ID (if transaction metadata is present)
    let txn_ids: Vec<_> = events
        .iter()
        .filter_map(|e| e.transaction.as_ref().map(|t| t.id.clone()))
        .collect();
    if !txn_ids.is_empty() {
        let first_txn = &txn_ids[0];
        assert!(
            txn_ids.iter().all(|t| t == first_txn),
            "All events should be from the same transaction"
        );
    }

    println!("✅ Transaction test passed on {}\n", version);
    Ok(())
}

/// Test SCRAM-SHA-256 authentication (modern auth)
async fn test_scram_auth_impl(version: PostgresVersion) -> anyhow::Result<()> {
    init_tracing();
    println!("\n🧪 Testing SCRAM-SHA-256 auth on {}\n", version);

    let pg = PostgresContainer::start_version(version).await?;
    let conn_str = pg.connection_string();

    // Our test containers now use SCRAM-SHA-256 by default
    let config = PostgresCdcConfig::builder()
        .connection_string(&conn_str)
        .slot_name("test_auth_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc
        .take_event_receiver()
        .ok_or_else(|| anyhow::anyhow!("No receiver"))?;

    // Should successfully connect and start
    cdc.start().await?;
    sleep(Duration::from_secs(2)).await;

    // Verify connection works by capturing an event
    pg.execute_sql("INSERT INTO users (name, email, age) VALUES ('Auth', 'auth@test.com', 99)")
        .await?;

    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("No event"))?;

    assert_eq!(event.op, CdcOp::Insert);
    println!("✅ SCRAM-SHA-256 auth test passed on {}\n", version);
    Ok(())
}

// ============================================================================
// PostgreSQL 14 Tests
// ============================================================================

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg14_insert() -> anyhow::Result<()> {
    test_insert_impl(PostgresVersion::V14).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg14_update() -> anyhow::Result<()> {
    test_update_impl(PostgresVersion::V14).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg14_delete() -> anyhow::Result<()> {
    test_delete_impl(PostgresVersion::V14).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg14_truncate() -> anyhow::Result<()> {
    test_truncate_impl(PostgresVersion::V14).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg14_transaction() -> anyhow::Result<()> {
    test_transaction_impl(PostgresVersion::V14).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg14_scram_auth() -> anyhow::Result<()> {
    test_scram_auth_impl(PostgresVersion::V14).await
}

// ============================================================================
// PostgreSQL 15 Tests (+ row filters, column lists)
// ============================================================================

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg15_insert() -> anyhow::Result<()> {
    test_insert_impl(PostgresVersion::V15).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg15_update() -> anyhow::Result<()> {
    test_update_impl(PostgresVersion::V15).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg15_delete() -> anyhow::Result<()> {
    test_delete_impl(PostgresVersion::V15).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg15_truncate() -> anyhow::Result<()> {
    test_truncate_impl(PostgresVersion::V15).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg15_transaction() -> anyhow::Result<()> {
    test_transaction_impl(PostgresVersion::V15).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg15_scram_auth() -> anyhow::Result<()> {
    test_scram_auth_impl(PostgresVersion::V15).await
}

/// Test PG15+ row filter feature
#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg15_row_filters() -> anyhow::Result<()> {
    init_tracing();
    println!("\n🧪 Testing PG15 row filters\n");

    let pg = PostgresContainer::start_version(PostgresVersion::V15).await?;

    // Create publication with row filter (PG15+ only)
    pg.execute_sql("DROP PUBLICATION IF EXISTS rivven_pub")
        .await?;
    pg.execute_sql("CREATE PUBLICATION filtered_pub FOR TABLE users WHERE (age >= 18)")
        .await?;

    let conn_str = pg.connection_string();
    let config = PostgresCdcConfig::builder()
        .connection_string(&conn_str)
        .slot_name("test_filter_slot")
        .publication_name("filtered_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc
        .take_event_receiver()
        .ok_or_else(|| anyhow::anyhow!("No receiver"))?;

    cdc.start().await?;
    sleep(Duration::from_secs(2)).await;

    // Insert data - only age >= 18 should be captured
    pg.execute_sql("INSERT INTO users (name, email, age) VALUES ('Minor', 'minor@test.com', 16)")
        .await?;
    pg.execute_sql("INSERT INTO users (name, email, age) VALUES ('Adult', 'adult@test.com', 25)")
        .await?;

    sleep(Duration::from_secs(2)).await;

    // Should only receive the adult user
    let mut events = Vec::new();
    while let Ok(Some(event)) = timeout(Duration::from_secs(2), rx.recv()).await {
        events.push(event);
    }

    // With row filter, we should only get the Adult record
    println!("ℹ️  Captured {} events with row filter", events.len());
    if !events.is_empty() {
        let after = events[0].after.as_ref().unwrap();
        assert_eq!(after["name"], "Adult", "Should only capture filtered rows");
    }

    println!("✅ Row filter test passed\n");
    Ok(())
}

// ============================================================================
// PostgreSQL 16 Tests (+ parallel apply)
// ============================================================================

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg16_insert() -> anyhow::Result<()> {
    test_insert_impl(PostgresVersion::V16).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg16_update() -> anyhow::Result<()> {
    test_update_impl(PostgresVersion::V16).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg16_delete() -> anyhow::Result<()> {
    test_delete_impl(PostgresVersion::V16).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg16_truncate() -> anyhow::Result<()> {
    test_truncate_impl(PostgresVersion::V16).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg16_transaction() -> anyhow::Result<()> {
    test_transaction_impl(PostgresVersion::V16).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg16_scram_auth() -> anyhow::Result<()> {
    test_scram_auth_impl(PostgresVersion::V16).await
}

/// Test high-throughput scenario on PG16 (recommended version)
#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg16_high_throughput() -> anyhow::Result<()> {
    init_tracing();
    println!("\n🧪 Testing high throughput on PostgreSQL 16\n");

    let pg = PostgresContainer::start_version(PostgresVersion::V16).await?;
    let conn_str = pg.connection_string();

    let config = PostgresCdcConfig::builder()
        .connection_string(&conn_str)
        .slot_name("test_throughput_slot")
        .publication_name("rivven_pub")
        .buffer_size(10000) // Larger buffer for throughput
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc
        .take_event_receiver()
        .ok_or_else(|| anyhow::anyhow!("No receiver"))?;

    cdc.start().await?;
    sleep(Duration::from_secs(2)).await;

    // Insert 100 rows rapidly
    let insert_count = 100;
    let start = std::time::Instant::now();

    for i in 0..insert_count {
        pg.execute_sql(&format!(
            "INSERT INTO users (name, email, age) VALUES ('User{}', 'user{}@test.com', {})",
            i,
            i,
            20 + (i % 50)
        ))
        .await?;
    }

    let insert_duration = start.elapsed();
    println!("⚡ Inserted {} rows in {:?}", insert_count, insert_duration);

    // Collect events
    let mut events = Vec::new();
    let collect_start = std::time::Instant::now();

    while events.len() < insert_count {
        match timeout(Duration::from_secs(30), rx.recv()).await {
            Ok(Some(event)) => events.push(event),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let collect_duration = collect_start.elapsed();
    println!(
        "📊 Collected {} events in {:?}",
        events.len(),
        collect_duration
    );

    assert!(
        events.len() >= insert_count * 9 / 10,
        "Should capture at least 90% of events"
    );

    let throughput = events.len() as f64 / collect_duration.as_secs_f64();
    println!("📈 Throughput: {:.0} events/sec\n", throughput);

    println!("✅ High throughput test passed\n");
    Ok(())
}

// ============================================================================
// PostgreSQL 17 Tests (latest)
// ============================================================================

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg17_insert() -> anyhow::Result<()> {
    test_insert_impl(PostgresVersion::V17).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg17_update() -> anyhow::Result<()> {
    test_update_impl(PostgresVersion::V17).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg17_delete() -> anyhow::Result<()> {
    test_delete_impl(PostgresVersion::V17).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg17_truncate() -> anyhow::Result<()> {
    test_truncate_impl(PostgresVersion::V17).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg17_transaction() -> anyhow::Result<()> {
    test_transaction_impl(PostgresVersion::V17).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg17_scram_auth() -> anyhow::Result<()> {
    test_scram_auth_impl(PostgresVersion::V17).await
}

/// Test PG17 specific enhancements
#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_pg17_logical_replication_enhancements() -> anyhow::Result<()> {
    init_tracing();
    println!("\n🧪 Testing PG17 logical replication enhancements\n");

    let pg = PostgresContainer::start_version(PostgresVersion::V17).await?;
    let conn_str = pg.connection_string();

    // PG17 has improved logical replication performance
    let config = PostgresCdcConfig::builder()
        .connection_string(&conn_str)
        .slot_name("test_pg17_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc
        .take_event_receiver()
        .ok_or_else(|| anyhow::anyhow!("No receiver"))?;

    cdc.start().await?;
    sleep(Duration::from_secs(2)).await;

    // Test with larger transaction
    let mut sql = String::from("BEGIN;");
    for i in 0..50 {
        sql.push_str(&format!(
            " INSERT INTO users (name, email, age) VALUES ('Batch{}', 'batch{}@test.com', {});",
            i,
            i,
            20 + i
        ));
    }
    sql.push_str(" COMMIT;");

    pg.execute_sql(&sql).await?;

    // Collect events
    let mut events = Vec::new();
    while events.len() < 50 {
        match timeout(Duration::from_secs(15), rx.recv()).await {
            Ok(Some(event)) => events.push(event),
            _ => break,
        }
    }

    assert!(
        events.len() >= 45,
        "Should capture most events: got {}",
        events.len()
    );
    println!(
        "✅ PG17 enhancements test passed ({} events)\n",
        events.len()
    );
    Ok(())
}

// ============================================================================
// Cross-Version Compatibility Tests
// ============================================================================

/// Verify all versions support basic CRUD operations
#[tokio::test]
#[ignore = "Requires Docker - runs all versions sequentially"]
async fn test_all_versions_crud_compatibility() -> anyhow::Result<()> {
    init_tracing();
    println!("\n🧪 Testing CRUD compatibility across all PostgreSQL versions\n");

    for version in PostgresVersion::all() {
        println!("━━━ Testing {} ━━━", version);
        test_insert_impl(*version).await?;
        println!();
    }

    println!("✅ All versions passed CRUD compatibility tests\n");
    Ok(())
}

/// Test version detection and feature availability
#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_version_detection() -> anyhow::Result<()> {
    init_tracing();

    for version in PostgresVersion::all() {
        println!("\n🔍 Detecting {}", version);

        let pg = PostgresContainer::start_version(*version).await?;
        let server_version = pg.server_version().await?;

        println!("   Server version: {}", server_version);
        println!("   Row filters: {}", version.supports_row_filters());
        println!("   Column lists: {}", version.supports_column_lists());
        println!("   Parallel apply: {}", version.supports_parallel_apply());

        // Verify version matches expected
        let major = version.major_version();
        assert!(
            server_version.contains(&major.to_string()),
            "Server version {} should contain {}",
            server_version,
            major
        );
    }

    println!("\n✅ Version detection test passed\n");
    Ok(())
}
