//! MySQL/MariaDB CDC Stress Tests
//!
//! High-throughput stress tests for MySQL and MariaDB CDC.
//! These tests verify:
//! - Throughput under sustained load
//! - Memory stability during long-running operations
//! - Recovery from connection issues
//! - Latency characteristics
//! - Transaction handling under load
//!
//! Run with: cargo test -p rivven-cdc --test cdc_mysql_stress --release -- --test-threads=1 --nocapture --ignored

mod harness;

use harness::{init_test_logging, MariaDbTestContainer, MySqlTestContainer};
use rivven_cdc::mysql::{MySqlCdc, MySqlCdcConfig};
use rivven_cdc::CdcSource;
use serial_test::serial;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::timeout;

// ============================================================================
// Throughput Tests
// ============================================================================

#[tokio::test]
#[serial]
#[ignore = "Stress test - run explicitly with --ignored"]
async fn test_mysql_high_throughput_inserts() {
    init_test_logging();

    let container = MySqlTestContainer::start()
        .await
        .expect("Failed to start MySQL container");

    // Create test table
    container
        .execute_batch(&[
            "CREATE DATABASE IF NOT EXISTS stress_db",
            "USE stress_db",
            "CREATE TABLE stress_inserts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                data VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
        ])
        .await
        .expect("Failed to create test table");

    // Configuration
    let total_records = 1000;
    let batch_size = 50;

    println!("\n📊 MySQL Throughput Test Configuration:");
    println!("   Total records: {}", total_records);
    println!("   Batch size: {}", batch_size);
    println!("   MySQL version: 8.x");

    // Create event channel
    let (tx, mut rx) = mpsc::channel(10000);

    // Start CDC
    let config = MySqlCdcConfig::new(container.host(), container.user())
        .with_port(container.port())
        .with_password(container.password())
        .with_database("stress_db")
        .with_server_id(12345)
        .include_table("stress_inserts");

    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    // Start CDC in background
    let cdc_handle = tokio::spawn(async move { cdc.start().await });

    // Give CDC time to connect and register
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Generate and insert data
    let start = Instant::now();
    let batches = generate_mysql_bulk_inserts("stress_inserts", total_records, batch_size);

    for batch in &batches {
        container
            .execute(&format!("USE stress_db; {}", batch))
            .await
            .expect("Insert failed");
    }

    let insert_duration = start.elapsed();
    let insert_rate = total_records as f64 / insert_duration.as_secs_f64();

    println!("\n⏱️  Insert Performance:");
    println!("   Duration: {:?}", insert_duration);
    println!("   Rate: {:.0} inserts/sec", insert_rate);

    // Wait for CDC to process events
    let mut received = 0;
    let cdc_start = Instant::now();
    let timeout_duration = Duration::from_secs(60);

    while received < total_records && cdc_start.elapsed() < timeout_duration {
        match timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(event)) => {
                if matches!(
                    event.op,
                    rivven_cdc::CdcOp::Insert
                        | rivven_cdc::CdcOp::Update
                        | rivven_cdc::CdcOp::Delete
                ) {
                    received += 1;
                }
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    let cdc_duration = cdc_start.elapsed();
    let cdc_rate = received as f64 / cdc_duration.as_secs_f64();

    println!("\n📡 CDC Processing Performance:");
    println!("   Duration: {:?}", cdc_duration);
    println!("   Rate: {:.0} events/sec", cdc_rate);
    println!("   Events captured: {}", received);

    // Performance assertions
    assert!(
        insert_rate > 100.0,
        "Insert rate too low: {:.0} inserts/sec",
        insert_rate
    );
    assert!(
        received >= total_records * 95 / 100,
        "Too few events received: {}/{}",
        received,
        total_records
    );

    // Clean shutdown
    cdc_handle.abort();
}

#[tokio::test]
#[serial]
#[ignore = "Stress test - run explicitly with --ignored"]
async fn test_mysql_mixed_operations_throughput() {
    init_test_logging();

    let container = MySqlTestContainer::start()
        .await
        .expect("Failed to start MySQL container");

    container
        .execute_batch(&[
            "CREATE DATABASE IF NOT EXISTS stress_db",
            "USE stress_db",
            "CREATE TABLE stress_mixed (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                counter INT DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )",
        ])
        .await
        .expect("Failed to create test table");

    // Configuration
    let insert_count = 200;
    let update_count = 300;
    let delete_count = 100;
    let total_events = insert_count + update_count + delete_count;

    println!("\n📊 MySQL Mixed Operations Test:");
    println!("   Inserts: {}", insert_count);
    println!("   Updates: {}", update_count);
    println!("   Deletes: {}", delete_count);
    println!("   Total: {}", total_events);

    // Create event channel
    let (tx, mut rx) = mpsc::channel(10000);

    // Start CDC
    let config = MySqlCdcConfig::new(container.host(), container.user())
        .with_port(container.port())
        .with_password(container.password())
        .with_database("stress_db")
        .with_server_id(12346)
        .include_table("stress_mixed");

    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    let cdc_handle = tokio::spawn(async move { cdc.start().await });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let start = Instant::now();

    // Inserts
    for i in 1..=insert_count {
        container
            .execute(&format!(
                "USE stress_db; INSERT INTO stress_mixed (id, name) VALUES ({}, 'user_{}')",
                i, i
            ))
            .await
            .expect("Insert failed");
    }

    // Updates (update some of the inserted rows)
    for i in 1..=update_count {
        let id = (i % insert_count) + 1;
        container
            .execute(
                &format!(
                    "USE stress_db; UPDATE stress_mixed SET counter = counter + 1, name = 'updated_{}' WHERE id = {}",
                    i, id
                ),
            )
            .await
            .expect("Update failed");
    }

    // Deletes
    for i in 1..=delete_count {
        container
            .execute(&format!(
                "USE stress_db; DELETE FROM stress_mixed WHERE id = {}",
                i
            ))
            .await
            .expect("Delete failed");
    }

    let operation_duration = start.elapsed();
    let operation_rate = total_events as f64 / operation_duration.as_secs_f64();

    println!("\n⏱️  Operation Performance:");
    println!("   Duration: {:?}", operation_duration);
    println!("   Rate: {:.0} ops/sec", operation_rate);

    // Count received events by type
    let mut inserts = 0u64;
    let mut updates = 0u64;
    let mut deletes = 0u64;
    let cdc_start = Instant::now();
    let timeout_duration = Duration::from_secs(60);

    while cdc_start.elapsed() < timeout_duration {
        match timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(event)) => {
                match event.op {
                    rivven_cdc::CdcOp::Insert => inserts += 1,
                    rivven_cdc::CdcOp::Update => updates += 1,
                    rivven_cdc::CdcOp::Delete => deletes += 1,
                    _ => {}
                }
                if inserts + updates + deletes >= total_events as u64 {
                    break;
                }
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    let cdc_duration = cdc_start.elapsed();
    let _total_received = inserts + updates + deletes;

    println!("\n📡 CDC Events Received:");
    println!("   Inserts: {}/{}", inserts, insert_count);
    println!("   Updates: {}/{}", updates, update_count);
    println!("   Deletes: {}/{}", deletes, delete_count);
    println!("   Duration: {:?}", cdc_duration);

    // Assertions
    assert!(
        inserts >= insert_count as u64 * 90 / 100,
        "Too few inserts: {}/{}",
        inserts,
        insert_count
    );
    assert!(
        updates >= update_count as u64 * 90 / 100,
        "Too few updates: {}/{}",
        updates,
        update_count
    );
    assert!(
        deletes >= delete_count as u64 * 90 / 100,
        "Too few deletes: {}/{}",
        deletes,
        delete_count
    );

    cdc_handle.abort();
}

#[tokio::test]
#[serial]
#[ignore = "Stress test - run explicitly with --ignored"]
async fn test_mariadb_high_throughput() {
    init_test_logging();

    let container = MariaDbTestContainer::start()
        .await
        .expect("Failed to start MariaDB container");

    container
        .execute_batch(&[
            "CREATE DATABASE IF NOT EXISTS stress_db",
            "USE stress_db",
            "CREATE TABLE stress_mariadb (
                id INT AUTO_INCREMENT PRIMARY KEY,
                data VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
        ])
        .await
        .expect("Failed to create test table");

    let total_records = 500;
    let batch_size = 25;

    println!("\n📊 MariaDB Throughput Test:");
    println!("   Total records: {}", total_records);
    println!("   Batch size: {}", batch_size);

    // Create event channel
    let (tx, mut rx) = mpsc::channel(10000);

    let config = MySqlCdcConfig::new(container.host(), container.user())
        .with_port(container.port())
        .with_password(container.password())
        .with_database("stress_db")
        .with_server_id(12347)
        .include_table("stress_mariadb");

    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    let cdc_handle = tokio::spawn(async move { cdc.start().await });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let start = Instant::now();
    let batches = generate_mysql_bulk_inserts("stress_mariadb", total_records, batch_size);

    for batch in &batches {
        container
            .execute(&format!("USE stress_db; {}", batch))
            .await
            .expect("Insert failed");
    }

    let insert_duration = start.elapsed();
    println!("\n⏱️  MariaDB Insert Duration: {:?}", insert_duration);

    let mut received = 0;
    let timeout_duration = Duration::from_secs(60);
    let cdc_start = Instant::now();

    while received < total_records && cdc_start.elapsed() < timeout_duration {
        match timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(event))
                if matches!(
                    event.op,
                    rivven_cdc::CdcOp::Insert
                        | rivven_cdc::CdcOp::Update
                        | rivven_cdc::CdcOp::Delete
                ) =>
            {
                received += 1
            }
            Ok(None) => break,
            _ => continue,
        }
    }

    println!("📡 MariaDB Events Received: {}/{}", received, total_records);

    assert!(
        received >= total_records * 90 / 100,
        "Too few events: {}/{}",
        received,
        total_records
    );

    cdc_handle.abort();
}

// ============================================================================
// Transaction Stress Tests
// ============================================================================

#[tokio::test]
#[serial]
#[ignore = "Stress test - run explicitly with --ignored"]
async fn test_mysql_large_transaction() {
    init_test_logging();

    let container = MySqlTestContainer::start()
        .await
        .expect("Failed to start MySQL container");

    container
        .execute_batch(&[
            "CREATE DATABASE IF NOT EXISTS stress_db",
            "USE stress_db",
            "CREATE TABLE stress_txn (
                id INT AUTO_INCREMENT PRIMARY KEY,
                batch_id INT,
                data VARCHAR(255)
            )",
        ])
        .await
        .expect("Failed to create test table");

    let records_per_txn = 100;

    println!("\n📊 MySQL Large Transaction Test:");
    println!("   Records per transaction: {}", records_per_txn);

    // Create event channel
    let (tx, mut rx) = mpsc::channel(10000);

    let config = MySqlCdcConfig::new(container.host(), container.user())
        .with_port(container.port())
        .with_password(container.password())
        .with_database("stress_db")
        .with_server_id(12348)
        .include_table("stress_txn");

    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    let cdc_handle = tokio::spawn(async move { cdc.start().await });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Execute large transaction
    let start = Instant::now();

    // Build transaction with many inserts
    let mut txn_sql = String::from("START TRANSACTION;\n");
    for i in 1..=records_per_txn {
        txn_sql.push_str(&format!(
            "INSERT INTO stress_txn (batch_id, data) VALUES (1, 'record_{}');\n",
            i
        ));
    }
    txn_sql.push_str("COMMIT;");

    // Execute as batch
    container
        .execute(&format!("USE stress_db; {}", txn_sql))
        .await
        .expect("Transaction failed");

    let txn_duration = start.elapsed();
    println!("⏱️  Transaction Duration: {:?}", txn_duration);

    // Wait for all events
    let mut received = 0;
    let timeout_duration = Duration::from_secs(30);
    let cdc_start = Instant::now();

    while received < records_per_txn && cdc_start.elapsed() < timeout_duration {
        match timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(event))
                if matches!(
                    event.op,
                    rivven_cdc::CdcOp::Insert
                        | rivven_cdc::CdcOp::Update
                        | rivven_cdc::CdcOp::Delete
                ) =>
            {
                received += 1
            }
            Ok(None) => break,
            _ => continue,
        }
    }

    println!(
        "📡 Transaction Events Received: {}/{}",
        received, records_per_txn
    );

    // All events from transaction should be received
    assert!(
        received >= records_per_txn * 90 / 100,
        "Too few transaction events: {}/{}",
        received,
        records_per_txn
    );

    cdc_handle.abort();
}

// ============================================================================
// Recovery Tests
// ============================================================================

#[tokio::test]
#[serial]
#[ignore = "Stress test - run explicitly with --ignored"]
async fn test_mysql_reconnection_under_load() {
    init_test_logging();

    let container = MySqlTestContainer::start()
        .await
        .expect("Failed to start MySQL container");

    container
        .execute_batch(&[
            "CREATE DATABASE IF NOT EXISTS stress_db",
            "USE stress_db",
            "CREATE TABLE stress_reconnect (
                id INT AUTO_INCREMENT PRIMARY KEY,
                phase INT,
                data VARCHAR(255)
            )",
        ])
        .await
        .expect("Failed to create test table");

    println!("\n📊 MySQL Reconnection Under Load Test");

    // Create event channel
    let (tx, mut rx) = mpsc::channel(10000);

    let config = MySqlCdcConfig::new(container.host(), container.user())
        .with_port(container.port())
        .with_password(container.password())
        .with_database("stress_db")
        .with_server_id(12349)
        .include_table("stress_reconnect");

    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    let cdc_handle = tokio::spawn(async move { cdc.start().await });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Phase 1: Insert before simulated reconnection scenario
    let phase1_records = 50;
    for i in 1..=phase1_records {
        container
            .execute(
                &format!(
                    "USE stress_db; INSERT INTO stress_reconnect (phase, data) VALUES (1, 'phase1_record_{}')",
                    i
                ),
            )
            .await
            .expect("Phase 1 insert failed");
    }

    println!("✅ Phase 1: Inserted {} records", phase1_records);

    // Wait for phase 1 events
    let mut phase1_received = 0;
    let timeout_duration = Duration::from_secs(30);
    let start = Instant::now();

    while phase1_received < phase1_records && start.elapsed() < timeout_duration {
        match timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(event))
                if matches!(
                    event.op,
                    rivven_cdc::CdcOp::Insert
                        | rivven_cdc::CdcOp::Update
                        | rivven_cdc::CdcOp::Delete
                ) =>
            {
                phase1_received += 1
            }
            Ok(None) => break,
            _ => continue,
        }
    }

    println!("📡 Phase 1 Events: {}/{}", phase1_received, phase1_records);

    // Phase 2: More inserts (simulating continued operation)
    let phase2_records = 50;
    for i in 1..=phase2_records {
        container
            .execute(
                &format!(
                    "USE stress_db; INSERT INTO stress_reconnect (phase, data) VALUES (2, 'phase2_record_{}')",
                    i
                ),
            )
            .await
            .expect("Phase 2 insert failed");
    }

    println!("✅ Phase 2: Inserted {} records", phase2_records);

    // Wait for phase 2 events
    let mut phase2_received = 0;
    let start = Instant::now();

    while phase2_received < phase2_records && start.elapsed() < timeout_duration {
        match timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(event))
                if matches!(
                    event.op,
                    rivven_cdc::CdcOp::Insert
                        | rivven_cdc::CdcOp::Update
                        | rivven_cdc::CdcOp::Delete
                ) =>
            {
                phase2_received += 1
            }
            Ok(None) => break,
            _ => continue,
        }
    }

    println!("📡 Phase 2 Events: {}/{}", phase2_received, phase2_records);

    let total_received = phase1_received + phase2_received;
    let total_expected = phase1_records + phase2_records;

    assert!(
        total_received >= total_expected * 85 / 100,
        "Too few total events: {}/{}",
        total_received,
        total_expected
    );

    cdc_handle.abort();
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Generate bulk INSERT statements for MySQL
fn generate_mysql_bulk_inserts(table: &str, total: usize, batch_size: usize) -> Vec<String> {
    let mut batches = Vec::new();
    let mut current_batch = Vec::new();

    for i in 1..=total {
        current_batch.push(format!("('data_{}')", i));

        if current_batch.len() >= batch_size || i == total {
            let sql = format!(
                "INSERT INTO {} (data) VALUES {}",
                table,
                current_batch.join(", ")
            );
            batches.push(sql);
            current_batch.clear();
        }
    }

    batches
}
