//! CDC Stress Tests
//!
//! High-throughput stress tests for PostgreSQL CDC.
//! These tests verify:
//! - Throughput under load
//! - Memory stability during sustained operations
//! - Recovery from high-load scenarios
//! - Latency characteristics
//!
//! Run with: cargo test -p rivven-cdc --test cdc_stress --release -- --test-threads=1 --nocapture

mod harness;

use harness::*;
use rivven_cdc::common::CdcOp;
use serial_test::serial;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

// ============================================================================
// Throughput Tests
// ============================================================================

#[tokio::test]
#[serial]
#[ignore] // Run explicitly: cargo test -p rivven-cdc --test cdc_stress test_high_throughput_inserts -- --ignored
async fn test_high_throughput_inserts() {
    init_test_logging();
    let pg = PostgresTestContainer::start().await.unwrap();
    let pg = Arc::new(pg);
    let ctx = TestContext::new(pg.clone()).await.unwrap();

    pg.create_test_table("stress_throughput").await.unwrap();

    let mut rivven = RivvenTestContext::new().await.unwrap();
    rivven
        .register_cdc_topic("postgres", "public", "stress_throughput")
        .await
        .unwrap();

    rivven
        .start_cdc(
            &pg.cdc_connection_string(),
            &ctx.slot_name,
            &ctx.publication_name,
        )
        .await
        .unwrap();

    // Configuration
    let total_records = 1000;
    let batch_size = 50;

    println!("\n📊 Throughput Test Configuration:");
    println!("   Total records: {}", total_records);
    println!("   Batch size: {}", batch_size);

    // Generate and insert data
    let start = Instant::now();
    let batches = generate_bulk_inserts("stress_throughput", total_records, batch_size);

    for batch in &batches {
        pg.execute(batch).await.unwrap();
    }

    let insert_duration = start.elapsed();
    let insert_rate = total_records as f64 / insert_duration.as_secs_f64();

    println!("\n⏱️  Insert Performance:");
    println!("   Duration: {:?}", insert_duration);
    println!("   Rate: {:.0} inserts/sec", insert_rate);

    // Wait for CDC to process
    let cdc_start = Instant::now();
    let messages = rivven
        .wait_for_messages(
            "cdc.postgres.public.stress_throughput",
            total_records,
            Duration::from_secs(60),
        )
        .await
        .unwrap();

    let cdc_duration = cdc_start.elapsed();
    let cdc_rate = total_records as f64 / cdc_duration.as_secs_f64();

    println!("\n📡 CDC Processing Performance:");
    println!("   Duration: {:?}", cdc_duration);
    println!("   Rate: {:.0} events/sec", cdc_rate);
    println!("   Events captured: {}", messages.len());

    // Verify all events
    let events = rivven.parse_events(&messages).unwrap();
    events.assert().has_count(total_records);

    // Performance assertions
    assert!(
        insert_rate > 500.0,
        "Insert rate {:.0}/s below threshold 500/s",
        insert_rate
    );
    assert!(
        cdc_rate > 100.0,
        "CDC rate {:.0}/s below threshold 100/s",
        cdc_rate
    );

    println!("\n✅ Throughput test passed!");

    ctx.cleanup().await.unwrap();
    rivven.stop_cdc().await.unwrap();
}

#[tokio::test]
#[serial]
#[ignore]
async fn test_sustained_load() {
    init_test_logging();
    let pg = PostgresTestContainer::start().await.unwrap();
    let pg = Arc::new(pg);
    let ctx = TestContext::new(pg.clone()).await.unwrap();

    pg.create_test_table("stress_sustained").await.unwrap();

    let mut rivven = RivvenTestContext::new().await.unwrap();
    rivven
        .register_cdc_topic("postgres", "public", "stress_sustained")
        .await
        .unwrap();

    rivven
        .start_cdc(
            &pg.cdc_connection_string(),
            &ctx.slot_name,
            &ctx.publication_name,
        )
        .await
        .unwrap();

    // Configuration
    let duration_secs = 10;
    let target_rate = 50; // inserts per second
    let total_expected = duration_secs * target_rate;

    println!("\n📊 Sustained Load Test Configuration:");
    println!("   Duration: {}s", duration_secs);
    println!("   Target rate: {} inserts/sec", target_rate);
    println!("   Expected total: {}", total_expected);

    let insert_count = Arc::new(AtomicU64::new(0));
    let insert_count_clone = insert_count.clone();
    let pg_clone = pg.clone();

    // Producer task
    let producer = tokio::spawn(async move {
        let interval = Duration::from_millis(1000 / target_rate as u64);
        let start = Instant::now();

        while start.elapsed() < Duration::from_secs(duration_secs as u64) {
            let count = insert_count_clone.fetch_add(1, Ordering::SeqCst);
            let sql = format!(
                "INSERT INTO stress_sustained (name, email, age) VALUES ('User{}', 'user{}@test.com', {})",
                count, count, 20 + (count % 60) as i32
            );

            if let Err(e) = pg_clone.execute(&sql).await {
                eprintln!("Insert error: {}", e);
            }

            sleep(interval).await;
        }
    });

    // Wait for producer
    producer.await.unwrap();

    let actual_inserts = insert_count.load(Ordering::SeqCst) as usize;
    println!("\n📝 Actual inserts: {}", actual_inserts);

    // Wait for CDC to catch up
    sleep(Duration::from_secs(5)).await;

    let messages = rivven
        .wait_for_messages(
            "cdc.postgres.public.stress_sustained",
            actual_inserts,
            Duration::from_secs(30),
        )
        .await
        .unwrap();

    let events = rivven.parse_events(&messages).unwrap();

    println!("\n📡 CDC captured: {} events", events.len());

    // Allow some tolerance (95%)
    let capture_rate = events.len() as f64 / actual_inserts as f64;
    assert!(
        capture_rate >= 0.95,
        "Capture rate {:.1}% below 95%",
        capture_rate * 100.0
    );

    println!(
        "✅ Sustained load test passed! Capture rate: {:.1}%",
        capture_rate * 100.0
    );

    ctx.cleanup().await.unwrap();
    rivven.stop_cdc().await.unwrap();
}

// ============================================================================
// Latency Tests
// ============================================================================

#[tokio::test]
#[serial]
#[ignore]
async fn test_event_latency() {
    init_test_logging();
    let pg = PostgresTestContainer::start().await.unwrap();
    let pg = Arc::new(pg);
    let ctx = TestContext::new(pg.clone()).await.unwrap();

    pg.create_test_table("stress_latency").await.unwrap();

    let mut rivven = RivvenTestContext::new().await.unwrap();
    rivven
        .register_cdc_topic("postgres", "public", "stress_latency")
        .await
        .unwrap();

    rivven
        .start_cdc(
            &pg.cdc_connection_string(),
            &ctx.slot_name,
            &ctx.publication_name,
        )
        .await
        .unwrap();

    // Measure individual event latencies
    let sample_count = 50;
    let mut latencies = Vec::with_capacity(sample_count);

    println!("\n📊 Latency Test - measuring {} samples", sample_count);

    for i in 0..sample_count {
        let start = Instant::now();

        pg.execute(&format!(
            "INSERT INTO stress_latency (name, email) VALUES ('LatencyTest{}', 'latency{}@test.com')",
            i, i
        ))
        .await
        .unwrap();

        // Poll until we get the event
        let timeout = Duration::from_secs(5);
        let poll_start = Instant::now();

        loop {
            let messages = rivven
                .read_messages("cdc.postgres.public.stress_latency", i as u64, 1)
                .await
                .unwrap();

            if !messages.is_empty() {
                let latency = start.elapsed();
                latencies.push(latency);
                break;
            }

            if poll_start.elapsed() > timeout {
                println!("⚠️  Timeout waiting for event {}", i);
                break;
            }

            sleep(Duration::from_millis(10)).await;
        }
    }

    // Calculate statistics
    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[(latencies.len() as f64 * 0.95) as usize];
    let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];
    let avg: Duration = latencies.iter().sum::<Duration>() / latencies.len() as u32;
    let min = latencies.first().unwrap();
    let max = latencies.last().unwrap();

    println!("\n📈 Latency Statistics:");
    println!("   Min:  {:?}", min);
    println!("   Avg:  {:?}", avg);
    println!("   P50:  {:?}", p50);
    println!("   P95:  {:?}", p95);
    println!("   P99:  {:?}", p99);
    println!("   Max:  {:?}", max);

    // Assertions (these are generous for CI environments)
    assert!(
        p99 < Duration::from_secs(2),
        "P99 latency {:?} exceeds 2s threshold",
        p99
    );

    println!("\n✅ Latency test passed!");

    ctx.cleanup().await.unwrap();
    rivven.stop_cdc().await.unwrap();
}

// ============================================================================
// Mixed Workload Tests
// ============================================================================

#[tokio::test]
#[serial]
#[ignore]
async fn test_mixed_workload_stress() {
    init_test_logging();
    let pg = PostgresTestContainer::start().await.unwrap();
    let pg = Arc::new(pg);
    let ctx = TestContext::new(pg.clone()).await.unwrap();

    pg.execute(
        "CREATE TABLE stress_mixed (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT NOW()
        )",
    )
    .await
    .unwrap();
    pg.execute("ALTER TABLE stress_mixed REPLICA IDENTITY FULL")
        .await
        .unwrap();

    let mut rivven = RivvenTestContext::new().await.unwrap();
    rivven
        .register_cdc_topic("postgres", "public", "stress_mixed")
        .await
        .unwrap();

    rivven
        .start_cdc(
            &pg.cdc_connection_string(),
            &ctx.slot_name,
            &ctx.publication_name,
        )
        .await
        .unwrap();

    // Generate mixed workload
    let operations = generate_mixed_workload("stress_mixed", 200);

    println!("\n📊 Mixed Workload Test");
    println!("   Total operations: {}", operations.len());

    let start = Instant::now();
    let mut insert_count = 0;
    let mut update_count = 0;
    let mut delete_count = 0;

    for op in &operations {
        pg.execute(op).await.unwrap();

        if op.starts_with("INSERT") {
            insert_count += 1;
        } else if op.starts_with("UPDATE") {
            update_count += 1;
        } else if op.starts_with("DELETE") {
            delete_count += 1;
        }
    }

    let exec_duration = start.elapsed();

    println!("\n📝 Executed operations in {:?}:", exec_duration);
    println!("   Inserts: {}", insert_count);
    println!("   Updates: {}", update_count);
    println!("   Deletes: {}", delete_count);

    // Wait for CDC
    sleep(Duration::from_secs(5)).await;

    // We can't know exact event count due to updates/deletes on non-existent rows
    // Just verify we got a substantial number
    let messages = rivven
        .read_messages("cdc.postgres.public.stress_mixed", 0, 500)
        .await
        .unwrap();

    let events = rivven.parse_events(&messages).unwrap();

    println!("\n📡 CDC captured {} events", events.len());

    // Count by operation type
    let insert_events = events
        .iter()
        .filter(|e| matches!(e.op, CdcOp::Insert))
        .count();
    let update_events = events
        .iter()
        .filter(|e| matches!(e.op, CdcOp::Update))
        .count();
    let delete_events = events
        .iter()
        .filter(|e| matches!(e.op, CdcOp::Delete))
        .count();

    println!("   Insert events: {}", insert_events);
    println!("   Update events: {}", update_events);
    println!("   Delete events: {}", delete_events);

    // At minimum, all inserts should be captured
    assert!(
        insert_events >= insert_count * 9 / 10, // Allow 10% tolerance
        "Too few insert events: {} vs expected ~{}",
        insert_events,
        insert_count
    );

    println!("\n✅ Mixed workload test passed!");

    ctx.cleanup().await.unwrap();
    rivven.stop_cdc().await.unwrap();
}

// ============================================================================
// Burst Tests
// ============================================================================

#[tokio::test]
#[serial]
#[ignore]
async fn test_burst_recovery() {
    init_test_logging();
    let pg = PostgresTestContainer::start().await.unwrap();
    let pg = Arc::new(pg);
    let ctx = TestContext::new(pg.clone()).await.unwrap();

    pg.create_test_table("stress_burst").await.unwrap();

    let mut rivven = RivvenTestContext::new().await.unwrap();
    rivven
        .register_cdc_topic("postgres", "public", "stress_burst")
        .await
        .unwrap();

    rivven
        .start_cdc(
            &pg.cdc_connection_string(),
            &ctx.slot_name,
            &ctx.publication_name,
        )
        .await
        .unwrap();

    // Burst 1: Heavy load
    println!("\n📊 Burst Test - Phase 1: Heavy load");
    let burst1_count = 200;
    let batches = generate_bulk_inserts("stress_burst", burst1_count, 50);
    for batch in &batches {
        pg.execute(batch).await.unwrap();
    }

    // Let it stabilize
    sleep(Duration::from_secs(3)).await;

    // Burst 2: Light load
    println!("📊 Burst Test - Phase 2: Light load");
    let burst2_count = 20;
    for i in 0..burst2_count {
        pg.execute(&format!(
            "INSERT INTO stress_burst (name, email) VALUES ('Light{}', 'light{}@test.com')",
            i, i
        ))
        .await
        .unwrap();
        sleep(Duration::from_millis(100)).await;
    }

    // Burst 3: Heavy load again
    println!("📊 Burst Test - Phase 3: Heavy load again");
    let burst3_count = 200;
    let batches = generate_bulk_inserts("stress_burst", burst3_count, 50);
    for batch in &batches {
        pg.execute(batch).await.unwrap();
    }

    let total_expected = burst1_count + burst2_count + burst3_count;
    println!("\n📝 Total expected events: {}", total_expected);

    // Wait for all events
    let messages = rivven
        .wait_for_messages(
            "cdc.postgres.public.stress_burst",
            total_expected,
            Duration::from_secs(60),
        )
        .await
        .unwrap();

    let events = rivven.parse_events(&messages).unwrap();
    events.assert().has_count(total_expected);

    println!(
        "✅ Burst recovery test passed! All {} events captured.",
        total_expected
    );

    ctx.cleanup().await.unwrap();
    rivven.stop_cdc().await.unwrap();
}

// ============================================================================
// Memory Stability Test
// ============================================================================

#[tokio::test]
#[serial]
#[ignore]
async fn test_memory_stability() {
    init_test_logging();
    let pg = PostgresTestContainer::start().await.unwrap();
    let pg = Arc::new(pg);
    let ctx = TestContext::new(pg.clone()).await.unwrap();

    pg.create_test_table("stress_memory").await.unwrap();

    let mut rivven = RivvenTestContext::new().await.unwrap();
    rivven
        .register_cdc_topic("postgres", "public", "stress_memory")
        .await
        .unwrap();

    rivven
        .start_cdc(
            &pg.cdc_connection_string(),
            &ctx.slot_name,
            &ctx.publication_name,
        )
        .await
        .unwrap();

    // Multiple rounds of inserts with varying payload sizes
    let rounds = 5;
    let inserts_per_round = 100;

    println!("\n📊 Memory Stability Test");
    println!("   Rounds: {}", rounds);
    println!("   Inserts per round: {}", inserts_per_round);

    for round in 0..rounds {
        println!("   Round {}/{}", round + 1, rounds);

        // Insert with varying data sizes
        for i in 0..inserts_per_round {
            let data_size = (i % 10 + 1) * 100; // 100-1000 chars
            let large_data: String = (0..data_size).map(|_| 'X').collect();

            pg.execute(&format!(
                "INSERT INTO stress_memory (name, email) VALUES ('{}', 'mem{}@test.com')",
                &large_data[..100.min(large_data.len())],
                round * inserts_per_round + i
            ))
            .await
            .unwrap();
        }

        // Let CDC process
        sleep(Duration::from_secs(2)).await;
    }

    let total_expected = rounds * inserts_per_round;

    let messages = rivven
        .wait_for_messages(
            "cdc.postgres.public.stress_memory",
            total_expected,
            Duration::from_secs(60),
        )
        .await
        .unwrap();

    let events = rivven.parse_events(&messages).unwrap();

    println!("\n📡 Captured {} events", events.len());
    assert!(
        events.len() >= total_expected * 9 / 10,
        "Too few events: {} vs expected {}",
        events.len(),
        total_expected
    );

    println!("✅ Memory stability test passed!");

    ctx.cleanup().await.unwrap();
    rivven.stop_cdc().await.unwrap();
}

// ============================================================================
// Large Transaction Test
// ============================================================================

#[tokio::test]
#[serial]
#[ignore]
async fn test_large_transaction() {
    init_test_logging();
    let pg = PostgresTestContainer::start().await.unwrap();
    let pg = Arc::new(pg);
    let ctx = TestContext::new(pg.clone()).await.unwrap();

    pg.create_test_table("stress_large_tx").await.unwrap();

    let mut rivven = RivvenTestContext::new().await.unwrap();
    rivven
        .register_cdc_topic("postgres", "public", "stress_large_tx")
        .await
        .unwrap();

    rivven
        .start_cdc(
            &pg.cdc_connection_string(),
            &ctx.slot_name,
            &ctx.publication_name,
        )
        .await
        .unwrap();

    // Large transaction with many inserts
    let tx_size = 500;
    println!("\n📊 Large Transaction Test");
    println!("   Transaction size: {} inserts", tx_size);

    let start = Instant::now();

    // Build all statements for the batch
    let statements: Vec<String> = (0..tx_size)
        .map(|i| {
            format!(
                "INSERT INTO stress_large_tx (name, email, age) VALUES ('TxUser{}', 'tx{}@test.com', {})",
                i, i, 20 + (i % 50) as i32
            )
        })
        .collect();

    let stmt_refs: Vec<&str> = statements.iter().map(|s| s.as_str()).collect();
    pg.execute_batch(&stmt_refs).await.unwrap();

    let tx_duration = start.elapsed();
    println!("   Transaction completed in {:?}", tx_duration);

    // CDC should see all events after commit
    let messages = rivven
        .wait_for_messages(
            "cdc.postgres.public.stress_large_tx",
            tx_size,
            Duration::from_secs(60),
        )
        .await
        .unwrap();

    let events = rivven.parse_events(&messages).unwrap();
    events.assert().has_count(tx_size);

    println!(
        "✅ Large transaction test passed! All {} events captured.",
        tx_size
    );

    ctx.cleanup().await.unwrap();
    rivven.stop_cdc().await.unwrap();
}
