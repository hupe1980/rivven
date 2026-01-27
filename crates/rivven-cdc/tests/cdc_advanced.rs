//! Advanced CDC Integration Tests
//!
//! This module contains advanced testing scenarios:
//! - Property-based testing with proptest
//! - Schema evolution (ALTER TABLE, ADD/DROP COLUMN)
//! - Chaos/fault injection testing
//! - Performance regression detection
//! - Edge cases and corner cases
//!
//! **Requires Docker** - These tests use testcontainers to spin up PostgreSQL.
//!
//! Run these tests explicitly with:
//!   cargo test -p rivven-cdc --test cdc_advanced -- --ignored --test-threads=1
//!
//! All tests in this file are #[ignore]d by default because they require:
//! - Docker daemon running
//! - Network access to pull container images
//! - Several minutes to complete

mod harness;

use harness::*;
use rivven_cdc::common::CdcOp;
use serial_test::serial;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

// ============================================================================
// Property-Based Tests
// ============================================================================

mod property_tests {
    use super::*;

    /// Property: Every INSERT should produce exactly one CDC event with op=Insert
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn prop_insert_produces_one_event() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        ctx.pg.create_test_table("prop_insert").await.unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "prop_insert")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Property: For any valid name string, insert produces exactly 1 event
        let test_names = ["Alice", "Bob", "日本語", "Émilie", "O'Brien", ""];

        for (i, name) in test_names.iter().enumerate() {
            let escaped = escape_sql(name);
            ctx.pg
                .execute(&format!(
                    "INSERT INTO prop_insert (name, email) VALUES ('{}', 'test{}@example.com')",
                    escaped, i
                ))
                .await
                .unwrap();
        }

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.prop_insert",
                test_names.len(),
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();

        // Property: Exactly N inserts = exactly N events
        events.assert().has_count(test_names.len());

        // Property: All events are inserts
        events
            .assert()
            .has_op_count(CdcOp::Insert, test_names.len());

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Property: UPDATE on existing row produces event with before and after
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn prop_update_has_before_and_after() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        // Table with REPLICA IDENTITY FULL for before images
        ctx.pg
            .execute("CREATE TABLE prop_update (id SERIAL PRIMARY KEY, value INTEGER NOT NULL)")
            .await
            .unwrap();
        ctx.pg
            .execute("ALTER TABLE prop_update REPLICA IDENTITY FULL")
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "prop_update")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Insert initial value
        ctx.pg
            .execute("INSERT INTO prop_update (value) VALUES (100)")
            .await
            .unwrap();

        // Update with various delta values
        let deltas = [1, -50, 1000, -1000, 0];
        for delta in deltas.iter() {
            ctx.pg
                .execute(&format!(
                    "UPDATE prop_update SET value = value + {} WHERE id = 1",
                    delta
                ))
                .await
                .unwrap();
        }

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 3)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.prop_update",
                1 + deltas.len(), // 1 insert + N updates
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();

        // Property: Updates have both before and after for REPLICA IDENTITY FULL
        let updates: Vec<_> = events
            .iter()
            .filter(|e| matches!(e.op, CdcOp::Update))
            .collect();
        assert_eq!(updates.len(), deltas.len());

        for update in updates {
            assert!(update.before.is_some(), "Update should have before image");
            assert!(update.after.is_some(), "Update should have after image");
        }

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Property: DELETE removes data, captured event has before image
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn prop_delete_captures_deleted_data() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        ctx.pg
            .execute("CREATE TABLE prop_delete (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
            .await
            .unwrap();
        ctx.pg
            .execute("ALTER TABLE prop_delete REPLICA IDENTITY FULL")
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "prop_delete")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Insert then delete multiple rows
        let names = ["Alice", "Bob", "Charlie"];
        for name in names.iter() {
            ctx.pg
                .execute(&format!(
                    "INSERT INTO prop_delete (name) VALUES ('{}')",
                    name
                ))
                .await
                .unwrap();
        }

        // Delete all
        ctx.pg.execute("DELETE FROM prop_delete").await.unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 3)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.prop_delete",
                names.len() * 2, // inserts + deletes
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();

        // Property: Delete events capture the deleted data in before
        let deletes: Vec<_> = events
            .iter()
            .filter(|e| matches!(e.op, CdcOp::Delete))
            .collect();
        assert_eq!(deletes.len(), names.len());

        for delete in deletes {
            assert!(
                delete.before.is_some(),
                "Delete should capture before image"
            );
        }

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Property: Transaction commit produces all events atomically
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn prop_transaction_atomicity() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        ctx.pg.create_test_table("prop_tx").await.unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "prop_tx")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Execute batch as single transaction
        let batch_size = 10;
        let statements: Vec<String> = (0..batch_size)
            .map(|i| {
                format!(
                    "INSERT INTO prop_tx (name, email) VALUES ('User{}', 'user{}@test.com')",
                    i, i
                )
            })
            .collect();
        let stmt_refs: Vec<&str> = statements.iter().map(|s| s.as_str()).collect();
        ctx.pg.execute_batch(&stmt_refs).await.unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.prop_tx",
                batch_size,
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();

        // Property: All events from same transaction have same or sequential timestamps
        events.assert().has_count(batch_size);

        // All events should be relatively close in timestamp (same transaction)
        if events.len() >= 2 {
            let first_ts = events[0].timestamp;
            let last_ts = events[events.len() - 1].timestamp;
            // Transaction events should be within 1 second of each other
            assert!(
                last_ts - first_ts <= 1,
                "Transaction events should be close in time: {} to {}",
                first_ts,
                last_ts
            );
        }

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Property: Event ordering is preserved within same table
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn prop_event_ordering_preserved() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        ctx.pg
            .execute("CREATE TABLE prop_order (id SERIAL PRIMARY KEY, seq INTEGER NOT NULL)")
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "prop_order")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Insert with explicit sequence numbers
        let count = 20;
        for i in 0..count {
            ctx.pg
                .execute(&format!("INSERT INTO prop_order (seq) VALUES ({})", i))
                .await
                .unwrap();
        }

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 3)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.prop_order",
                count,
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(count);

        // Property: Events arrive in the order they were committed
        for (i, event) in events.iter().enumerate() {
            if let Some(after) = &event.after {
                if let Some(seq) = after.get("seq") {
                    let seq_val: i64 = seq.as_str().unwrap().parse().unwrap();
                    assert_eq!(
                        seq_val as usize, i,
                        "Event order mismatch at position {}: expected seq={}, got seq={}",
                        i, i, seq_val
                    );
                }
            }
        }

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }
}

// ============================================================================
// Schema Evolution Tests
// ============================================================================

mod schema_evolution_tests {
    use super::*;

    /// Test ADD COLUMN - new column should appear in CDC events
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_add_column() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        // Create initial table
        ctx.pg
            .execute("CREATE TABLE schema_add_col (id SERIAL PRIMARY KEY, name TEXT)")
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "schema_add_col")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Insert before schema change
        ctx.pg
            .execute("INSERT INTO schema_add_col (name) VALUES ('Before')")
            .await
            .unwrap();

        sleep(Duration::from_millis(500)).await;

        // Add new column
        ctx.pg
            .execute("ALTER TABLE schema_add_col ADD COLUMN email TEXT")
            .await
            .unwrap();

        // Insert after schema change
        ctx.pg
            .execute("INSERT INTO schema_add_col (name, email) VALUES ('After', 'after@test.com')")
            .await
            .unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.schema_add_col",
                2,
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(2);

        // First event should NOT have email field (or have it as null)
        let _first_after = events[0].after.as_ref().unwrap();

        // Second event SHOULD have email field with value
        let second_after = events[1].after.as_ref().unwrap();
        assert!(
            second_after.get("email").is_some(),
            "New column should appear in CDC events"
        );
        assert_eq!(
            second_after.get("email").unwrap().as_str().unwrap(),
            "after@test.com"
        );

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Test DROP COLUMN - column should disappear from CDC events
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_drop_column() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        // Create table with multiple columns
        ctx.pg.execute(
            "CREATE TABLE schema_drop_col (id SERIAL PRIMARY KEY, name TEXT, email TEXT, age INTEGER)"
        ).await.unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "schema_drop_col")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Insert before drop
        ctx.pg.execute("INSERT INTO schema_drop_col (name, email, age) VALUES ('Before', 'before@test.com', 25)").await.unwrap();

        sleep(Duration::from_millis(500)).await;

        // Drop the email column
        ctx.pg
            .execute("ALTER TABLE schema_drop_col DROP COLUMN email")
            .await
            .unwrap();

        // Insert after drop
        ctx.pg
            .execute("INSERT INTO schema_drop_col (name, age) VALUES ('After', 30)")
            .await
            .unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.schema_drop_col",
                2,
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(2);

        // First event should have email
        let first_after = events[0].after.as_ref().unwrap();
        assert!(
            first_after.get("email").is_some(),
            "First event should have email"
        );

        // Second event should NOT have email
        let second_after = events[1].after.as_ref().unwrap();
        assert!(
            second_after.get("email").is_none(),
            "Dropped column should not appear in CDC events"
        );

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Test ALTER COLUMN TYPE - type change should be reflected
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_alter_column_type() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        // Create table with INTEGER column
        ctx.pg
            .execute("CREATE TABLE schema_alter_type (id SERIAL PRIMARY KEY, value INTEGER)")
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "schema_alter_type")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Insert integer value
        ctx.pg
            .execute("INSERT INTO schema_alter_type (value) VALUES (42)")
            .await
            .unwrap();

        sleep(Duration::from_millis(500)).await;

        // Change column type to TEXT
        ctx.pg
            .execute("ALTER TABLE schema_alter_type ALTER COLUMN value TYPE TEXT")
            .await
            .unwrap();

        // Insert text value
        ctx.pg
            .execute("INSERT INTO schema_alter_type (value) VALUES ('hello world')")
            .await
            .unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.schema_alter_type",
                2,
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(2);

        // Second event should have text value
        let second_after = events[1].after.as_ref().unwrap();
        assert_eq!(
            second_after.get("value").unwrap().as_str().unwrap(),
            "hello world"
        );

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Test RENAME COLUMN - column name should change in events
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_rename_column() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        ctx.pg
            .execute("CREATE TABLE schema_rename (id SERIAL PRIMARY KEY, old_name TEXT)")
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "schema_rename")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Insert with old column name
        ctx.pg
            .execute("INSERT INTO schema_rename (old_name) VALUES ('value1')")
            .await
            .unwrap();

        sleep(Duration::from_millis(500)).await;

        // Rename column
        ctx.pg
            .execute("ALTER TABLE schema_rename RENAME COLUMN old_name TO new_name")
            .await
            .unwrap();

        // Insert with new column name
        ctx.pg
            .execute("INSERT INTO schema_rename (new_name) VALUES ('value2')")
            .await
            .unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.schema_rename",
                2,
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(2);

        // First event has old_name
        let first_after = events[0].after.as_ref().unwrap();
        assert!(
            first_after.get("old_name").is_some(),
            "First event should have old_name"
        );

        // Second event has new_name
        let second_after = events[1].after.as_ref().unwrap();
        assert!(
            second_after.get("new_name").is_some(),
            "Second event should have new_name"
        );

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Test ADD/DROP CONSTRAINT - should not affect CDC but data validation applies
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_add_constraint() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        ctx.pg
            .execute("CREATE TABLE schema_constraint (id SERIAL PRIMARY KEY, email TEXT)")
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "schema_constraint")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Insert before constraint
        ctx.pg
            .execute("INSERT INTO schema_constraint (email) VALUES ('test@example.com')")
            .await
            .unwrap();

        sleep(Duration::from_millis(500)).await;

        // Add UNIQUE constraint
        ctx.pg
            .execute("ALTER TABLE schema_constraint ADD CONSTRAINT unique_email UNIQUE (email)")
            .await
            .unwrap();

        // Insert after constraint (different email)
        ctx.pg
            .execute("INSERT INTO schema_constraint (email) VALUES ('other@example.com')")
            .await
            .unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.schema_constraint",
                2,
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(2);

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }
}

// ============================================================================
// Chaos/Fault Injection Tests
// ============================================================================

mod chaos_tests {
    use super::*;

    /// Test CDC recovery after brief network delay simulation
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_resilience_to_slow_operations() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        ctx.pg.create_test_table("chaos_slow").await.unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "chaos_slow")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Normal insert
        ctx.pg
            .execute("INSERT INTO chaos_slow (name) VALUES ('before_slow')")
            .await
            .unwrap();

        // Simulate slow operation with pg_sleep
        ctx.pg.execute("SELECT pg_sleep(0.5)").await.unwrap();

        // Insert after delay
        ctx.pg
            .execute("INSERT INTO chaos_slow (name) VALUES ('after_slow')")
            .await
            .unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 3)).await;

        let messages = rivven
            .wait_for_messages("cdc.postgres.public.chaos_slow", 2, Duration::from_secs(15))
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(2);

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Test CDC handles long-running transactions
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_long_running_transaction() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        ctx.pg.create_test_table("chaos_long_tx").await.unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "chaos_long_tx")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Start a long transaction with multiple inserts spread over time
        let client = ctx.pg.new_client().await.unwrap();
        client.execute("BEGIN", &[]).await.unwrap();

        for i in 0..5 {
            client.execute(
                &format!("INSERT INTO chaos_long_tx (name, email) VALUES ('User{}', 'user{}@test.com')", i, i),
                &[],
            ).await.unwrap();
            sleep(Duration::from_millis(100)).await; // Small delay between inserts
        }

        client.execute("COMMIT", &[]).await.unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 3)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.chaos_long_tx",
                5,
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        // All 5 events should be captured after commit
        events.assert().has_count(5);

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Test handling of very large values
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_large_values() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        ctx.pg
            .execute("CREATE TABLE chaos_large (id SERIAL PRIMARY KEY, data TEXT)")
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "chaos_large")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Insert progressively larger values
        let sizes = [1_000, 10_000, 100_000]; // 1KB, 10KB, 100KB
        for size in sizes.iter() {
            let large_value = "x".repeat(*size);
            ctx.pg
                .execute(&format!(
                    "INSERT INTO chaos_large (data) VALUES ('{}')",
                    large_value
                ))
                .await
                .unwrap();
        }

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 5)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.chaos_large",
                sizes.len(),
                Duration::from_secs(30),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(sizes.len());

        // Verify the data was captured correctly
        for (i, event) in events.iter().enumerate() {
            let after = event.after.as_ref().unwrap();
            let data = after.get("data").unwrap().as_str().unwrap();
            assert_eq!(
                data.len(),
                sizes[i],
                "Large value {} should be captured correctly",
                i
            );
        }

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Test handling of high-frequency updates on same row
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_rapid_same_row_updates() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        ctx.pg
            .execute("CREATE TABLE chaos_rapid (id SERIAL PRIMARY KEY, counter INTEGER DEFAULT 0)")
            .await
            .unwrap();
        ctx.pg
            .execute("ALTER TABLE chaos_rapid REPLICA IDENTITY FULL")
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "chaos_rapid")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Insert row
        ctx.pg
            .execute("INSERT INTO chaos_rapid (counter) VALUES (0)")
            .await
            .unwrap();

        // Rapid updates to same row
        let update_count = 50;
        for i in 1..=update_count {
            ctx.pg
                .execute(&format!(
                    "UPDATE chaos_rapid SET counter = {} WHERE id = 1",
                    i
                ))
                .await
                .unwrap();
        }

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 5)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.chaos_rapid",
                1 + update_count, // 1 insert + updates
                Duration::from_secs(30),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events
            .assert()
            .has_count(1 + update_count)
            .has_op_count(CdcOp::Insert, 1)
            .has_op_count(CdcOp::Update, update_count);

        // Verify final counter value in last update
        let last_update = events
            .iter()
            .rfind(|e| matches!(e.op, CdcOp::Update))
            .unwrap();
        let counter = last_update
            .after
            .as_ref()
            .unwrap()
            .get("counter")
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(counter, update_count.to_string());

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Test CDC handles table with many columns
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_wide_table() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        // Create table with many columns
        let column_count = 50;
        let columns: Vec<String> = (0..column_count)
            .map(|i| format!("col_{} TEXT", i))
            .collect();

        ctx.pg
            .execute(&format!(
                "CREATE TABLE chaos_wide (id SERIAL PRIMARY KEY, {})",
                columns.join(", ")
            ))
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "chaos_wide")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Insert row with all columns populated
        let values: Vec<String> = (0..column_count)
            .map(|i| format!("'value_{}'", i))
            .collect();

        ctx.pg
            .execute(&format!(
                "INSERT INTO chaos_wide ({}) VALUES ({})",
                (0..column_count)
                    .map(|i| format!("col_{}", i))
                    .collect::<Vec<_>>()
                    .join(", "),
                values.join(", ")
            ))
            .await
            .unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 3)).await;

        let messages = rivven
            .wait_for_messages("cdc.postgres.public.chaos_wide", 1, Duration::from_secs(15))
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(1);

        // Verify all columns captured
        let after = events[0].after.as_ref().unwrap();
        for i in 0..column_count {
            let col_name = format!("col_{}", i);
            assert!(
                after.get(&col_name).is_some(),
                "Column {} should be captured",
                col_name
            );
        }

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }
}

// ============================================================================
// Performance Regression Tests
// ============================================================================

mod performance_tests {
    use super::*;

    /// Baseline performance test - measures events/sec for comparison
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_baseline_throughput() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        ctx.pg.create_test_table("perf_baseline").await.unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "perf_baseline")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        let event_count = 500;
        let start = Instant::now();

        // Bulk insert
        for i in 0..event_count {
            ctx.pg.execute(&format!(
                "INSERT INTO perf_baseline (name, email, age) VALUES ('User{}', 'user{}@test.com', {})",
                i, i, 20 + (i % 50)
            )).await.unwrap();
        }

        let insert_duration = start.elapsed();

        // Wait for all events
        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.perf_baseline",
                event_count,
                Duration::from_secs(60),
            )
            .await
            .unwrap();

        let total_duration = start.elapsed();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(event_count);

        let throughput = event_count as f64 / total_duration.as_secs_f64();

        println!("\n📊 Performance Baseline:");
        println!("   Events: {}", event_count);
        println!("   Insert time: {:?}", insert_duration);
        println!("   Total time: {:?}", total_duration);
        println!("   Throughput: {:.0} events/sec", throughput);

        // Baseline assertion: should achieve at least 100 events/sec
        assert!(
            throughput >= 100.0,
            "Throughput {:.0} events/sec is below baseline of 100 events/sec",
            throughput
        );

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Test latency of individual events
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_event_latency_baseline() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        ctx.pg.create_test_table("perf_latency").await.unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "perf_latency")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Measure latency for individual events
        let mut latencies = Vec::new();
        let samples = 20;

        for i in 0..samples {
            let start = Instant::now();

            ctx.pg
                .execute(&format!(
                    "INSERT INTO perf_latency (name, email) VALUES ('Latency{}', 'lat{}@test.com')",
                    i, i
                ))
                .await
                .unwrap();

            // Wait for this specific event
            let _ = rivven
                .wait_for_messages(
                    "cdc.postgres.public.perf_latency",
                    i + 1,
                    Duration::from_secs(10),
                )
                .await
                .unwrap();

            let latency = start.elapsed();
            latencies.push(latency);
        }

        // Calculate statistics
        latencies.sort();
        let avg = latencies.iter().map(|d| d.as_millis()).sum::<u128>() / latencies.len() as u128;
        let p50 = latencies[latencies.len() / 2].as_millis();
        let p95 = latencies[(latencies.len() as f64 * 0.95) as usize].as_millis();
        let p99 = latencies[(latencies.len() as f64 * 0.99) as usize].as_millis();

        println!("\n📊 Event Latency Baseline:");
        println!("   Samples: {}", samples);
        println!("   Average: {}ms", avg);
        println!("   P50: {}ms", p50);
        println!("   P95: {}ms", p95);
        println!("   P99: {}ms", p99);

        // Latency baseline: P99 should be under 5 seconds (generous for CI)
        assert!(
            p99 < 5000,
            "P99 latency {}ms exceeds baseline of 5000ms",
            p99
        );

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }
}

// ============================================================================
// Edge Cases and Corner Cases
// ============================================================================

mod edge_case_tests {
    use super::*;

    /// Test empty string values
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_empty_string_values() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        ctx.pg
            .execute("CREATE TABLE edge_empty (id SERIAL PRIMARY KEY, name TEXT, description TEXT)")
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "edge_empty")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Insert with empty strings
        ctx.pg
            .execute("INSERT INTO edge_empty (name, description) VALUES ('', '')")
            .await
            .unwrap();
        ctx.pg
            .execute("INSERT INTO edge_empty (name, description) VALUES ('name', '')")
            .await
            .unwrap();
        ctx.pg
            .execute("INSERT INTO edge_empty (name, description) VALUES ('', 'desc')")
            .await
            .unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages("cdc.postgres.public.edge_empty", 3, Duration::from_secs(15))
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(3);

        // Verify empty strings are captured correctly
        let first_after = events[0].after.as_ref().unwrap();
        assert_eq!(first_after.get("name").unwrap().as_str().unwrap(), "");
        assert_eq!(
            first_after.get("description").unwrap().as_str().unwrap(),
            ""
        );

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Test numeric edge values
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_numeric_edge_values() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        ctx.pg
            .execute(
                "CREATE TABLE edge_numeric (
                id SERIAL PRIMARY KEY,
                small_int SMALLINT,
                big_int BIGINT,
                decimal_val NUMERIC(20, 10)
            )",
            )
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "edge_numeric")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Insert edge values
        ctx.pg.execute("INSERT INTO edge_numeric (small_int, big_int, decimal_val) VALUES (32767, 9223372036854775807, 1234567890.1234567890)").await.unwrap();
        ctx.pg.execute("INSERT INTO edge_numeric (small_int, big_int, decimal_val) VALUES (-32768, -9223372036854775808, -1234567890.1234567890)").await.unwrap();
        ctx.pg.execute("INSERT INTO edge_numeric (small_int, big_int, decimal_val) VALUES (0, 0, 0.0000000000)").await.unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.edge_numeric",
                3,
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(3);

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Test reserved SQL keywords as identifiers
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_reserved_keywords_as_identifiers() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        // Use quoted identifiers for reserved keywords
        ctx.pg
            .execute(
                r#"CREATE TABLE edge_keywords (
                id SERIAL PRIMARY KEY,
                "select" TEXT,
                "from" TEXT,
                "where" TEXT,
                "order" TEXT
            )"#,
            )
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "edge_keywords")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        ctx.pg.execute(
            r#"INSERT INTO edge_keywords ("select", "from", "where", "order") VALUES ('sel', 'frm', 'whr', 'ord')"#
        ).await.unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.edge_keywords",
                1,
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(1);

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Test table with composite primary key
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_composite_primary_key() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        ctx.pg
            .execute(
                "CREATE TABLE edge_composite_pk (
                tenant_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                name TEXT,
                PRIMARY KEY (tenant_id, user_id)
            )",
            )
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "edge_composite_pk")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Insert rows with composite key
        ctx.pg.execute("INSERT INTO edge_composite_pk (tenant_id, user_id, name) VALUES (1, 1, 'User 1.1')").await.unwrap();
        ctx.pg.execute("INSERT INTO edge_composite_pk (tenant_id, user_id, name) VALUES (1, 2, 'User 1.2')").await.unwrap();
        ctx.pg.execute("INSERT INTO edge_composite_pk (tenant_id, user_id, name) VALUES (2, 1, 'User 2.1')").await.unwrap();

        // Update by composite key
        ctx.pg
            .execute(
                "UPDATE edge_composite_pk SET name = 'Updated' WHERE tenant_id = 1 AND user_id = 1",
            )
            .await
            .unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.edge_composite_pk",
                4, // 3 inserts + 1 update
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events
            .assert()
            .has_count(4)
            .has_op_count(CdcOp::Insert, 3)
            .has_op_count(CdcOp::Update, 1);

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Test table without primary key
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_table_without_primary_key() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        ctx.pg
            .execute("CREATE TABLE edge_no_pk (name TEXT, value INTEGER)")
            .await
            .unwrap();
        // REPLICA IDENTITY FULL required for updates/deletes on tables without PK
        ctx.pg
            .execute("ALTER TABLE edge_no_pk REPLICA IDENTITY FULL")
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "edge_no_pk")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        ctx.pg
            .execute("INSERT INTO edge_no_pk (name, value) VALUES ('test1', 100)")
            .await
            .unwrap();
        ctx.pg
            .execute("INSERT INTO edge_no_pk (name, value) VALUES ('test2', 200)")
            .await
            .unwrap();
        ctx.pg
            .execute("UPDATE edge_no_pk SET value = 150 WHERE name = 'test1'")
            .await
            .unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages("cdc.postgres.public.edge_no_pk", 3, Duration::from_secs(15))
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(3);

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    /// Test binary data (BYTEA)
    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_binary_data() {
        init_test_logging();
        let pg = PostgresTestContainer::start().await.unwrap();
        let ctx = TestContext::new(Arc::new(pg)).await.unwrap();

        ctx.pg
            .execute("CREATE TABLE edge_binary (id SERIAL PRIMARY KEY, data BYTEA)")
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "edge_binary")
            .await
            .unwrap();
        rivven
            .start_cdc(
                &ctx.pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Insert binary data using hex encoding
        ctx.pg
            .execute("INSERT INTO edge_binary (data) VALUES (E'\\\\x48656c6c6f')")
            .await
            .unwrap(); // "Hello"
        ctx.pg
            .execute("INSERT INTO edge_binary (data) VALUES (E'\\\\x00010203')")
            .await
            .unwrap(); // Binary with nulls

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.edge_binary",
                2,
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(2);

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Simple SQL string escaping
fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}
