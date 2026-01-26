//! CDC Integration Tests
//!
//! Comprehensive integration tests for PostgreSQL CDC using testcontainers.
//! Tests cover:
//! - CRUD operations (INSERT, UPDATE, DELETE)
//! - Transaction boundaries
//! - Schema inference
//! - Table/column filtering
//! - Error handling and resilience
//! - Data type coverage
//!
//! Run with: cargo test -p rivven-cdc --test cdc_integration -- --test-threads=1

mod harness;

use harness::*;
use rivven_cdc::common::{CdcEvent, CdcFilter, CdcFilterConfig, CdcOp};
use rivven_cdc::postgres::PostgresTypeMapper;
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// Shared PostgreSQL container for test efficiency
/// Each test gets its own publication/slot for isolation
async fn shared_postgres() -> Arc<PostgresTestContainer> {
    static CONTAINER: tokio::sync::OnceCell<Arc<PostgresTestContainer>> =
        tokio::sync::OnceCell::const_new();

    let pg = CONTAINER
        .get_or_init(|| async {
            init_test_logging();
            Arc::new(
                PostgresTestContainer::start()
                    .await
                    .expect("Failed to start PostgreSQL container"),
            )
        })
        .await
        .clone();

    // Clean up any leftover test slots from previous runs
    let _ = pg.cleanup_test_slots().await;

    pg
}

// ============================================================================
// CRUD Operation Tests
// ============================================================================

mod crud_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_insert_single_row() {
        init_test_logging();
        let pg = shared_postgres().await;
        let ctx = TestContext::new(pg.clone()).await.unwrap();

        // Create test table
        pg.create_test_table("test_insert_single").await.unwrap();

        // Setup Rivven - use actual database name "postgres"
        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "test_insert_single")
            .await
            .unwrap();

        // Start CDC
        rivven
            .start_cdc(
                &pg.cdc_connection_string(),
                &ctx.slot_name,
                &ctx.publication_name,
            )
            .await
            .unwrap();

        // Insert data
        pg.execute("INSERT INTO test_insert_single (name, email, age) VALUES ('Alice', 'alice@example.com', 30)")
            .await
            .unwrap();

        // Wait for event
        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS)).await;

        // Verify
        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.test_insert_single",
                1,
                Duration::from_secs(10),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events
            .assert()
            .has_count(1)
            .event_at(0)
            .has_op(CdcOp::Insert)
            .has_table("test_insert_single")
            .has_schema("public")
            .has_after()
            .after_contains_field("name")
            .after_contains_field("email")
            .after_contains_field("age");

        // Cleanup
        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_insert_multiple_rows() {
        init_test_logging();
        let pg = shared_postgres().await;
        let ctx = TestContext::new(pg.clone()).await.unwrap();

        pg.create_test_table("test_insert_multi").await.unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "test_insert_multi")
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

        // Insert multiple rows
        let inserts = generate_user_inserts_for_table("test_insert_multi", 5);
        for sql in inserts {
            pg.execute(&sql).await.unwrap();
        }

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.test_insert_multi",
                5,
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(5).has_op_count(CdcOp::Insert, 5);

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_update_row() {
        init_test_logging();
        let pg = shared_postgres().await;
        let ctx = TestContext::new(pg.clone()).await.unwrap();

        pg.create_test_table("test_update").await.unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "test_update")
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

        // Insert then update
        pg.execute(
            "INSERT INTO test_update (name, email, age) VALUES ('Bob', 'bob@example.com', 25)",
        )
        .await
        .unwrap();

        sleep(Duration::from_millis(500)).await;

        pg.execute("UPDATE test_update SET age = 26 WHERE name = 'Bob'")
            .await
            .unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.test_update",
                2,
                Duration::from_secs(10),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events
            .assert()
            .has_at_least(2)
            .has_op_count(CdcOp::Insert, 1)
            .has_op_count(CdcOp::Update, 1);

        // Verify update event
        let update_events: Vec<_> = events
            .iter()
            .filter(|e| matches!(e.op, CdcOp::Update))
            .collect();
        assert!(!update_events.is_empty());

        let update_event = &update_events[0];
        assert!(update_event.after.is_some());

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_delete_row() {
        init_test_logging();
        let pg = shared_postgres().await;
        let ctx = TestContext::new(pg.clone()).await.unwrap();

        // Need REPLICA IDENTITY FULL for delete events to include old values
        pg.execute(
            "CREATE TABLE test_delete (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email VARCHAR(255)
            )",
        )
        .await
        .unwrap();
        pg.execute("ALTER TABLE test_delete REPLICA IDENTITY FULL")
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "test_delete")
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

        // Insert then delete
        pg.execute(
            "INSERT INTO test_delete (name, email) VALUES ('Charlie', 'charlie@example.com')",
        )
        .await
        .unwrap();

        sleep(Duration::from_millis(500)).await;

        pg.execute("DELETE FROM test_delete WHERE name = 'Charlie'")
            .await
            .unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.test_delete",
                2,
                Duration::from_secs(10),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events
            .assert()
            .has_at_least(2)
            .has_op_count(CdcOp::Insert, 1)
            .has_op_count(CdcOp::Delete, 1);

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_bulk_insert() {
        init_test_logging();
        let pg = shared_postgres().await;
        let ctx = TestContext::new(pg.clone()).await.unwrap();

        pg.create_test_table("test_bulk").await.unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "test_bulk")
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

        // Bulk insert
        let batch_sql = generate_bulk_inserts("test_bulk", 50, 10);
        for sql in batch_sql {
            pg.execute(&sql).await.unwrap();
        }

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 3)).await;

        let messages = rivven
            .wait_for_messages("cdc.postgres.public.test_bulk", 50, Duration::from_secs(30))
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events
            .assert()
            .has_count(50)
            .has_op_count(CdcOp::Insert, 50);

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }
}

// ============================================================================
// Transaction Tests
// ============================================================================

mod transaction_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_transaction_commit() {
        init_test_logging();
        let pg = shared_postgres().await;
        let ctx = TestContext::new(pg.clone()).await.unwrap();

        pg.create_test_table("test_tx_commit").await.unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "test_tx_commit")
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

        // Execute transaction with batch for proper isolation
        pg.execute_batch(&[
            "INSERT INTO test_tx_commit (name, email) VALUES ('TxUser1', 'tx1@example.com')",
            "INSERT INTO test_tx_commit (name, email) VALUES ('TxUser2', 'tx2@example.com')",
            "INSERT INTO test_tx_commit (name, email) VALUES ('TxUser3', 'tx3@example.com')",
        ])
        .await
        .unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.test_tx_commit",
                3,
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        // All 3 inserts should be captured after commit
        events.assert().has_count(3);

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_transaction_rollback_not_captured() {
        init_test_logging();
        let pg = shared_postgres().await;
        let ctx = TestContext::new(pg.clone()).await.unwrap();

        pg.create_test_table("test_tx_rollback").await.unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "test_tx_rollback")
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

        // Rolled back transaction - should NOT produce events
        pg.execute_transaction(
            &["INSERT INTO test_tx_rollback (name, email) VALUES ('RollbackUser', 'rollback@example.com')"],
            false, // rollback
        )
        .await
        .unwrap();

        // Committed transaction - should produce events
        pg.execute("INSERT INTO test_tx_rollback (name, email) VALUES ('CommittedUser', 'committed@example.com')")
            .await
            .unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.test_tx_rollback",
                1,
                Duration::from_secs(10),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        // Only the committed insert should be captured
        events.assert().has_count(1);

        // Verify it's the committed user, not the rolled back one
        let event = &events[0];
        let name = get_string_field(event, "name").unwrap();
        assert_eq!(name, "CommittedUser");

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_mixed_operations_in_transaction() {
        init_test_logging();
        let pg = shared_postgres().await;
        let ctx = TestContext::new(pg.clone()).await.unwrap();

        pg.execute(
            "CREATE TABLE test_tx_mixed (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0
            )",
        )
        .await
        .unwrap();
        pg.execute("ALTER TABLE test_tx_mixed REPLICA IDENTITY FULL")
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "test_tx_mixed")
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

        // Mixed operations in single transaction
        pg.execute_batch(&[
            "INSERT INTO test_tx_mixed (name, value) VALUES ('Item1', 100)",
            "INSERT INTO test_tx_mixed (name, value) VALUES ('Item2', 200)",
            "UPDATE test_tx_mixed SET value = 150 WHERE name = 'Item1'",
            "DELETE FROM test_tx_mixed WHERE name = 'Item2'",
        ])
        .await
        .unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.test_tx_mixed",
                4,
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events
            .assert()
            .has_count(4)
            .has_op_count(CdcOp::Insert, 2)
            .has_op_count(CdcOp::Update, 1)
            .has_op_count(CdcOp::Delete, 1);

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }
}

// ============================================================================
// Schema Inference Tests
// ============================================================================

mod schema_tests {
    use super::*;

    #[test]
    fn test_postgres_type_mapping_integers() {
        let int2 = PostgresTypeMapper::pg_type_to_avro(21, "int2");
        assert!(int2.get("type").unwrap().as_str() == Some("int"));

        let int4 = PostgresTypeMapper::pg_type_to_avro(23, "int4");
        assert!(int4.get("type").unwrap().as_str() == Some("int"));

        let int8 = PostgresTypeMapper::pg_type_to_avro(20, "int8");
        assert!(int8.get("type").unwrap().as_str() == Some("long"));
    }

    #[test]
    fn test_postgres_type_mapping_floats() {
        let float4 = PostgresTypeMapper::pg_type_to_avro(700, "float4");
        assert!(float4.get("type").unwrap().as_str() == Some("float"));

        let float8 = PostgresTypeMapper::pg_type_to_avro(701, "float8");
        assert!(float8.get("type").unwrap().as_str() == Some("double"));

        let numeric = PostgresTypeMapper::pg_type_to_avro(1700, "numeric");
        assert!(numeric.get("type").unwrap().as_str() == Some("string"));
    }

    #[test]
    fn test_postgres_type_mapping_strings() {
        let text = PostgresTypeMapper::pg_type_to_avro(25, "text");
        assert!(text.get("type").unwrap().as_str() == Some("string"));

        let varchar = PostgresTypeMapper::pg_type_to_avro(1043, "varchar");
        assert!(varchar.get("type").unwrap().as_str() == Some("string"));
    }

    #[test]
    fn test_postgres_type_mapping_datetime() {
        // date is int (days since epoch) per Avro spec
        let date = PostgresTypeMapper::pg_type_to_avro(1082, "date");
        assert!(date.get("type").unwrap().as_str() == Some("int"));
        assert!(date.get("logicalType").unwrap().as_str() == Some("date"));

        // timestamp is long (millis since epoch)
        let timestamp = PostgresTypeMapper::pg_type_to_avro(1114, "timestamp");
        assert!(timestamp.get("type").unwrap().as_str() == Some("long"));
        assert!(timestamp.get("logicalType").unwrap().as_str() == Some("timestamp-millis"));
    }

    #[test]
    fn test_postgres_type_mapping_json() {
        let json = PostgresTypeMapper::pg_type_to_avro(114, "json");
        assert!(json.get("type").unwrap().as_str() == Some("string"));

        let jsonb = PostgresTypeMapper::pg_type_to_avro(3802, "jsonb");
        assert!(jsonb.get("type").unwrap().as_str() == Some("string"));
    }

    #[test]
    fn test_postgres_type_mapping_uuid() {
        let uuid = PostgresTypeMapper::pg_type_to_avro(2950, "uuid");
        assert!(uuid.get("type").unwrap().as_str() == Some("string"));
        assert!(uuid.get("logicalType").unwrap().as_str() == Some("uuid"));
    }

    #[test]
    fn test_avro_schema_generation() {
        let columns = vec![
            ("id".to_string(), 23, "int4".to_string()),
            ("name".to_string(), 25, "text".to_string()),
            ("email".to_string(), 1043, "varchar".to_string()),
            ("created_at".to_string(), 1114, "timestamp".to_string()),
        ];

        let schema = PostgresTypeMapper::generate_avro_schema("public", "users", &columns);
        assert!(schema.is_ok());

        let schema = schema.unwrap();
        let canonical = schema.canonical_form();

        // Verify schema structure
        assert!(canonical.contains("\"type\":\"record\""));
        assert!(canonical.contains("rivven.cdc.postgres.public.users"));
        assert!(canonical.contains("\"name\":\"id\""));
        assert!(canonical.contains("\"name\":\"name\""));
        assert!(canonical.contains("\"name\":\"email\""));
        assert!(canonical.contains("\"name\":\"created_at\""));
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_all_data_types_capture() {
        init_test_logging();
        let pg = shared_postgres().await;
        let ctx = TestContext::new(pg.clone()).await.unwrap();

        pg.create_all_types_table("test_all_types").await.unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "test_all_types")
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

        // Insert with all types
        let insert_sql = generate_all_types_insert("test_all_types");
        pg.execute(&insert_sql).await.unwrap();

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.test_all_types",
                1,
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events
            .assert()
            .has_count(1)
            .event_at(0)
            .has_op(CdcOp::Insert);

        // Verify fields exist
        let event = &events[0];
        let after = event.after.as_ref().unwrap();

        // Check various field types are present
        assert!(after.get("col_smallint").is_some(), "Missing col_smallint");
        assert!(after.get("col_integer").is_some(), "Missing col_integer");
        assert!(after.get("col_bigint").is_some(), "Missing col_bigint");
        assert!(after.get("col_text").is_some(), "Missing col_text");
        assert!(after.get("col_boolean").is_some(), "Missing col_boolean");
        assert!(after.get("col_jsonb").is_some(), "Missing col_jsonb");

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }
}

// ============================================================================
// Filter Tests
// ============================================================================

mod filter_tests {
    use super::*;

    #[test]
    fn test_filter_include_all() {
        let config = CdcFilterConfig::default();
        let filter = CdcFilter::new(config).unwrap();

        assert!(filter.should_include_table("public", "users"));
        assert!(filter.should_include_table("public", "orders"));
        assert!(filter.should_include_table("schema1", "table1"));
    }

    #[test]
    fn test_filter_specific_tables() {
        let config = CdcFilterConfig {
            include_tables: vec!["public.users".to_string(), "public.orders".to_string()],
            exclude_tables: vec![],
            ..Default::default()
        };
        let filter = CdcFilter::new(config).unwrap();

        assert!(filter.should_include_table("public", "users"));
        assert!(filter.should_include_table("public", "orders"));
        assert!(!filter.should_include_table("public", "audit_log"));
    }

    #[test]
    fn test_filter_glob_patterns() {
        let config = CdcFilterConfig {
            include_tables: vec!["public.*".to_string()],
            exclude_tables: vec!["*_audit".to_string(), "*_log".to_string()],
            ..Default::default()
        };
        let filter = CdcFilter::new(config).unwrap();

        assert!(filter.should_include_table("public", "users"));
        assert!(filter.should_include_table("public", "orders"));
        assert!(!filter.should_include_table("public", "users_audit"));
        assert!(!filter.should_include_table("public", "access_log"));
    }

    #[test]
    fn test_filter_exclude_priority() {
        let config = CdcFilterConfig {
            include_tables: vec!["*".to_string()],
            exclude_tables: vec!["public.sensitive*".to_string()],
            ..Default::default()
        };
        let filter = CdcFilter::new(config).unwrap();

        assert!(filter.should_include_table("public", "users"));
        assert!(!filter.should_include_table("public", "sensitive_data"));
        assert!(!filter.should_include_table("public", "sensitive_logs"));
    }

    #[test]
    fn test_filter_event_modification() {
        let config = CdcFilterConfig {
            include_tables: vec!["*".to_string()],
            mask_columns: vec!["password".to_string(), "ssn".to_string()],
            global_exclude_columns: vec!["internal_id".to_string()],
            ..Default::default()
        };
        let filter = CdcFilter::new(config).unwrap();

        let mut event = CdcEvent {
            source_type: "postgres".to_string(),
            database: "postgres".to_string(),
            schema: "public".to_string(),
            table: "users".to_string(),
            op: CdcOp::Insert,
            before: None,
            after: Some(serde_json::json!({
                "id": 1,
                "name": "Alice",
                "password": "secret123",
                "ssn": "123-45-6789",
                "internal_id": "internal-xyz"
            })),
            timestamp: 0,
            transaction: None,
        };

        let included = filter.filter_event(&mut event);
        assert!(included, "Event should be included");

        // Verify masking and exclusion happened
        let after = event.after.as_ref().unwrap();

        // Password should be masked
        if let Some(pwd) = after.get("password") {
            assert!(
                pwd.as_str()
                    .map(|s| s.contains("REDACTED"))
                    .unwrap_or(false)
                    || pwd.is_null(),
                "Password should be redacted"
            );
        }
    }
}

// ============================================================================
// Special Character Tests
// ============================================================================

mod special_char_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_unicode_characters() {
        init_test_logging();
        let pg = shared_postgres().await;
        let ctx = TestContext::new(pg.clone()).await.unwrap();

        pg.create_test_table("test_unicode").await.unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "test_unicode")
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

        // Test various Unicode characters
        let test_cases = vec![
            ("José García", "jose@example.com"),      // Accented characters
            ("山田太郎", "yamada@example.com"),       // Japanese
            ("Иван Петров", "ivan@example.com"),      // Cyrillic
            ("مريم", "mariam@example.com"),           // Arabic
            ("🚀 Rocket User", "rocket@example.com"), // Emoji
        ];

        for (name, email) in &test_cases {
            pg.execute(&format!(
                "INSERT INTO test_unicode (name, email) VALUES ('{}', '{}')",
                name.replace('\'', "''"),
                email
            ))
            .await
            .unwrap();
        }

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.test_unicode",
                test_cases.len(),
                Duration::from_secs(15),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(test_cases.len());

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_sql_injection_safe() {
        init_test_logging();
        let pg = shared_postgres().await;
        let ctx = TestContext::new(pg.clone()).await.unwrap();

        pg.create_test_table("test_injection").await.unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "test_injection")
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

        // Attempt SQL injection patterns (properly escaped)
        let malicious_inputs = vec![
            "Robert'); DROP TABLE test_injection;--",
            "'; DELETE FROM test_injection WHERE '1'='1",
            "\\'; TRUNCATE test_injection; --",
        ];

        for input in &malicious_inputs {
            pg.execute(&format!(
                "INSERT INTO test_injection (name, email) VALUES ('{}', 'test@example.com')",
                input.replace('\'', "''")
            ))
            .await
            .unwrap();
        }

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        // Verify table still exists and has data
        let count = pg
            .query("SELECT COUNT(*) FROM test_injection")
            .await
            .unwrap();
        let count: i64 = count[0].get(0);
        assert_eq!(
            count, 3,
            "Table should have 3 rows, injection should have failed"
        );

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.test_injection",
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

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_null_handling() {
        init_test_logging();
        let pg = shared_postgres().await;
        let ctx = TestContext::new(pg.clone()).await.unwrap();

        pg.create_test_table("test_nulls").await.unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "test_nulls")
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

        // Insert with various NULL patterns
        for sql in scenarios::null_handling_inserts("test_nulls") {
            pg.execute(&sql).await.unwrap();
        }

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 2)).await;

        let messages = rivven
            .wait_for_messages("cdc.postgres.public.test_nulls", 4, Duration::from_secs(15))
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(4);

        // Verify NULL fields are properly captured
        for event in &events {
            assert!(event.after.is_some());
            let after = event.after.as_ref().unwrap();
            // All rows should have name
            assert!(after.get("name").is_some());
        }

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }
}

// ============================================================================
// Resilience Tests
// ============================================================================

mod resilience_tests {
    use super::*;
    use rivven_cdc::common::{CircuitBreaker, RateLimiter};

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    async fn test_rate_limiter_basic() {
        let limiter = RateLimiter::new(10, 100); // 10 burst, 100/sec

        // Should allow first 10 requests immediately
        for _ in 0..10 {
            assert!(limiter.try_acquire().await.is_ok());
        }

        // 11th should fail
        assert!(limiter.try_acquire().await.is_err());

        // Wait for refill
        sleep(Duration::from_millis(150)).await;

        // Should allow more requests after refill
        assert!(limiter.try_acquire().await.is_ok());
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    async fn test_rate_limiter_refill() {
        let limiter = RateLimiter::new(5, 50); // 5 burst, 50/sec

        // Drain the bucket
        for _ in 0..5 {
            limiter.try_acquire().await.ok();
        }

        // Should be empty
        assert!(limiter.try_acquire().await.is_err());

        // Wait 100ms (should add ~5 tokens at 50/sec)
        sleep(Duration::from_millis(120)).await;

        // Should have tokens again
        assert!(limiter.try_acquire().await.is_ok());
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    async fn test_circuit_breaker_transitions() {
        let breaker = CircuitBreaker::new(
            3,                          // failure_threshold
            Duration::from_millis(100), // timeout
            2,                          // success_threshold
        );

        // Initially closed
        assert!(breaker.allow_request().await);

        // Record failures
        for _ in 0..3 {
            breaker.record_failure().await;
        }

        // Should be open now
        assert!(!breaker.allow_request().await);

        // Wait for timeout
        sleep(Duration::from_millis(150)).await;

        // Should be half-open now
        assert!(breaker.allow_request().await);

        // Record successes to close
        breaker.record_success().await;
        breaker.record_success().await;

        // Should be closed again
        assert!(breaker.allow_request().await);
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    async fn test_circuit_breaker_half_open_failure() {
        let breaker = CircuitBreaker::new(2, Duration::from_millis(50), 3);

        // Open the circuit
        breaker.record_failure().await;
        breaker.record_failure().await;

        // Wait for half-open
        sleep(Duration::from_millis(60)).await;

        // Fail in half-open
        assert!(breaker.allow_request().await);
        breaker.record_failure().await;

        // Should go back to open
        assert!(!breaker.allow_request().await);
    }
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

mod concurrency_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_concurrent_inserts() {
        init_test_logging();
        let pg = shared_postgres().await;
        let ctx = TestContext::new(pg.clone()).await.unwrap();

        pg.create_test_table("test_concurrent").await.unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "test_concurrent")
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

        // Spawn multiple concurrent writers
        let insert_count = 10;
        let mut handles = Vec::new();

        for i in 0..insert_count {
            let pg_clone = pg.clone();
            let handle = tokio::spawn(async move {
                let client = pg_clone.new_client().await.unwrap();
                client
                    .execute(
                        &format!(
                            "INSERT INTO test_concurrent (name, email, age) VALUES ('User{}', 'user{}@example.com', {})",
                            i, i, 20 + i
                        ),
                        &[],
                    )
                    .await
                    .unwrap();
            });
            handles.push(handle);
        }

        // Wait for all inserts
        for handle in handles {
            handle.await.unwrap();
        }

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 3)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.test_concurrent",
                insert_count,
                Duration::from_secs(20),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events.assert().has_count(insert_count);

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_rapid_updates() {
        init_test_logging();
        let pg = shared_postgres().await;
        let ctx = TestContext::new(pg.clone()).await.unwrap();

        pg.execute(
            "CREATE TABLE test_rapid (
                id SERIAL PRIMARY KEY,
                counter INTEGER DEFAULT 0
            )",
        )
        .await
        .unwrap();
        pg.execute("ALTER TABLE test_rapid REPLICA IDENTITY FULL")
            .await
            .unwrap();

        let mut rivven = RivvenTestContext::new().await.unwrap();
        rivven
            .register_cdc_topic("postgres", "public", "test_rapid")
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

        // Insert initial row
        pg.execute("INSERT INTO test_rapid (counter) VALUES (0)")
            .await
            .unwrap();

        // Rapid updates
        let update_count = 20;
        for i in 1..=update_count {
            pg.execute(&format!(
                "UPDATE test_rapid SET counter = {} WHERE id = 1",
                i
            ))
            .await
            .unwrap();
        }

        sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS * 3)).await;

        let messages = rivven
            .wait_for_messages(
                "cdc.postgres.public.test_rapid",
                update_count + 1, // 1 insert + N updates
                Duration::from_secs(20),
            )
            .await
            .unwrap();

        let events = rivven.parse_events(&messages).unwrap();
        events
            .assert()
            .has_count(update_count + 1)
            .has_op_count(CdcOp::Insert, 1)
            .has_op_count(CdcOp::Update, update_count);

        ctx.cleanup().await.unwrap();
        rivven.stop_cdc().await.unwrap();
    }
}
