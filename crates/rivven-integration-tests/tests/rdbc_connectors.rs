//! RDBC Connector Integration Tests
//!
//! **REAL** integration tests for Rivven's RDBC source and sink connectors.
//! These tests verify that the query-based connectors correctly read from
//! and write to databases using connection pooling.
//!
//! Run with: cargo test -p rivven-integration-tests --test rdbc_connectors -- --nocapture
//!
//! Requirements: Docker must be running for testcontainers.

use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use rivven_connect::connectors::rdbc::{
    RdbcQueryMode, RdbcSink, RdbcSinkConfig, RdbcSource, RdbcSourceConfig, RdbcWriteMode,
};
use rivven_connect::traits::{ConfiguredCatalog, Sink, Source, SourceEvent};
use rivven_integration_tests::fixtures::TestPostgres;
use rivven_integration_tests::helpers::*;
use tokio::time::timeout;
use tracing::info;

// ============================================================================
// Helper Functions
// ============================================================================

/// Setup test table for RDBC source tests
async fn setup_rdbc_source_table(pg: &TestPostgres) -> Result<()> {
    let client = pg.connect().await?;

    client
        .batch_execute(
            r#"
            CREATE TABLE IF NOT EXISTS rdbc_source_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value INTEGER NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            
            -- Truncate to ensure clean state
            TRUNCATE TABLE rdbc_source_test;
            "#,
        )
        .await?;

    Ok(())
}

/// Setup test table for RDBC sink tests
async fn setup_rdbc_sink_table(pg: &TestPostgres) -> Result<()> {
    let client = pg.connect().await?;

    client
        .batch_execute(
            r#"
            CREATE TABLE IF NOT EXISTS rdbc_sink_test (
                id BIGINT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value BIGINT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            
            -- Truncate to ensure clean state
            TRUNCATE TABLE rdbc_sink_test;
            "#,
        )
        .await?;

    Ok(())
}

/// Insert test records for source tests
async fn insert_source_test_data(pg: &TestPostgres, count: i32) -> Result<()> {
    let client = pg.connect().await?;

    for i in 1..=count {
        client
            .execute(
                "INSERT INTO rdbc_source_test (name, value) VALUES ($1, $2)",
                &[&format!("item_{}", i), &(i * 10)],
            )
            .await?;
    }

    Ok(())
}

// ============================================================================
// Infrastructure Tests
// ============================================================================

/// Test that PostgreSQL container starts correctly for RDBC tests
#[tokio::test]
async fn test_rdbc_postgres_container() -> Result<()> {
    init_tracing();

    info!("Starting PostgreSQL container for RDBC tests...");
    let pg = TestPostgres::start().await?;

    // Verify connection
    let client = pg.connect().await?;
    let rows = client.query("SELECT 1 as value", &[]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("value"), 1);

    info!("RDBC PostgreSQL container test passed");
    Ok(())
}

/// Test table setup for RDBC connectors
#[tokio::test]
async fn test_rdbc_table_setup() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    setup_rdbc_source_table(&pg).await?;
    setup_rdbc_sink_table(&pg).await?;

    let client = pg.connect().await?;

    // Verify source table
    let rows = client
        .query(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'rdbc_source_test'",
            &[],
        )
        .await?;
    assert!(rows.len() >= 4);

    // Verify sink table
    let rows = client
        .query(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'rdbc_sink_test'",
            &[],
        )
        .await?;
    assert!(rows.len() >= 4);

    info!("RDBC table setup test passed");
    Ok(())
}

// ============================================================================
// RDBC Source Tests
// ============================================================================

/// Test RDBC source check operation
#[tokio::test]
async fn test_rdbc_source_check() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    setup_rdbc_source_table(&pg).await?;

    let config = RdbcSourceConfig {
        connection_url: pg.cdc_connection_url().into(),
        table: "rdbc_source_test".to_string(),
        mode: RdbcQueryMode::Bulk,
        pool_size: 2,
        ..Default::default()
    };

    let source = RdbcSource;
    let result = source.check(&config).await?;

    assert!(result.is_success(), "Check should succeed: {:?}", result);
    info!("RDBC source check test passed");
    Ok(())
}

/// Test RDBC source discover operation
#[tokio::test]
async fn test_rdbc_source_discover() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    setup_rdbc_source_table(&pg).await?;

    let config = RdbcSourceConfig {
        connection_url: pg.cdc_connection_url().into(),
        table: "rdbc_source_test".to_string(),
        mode: RdbcQueryMode::Bulk,
        pool_size: 1,
        ..Default::default()
    };

    let source = RdbcSource;
    let catalog = source.discover(&config).await?;

    assert!(!catalog.streams.is_empty());
    assert_eq!(catalog.streams[0].name, "rdbc_source_test");

    info!("RDBC source discover test passed");
    Ok(())
}

/// Test RDBC source bulk read (full refresh)
#[tokio::test]
async fn test_rdbc_source_bulk_read() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    setup_rdbc_source_table(&pg).await?;
    insert_source_test_data(&pg, 5).await?;

    let config = RdbcSourceConfig {
        connection_url: pg.cdc_connection_url().into(),
        table: "rdbc_source_test".to_string(),
        mode: RdbcQueryMode::Bulk,
        batch_size: 10,
        pool_size: 2,
        ..Default::default()
    };

    let source = RdbcSource;
    let catalog = ConfiguredCatalog::new();

    // Read with timeout (bulk mode should complete quickly)
    let mut stream = source.read(&config, &catalog, None).await?;

    let mut record_count = 0;
    let read_timeout = Duration::from_secs(5);

    // Collect records until we get a state event or timeout
    loop {
        match timeout(read_timeout, stream.next()).await {
            Ok(Some(Ok(event))) => match event.event_type {
                rivven_connect::traits::SourceEventType::Record => {
                    record_count += 1;
                    info!("Received record: {:?}", event.data);
                }
                rivven_connect::traits::SourceEventType::State => {
                    info!("Received state event, breaking");
                    break;
                }
                _ => {}
            },
            Ok(Some(Err(e))) => {
                anyhow::bail!("Error reading: {}", e);
            }
            Ok(None) => {
                info!("Stream ended");
                break;
            }
            Err(_) => {
                info!("Timeout reached (expected for bulk mode)");
                break;
            }
        }
    }

    assert_eq!(record_count, 5, "Should read 5 records");
    info!("RDBC source bulk read test passed");
    Ok(())
}

/// Test RDBC source incrementing mode
#[tokio::test]
async fn test_rdbc_source_incrementing() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    setup_rdbc_source_table(&pg).await?;

    // Insert initial data
    insert_source_test_data(&pg, 3).await?;

    let config = RdbcSourceConfig {
        connection_url: pg.cdc_connection_url().into(),
        table: "rdbc_source_test".to_string(),
        mode: RdbcQueryMode::Incrementing,
        incrementing_column: Some("id".to_string()),
        batch_size: 10,
        poll_interval_ms: 100, // Fast polling for tests
        pool_size: 2,
        ..Default::default()
    };

    let source = RdbcSource;
    let catalog = ConfiguredCatalog::new();

    let mut stream = source.read(&config, &catalog, None).await?;

    // Collect first batch of records
    let mut record_count = 0;
    let read_timeout = Duration::from_secs(3);

    loop {
        match timeout(read_timeout, stream.next()).await {
            Ok(Some(Ok(event))) => {
                if matches!(
                    event.event_type,
                    rivven_connect::traits::SourceEventType::Record
                ) {
                    record_count += 1;
                    info!("Incrementing mode: received record #{}", record_count);
                    if record_count >= 3 {
                        break;
                    }
                }
            }
            Ok(Some(Err(e))) => anyhow::bail!("Error: {}", e),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    assert_eq!(record_count, 3, "Should read 3 initial records");
    info!("RDBC source incrementing mode test passed");
    Ok(())
}

// ============================================================================
// RDBC Sink Tests
// ============================================================================

/// Test RDBC sink check operation
#[tokio::test]
async fn test_rdbc_sink_check() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    setup_rdbc_sink_table(&pg).await?;

    let config = RdbcSinkConfig {
        connection_url: pg.cdc_connection_url().into(),
        table: "rdbc_sink_test".to_string(),
        write_mode: RdbcWriteMode::Insert,
        pool_size: 4,
        ..Default::default()
    };

    let sink = RdbcSink;
    let result = sink.check(&config).await?;

    assert!(result.is_success(), "Check should succeed: {:?}", result);
    info!("RDBC sink check test passed");
    Ok(())
}

/// Test RDBC sink insert mode
#[tokio::test]
async fn test_rdbc_sink_insert() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    setup_rdbc_sink_table(&pg).await?;

    let config = RdbcSinkConfig {
        connection_url: pg.cdc_connection_url().into(),
        table: "rdbc_sink_test".to_string(),
        write_mode: RdbcWriteMode::Insert,
        batch_size: 10,
        batch_timeout_ms: 1000,
        pool_size: 2,
        ..Default::default()
    };

    // Create test events
    let events: Vec<SourceEvent> = (1..=5)
        .map(|i| {
            SourceEvent::record(
                "rdbc_sink_test",
                serde_json::json!({
                    "id": i,
                    "name": format!("sink_item_{}", i),
                    "value": i * 100
                }),
            )
        })
        .collect();

    let event_stream = futures::stream::iter(events).boxed();

    let sink = RdbcSink;
    let result = sink.write(&config, event_stream).await?;

    info!("Write result: {:?}", result);
    assert_eq!(result.records_written, 5, "Should write 5 records");

    // Verify data in database
    let client = pg.connect().await?;
    let rows = client
        .query("SELECT COUNT(*) as cnt FROM rdbc_sink_test", &[])
        .await?;
    let count: i64 = rows[0].get("cnt");
    assert_eq!(count, 5);

    info!("RDBC sink insert test passed");
    Ok(())
}

/// Test RDBC sink upsert mode
#[tokio::test]
async fn test_rdbc_sink_upsert() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    setup_rdbc_sink_table(&pg).await?;

    let config = RdbcSinkConfig {
        connection_url: pg.cdc_connection_url().into(),
        table: "rdbc_sink_test".to_string(),
        write_mode: RdbcWriteMode::Upsert,
        pk_columns: Some(vec!["id".to_string()]),
        batch_size: 10,
        batch_timeout_ms: 1000,
        pool_size: 2,
        ..Default::default()
    };

    // First insert
    let events: Vec<SourceEvent> = (1..=3)
        .map(|i| {
            SourceEvent::record(
                "rdbc_sink_test",
                serde_json::json!({
                    "id": i,
                    "name": format!("original_{}", i),
                    "value": i * 10
                }),
            )
        })
        .collect();

    let sink = RdbcSink;
    let event_stream = futures::stream::iter(events).boxed();
    let result = sink.write(&config, event_stream).await?;
    assert_eq!(result.records_written, 3);

    // Upsert with updated values
    let events: Vec<SourceEvent> = (1..=3)
        .map(|i| {
            SourceEvent::record(
                "rdbc_sink_test",
                serde_json::json!({
                    "id": i,
                    "name": format!("updated_{}", i),
                    "value": i * 100
                }),
            )
        })
        .collect();

    let event_stream = futures::stream::iter(events).boxed();
    let result = sink.write(&config, event_stream).await?;
    assert_eq!(result.records_written, 3);

    // Verify updates
    let client = pg.connect().await?;
    let rows = client
        .query("SELECT name, value FROM rdbc_sink_test WHERE id = 1", &[])
        .await?;
    let name: String = rows[0].get("name");
    let value: i64 = rows[0].get("value");

    assert_eq!(name, "updated_1");
    assert_eq!(value, 100);

    // Verify count (should still be 3, not 6)
    let rows = client
        .query("SELECT COUNT(*) as cnt FROM rdbc_sink_test", &[])
        .await?;
    let count: i64 = rows[0].get("cnt");
    assert_eq!(count, 3);

    info!("RDBC sink upsert test passed");
    Ok(())
}

/// Test RDBC sink transactional mode
#[tokio::test]
async fn test_rdbc_sink_transactional() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    setup_rdbc_sink_table(&pg).await?;

    let config = RdbcSinkConfig {
        connection_url: pg.cdc_connection_url().into(),
        table: "rdbc_sink_test".to_string(),
        write_mode: RdbcWriteMode::Insert,
        transactional: true, // Enable transactional mode
        batch_size: 10,
        batch_timeout_ms: 1000,
        pool_size: 2,
        ..Default::default()
    };

    let events: Vec<SourceEvent> = (1..=10)
        .map(|i| {
            SourceEvent::record(
                "rdbc_sink_test",
                serde_json::json!({
                    "id": i,
                    "name": format!("tx_item_{}", i),
                    "value": i
                }),
            )
        })
        .collect();

    let sink = RdbcSink;
    let event_stream = futures::stream::iter(events).boxed();
    let result = sink.write(&config, event_stream).await?;

    assert_eq!(result.records_written, 10);

    // Verify all records were written
    let client = pg.connect().await?;
    let rows = client
        .query("SELECT COUNT(*) as cnt FROM rdbc_sink_test", &[])
        .await?;
    let count: i64 = rows[0].get("cnt");
    assert_eq!(count, 10);

    info!("RDBC sink transactional test passed");
    Ok(())
}

// ============================================================================
// Connection Pool Tests
// ============================================================================

/// Test that connection pool works with larger pool size
#[tokio::test]
async fn test_rdbc_connection_pool() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    setup_rdbc_sink_table(&pg).await?;

    // Use a larger pool size
    let config = RdbcSinkConfig {
        connection_url: pg.cdc_connection_url().into(),
        table: "rdbc_sink_test".to_string(),
        write_mode: RdbcWriteMode::Insert,
        batch_size: 5,         // Smaller batches to exercise pool more
        batch_timeout_ms: 100, // Fast flush
        pool_size: 8,          // Larger pool
        ..Default::default()
    };

    // Create many events to exercise pool
    let events: Vec<SourceEvent> = (1..=50)
        .map(|i| {
            SourceEvent::record(
                "rdbc_sink_test",
                serde_json::json!({
                    "id": i,
                    "name": format!("pool_test_{}", i),
                    "value": i
                }),
            )
        })
        .collect();

    let sink = RdbcSink;
    let event_stream = futures::stream::iter(events).boxed();
    let result = sink.write(&config, event_stream).await?;

    assert_eq!(result.records_written, 50);

    // Verify all records
    let client = pg.connect().await?;
    let rows = client
        .query("SELECT COUNT(*) as cnt FROM rdbc_sink_test", &[])
        .await?;
    let count: i64 = rows[0].get("cnt");
    assert_eq!(count, 50);

    info!("RDBC connection pool test passed with {} records", count);
    Ok(())
}

/// Test pool handles minimum size correctly
#[tokio::test]
async fn test_rdbc_pool_min_size() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    setup_rdbc_source_table(&pg).await?;
    insert_source_test_data(&pg, 3).await?;

    // Minimum pool size of 1
    let config = RdbcSourceConfig {
        connection_url: pg.cdc_connection_url().into(),
        table: "rdbc_source_test".to_string(),
        mode: RdbcQueryMode::Bulk,
        pool_size: 1, // Minimum pool
        batch_size: 100,
        ..Default::default()
    };

    let source = RdbcSource;
    let result = source.check(&config).await?;
    assert!(result.is_success());

    info!("RDBC pool min size test passed");
    Ok(())
}

// ============================================================================
// End-to-End Pipeline Tests
// ============================================================================

/// Test source-to-sink data pipeline
#[tokio::test]
async fn test_rdbc_source_to_sink_pipeline() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;

    // Setup source and sink tables
    let client = pg.connect().await?;
    client
        .batch_execute(
            r#"
            CREATE TABLE pipeline_source (
                id SERIAL PRIMARY KEY,
                data VARCHAR(100) NOT NULL
            );
            
            CREATE TABLE pipeline_sink (
                id BIGINT PRIMARY KEY,
                data VARCHAR(100) NOT NULL
            );
            
            INSERT INTO pipeline_source (data) VALUES 
                ('alpha'), ('beta'), ('gamma'), ('delta'), ('epsilon');
            "#,
        )
        .await?;

    // Read from source
    let source_config = RdbcSourceConfig {
        connection_url: pg.cdc_connection_url().into(),
        table: "pipeline_source".to_string(),
        mode: RdbcQueryMode::Bulk,
        batch_size: 100,
        pool_size: 2,
        ..Default::default()
    };

    let source = RdbcSource;
    let catalog = ConfiguredCatalog::new();
    let mut stream = source.read(&source_config, &catalog, None).await?;

    // Collect source events
    let mut source_events = Vec::new();
    let read_timeout = Duration::from_secs(5);

    loop {
        match timeout(read_timeout, stream.next()).await {
            Ok(Some(Ok(event))) => {
                if matches!(
                    event.event_type,
                    rivven_connect::traits::SourceEventType::Record
                ) {
                    // Transform: change stream name for sink
                    let transformed = SourceEvent::record("pipeline_sink", event.data.clone());
                    source_events.push(transformed);
                }
                if matches!(
                    event.event_type,
                    rivven_connect::traits::SourceEventType::State
                ) {
                    break;
                }
            }
            Ok(Some(Err(e))) => anyhow::bail!("Source error: {}", e),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    assert_eq!(source_events.len(), 5, "Should read 5 source records");

    // Write to sink
    let sink_config = RdbcSinkConfig {
        connection_url: pg.cdc_connection_url().into(),
        table: "pipeline_sink".to_string(),
        write_mode: RdbcWriteMode::Insert,
        batch_size: 10,
        batch_timeout_ms: 1000,
        pool_size: 2,
        ..Default::default()
    };

    let sink = RdbcSink;
    let event_stream = futures::stream::iter(source_events).boxed();
    let result = sink.write(&sink_config, event_stream).await?;

    assert_eq!(result.records_written, 5);

    // Verify pipeline completed
    let rows = client
        .query("SELECT COUNT(*) as cnt FROM pipeline_sink", &[])
        .await?;
    let count: i64 = rows[0].get("cnt");
    assert_eq!(count, 5);

    info!("RDBC source-to-sink pipeline test passed");
    Ok(())
}

// ============================================================================
// Advanced Feature Tests
// ============================================================================

/// Test RDBC source timestamp mode
#[tokio::test]
async fn test_rdbc_source_timestamp() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;

    // Create table with timestamp column
    let client = pg.connect().await?;
    client
        .batch_execute(
            r#"
            CREATE TABLE timestamp_test (
                id SERIAL PRIMARY KEY,
                value INTEGER NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            INSERT INTO timestamp_test (value) VALUES (10), (20), (30);
            "#,
        )
        .await?;

    let config = RdbcSourceConfig {
        connection_url: pg.cdc_connection_url().into(),
        table: "timestamp_test".to_string(),
        mode: RdbcQueryMode::Timestamp,
        timestamp_column: Some("updated_at".to_string()),
        batch_size: 10,
        poll_interval_ms: 100,
        pool_size: 1,
        ..Default::default()
    };

    let source = RdbcSource;
    let catalog = ConfiguredCatalog::new();
    let mut stream = source.read(&config, &catalog, None).await?;

    let mut record_count = 0;
    let read_timeout = Duration::from_secs(3);

    loop {
        match timeout(read_timeout, stream.next()).await {
            Ok(Some(Ok(event))) => {
                if matches!(
                    event.event_type,
                    rivven_connect::traits::SourceEventType::Record
                ) {
                    record_count += 1;
                    if record_count >= 3 {
                        break;
                    }
                }
            }
            Ok(Some(Err(e))) => anyhow::bail!("Error: {}", e),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    assert_eq!(record_count, 3, "Should read 3 records in timestamp mode");
    info!("RDBC source timestamp mode test passed");
    Ok(())
}

/// Test RDBC sink delete event handling
#[tokio::test]
async fn test_rdbc_sink_delete() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;

    // Create table and insert data
    let client = pg.connect().await?;
    client
        .batch_execute(
            r#"
            CREATE TABLE delete_test (
                id BIGINT PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            );
            INSERT INTO delete_test (id, name) VALUES (1, 'keep'), (2, 'delete_me'), (3, 'keep');
            "#,
        )
        .await?;

    let config = RdbcSinkConfig {
        connection_url: pg.cdc_connection_url().into(),
        table: "delete_test".to_string(),
        write_mode: RdbcWriteMode::Upsert,
        pk_columns: Some(vec!["id".to_string()]),
        delete_enabled: true,
        batch_size: 10,
        batch_timeout_ms: 1000,
        pool_size: 2,
        ..Default::default()
    };

    // Create delete event for id=2
    let events = vec![SourceEvent::delete(
        "delete_test",
        serde_json::json!({ "id": 2 }),
    )];

    let sink = RdbcSink;
    let event_stream = futures::stream::iter(events).boxed();
    let _result = sink.write(&config, event_stream).await?;

    // Verify deletion
    let rows = client
        .query("SELECT COUNT(*) as cnt FROM delete_test", &[])
        .await?;
    let count: i64 = rows[0].get("cnt");
    assert_eq!(count, 2, "Should have 2 rows after delete");

    // Verify the correct row was deleted
    let rows = client
        .query("SELECT id FROM delete_test ORDER BY id", &[])
        .await?;
    let ids: Vec<i64> = rows.iter().map(|r| r.get("id")).collect();
    assert_eq!(ids, vec![1, 3]);

    info!("RDBC sink delete test passed");
    Ok(())
}

/// Test RDBC with schema qualification
#[tokio::test]
async fn test_rdbc_schema_qualification() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;

    // Create schema and table
    let client = pg.connect().await?;
    client
        .batch_execute(
            r#"
            CREATE SCHEMA IF NOT EXISTS custom_schema;
            CREATE TABLE custom_schema.qualified_table (
                id BIGINT PRIMARY KEY,
                data VARCHAR(100) NOT NULL
            );
            "#,
        )
        .await?;

    // Test sink with schema
    let config = RdbcSinkConfig {
        connection_url: pg.cdc_connection_url().into(),
        schema: Some("custom_schema".to_string()),
        table: "qualified_table".to_string(),
        write_mode: RdbcWriteMode::Insert,
        batch_size: 10,
        batch_timeout_ms: 1000,
        pool_size: 1,
        ..Default::default()
    };

    let events = vec![
        SourceEvent::record(
            "qualified_table",
            serde_json::json!({ "id": 1, "data": "test1" }),
        ),
        SourceEvent::record(
            "qualified_table",
            serde_json::json!({ "id": 2, "data": "test2" }),
        ),
    ];

    let sink = RdbcSink;
    let event_stream = futures::stream::iter(events).boxed();
    let result = sink.write(&config, event_stream).await?;

    assert_eq!(result.records_written, 2);

    // Verify data in schema-qualified table
    let rows = client
        .query(
            "SELECT COUNT(*) as cnt FROM custom_schema.qualified_table",
            &[],
        )
        .await?;
    let count: i64 = rows[0].get("cnt");
    assert_eq!(count, 2);

    info!("RDBC schema qualification test passed");
    Ok(())
}

/// Test RDBC source with state resume (offset tracking)
#[tokio::test]
async fn test_rdbc_source_state_resume() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;

    // Create table with data
    let client = pg.connect().await?;
    client
        .batch_execute(
            r#"
            CREATE TABLE resume_test (
                id SERIAL PRIMARY KEY,
                value INTEGER NOT NULL
            );
            INSERT INTO resume_test (value) VALUES (10), (20), (30), (40), (50);
            "#,
        )
        .await?;

    let config = RdbcSourceConfig {
        connection_url: pg.cdc_connection_url().into(),
        table: "resume_test".to_string(),
        mode: RdbcQueryMode::Incrementing,
        incrementing_column: Some("id".to_string()),
        batch_size: 2,
        poll_interval_ms: 100,
        pool_size: 1,
        ..Default::default()
    };

    // First read: get first 2 records
    let source = RdbcSource;
    let catalog = ConfiguredCatalog::new();
    let mut stream = source.read(&config, &catalog, None).await?;

    let mut first_batch_ids = Vec::new();
    let read_timeout = Duration::from_secs(3);

    loop {
        match timeout(read_timeout, stream.next()).await {
            Ok(Some(Ok(event))) => {
                if matches!(
                    event.event_type,
                    rivven_connect::traits::SourceEventType::Record
                ) {
                    if let Some(id) = event.data.get("id").and_then(|v| v.as_i64()) {
                        first_batch_ids.push(id);
                        if first_batch_ids.len() >= 2 {
                            break;
                        }
                    }
                }
            }
            Ok(Some(Err(e))) => anyhow::bail!("Error: {}", e),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    assert_eq!(
        first_batch_ids,
        vec![1, 2],
        "First batch should have ids 1, 2"
    );
    info!("RDBC source state resume test passed (first batch)");

    // TODO: Full state resume testing requires State serialization support
    // which would be implemented via the State struct

    Ok(())
}

/// Test RDBC error handling for invalid connection
#[tokio::test]
async fn test_rdbc_error_handling() -> Result<()> {
    init_tracing();

    let config = RdbcSourceConfig {
        connection_url: "postgres://invalid:invalid@nonexistent:5432/invalid"
            .to_string()
            .into(),
        table: "nonexistent".to_string(),
        mode: RdbcQueryMode::Bulk,
        pool_size: 1,
        ..Default::default()
    };

    let source = RdbcSource;
    let result = source.check(&config).await?;

    // Check should fail gracefully
    assert!(
        !result.is_success(),
        "Check should fail for invalid connection"
    );
    info!("RDBC error handling test passed");
    Ok(())
}

/// Stress test: large batch performance
#[tokio::test]
async fn test_rdbc_large_batch_performance() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    setup_rdbc_sink_table(&pg).await?;

    let config = RdbcSinkConfig {
        connection_url: pg.cdc_connection_url().into(),
        table: "rdbc_sink_test".to_string(),
        write_mode: RdbcWriteMode::Insert,
        batch_size: 100,
        batch_timeout_ms: 5000,
        pool_size: 4,
        ..Default::default()
    };

    // Create 500 events
    let events: Vec<SourceEvent> = (1..=500)
        .map(|i| {
            SourceEvent::record(
                "rdbc_sink_test",
                serde_json::json!({
                    "id": i,
                    "name": format!("stress_test_{}", i),
                    "value": i * 2
                }),
            )
        })
        .collect();

    let start = std::time::Instant::now();
    let sink = RdbcSink;
    let event_stream = futures::stream::iter(events).boxed();
    let result = sink.write(&config, event_stream).await?;
    let elapsed = start.elapsed();

    assert_eq!(result.records_written, 500);

    // Verify all records
    let client = pg.connect().await?;
    let rows = client
        .query("SELECT COUNT(*) as cnt FROM rdbc_sink_test", &[])
        .await?;
    let count: i64 = rows[0].get("cnt");
    assert_eq!(count, 500);

    info!(
        "RDBC large batch test passed: 500 records in {:?} ({:.0} records/sec)",
        elapsed,
        500.0 / elapsed.as_secs_f64()
    );
    Ok(())
}

/// Test RDBC sink update mode
#[tokio::test]
async fn test_rdbc_sink_update() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;

    let client = pg.connect().await?;
    client
        .batch_execute(
            r#"
            CREATE TABLE update_test (
                id BIGINT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                counter BIGINT NOT NULL DEFAULT 0
            );
            INSERT INTO update_test (id, name, counter) VALUES 
                (1, 'item1', 100),
                (2, 'item2', 200),
                (3, 'item3', 300);
            "#,
        )
        .await?;

    let config = RdbcSinkConfig {
        connection_url: pg.cdc_connection_url().into(),
        table: "update_test".to_string(),
        write_mode: RdbcWriteMode::Update,
        pk_columns: Some(vec!["id".to_string()]),
        batch_size: 10,
        batch_timeout_ms: 1000,
        pool_size: 2,
        ..Default::default()
    };

    // Update events (change name and counter for id=1 and id=2)
    let events = vec![
        SourceEvent::record(
            "update_test",
            serde_json::json!({ "id": 1, "name": "updated1", "counter": 999 }),
        ),
        SourceEvent::record(
            "update_test",
            serde_json::json!({ "id": 2, "name": "updated2", "counter": 888 }),
        ),
    ];

    let sink = RdbcSink;
    let event_stream = futures::stream::iter(events).boxed();
    let result = sink.write(&config, event_stream).await?;

    assert_eq!(result.records_written, 2);

    // Verify updates
    let rows = client
        .query("SELECT name, counter FROM update_test WHERE id = 1", &[])
        .await?;
    let name: String = rows[0].get("name");
    let counter: i64 = rows[0].get("counter");
    assert_eq!(name, "updated1");
    assert_eq!(counter, 999);

    // Verify id=3 unchanged
    let rows = client
        .query("SELECT counter FROM update_test WHERE id = 3", &[])
        .await?;
    let counter: i64 = rows[0].get("counter");
    assert_eq!(counter, 300);

    info!("RDBC sink update mode test passed");
    Ok(())
}
