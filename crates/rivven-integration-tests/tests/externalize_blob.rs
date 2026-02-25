//! ExternalizeBlob SMT Integration Tests
//!
//! Integration tests for the ExternalizeBlob Single Message Transform.
//! Tests verify that large blobs are correctly externalized to object storage
//! and replaced with reference URLs.
//!
//! Run with: cargo test -p rivven-integration-tests --test externalize_blob -- --nocapture
//!
//! These tests use local filesystem storage (via object_store crate) for testing.

use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use rivven_cdc::common::{CdcEvent, CdcOp, CdcSource};
use rivven_cdc::postgres::{PostgresCdc, PostgresCdcConfig};
use rivven_cdc::{ExternalizeBlob, MaskField, Smt, SmtChain};
use rivven_client::Client;
use rivven_connect::connectors::cdc::features::{
    CdcFeatureConfig, CdcFeatureProcessor, ProcessedEvent,
};
use rivven_connect::connectors::cdc::{SmtPredicateConfig, SmtTransformConfig, SmtTransformType};
use rivven_integration_tests::fixtures::{TestBroker, TestPostgres};
use rivven_integration_tests::helpers::*;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::time::timeout;
use tracing::info;

// ============================================================================
// Helper Functions
// ============================================================================

/// Create an ExternalizeBlob with local storage (helper to convert error types)
fn create_local_smt(path: &Path) -> Result<ExternalizeBlob> {
    ExternalizeBlob::local(path)
        .map_err(|e| anyhow::anyhow!("Failed to create ExternalizeBlob: {}", e))
}

/// Create a test CDC event with the given data
fn make_test_event(table: &str, data: Value) -> CdcEvent {
    CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: table.to_string(),
        op: CdcOp::Insert,
        before: None,
        after: Some(data),
        timestamp: chrono::Utc::now().timestamp(),
        transaction: None,
    }
}

/// Verify that a field has been externalized (contains reference structure)
fn is_externalized(value: &Value) -> bool {
    value
        .get("__externalized")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

/// Extract the URL from an externalized reference
fn get_externalized_url(value: &Value) -> Option<&str> {
    value.get("url").and_then(|v| v.as_str())
}

/// Read the content of an externalized blob from local storage
fn read_externalized_content(url: &str) -> Result<Vec<u8>> {
    // URL format: file:///path/to/file
    let path = url
        .strip_prefix("file://")
        .ok_or_else(|| anyhow::anyhow!("Expected file:// URL, got: {}", url))?;
    Ok(std::fs::read(path)?)
}

// ============================================================================
// Basic Functionality Tests
// ============================================================================

/// Test that small values are NOT externalized
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_small_values_not_externalized() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;
    info!("Using temp directory: {:?}", temp_dir.path());

    let smt = create_local_smt(temp_dir.path())?.size_threshold(1000); // 1KB threshold

    let event = make_test_event(
        "users",
        json!({
            "id": 1,
            "name": "Alice",
            "email": "alice@example.com",
            "small_data": "This is a short string"
        }),
    );

    let result = smt.apply(event).expect("should return event");
    let after = result.after.expect("should have after");

    // All fields should remain unchanged (not externalized)
    assert_eq!(after["id"], 1);
    assert_eq!(after["name"], "Alice");
    assert_eq!(after["email"], "alice@example.com");
    assert_eq!(after["small_data"], "This is a short string");

    // Verify no field is externalized
    assert!(!is_externalized(&after["small_data"]));

    info!("Small values test passed");
    Ok(())
}

/// Test that large values ARE externalized
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_large_values_externalized() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;
    info!("Using temp directory: {:?}", temp_dir.path());

    let smt = create_local_smt(temp_dir.path())?.size_threshold(100); // Low threshold for testing

    // Create a large value - use non-base64 characters to avoid decoding
    // Spaces and special chars make this NOT valid base64
    let large_data = "This is test content! ".repeat(10); // ~220 bytes
    let expected_size = large_data.len();

    let event = make_test_event(
        "documents",
        json!({
            "id": 1,
            "title": "Test Document",
            "content": large_data
        }),
    );

    let result = smt.apply(event).expect("should return event");
    let after = result.after.expect("should have after");

    // Small fields unchanged
    assert_eq!(after["id"], 1);
    assert_eq!(after["title"], "Test Document");

    // Large field should be externalized
    let content_ref = &after["content"];
    assert!(
        is_externalized(content_ref),
        "content should be externalized"
    );

    // Verify reference structure
    assert!(content_ref["url"].is_string());
    assert!(content_ref["size"].is_number());
    assert!(content_ref["sha256"].is_string());
    assert!(content_ref["content_type"].is_string());

    // Verify size matches the original string bytes
    assert_eq!(
        content_ref["size"].as_u64().unwrap() as usize,
        expected_size
    );

    // Verify SHA-256 hash is 64 hex characters
    let sha256 = content_ref["sha256"].as_str().unwrap();
    assert_eq!(sha256.len(), 64);

    info!("Large values externalization test passed");
    Ok(())
}

/// Test that externalized content can be read back
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_externalized_content_readable() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;
    info!("Using temp directory: {:?}", temp_dir.path());

    let smt = create_local_smt(temp_dir.path())?.size_threshold(50);

    let original_content = "Hello, World! This is test content that will be externalized.";

    let event = make_test_event(
        "files",
        json!({
            "id": 42,
            "filename": "test.txt",
            "data": original_content
        }),
    );

    let result = smt.apply(event).expect("should return event");
    let after = result.after.expect("should have after");

    // Get the externalized reference
    let data_ref = &after["data"];
    assert!(is_externalized(data_ref));

    let url = get_externalized_url(data_ref).expect("should have URL");
    info!("Externalized to: {}", url);

    // Read back the content
    let stored_content = read_externalized_content(url)?;
    let stored_str = String::from_utf8(stored_content)?;

    assert_eq!(stored_str, original_content);

    info!("Content readable test passed");
    Ok(())
}

/// Test base64-encoded binary data
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_base64_binary_data() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let smt = create_local_smt(temp_dir.path())?.size_threshold(20);

    // Create binary data and encode as base64
    let binary_data: Vec<u8> = (0..100u8).collect();
    let base64_encoded =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &binary_data);

    let event = make_test_event(
        "images",
        json!({
            "id": 1,
            "name": "test.bin",
            "data": base64_encoded
        }),
    );

    let result = smt.apply(event).expect("should return event");
    let after = result.after.expect("should have after");

    let data_ref = &after["data"];
    assert!(is_externalized(data_ref));

    // Read back and verify binary content
    let url = get_externalized_url(data_ref).unwrap();
    let stored_bytes = read_externalized_content(url)?;

    // The stored content should be the decoded binary data
    assert_eq!(stored_bytes, binary_data);

    info!("Base64 binary data test passed");
    Ok(())
}

// ============================================================================
// Configuration Tests
// ============================================================================

/// Test field filtering - only externalize specified fields
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_field_filtering() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let smt = create_local_smt(temp_dir.path())?
        .size_threshold(20)
        .fields(["image_data", "document_content"]); // Only these fields

    let large_text = "X".repeat(100);

    let event = make_test_event(
        "records",
        json!({
            "id": 1,
            "description": large_text.clone(),  // Large but NOT in fields list
            "image_data": large_text.clone(),   // Large and IN fields list
            "document_content": large_text.clone() // Large and IN fields list
        }),
    );

    let result = smt.apply(event).expect("should return event");
    let after = result.after.expect("should have after");

    // description should NOT be externalized (not in fields list)
    assert!(!is_externalized(&after["description"]));
    assert_eq!(after["description"].as_str().unwrap().len(), 100);

    // image_data SHOULD be externalized
    assert!(is_externalized(&after["image_data"]));

    // document_content SHOULD be externalized
    assert!(is_externalized(&after["document_content"]));

    info!("Field filtering test passed");
    Ok(())
}

/// Test custom prefix configuration
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_custom_prefix() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let smt = create_local_smt(temp_dir.path())?
        .size_threshold(20)
        .prefix("my-app/production/blobs");

    let event = make_test_event(
        "uploads",
        json!({
            "id": 1,
            "file_data": "A".repeat(100)
        }),
    );

    let result = smt.apply(event).expect("should return event");
    let after = result.after.expect("should have after");

    let url = get_externalized_url(&after["file_data"]).unwrap();

    // URL should contain the prefix
    assert!(
        url.contains("my-app/production/blobs"),
        "URL should contain prefix: {}",
        url
    );

    // URL should also contain table and field names
    assert!(
        url.contains("uploads"),
        "URL should contain table name: {}",
        url
    );
    assert!(
        url.contains("file_data"),
        "URL should contain field name: {}",
        url
    );

    info!("Custom prefix test passed");
    Ok(())
}

/// Test size threshold variations
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_size_threshold_boundary() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    // Set threshold at exactly 50 bytes
    let smt = create_local_smt(temp_dir.path())?.size_threshold(50);

    let event = make_test_event(
        "test",
        json!({
            "exactly_50": "X".repeat(50),  // Exactly at threshold - should externalize
            "below_50": "Y".repeat(49),    // Below threshold - should NOT externalize
            "above_50": "Z".repeat(51)     // Above threshold - should externalize
        }),
    );

    let result = smt.apply(event).expect("should return event");
    let after = result.after.expect("should have after");

    // At threshold: should externalize
    assert!(
        is_externalized(&after["exactly_50"]),
        "exactly 50 bytes should externalize"
    );

    // Below threshold: should NOT externalize
    assert!(
        !is_externalized(&after["below_50"]),
        "below 50 bytes should NOT externalize"
    );

    // Above threshold: should externalize
    assert!(
        is_externalized(&after["above_50"]),
        "above 50 bytes should externalize"
    );

    info!("Size threshold boundary test passed");
    Ok(())
}

// ============================================================================
// SMT Chain Integration Tests
// ============================================================================

/// Test ExternalizeBlob in a chain with other SMTs
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_smt_chain_integration() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    // Create a chain: MaskField -> ExternalizeBlob
    let chain = SmtChain::new()
        .add(MaskField::new(["ssn", "credit_card"]))
        .add(create_local_smt(temp_dir.path())?.size_threshold(30));

    let event = make_test_event(
        "customers",
        json!({
            "id": 1,
            "name": "John Doe",
            "ssn": "123-45-6789",
            "credit_card": "4111111111111111",
            "document": "A".repeat(100)
        }),
    );

    let result = chain.apply(event).expect("should return event");
    let after = result.after.expect("should have after");

    // SSN should be masked (not the original value)
    assert_ne!(after["ssn"].as_str().unwrap(), "123-45-6789");

    // Credit card should be masked
    assert_ne!(after["credit_card"].as_str().unwrap(), "4111111111111111");

    // Document should be externalized
    assert!(is_externalized(&after["document"]));

    // Name should be unchanged (small, not masked)
    assert_eq!(after["name"], "John Doe");

    info!("SMT chain integration test passed");
    Ok(())
}

/// Test order-dependent chain behavior - externalize AFTER masking for correct behavior
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_smt_chain_order_matters() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    // Correct order: MaskField runs FIRST, then ExternalizeBlob
    // This is the recommended order for pipelines with sensitive data
    let chain = SmtChain::new()
        .add(MaskField::new(["password"])) // Mask first
        .add(create_local_smt(temp_dir.path())?.size_threshold(20)); // Then externalize large blobs

    // Password will be masked first, then large_doc externalized
    let event = make_test_event(
        "users",
        json!({
            "id": 1,
            "password": "secret123",
            "large_doc": "Document content here ".repeat(10) // ~220 bytes
        }),
    );

    let result = chain.apply(event).expect("should return event");
    let after = result.after.expect("should have after");

    // Password should be masked (MaskField ran first)
    let password = after["password"]
        .as_str()
        .expect("password should be string");
    assert_ne!(password, "secret123", "password should be masked");
    assert!(password.contains("*"), "password should contain asterisks");

    // Large doc should be externalized (ExternalizeBlob ran second)
    assert!(
        is_externalized(&after["large_doc"]),
        "large_doc should be externalized"
    );

    info!("SMT chain order test passed");
    Ok(())
}

// ============================================================================
// Event Type Tests
// ============================================================================

/// Test UPDATE events (both before and after)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_event_both_states() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let smt = create_local_smt(temp_dir.path())?.size_threshold(30);

    let old_content = "OLD_".repeat(20);
    let new_content = "NEW_".repeat(20);

    let event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "documents".to_string(),
        op: CdcOp::Update,
        before: Some(json!({
            "id": 1,
            "content": old_content
        })),
        after: Some(json!({
            "id": 1,
            "content": new_content
        })),
        timestamp: chrono::Utc::now().timestamp(),
        transaction: None,
    };

    let result = smt.apply(event).expect("should return event");

    // Both before and after should have externalized content
    let before = result.before.expect("should have before");
    let after = result.after.expect("should have after");

    assert!(
        is_externalized(&before["content"]),
        "before.content should be externalized"
    );
    assert!(
        is_externalized(&after["content"]),
        "after.content should be externalized"
    );

    // URLs should be different
    let before_url = get_externalized_url(&before["content"]).unwrap();
    let after_url = get_externalized_url(&after["content"]).unwrap();
    assert_ne!(before_url, after_url);

    // Verify content is different
    let before_bytes = read_externalized_content(before_url)?;
    let after_bytes = read_externalized_content(after_url)?;
    assert_ne!(before_bytes, after_bytes);

    info!("Update event test passed");
    Ok(())
}

/// Test DELETE events (only before state)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete_event() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let smt = create_local_smt(temp_dir.path())?.size_threshold(30);

    let content = "DELETED_".repeat(10);

    let event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "files".to_string(),
        op: CdcOp::Delete,
        before: Some(json!({
            "id": 1,
            "content": content
        })),
        after: None,
        timestamp: chrono::Utc::now().timestamp(),
        transaction: None,
    };

    let result = smt.apply(event).expect("should return event");

    // Before should have externalized content
    let before = result.before.expect("should have before");
    assert!(is_externalized(&before["content"]));

    // After should still be None
    assert!(result.after.is_none());

    info!("Delete event test passed");
    Ok(())
}

// ============================================================================
// PostgreSQL CDC Integration Test
// ============================================================================

/// Full integration test: PostgreSQL CDC -> ExternalizeBlob SMT
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_cdc_with_externalize_blob() -> Result<()> {
    init_tracing();

    // Start PostgreSQL container
    let pg = TestPostgres::start().await?;
    pg.setup_test_schema().await?;

    // Add a table with a large text column
    let client = pg.connect().await?;
    client
        .execute(
            "CREATE TABLE test_schema.documents (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )",
            &[],
        )
        .await?;

    // Set REPLICA IDENTITY FULL for the new table
    // Note: Publication is "FOR ALL TABLES" so new tables are automatically included
    client
        .execute(
            "ALTER TABLE test_schema.documents REPLICA IDENTITY FULL",
            &[],
        )
        .await?;

    pg.create_replication_slot("externalize_slot").await?;

    let config = PostgresCdcConfig::builder()
        .connection_string(pg.cdc_connection_url())
        .slot_name("externalize_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc.take_event_receiver().expect("event receiver");

    // Create ExternalizeBlob SMT with local storage
    let temp_dir = TempDir::new()?;
    let externalize_smt = create_local_smt(temp_dir.path())?
        .size_threshold(100)
        .fields(["content"])
        .prefix("cdc-blobs");

    // Start CDC
    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Insert a document with large content
    let large_content = "Lorem ipsum dolor sit amet, ".repeat(50);
    info!(
        "Inserting document with {} bytes of content",
        large_content.len()
    );

    client
        .execute(
            "INSERT INTO test_schema.documents (title, content) VALUES ($1, $2)",
            &[&"Test Document", &large_content],
        )
        .await?;

    // Wait for CDC event
    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive event");

    info!("Received CDC event for table: {}", event.table);

    // Apply ExternalizeBlob SMT
    let transformed = externalize_smt
        .apply(event)
        .expect("SMT should return event");
    let after = transformed.after.expect("should have after");

    // Title should be unchanged (small)
    assert!(after["title"].is_string());

    // Content should be externalized (large)
    assert!(
        is_externalized(&after["content"]),
        "content should be externalized"
    );

    // Verify the externalized content
    let url = get_externalized_url(&after["content"]).expect("should have URL");
    let stored = read_externalized_content(url)?;
    let stored_str = String::from_utf8(stored)?;

    assert_eq!(
        stored_str, large_content,
        "stored content should match original"
    );

    // Verify SHA-256 hash
    let sha256 = after["content"]["sha256"].as_str().unwrap();
    use sha2::{Digest, Sha256};
    let expected_hash = hex::encode(Sha256::digest(large_content.as_bytes()));
    assert_eq!(sha256, expected_hash, "SHA-256 should match");

    cdc.stop().await?;

    info!("PostgreSQL CDC with ExternalizeBlob test passed");
    Ok(())
}

/// Test CDC pipeline with multiple SMTs including ExternalizeBlob
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cdc_full_smt_pipeline() -> Result<()> {
    init_tracing();

    let pg = TestPostgres::start().await?;
    pg.setup_test_schema().await?;

    // Create table with sensitive and large fields
    let client = pg.connect().await?;
    client
        .execute(
            "CREATE TABLE test_schema.customer_files (
            id SERIAL PRIMARY KEY,
            customer_name VARCHAR(255) NOT NULL,
            ssn VARCHAR(20),
            file_content TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )",
            &[],
        )
        .await?;

    // Set REPLICA IDENTITY FULL for the new table
    // Note: Publication is "FOR ALL TABLES" so new tables are automatically included
    client
        .execute(
            "ALTER TABLE test_schema.customer_files REPLICA IDENTITY FULL",
            &[],
        )
        .await?;

    pg.create_replication_slot("pipeline_slot").await?;

    let config = PostgresCdcConfig::builder()
        .connection_string(pg.cdc_connection_url())
        .slot_name("pipeline_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc.take_event_receiver().expect("event receiver");

    // Create SMT pipeline
    let temp_dir = TempDir::new()?;
    let pipeline = SmtChain::new()
        // First: Mask sensitive fields
        .add(MaskField::new(["ssn"]))
        // Then: Externalize large blobs
        .add(
            create_local_smt(temp_dir.path())?
                .size_threshold(100)
                .prefix("cdc-pipeline"),
        );

    // Start CDC
    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Insert data
    let large_file = "FILE_DATA_".repeat(50);
    client.execute(
        "INSERT INTO test_schema.customer_files (customer_name, ssn, file_content) VALUES ($1, $2, $3)",
        &[&"John Doe", &"123-45-6789", &large_file]
    ).await?;

    // Wait for event
    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive event");

    // Apply full SMT pipeline
    let result = pipeline.apply(event).expect("pipeline should return event");
    let after = result.after.expect("should have after");

    // SSN should be masked
    let ssn = after["ssn"].as_str().expect("ssn should be string");
    assert_ne!(ssn, "123-45-6789", "SSN should be masked");
    assert!(ssn.contains("*"), "SSN should contain mask characters");

    // Name should be unchanged
    assert_eq!(after["customer_name"], "John Doe");

    // File content should be externalized
    assert!(is_externalized(&after["file_content"]));

    cdc.stop().await?;

    info!("CDC full SMT pipeline test passed");
    Ok(())
}

// ============================================================================
// Error Handling Tests
// ============================================================================

/// Test handling of null values
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_null_values_unchanged() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let smt = create_local_smt(temp_dir.path())?.size_threshold(10);

    let event = make_test_event(
        "test",
        json!({
            "id": 1,
            "nullable_field": null,
            "empty_string": "",
            "data": "some data"
        }),
    );

    let result = smt.apply(event).expect("should return event");
    let after = result.after.expect("should have after");

    // Null and empty values should remain unchanged
    assert!(after["nullable_field"].is_null());
    assert_eq!(after["empty_string"], "");

    info!("Null values test passed");
    Ok(())
}

/// Test nested JSON objects
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nested_json_externalization() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let smt = create_local_smt(temp_dir.path())?.size_threshold(50);

    // Create a nested JSON structure that's large
    let nested = json!({
        "level1": {
            "level2": {
                "data": "A".repeat(30),
                "more": "B".repeat(30)
            }
        },
        "array": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    });

    let event = make_test_event(
        "nested",
        json!({
            "id": 1,
            "metadata": nested
        }),
    );

    let result = smt.apply(event).expect("should return event");
    let after = result.after.expect("should have after");

    // The nested metadata field should be externalized as a whole
    assert!(is_externalized(&after["metadata"]));

    // Content type should be application/json
    assert_eq!(after["metadata"]["content_type"], "application/json");

    info!("Nested JSON test passed");
    Ok(())
}

/// Test concurrent event processing
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_concurrent_externalization() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;

    let smt = std::sync::Arc::new(create_local_smt(temp_dir.path())?.size_threshold(50));

    // Process multiple events concurrently
    let mut handles = Vec::new();

    for i in 0..10 {
        let smt_clone = smt.clone();
        let handle = tokio::spawn(async move {
            let event = make_test_event(
                "concurrent",
                json!({
                    "id": i,
                    "data": format!("Event {} data: {}", i, "X".repeat(100))
                }),
            );

            smt_clone.apply(event)
        });
        handles.push(handle);
    }

    // Wait for all to complete
    let results: Vec<_> = futures::future::join_all(handles).await;

    // Verify all succeeded
    for (i, result) in results.into_iter().enumerate() {
        let event = result?.unwrap_or_else(|| panic!("Event {} should succeed", i));
        let after = event.after.expect("should have after");
        assert!(
            is_externalized(&after["data"]),
            "Event {} data should be externalized",
            i
        );
    }

    info!("Concurrent processing test passed");
    Ok(())
}

// ============================================================================
// Broker Integration Tests
// ============================================================================

/// Test publishing externalized CDC events to a broker
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_externalized_events_to_broker() -> Result<()> {
    init_tracing();

    // Start broker
    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    // Create topic for CDC events
    let topic = unique_topic_name("cdc-externalized");
    client.create_topic(&topic, Some(1)).await?;
    info!("Created topic: {}", topic);

    // Setup ExternalizeBlob SMT
    let temp_dir = TempDir::new()?;
    let smt = create_local_smt(temp_dir.path())?
        .size_threshold(50)
        .prefix("broker-test");

    // Create large content that will be externalized
    let large_content = "Large document content: ".repeat(10);

    // Create and transform a CDC event
    let event = make_test_event(
        "documents",
        json!({
            "id": 1,
            "title": "Test Doc",
            "content": large_content
        }),
    );

    let transformed = smt.apply(event).expect("SMT should return event");

    // Verify content was externalized
    let after = transformed.after.as_ref().expect("should have after");
    assert!(
        is_externalized(&after["content"]),
        "content should be externalized"
    );

    // Publish transformed event to broker
    let event_bytes = serde_json::to_vec(&transformed)?;
    client.publish(&topic, event_bytes).await?;
    info!("Published externalized event to broker");

    // Consume and verify
    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);

    // Deserialize and verify the event structure is preserved
    let consumed_event: CdcEvent = serde_json::from_slice(&messages[0].value)?;
    assert_eq!(consumed_event.table, "documents");
    assert_eq!(consumed_event.op, CdcOp::Insert);

    let consumed_after = consumed_event.after.expect("should have after");
    assert_eq!(consumed_after["id"], 1);
    assert_eq!(consumed_after["title"], "Test Doc");
    assert!(is_externalized(&consumed_after["content"]));

    // Verify the externalized reference is complete
    let content_ref = &consumed_after["content"];
    assert!(content_ref["url"].is_string());
    assert!(content_ref["size"].is_number());
    assert!(content_ref["sha256"].is_string());

    broker.shutdown().await?;
    info!("Externalized events to broker test passed");
    Ok(())
}

/// Test full CDC pipeline: PostgreSQL -> SMT -> Broker -> Consumer
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_full_cdc_broker_pipeline() -> Result<()> {
    init_tracing();

    // Start infrastructure
    let pg = TestPostgres::start().await?;
    let broker = TestBroker::start().await?;

    pg.setup_test_schema().await?;

    // Setup database schema
    let db_client = pg.connect().await?;
    db_client
        .execute(
            "CREATE TABLE test_schema.files (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )",
            &[],
        )
        .await?;
    db_client
        .execute("ALTER TABLE test_schema.files REPLICA IDENTITY FULL", &[])
        .await?;

    pg.create_replication_slot("broker_pipeline_slot").await?;

    // Setup CDC
    let config = PostgresCdcConfig::builder()
        .connection_string(pg.cdc_connection_url())
        .slot_name("broker_pipeline_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(config);
    let mut rx = cdc.take_event_receiver().expect("event receiver");

    // Setup ExternalizeBlob SMT
    let temp_dir = TempDir::new()?;
    let smt = create_local_smt(temp_dir.path())?
        .size_threshold(100)
        .fields(["content"])
        .prefix("cdc-pipeline");

    // Setup broker client
    let mut client = Client::connect(&broker.connection_string()).await?;
    let topic = unique_topic_name("cdc-files");
    client.create_topic(&topic, Some(1)).await?;
    info!("Created topic: {}", topic);

    // Start CDC
    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Insert data with large content
    let large_content = "FILE_CONTENT_".repeat(20);
    db_client
        .execute(
            "INSERT INTO test_schema.files (name, content) VALUES ($1, $2)",
            &[&"large_file.txt", &large_content],
        )
        .await?;
    info!(
        "Inserted file with {} bytes of content",
        large_content.len()
    );

    // Receive CDC event
    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive event");
    info!("Received CDC event for table: {}", event.table);

    // Apply SMT
    let transformed = smt.apply(event).expect("SMT should return event");
    let after = transformed.after.as_ref().expect("should have after");
    assert!(
        is_externalized(&after["content"]),
        "content should be externalized"
    );

    // Publish to broker
    let event_bytes = serde_json::to_vec(&transformed)?;
    client.publish(&topic, event_bytes).await?;
    info!("Published transformed event to broker");

    // Consume from broker
    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);

    // Verify consumed event
    let consumed: CdcEvent = serde_json::from_slice(&messages[0].value)?;
    assert_eq!(consumed.table, "files");
    assert_eq!(consumed.op, CdcOp::Insert);

    let consumed_after = consumed.after.expect("should have after");
    assert_eq!(consumed_after["name"], "large_file.txt");
    assert!(is_externalized(&consumed_after["content"]));

    // Verify we can read back the externalized content
    let url = get_externalized_url(&consumed_after["content"]).expect("should have URL");
    let stored = read_externalized_content(url)?;
    let stored_str = String::from_utf8(stored)?;
    assert_eq!(stored_str, large_content);

    cdc.stop().await?;
    broker.shutdown().await?;

    info!("Full CDC broker pipeline test passed");
    Ok(())
}

/// Test multiple events through broker with SMT chain
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker_smt_chain_multiple_events() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("smt-chain");
    client.create_topic(&topic, Some(1)).await?;

    // Create SMT chain: MaskField -> ExternalizeBlob
    let temp_dir = TempDir::new()?;
    let chain = SmtChain::new()
        .add(MaskField::new(["ssn"]))
        .add(create_local_smt(temp_dir.path())?.size_threshold(50));

    // Process multiple events
    for i in 0..5 {
        let event = make_test_event(
            "customers",
            json!({
                "id": i,
                "name": format!("Customer {}", i),
                "ssn": "123-45-6789",
                "notes": format!("This is a long note for customer {}: ", i).repeat(5)
            }),
        );

        let transformed = chain.apply(event).expect("chain should return event");
        let event_bytes = serde_json::to_vec(&transformed)?;
        client.publish(&topic, event_bytes).await?;
    }
    info!("Published 5 transformed events");

    // Consume all events
    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 5);

    // Verify each event
    for (i, msg) in messages.iter().enumerate() {
        let event: CdcEvent = serde_json::from_slice(&msg.value)?;
        let after = event.after.expect("should have after");

        // SSN should be masked
        let ssn = after["ssn"].as_str().unwrap();
        assert!(ssn.contains("*"), "SSN should be masked: {}", ssn);

        // Notes should be externalized (large)
        assert!(
            is_externalized(&after["notes"]),
            "Event {} notes should be externalized",
            i
        );

        // Name should be unchanged
        assert!(after["name"].as_str().unwrap().contains("Customer"));
    }

    broker.shutdown().await?;
    info!("Broker SMT chain test passed");
    Ok(())
}

/// Test consumer can reconstruct externalized content from broker messages
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker_consumer_content_reconstruction() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("reconstruct");
    client.create_topic(&topic, Some(1)).await?;

    let temp_dir = TempDir::new()?;
    let smt = create_local_smt(temp_dir.path())?.size_threshold(30);

    // Create binary-like content
    let binary_data: Vec<u8> = (0..200u8).collect();
    let base64_content =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &binary_data);

    let event = make_test_event(
        "binaries",
        json!({
            "id": 1,
            "filename": "data.bin",
            "data": base64_content
        }),
    );

    let transformed = smt.apply(event).expect("SMT should return event");
    client
        .publish(&topic, serde_json::to_vec(&transformed)?)
        .await?;

    // Consume from broker
    let messages = client.consume(&topic, 0, 0, 10).await?;
    let consumed: CdcEvent = serde_json::from_slice(&messages[0].value)?;
    let after = consumed.after.expect("should have after");

    // Consumer would typically:
    // 1. Check if field is externalized
    // 2. Fetch content from object storage using URL
    // 3. Reconstruct original data

    assert!(is_externalized(&after["data"]));
    let ref_info = &after["data"];

    // Verify reference metadata is preserved through broker
    assert!(ref_info["url"].is_string());
    assert_eq!(
        ref_info["size"].as_u64().unwrap() as usize,
        binary_data.len()
    );
    assert!(ref_info["sha256"].is_string());

    // Reconstruct content (as consumer would do)
    let url = get_externalized_url(&after["data"]).unwrap();
    let reconstructed = read_externalized_content(url)?;
    assert_eq!(reconstructed, binary_data);

    // Verify hash matches
    use sha2::{Digest, Sha256};
    let computed_hash = hex::encode(Sha256::digest(&reconstructed));
    assert_eq!(computed_hash, ref_info["sha256"].as_str().unwrap());

    broker.shutdown().await?;
    info!("Broker consumer content reconstruction test passed");
    Ok(())
}

// ============================================================================
// Real Integration Tests with CdcFeatureProcessor
// ============================================================================

/// Test ExternalizeBlob SMT configured via CdcFeatureProcessor
/// This is a real integration test using the rivven-connect SDK
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cdc_feature_processor_with_externalize_blob() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_string_lossy().to_string();
    info!("Using temp directory for blob storage: {}", temp_path);

    // Configure CdcFeatureProcessor with ExternalizeBlob SMT
    let mut config = CdcFeatureConfig::default();

    // Add ExternalizeBlob transform
    let mut externalize_config = std::collections::HashMap::new();
    externalize_config.insert("storage_type".to_string(), serde_json::json!("local"));
    externalize_config.insert("path".to_string(), serde_json::json!(temp_path));
    externalize_config.insert("size_threshold".to_string(), serde_json::json!(100));
    externalize_config.insert("prefix".to_string(), serde_json::json!("cdc-blobs"));

    config.transforms.push(SmtTransformConfig {
        transform_type: SmtTransformType::ExternalizeBlob,
        name: Some("externalize_large_content".to_string()),
        predicate: None,
        config: externalize_config,
    });

    let processor = CdcFeatureProcessor::new(config).unwrap();
    assert!(
        processor.has_transforms(),
        "Processor should have transforms configured"
    );

    // Create a test event with large content
    let large_content = "Large document content that exceeds threshold. ".repeat(5);
    let event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "documents".to_string(),
        op: CdcOp::Insert,
        before: None,
        after: Some(json!({
            "id": 1,
            "title": "Test Document",
            "content": large_content
        })),
        timestamp: chrono::Utc::now().timestamp(),
        transaction: None,
    };

    // Process through CdcFeatureProcessor
    let result: Option<ProcessedEvent> = processor.process(event).await;

    // Verify the event was processed
    let stats = processor.stats().await;
    assert_eq!(stats.events_processed, 1, "Should have processed 1 event");

    // Verify the result
    let processed = result.expect("Should have processed result");
    let after = processed.event.after.expect("Should have after");

    // Title should be unchanged (small)
    assert_eq!(after["title"], "Test Document");

    // Content should be externalized (large)
    assert!(
        is_externalized(&after["content"]),
        "content should be externalized"
    );

    // Verify externalized reference structure
    let content_ref = &after["content"];
    assert!(content_ref["url"].is_string(), "Should have URL");
    assert!(content_ref["size"].is_number(), "Should have size");
    assert!(content_ref["sha256"].is_string(), "Should have SHA-256");

    // Verify we can read the content back
    let url = get_externalized_url(content_ref).expect("Should have URL");
    let stored = read_externalized_content(url)?;
    let stored_str = String::from_utf8(stored)?;
    assert_eq!(stored_str, large_content);

    info!("CdcFeatureProcessor with ExternalizeBlob test passed");
    Ok(())
}

/// Full end-to-end test: PostgreSQL -> rivven-connect CdcFeatureProcessor -> Broker -> Consumer
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_to_connect_to_broker_pipeline() -> Result<()> {
    init_tracing();

    // Start infrastructure
    let pg = TestPostgres::start().await?;
    let broker = TestBroker::start().await?;

    pg.setup_test_schema().await?;

    // Create table with large content column
    let db_client = pg.connect().await?;
    db_client
        .execute(
            "CREATE TABLE test_schema.documents (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            content TEXT NOT NULL
        )",
            &[],
        )
        .await?;
    db_client
        .execute(
            "ALTER TABLE test_schema.documents REPLICA IDENTITY FULL",
            &[],
        )
        .await?;

    pg.create_replication_slot("connect_pipeline_slot").await?;

    // Setup CDC
    let cdc_config = PostgresCdcConfig::builder()
        .connection_string(pg.cdc_connection_url())
        .slot_name("connect_pipeline_slot")
        .publication_name("rivven_pub")
        .build()?;

    let mut cdc = PostgresCdc::new(cdc_config);
    let mut rx = cdc.take_event_receiver().expect("event receiver");

    // Setup CdcFeatureProcessor with ExternalizeBlob + MaskField
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    let mut feature_config = CdcFeatureConfig::default();

    // Add ExternalizeBlob transform
    let mut externalize_config = std::collections::HashMap::new();
    externalize_config.insert("storage_type".to_string(), serde_json::json!("local"));
    externalize_config.insert("path".to_string(), serde_json::json!(temp_path));
    externalize_config.insert("size_threshold".to_string(), serde_json::json!(100));
    externalize_config.insert("fields".to_string(), serde_json::json!(["content"]));
    externalize_config.insert("prefix".to_string(), serde_json::json!("connect-pipeline"));

    feature_config.transforms.push(SmtTransformConfig {
        transform_type: SmtTransformType::ExternalizeBlob,
        name: Some("externalize_content".to_string()),
        predicate: None,
        config: externalize_config,
    });

    let processor = CdcFeatureProcessor::new(feature_config).unwrap();
    info!("CdcFeatureProcessor configured: {}", processor);

    // Setup broker client
    let mut client = Client::connect(&broker.connection_string()).await?;
    let topic = unique_topic_name("connect-cdc-documents");
    client.create_topic(&topic, Some(1)).await?;
    info!("Created topic: {}", topic);

    // Start CDC
    cdc.start().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Insert large document
    let large_content = "DOCUMENT_CONTENT_".repeat(20);
    db_client
        .execute(
            "INSERT INTO test_schema.documents (title, content) VALUES ($1, $2)",
            &[&"Important Document", &large_content],
        )
        .await?;
    info!("Inserted document with {} bytes", large_content.len());

    // Receive CDC event
    let event = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("should receive event");
    info!(
        "Received CDC event: table={}, op={:?}",
        event.table, event.op
    );

    // Process through CdcFeatureProcessor (this is what rivven-connect does)
    let processed: ProcessedEvent = processor
        .process(event)
        .await
        .expect("should process event");

    // Serialize and publish to broker
    let event_bytes = serde_json::to_vec(&processed.event)?;
    client.publish(&topic, event_bytes).await?;
    info!("Published processed event to broker");

    // Consume from broker (simulating downstream consumer)
    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);

    // Verify consumed event
    let consumed: CdcEvent = serde_json::from_slice(&messages[0].value)?;
    assert_eq!(consumed.table, "documents");
    assert_eq!(consumed.op, CdcOp::Insert);

    let consumed_after = consumed.after.expect("should have after");

    // Title should be unchanged
    assert_eq!(consumed_after["title"], "Important Document");

    // Content should be externalized
    assert!(
        is_externalized(&consumed_after["content"]),
        "content should be externalized"
    );

    // Consumer can fetch the externalized content using the URL
    let url = get_externalized_url(&consumed_after["content"]).expect("should have URL");
    let stored = read_externalized_content(url)?;
    let stored_str = String::from_utf8(stored)?;
    assert_eq!(
        stored_str, large_content,
        "stored content should match original"
    );

    // Verify stats
    let stats = processor.stats().await;
    assert_eq!(stats.events_processed, 1);

    cdc.stop().await?;
    broker.shutdown().await?;

    info!("PostgreSQL -> rivven-connect -> Broker pipeline test passed");
    Ok(())
}

/// Test SMT chain with MaskField + ExternalizeBlob through CdcFeatureProcessor
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_connect_smt_chain_mask_then_externalize() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    // Configure processor with SMT chain: MaskField -> ExternalizeBlob
    let mut config = CdcFeatureConfig::default();

    // First transform: Mask sensitive fields
    let mut mask_config = std::collections::HashMap::new();
    mask_config.insert(
        "fields".to_string(),
        serde_json::json!(["ssn", "credit_card"]),
    );

    config.transforms.push(SmtTransformConfig {
        transform_type: SmtTransformType::MaskField,
        name: Some("mask_pii".to_string()),
        predicate: None,
        config: mask_config,
    });

    // Second transform: Externalize large blobs
    let mut externalize_config = std::collections::HashMap::new();
    externalize_config.insert("storage_type".to_string(), serde_json::json!("local"));
    externalize_config.insert("path".to_string(), serde_json::json!(temp_path));
    externalize_config.insert("size_threshold".to_string(), serde_json::json!(50));

    config.transforms.push(SmtTransformConfig {
        transform_type: SmtTransformType::ExternalizeBlob,
        name: Some("externalize_blobs".to_string()),
        predicate: None,
        config: externalize_config,
    });

    let processor = CdcFeatureProcessor::new(config).unwrap();
    assert_eq!(processor.transform_count(), 2, "Should have 2 transforms");

    // Create event with sensitive data and large content
    let large_document = "Important document content here. ".repeat(10);
    let event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "customers".to_string(),
        op: CdcOp::Insert,
        before: None,
        after: Some(json!({
            "id": 42,
            "name": "John Doe",
            "ssn": "123-45-6789",
            "credit_card": "4111111111111111",
            "document": large_document
        })),
        timestamp: chrono::Utc::now().timestamp(),
        transaction: None,
    };

    // Process through chain
    let result: ProcessedEvent = processor.process(event).await.expect("should process");
    let after = result.event.after.expect("should have after");

    // SSN should be masked
    let ssn = after["ssn"].as_str().expect("ssn should be string");
    assert!(ssn.contains("*"), "SSN should be masked: {}", ssn);
    assert_ne!(ssn, "123-45-6789", "SSN should not be original value");

    // Credit card should be masked
    let cc = after["credit_card"]
        .as_str()
        .expect("credit_card should be string");
    assert!(cc.contains("*"), "Credit card should be masked: {}", cc);

    // Name should be unchanged
    assert_eq!(after["name"], "John Doe");

    // Document should be externalized (large)
    assert!(
        is_externalized(&after["document"]),
        "document should be externalized"
    );

    // Verify externalized content
    let url = get_externalized_url(&after["document"]).expect("should have URL");
    let stored = read_externalized_content(url)?;
    let stored_str = String::from_utf8(stored)?;
    assert_eq!(stored_str, large_document);

    info!("Connect SMT chain (Mask + Externalize) test passed");
    Ok(())
}

/// Test per-table SMT predicates - ExternalizeBlob only applies to specific table
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_per_table_smt_predicate() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_string_lossy().to_string();
    info!("Using temp directory for blob storage: {}", temp_path);

    // Configure processor with ExternalizeBlob that only applies to "documents" table
    let mut config = CdcFeatureConfig::default();

    // ExternalizeBlob with table predicate - only apply to "documents" table
    let mut externalize_config = std::collections::HashMap::new();
    externalize_config.insert("storage_type".to_string(), serde_json::json!("local"));
    externalize_config.insert("path".to_string(), serde_json::json!(temp_path));
    externalize_config.insert("size_threshold".to_string(), serde_json::json!(50));

    config.transforms.push(SmtTransformConfig {
        transform_type: SmtTransformType::ExternalizeBlob,
        name: Some("externalize_documents_only".to_string()),
        predicate: Some(SmtPredicateConfig {
            table: Some("documents".to_string()), // Only apply to documents table
            ..Default::default()
        }),
        config: externalize_config,
    });

    let processor = CdcFeatureProcessor::new(config).unwrap();
    assert!(processor.has_transforms(), "Should have transforms");

    let large_content = "Large content that exceeds threshold. ".repeat(5);

    // Event 1: "documents" table - should have content externalized
    let documents_event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "documents".to_string(),
        op: CdcOp::Insert,
        before: None,
        after: Some(json!({
            "id": 1,
            "content": large_content.clone()
        })),
        timestamp: chrono::Utc::now().timestamp(),
        transaction: None,
    };

    // Event 2: "users" table - should NOT have content externalized (predicate doesn't match)
    let users_event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        op: CdcOp::Insert,
        before: None,
        after: Some(json!({
            "id": 2,
            "bio": large_content.clone()  // Same large content, but different table
        })),
        timestamp: chrono::Utc::now().timestamp(),
        transaction: None,
    };

    // Process documents event
    let doc_result: Option<ProcessedEvent> = processor.process(documents_event).await;
    let doc_processed = doc_result.expect("documents event should be processed");
    let doc_after = doc_processed.event.after.expect("should have after");

    // documents table: content SHOULD be externalized
    assert!(
        is_externalized(&doc_after["content"]),
        "documents.content should be externalized"
    );

    // Process users event
    let users_result: Option<ProcessedEvent> = processor.process(users_event).await;
    let users_processed = users_result.expect("users event should be processed");
    let users_after = users_processed.event.after.expect("should have after");

    // users table: bio should NOT be externalized (predicate doesn't match)
    assert!(
        !is_externalized(&users_after["bio"]),
        "users.bio should NOT be externalized (predicate table=documents doesn't match)"
    );
    assert_eq!(
        users_after["bio"].as_str().unwrap(),
        large_content,
        "users.bio should remain as original string"
    );

    info!("Per-table SMT predicate test passed");
    Ok(())
}

/// Test multiple tables predicate - ExternalizeBlob applies to multiple specified tables
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_multiple_tables_predicate() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    let mut config = CdcFeatureConfig::default();

    // ExternalizeBlob that applies to BOTH "documents" AND "files" tables
    let mut externalize_config = std::collections::HashMap::new();
    externalize_config.insert("storage_type".to_string(), serde_json::json!("local"));
    externalize_config.insert("path".to_string(), serde_json::json!(temp_path));
    externalize_config.insert("size_threshold".to_string(), serde_json::json!(50));

    config.transforms.push(SmtTransformConfig {
        transform_type: SmtTransformType::ExternalizeBlob,
        name: Some("externalize_docs_and_files".to_string()),
        predicate: Some(SmtPredicateConfig {
            tables: vec!["documents".to_string(), "files".to_string()],
            ..Default::default()
        }),
        config: externalize_config,
    });

    let processor = CdcFeatureProcessor::new(config).unwrap();
    let large_content = "Large content. ".repeat(10);

    // Test all three tables
    for (table, field, should_externalize) in [
        ("documents", "content", true),
        ("files", "data", true),
        ("users", "bio", false),
    ] {
        let event = CdcEvent {
            source_type: "postgres".to_string(),
            database: "testdb".to_string(),
            schema: "public".to_string(),
            table: table.to_string(),
            op: CdcOp::Insert,
            before: None,
            after: Some(json!({
                "id": 1,
                field: large_content.clone()
            })),
            timestamp: chrono::Utc::now().timestamp(),
            transaction: None,
        };

        let result: Option<ProcessedEvent> = processor.process(event).await;
        let processed = result.unwrap_or_else(|| panic!("{} event should be processed", table));
        let after = processed.event.after.expect("should have after");

        if should_externalize {
            assert!(
                is_externalized(&after[field]),
                "{}.{} should be externalized",
                table,
                field
            );
        } else {
            assert!(
                !is_externalized(&after[field]),
                "{}.{} should NOT be externalized",
                table,
                field
            );
        }
    }

    info!("Multiple tables predicate test passed");
    Ok(())
}

/// Test regex table predicate - ExternalizeBlob applies to tables matching pattern
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_regex_table_predicate() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    let mut config = CdcFeatureConfig::default();

    // ExternalizeBlob that applies to tables matching "doc.*" pattern
    let mut externalize_config = std::collections::HashMap::new();
    externalize_config.insert("storage_type".to_string(), serde_json::json!("local"));
    externalize_config.insert("path".to_string(), serde_json::json!(temp_path));
    externalize_config.insert("size_threshold".to_string(), serde_json::json!(50));

    config.transforms.push(SmtTransformConfig {
        transform_type: SmtTransformType::ExternalizeBlob,
        name: Some("externalize_doc_tables".to_string()),
        predicate: Some(SmtPredicateConfig {
            table: Some("^doc.*".to_string()), // Regex: tables starting with "doc"
            ..Default::default()
        }),
        config: externalize_config,
    });

    let processor = CdcFeatureProcessor::new(config).unwrap();
    let large_content = "Large content. ".repeat(10);

    // Test tables - some match "^doc.*", some don't
    for (table, should_externalize) in [
        ("documents", true),     // matches ^doc.*
        ("doc_archive", true),   // matches ^doc.*
        ("documentation", true), // matches ^doc.*
        ("users", false),        // doesn't match
        ("product_docs", false), // doesn't match (doc not at start)
    ] {
        let event = CdcEvent {
            source_type: "postgres".to_string(),
            database: "testdb".to_string(),
            schema: "public".to_string(),
            table: table.to_string(),
            op: CdcOp::Insert,
            before: None,
            after: Some(json!({
                "id": 1,
                "content": large_content.clone()
            })),
            timestamp: chrono::Utc::now().timestamp(),
            transaction: None,
        };

        let result: Option<ProcessedEvent> = processor.process(event).await;
        let processed = result.unwrap_or_else(|| panic!("{} event should be processed", table));
        let after = processed.event.after.expect("should have after");

        if should_externalize {
            assert!(
                is_externalized(&after["content"]),
                "table '{}' should have content externalized (matches ^doc.*)",
                table
            );
        } else {
            assert!(
                !is_externalized(&after["content"]),
                "table '{}' should NOT have content externalized (doesn't match ^doc.*)",
                table
            );
        }
    }

    info!("Regex table predicate test passed");
    Ok(())
}

/// Test negate predicate - ExternalizeBlob applies to all tables EXCEPT specified
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_negate_predicate() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    let mut config = CdcFeatureConfig::default();

    // ExternalizeBlob that applies to all tables EXCEPT "audit_log"
    let mut externalize_config = std::collections::HashMap::new();
    externalize_config.insert("storage_type".to_string(), serde_json::json!("local"));
    externalize_config.insert("path".to_string(), serde_json::json!(temp_path));
    externalize_config.insert("size_threshold".to_string(), serde_json::json!(50));

    config.transforms.push(SmtTransformConfig {
        transform_type: SmtTransformType::ExternalizeBlob,
        name: Some("externalize_except_audit".to_string()),
        predicate: Some(SmtPredicateConfig {
            table: Some("audit_log".to_string()),
            negate: true, // Apply when table is NOT audit_log
            ..Default::default()
        }),
        config: externalize_config,
    });

    let processor = CdcFeatureProcessor::new(config).unwrap();
    let large_content = "Large content. ".repeat(10);

    // Test tables
    for (table, should_externalize) in [
        ("documents", true),  // not audit_log -> applies
        ("users", true),      // not audit_log -> applies
        ("audit_log", false), // is audit_log -> negate means don't apply
    ] {
        let event = CdcEvent {
            source_type: "postgres".to_string(),
            database: "testdb".to_string(),
            schema: "public".to_string(),
            table: table.to_string(),
            op: CdcOp::Insert,
            before: None,
            after: Some(json!({
                "id": 1,
                "content": large_content.clone()
            })),
            timestamp: chrono::Utc::now().timestamp(),
            transaction: None,
        };

        let result: Option<ProcessedEvent> = processor.process(event).await;
        let processed = result.unwrap_or_else(|| panic!("{} event should be processed", table));
        let after = processed.event.after.expect("should have after");

        if should_externalize {
            assert!(
                is_externalized(&after["content"]),
                "table '{}' should have content externalized (negate=true, not audit_log)",
                table
            );
        } else {
            assert!(
                !is_externalized(&after["content"]),
                "table '{}' should NOT have content externalized (negate=true, is audit_log)",
                table
            );
        }
    }

    info!("Negate predicate test passed");
    Ok(())
}

/// Test operation-based predicates - ExternalizeBlob only applies on specific operations
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_operation_predicate() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    let mut config = CdcFeatureConfig::default();

    // ExternalizeBlob that only applies to INSERT and UPDATE operations
    let mut externalize_config = std::collections::HashMap::new();
    externalize_config.insert("storage_type".to_string(), serde_json::json!("local"));
    externalize_config.insert("path".to_string(), serde_json::json!(temp_path));
    externalize_config.insert("size_threshold".to_string(), serde_json::json!(50));

    config.transforms.push(SmtTransformConfig {
        transform_type: SmtTransformType::ExternalizeBlob,
        name: Some("externalize_on_insert_update".to_string()),
        predicate: Some(SmtPredicateConfig {
            operations: vec!["insert".to_string(), "update".to_string()],
            ..Default::default()
        }),
        config: externalize_config,
    });

    let processor = CdcFeatureProcessor::new(config).unwrap();
    let large_content = "Large content. ".repeat(10);

    // Test INSERT - should externalize
    let insert_event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "documents".to_string(),
        op: CdcOp::Insert,
        before: None,
        after: Some(json!({ "id": 1, "content": large_content.clone() })),
        timestamp: chrono::Utc::now().timestamp(),
        transaction: None,
    };

    let result: Option<ProcessedEvent> = processor.process(insert_event).await;
    let processed = result.expect("insert should be processed");
    assert!(
        is_externalized(&processed.event.after.unwrap()["content"]),
        "INSERT should have content externalized"
    );

    // Test UPDATE - should externalize
    let update_event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "documents".to_string(),
        op: CdcOp::Update,
        before: Some(json!({ "id": 1, "content": "old" })),
        after: Some(json!({ "id": 1, "content": large_content.clone() })),
        timestamp: chrono::Utc::now().timestamp(),
        transaction: None,
    };

    let result: Option<ProcessedEvent> = processor.process(update_event).await;
    let processed = result.expect("update should be processed");
    assert!(
        is_externalized(&processed.event.after.unwrap()["content"]),
        "UPDATE should have content externalized"
    );

    // Test DELETE - should NOT externalize (operation not in predicate)
    let delete_event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "documents".to_string(),
        op: CdcOp::Delete,
        before: Some(json!({ "id": 1, "content": large_content.clone() })),
        after: None,
        timestamp: chrono::Utc::now().timestamp(),
        transaction: None,
    };

    let result: Option<ProcessedEvent> = processor.process(delete_event).await;
    let processed = result.expect("delete should be processed");
    // DELETE has before but no after, check before wasn't externalized
    assert!(
        !is_externalized(&processed.event.before.unwrap()["content"]),
        "DELETE should NOT have content externalized (operation not in predicate)"
    );

    info!("Operation predicate test passed");
    Ok(())
}

/// Test combined predicates - table AND operation must both match
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_combined_predicates() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    let mut config = CdcFeatureConfig::default();

    // ExternalizeBlob that only applies to "documents" table AND only on INSERT
    let mut externalize_config = std::collections::HashMap::new();
    externalize_config.insert("storage_type".to_string(), serde_json::json!("local"));
    externalize_config.insert("path".to_string(), serde_json::json!(temp_path));
    externalize_config.insert("size_threshold".to_string(), serde_json::json!(50));

    config.transforms.push(SmtTransformConfig {
        transform_type: SmtTransformType::ExternalizeBlob,
        name: Some("externalize_docs_insert_only".to_string()),
        predicate: Some(SmtPredicateConfig {
            table: Some("documents".to_string()),
            operations: vec!["insert".to_string()],
            ..Default::default()
        }),
        config: externalize_config,
    });

    let processor = CdcFeatureProcessor::new(config).unwrap();
    let large_content = "Large content. ".repeat(10);

    // Test: documents + INSERT = MATCH -> externalize
    let event1 = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "documents".to_string(),
        op: CdcOp::Insert,
        before: None,
        after: Some(json!({ "id": 1, "content": large_content.clone() })),
        timestamp: chrono::Utc::now().timestamp(),
        transaction: None,
    };
    let result: Option<ProcessedEvent> = processor.process(event1).await;
    assert!(
        is_externalized(&result.unwrap().event.after.unwrap()["content"]),
        "documents + INSERT should externalize"
    );

    // Test: documents + UPDATE = NO MATCH (wrong operation)
    let event2 = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "documents".to_string(),
        op: CdcOp::Update,
        before: Some(json!({ "id": 1, "content": "old" })),
        after: Some(json!({ "id": 1, "content": large_content.clone() })),
        timestamp: chrono::Utc::now().timestamp(),
        transaction: None,
    };
    let result: Option<ProcessedEvent> = processor.process(event2).await;
    assert!(
        !is_externalized(&result.unwrap().event.after.unwrap()["content"]),
        "documents + UPDATE should NOT externalize (operation mismatch)"
    );

    // Test: users + INSERT = NO MATCH (wrong table)
    let event3 = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        op: CdcOp::Insert,
        before: None,
        after: Some(json!({ "id": 1, "content": large_content.clone() })),
        timestamp: chrono::Utc::now().timestamp(),
        transaction: None,
    };
    let result: Option<ProcessedEvent> = processor.process(event3).await;
    assert!(
        !is_externalized(&result.unwrap().event.after.unwrap()["content"]),
        "users + INSERT should NOT externalize (table mismatch)"
    );

    info!("Combined predicates test passed");
    Ok(())
}

/// Test schema-based predicate
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_schema_predicate() -> Result<()> {
    init_tracing();

    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    let mut config = CdcFeatureConfig::default();

    // ExternalizeBlob that only applies to "audit" schema
    let mut externalize_config = std::collections::HashMap::new();
    externalize_config.insert("storage_type".to_string(), serde_json::json!("local"));
    externalize_config.insert("path".to_string(), serde_json::json!(temp_path));
    externalize_config.insert("size_threshold".to_string(), serde_json::json!(50));

    config.transforms.push(SmtTransformConfig {
        transform_type: SmtTransformType::ExternalizeBlob,
        name: Some("externalize_audit_schema".to_string()),
        predicate: Some(SmtPredicateConfig {
            schema: Some("audit".to_string()),
            ..Default::default()
        }),
        config: externalize_config,
    });

    let processor = CdcFeatureProcessor::new(config).unwrap();
    let large_content = "Large content. ".repeat(10);

    // Test audit schema - should externalize
    let audit_event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "audit".to_string(),
        table: "logs".to_string(),
        op: CdcOp::Insert,
        before: None,
        after: Some(json!({ "id": 1, "data": large_content.clone() })),
        timestamp: chrono::Utc::now().timestamp(),
        transaction: None,
    };
    let result: Option<ProcessedEvent> = processor.process(audit_event).await;
    assert!(
        is_externalized(&result.unwrap().event.after.unwrap()["data"]),
        "audit schema should have data externalized"
    );

    // Test public schema - should NOT externalize
    let public_event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "logs".to_string(),
        op: CdcOp::Insert,
        before: None,
        after: Some(json!({ "id": 1, "data": large_content.clone() })),
        timestamp: chrono::Utc::now().timestamp(),
        transaction: None,
    };
    let result: Option<ProcessedEvent> = processor.process(public_event).await;
    assert!(
        !is_externalized(&result.unwrap().event.after.unwrap()["data"]),
        "public schema should NOT have data externalized"
    );

    info!("Schema predicate test passed");
    Ok(())
}
