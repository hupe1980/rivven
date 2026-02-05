//! Tests for rivven-rdbc sink module

use rivven_rdbc::prelude::*;
use std::collections::HashMap;
use std::time::Duration;

// ==================== WriteMode Tests ====================

#[test]
fn test_write_mode_default() {
    let mode = WriteMode::default();
    assert!(matches!(mode, WriteMode::Upsert));
}

#[test]
fn test_write_mode_variants() {
    assert_eq!(WriteMode::Insert, WriteMode::Insert);
    assert_eq!(WriteMode::Update, WriteMode::Update);
    assert_eq!(WriteMode::Upsert, WriteMode::Upsert);
    assert_eq!(WriteMode::Delete, WriteMode::Delete);
}

// ==================== BatchConfig Tests ====================

#[test]
fn test_batch_config_default() {
    let config = BatchConfig::default();

    assert_eq!(config.max_size, 1000);
    assert_eq!(config.max_latency, Duration::from_millis(100));
    assert_eq!(config.max_bytes, 16 * 1024 * 1024);
    assert!(config.ordered);
    assert_eq!(config.max_retries, 3);
    assert_eq!(config.retry_backoff, Duration::from_millis(100));
}

#[test]
fn test_batch_config_builder() {
    let config = BatchConfig::default()
        .with_size(500)
        .with_latency(Duration::from_millis(50))
        .with_max_bytes(8 * 1024 * 1024)
        .ordered(false)
        .with_retries(5, Duration::from_millis(200));

    assert_eq!(config.max_size, 500);
    assert_eq!(config.max_latency, Duration::from_millis(50));
    assert_eq!(config.max_bytes, 8 * 1024 * 1024);
    assert!(!config.ordered);
    assert_eq!(config.max_retries, 5);
    assert_eq!(config.retry_backoff, Duration::from_millis(200));
}

// ==================== SinkRecord Tests ====================

#[test]
fn test_sink_record_upsert() {
    let mut values = HashMap::new();
    values.insert("id".to_string(), Value::Int64(1));
    values.insert("name".to_string(), Value::String("Alice".to_string()));

    let record = SinkRecord::upsert(
        Some("public".to_string()),
        "users",
        vec![Value::Int64(1)],
        values,
    );

    assert_eq!(record.schema, Some("public".to_string()));
    assert_eq!(record.table, "users");
    assert_eq!(record.key.len(), 1);
    assert!(matches!(record.mode, WriteMode::Upsert));
    assert!(record.offset.is_none());
}

#[test]
fn test_sink_record_delete() {
    let record = SinkRecord::delete(Some("public".to_string()), "users", vec![Value::Int64(1)]);

    assert_eq!(record.schema, Some("public".to_string()));
    assert_eq!(record.table, "users");
    assert_eq!(record.key.len(), 1);
    assert!(matches!(record.mode, WriteMode::Delete));
    assert!(record.values.is_empty());
}

#[test]
fn test_sink_record_with_offset() {
    let record = SinkRecord::upsert(None, "users", vec![], HashMap::new()).with_offset(12345);

    assert_eq!(record.offset, Some(12345));
}

// ==================== BatchResult Tests ====================

#[test]
fn test_batch_result_success() {
    let result = BatchResult {
        success_count: 100,
        failure_count: 0,
        failed_records: vec![],
        duration: Duration::from_millis(50),
    };

    assert!(result.is_success());
}

#[test]
fn test_batch_result_partial_failure() {
    let failed_record = FailedRecord {
        record: SinkRecord::upsert(None, "users", vec![], HashMap::new()),
        error: "Constraint violation".to_string(),
        attempts: 3,
        retriable: false,
    };

    let result = BatchResult {
        success_count: 95,
        failure_count: 5,
        failed_records: vec![failed_record],
        duration: Duration::from_millis(100),
    };

    assert!(!result.is_success());
    assert_eq!(result.failed_records.len(), 1);
}

#[test]
fn test_batch_result_default() {
    let result = BatchResult::default();

    assert_eq!(result.success_count, 0);
    assert_eq!(result.failure_count, 0);
    assert!(result.is_success()); // 0 failures = success
}

// ==================== SinkStats Tests ====================

#[test]
fn test_sink_stats_default() {
    let stats = SinkStats::default();

    assert_eq!(stats.records_written, 0);
    assert_eq!(stats.records_failed, 0);
    assert_eq!(stats.batches_written, 0);
    assert_eq!(stats.batches_failed, 0);
    assert_eq!(stats.records_per_second, 0.0);
}

#[test]
fn test_atomic_sink_stats() {
    let stats = AtomicSinkStats::default();

    // Record a successful batch
    stats.record_batch(1000, Duration::from_millis(100));
    let snapshot = stats.snapshot();

    assert_eq!(snapshot.records_written, 1000);
    assert_eq!(snapshot.batches_written, 1);
    assert_eq!(snapshot.total_write_time_ms, 100);
    // 1000 records in 100ms = 10000 records/second
    assert!((snapshot.records_per_second - 10000.0).abs() < 1.0);

    // Record a failed batch
    stats.record_batch_failure(50);
    let snapshot = stats.snapshot();

    assert_eq!(snapshot.records_failed, 50);
    assert_eq!(snapshot.batches_failed, 1);
}

// ==================== TableSinkBuilder Tests ====================

#[test]
fn test_table_sink_builder_default() {
    let config = TableSinkBuilder::new().build();

    assert!(matches!(config.write_mode, WriteMode::Upsert));
    assert!(matches!(config.auto_ddl, AutoDdlMode::None));
    assert!(config.delete_enabled);
    assert!(config.pk_columns.is_none());
}

#[test]
fn test_table_sink_builder() {
    let config = TableSinkBuilder::new()
        .batch_size(500)
        .batch_latency(Duration::from_millis(50))
        .write_mode(WriteMode::Insert)
        .auto_ddl(AutoDdlMode::Create)
        .schema_evolution(SchemaEvolutionMode::None)
        .delete_enabled(false)
        .pk_columns(vec!["id".to_string()])
        .include_columns(vec!["id".to_string(), "name".to_string()])
        .exclude_columns(vec!["created_at".to_string()])
        .build();

    assert_eq!(config.batch.max_size, 500);
    assert_eq!(config.batch.max_latency, Duration::from_millis(50));
    assert!(matches!(config.write_mode, WriteMode::Insert));
    assert!(matches!(config.auto_ddl, AutoDdlMode::Create));
    assert!(matches!(config.schema_evolution, SchemaEvolutionMode::None));
    assert!(!config.delete_enabled);
    assert_eq!(config.pk_columns, Some(vec!["id".to_string()]));
    assert_eq!(
        config.include_columns,
        Some(vec!["id".to_string(), "name".to_string()])
    );
    assert_eq!(config.exclude_columns, vec!["created_at".to_string()]);
}

// ==================== FailedRecord Tests ====================

#[test]
fn test_failed_record() {
    let record = SinkRecord::upsert(None, "users", vec![Value::Int64(1)], HashMap::new());

    let failed = FailedRecord {
        record: record.clone(),
        error: "Unique constraint violation".to_string(),
        attempts: 3,
        retriable: false,
    };

    assert_eq!(failed.error, "Unique constraint violation");
    assert_eq!(failed.attempts, 3);
    assert!(!failed.retriable);
}

#[test]
fn test_failed_record_retriable() {
    let record = SinkRecord::upsert(None, "users", vec![], HashMap::new());

    let failed = FailedRecord {
        record,
        error: "Connection timeout".to_string(),
        attempts: 1,
        retriable: true,
    };

    assert!(failed.retriable);
    assert_eq!(failed.attempts, 1);
}
