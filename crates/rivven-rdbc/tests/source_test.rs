//! Tests for rivven-rdbc source module

use rivven_rdbc::prelude::*;
use std::collections::HashMap;
use std::time::Duration;

// ==================== QueryMode Tests ====================

#[test]
fn test_query_mode_bulk() {
    let mode = QueryMode::Bulk;
    assert!(!mode.is_incremental());
}

#[test]
fn test_query_mode_incrementing() {
    let mode = QueryMode::incrementing("id");
    assert!(mode.is_incremental());

    if let QueryMode::Incrementing { column } = mode {
        assert_eq!(column, "id");
    } else {
        panic!("Expected Incrementing mode");
    }
}

#[test]
fn test_query_mode_timestamp() {
    let mode = QueryMode::timestamp("updated_at");
    assert!(mode.is_incremental());

    if let QueryMode::Timestamp { column, delay } = mode {
        assert_eq!(column, "updated_at");
        assert!(delay.is_none());
    } else {
        panic!("Expected Timestamp mode");
    }
}

#[test]
fn test_query_mode_timestamp_with_delay() {
    let mode = QueryMode::timestamp_with_delay("updated_at", Duration::from_secs(5));

    if let QueryMode::Timestamp { column, delay } = mode {
        assert_eq!(column, "updated_at");
        assert_eq!(delay, Some(Duration::from_secs(5)));
    } else {
        panic!("Expected Timestamp mode");
    }
}

#[test]
fn test_query_mode_timestamp_incrementing() {
    let mode = QueryMode::timestamp_incrementing("updated_at", "id");
    assert!(mode.is_incremental());

    if let QueryMode::TimestampIncrementing {
        timestamp_column,
        incrementing_column,
        delay,
    } = mode
    {
        assert_eq!(timestamp_column, "updated_at");
        assert_eq!(incrementing_column, "id");
        assert!(delay.is_none());
    } else {
        panic!("Expected TimestampIncrementing mode");
    }
}

#[test]
fn test_query_mode_default() {
    let mode = QueryMode::default();
    assert!(matches!(mode, QueryMode::Bulk));
}

// ==================== SourceOffset Tests ====================

#[test]
fn test_source_offset_default() {
    let offset = SourceOffset::default();
    assert!(offset.is_empty());
    assert!(offset.incrementing.is_none());
    assert!(offset.timestamp.is_none());
}

#[test]
fn test_source_offset_with_incrementing() {
    let offset = SourceOffset::with_incrementing(100_i64);
    assert!(!offset.is_empty());
    assert!(offset.incrementing.is_some());
    assert!(offset.timestamp.is_none());
}

#[test]
fn test_source_offset_with_timestamp() {
    let offset = SourceOffset::with_timestamp("2025-01-01T00:00:00Z");
    assert!(!offset.is_empty());
    assert!(offset.incrementing.is_none());
    assert!(offset.timestamp.is_some());
}

#[test]
fn test_source_offset_set_values() {
    let mut offset = SourceOffset::default();

    offset.set_incrementing(42_i64);
    assert_eq!(offset.incrementing.as_ref().unwrap().as_i64(), Some(42));

    offset.set_timestamp("2025-02-05");
    assert!(offset.timestamp.is_some());
}

// ==================== TableSourceConfig Tests ====================

#[test]
fn test_table_source_config_bulk() {
    let config = TableSourceConfig::bulk("users");

    assert_eq!(config.table, "users");
    assert!(config.schema.is_none());
    assert!(matches!(config.mode, QueryMode::Bulk));
    assert_eq!(config.batch_size, 1000);
    assert_eq!(config.topic_name(), "users");
}

#[test]
fn test_table_source_config_incrementing() {
    let config = TableSourceConfig::incrementing("orders", "order_id");

    assert_eq!(config.table, "orders");
    assert!(matches!(config.mode, QueryMode::Incrementing { .. }));
}

#[test]
fn test_table_source_config_timestamp() {
    let config = TableSourceConfig::timestamp("events", "created_at");

    assert_eq!(config.table, "events");
    assert!(matches!(config.mode, QueryMode::Timestamp { .. }));
}

#[test]
fn test_table_source_config_builder_methods() {
    let config = TableSourceConfig::bulk("users")
        .with_schema("public")
        .with_columns(vec!["id".to_string(), "name".to_string()])
        .with_where("active = true")
        .with_batch_size(500)
        .with_poll_interval(Duration::from_secs(10))
        .with_topic("users-events");

    assert_eq!(config.schema, Some("public".to_string()));
    assert_eq!(
        config.columns,
        Some(vec!["id".to_string(), "name".to_string()])
    );
    assert_eq!(config.where_clause, Some("active = true".to_string()));
    assert_eq!(config.batch_size, 500);
    assert_eq!(config.poll_interval, Duration::from_secs(10));
    assert_eq!(config.topic_name(), "users-events");
}

// ==================== SourceRecord Tests ====================

#[test]
fn test_source_record_new() {
    let mut values = HashMap::new();
    values.insert("id".to_string(), Value::Int64(1));
    values.insert("name".to_string(), Value::String("Alice".to_string()));

    let record = SourceRecord::new(
        Some("public".to_string()),
        "users",
        vec![Value::Int64(1)],
        values,
        SourceOffset::with_incrementing(1_i64),
    );

    assert_eq!(record.schema, Some("public".to_string()));
    assert_eq!(record.table, "users");
    assert_eq!(record.key.len(), 1);
    assert!(record.partition_key.is_none());
}

#[test]
fn test_source_record_with_partition_key() {
    let record = SourceRecord::new(
        None,
        "events",
        vec![],
        HashMap::new(),
        SourceOffset::default(),
    )
    .with_partition_key("user-123");

    assert_eq!(record.partition_key, Some("user-123".to_string()));
}

// ==================== PollResult Tests ====================

#[test]
fn test_poll_result_empty() {
    let result = PollResult::empty(SourceOffset::default());

    assert!(result.is_empty());
    assert_eq!(result.len(), 0);
    assert!(!result.has_more);
}

#[test]
fn test_poll_result_with_records() {
    let record = SourceRecord::new(
        None,
        "users",
        vec![],
        HashMap::new(),
        SourceOffset::default(),
    );

    let result = PollResult {
        records: vec![record],
        offset: SourceOffset::with_incrementing(1_i64),
        has_more: true,
    };

    assert!(!result.is_empty());
    assert_eq!(result.len(), 1);
    assert!(result.has_more);
}

// ==================== SourceStats Tests ====================

#[test]
fn test_source_stats_default() {
    let stats = SourceStats::default();

    assert_eq!(stats.records_polled, 0);
    assert_eq!(stats.polls, 0);
    assert_eq!(stats.empty_polls, 0);
    assert_eq!(stats.avg_records_per_poll, 0.0);
}

#[test]
fn test_atomic_source_stats() {
    let stats = AtomicSourceStats::default();

    // Record a poll with 10 records
    stats.record_poll(10, 50);
    let snapshot = stats.snapshot();

    assert_eq!(snapshot.records_polled, 10);
    assert_eq!(snapshot.polls, 1);
    assert_eq!(snapshot.empty_polls, 0);
    assert_eq!(snapshot.total_poll_time_ms, 50);
    assert!((snapshot.avg_records_per_poll - 10.0).abs() < 0.001);

    // Record an empty poll
    stats.record_poll(0, 10);
    let snapshot = stats.snapshot();

    assert_eq!(snapshot.records_polled, 10);
    assert_eq!(snapshot.polls, 2);
    assert_eq!(snapshot.empty_polls, 1);
}
