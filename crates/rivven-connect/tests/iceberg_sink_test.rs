//! Integration tests for the Apache Iceberg sink
//!
//! These tests verify the Iceberg sink functionality including:
//! - Configuration validation
//! - Catalog property building
//! - Event to Arrow conversion
//! - Batch writing

#![cfg(feature = "iceberg")]

use rivven_connect::connectors::lakehouse::iceberg::{
    build_catalog_properties, CatalogConfig, CatalogType, CommitMode, GlueCatalogConfig,
    HiveCatalogConfig, IcebergSink, IcebergSinkConfig, PartitionStrategy, RestCatalogConfig,
    S3StorageConfig, SchemaEvolution,
};
use rivven_connect::prelude::*;
use std::collections::HashMap;

// ============================================================================
// Configuration Tests
// ============================================================================

#[test]
fn test_iceberg_config_deserialization_rest() {
    let yaml = r#"
        catalog:
          type: rest
          rest:
            uri: http://localhost:8181
            warehouse: s3://bucket/warehouse
        namespace: analytics
        table: events
        batch_size: 5000
        target_file_size_mb: 64
    "#;

    let config: IcebergSinkConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.catalog.catalog_type, CatalogType::Rest);
    assert_eq!(config.namespace, "analytics");
    assert_eq!(config.table, "events");
    assert_eq!(config.batch_size, 5000);
    assert_eq!(config.target_file_size_mb, 64);
}

#[test]
fn test_iceberg_config_deserialization_glue() {
    let yaml = r#"
        catalog:
          type: glue
          glue:
            region: us-west-2
            catalog_id: "123456789012"
          warehouse: s3://bucket/warehouse
        namespace: default
        table: orders
    "#;

    let config: IcebergSinkConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.catalog.catalog_type, CatalogType::Glue);
    assert!(config.catalog.glue.is_some());
    let glue = config.catalog.glue.unwrap();
    assert_eq!(glue.region, "us-west-2");
    assert_eq!(glue.catalog_id, Some("123456789012".to_string()));
}

#[test]
fn test_iceberg_config_deserialization_hive() {
    let yaml = r#"
        catalog:
          type: hive
          hive:
            uri: thrift://hive-metastore:9083
            warehouse: s3://bucket/warehouse
        namespace: default
        table: events
    "#;

    let config: IcebergSinkConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.catalog.catalog_type, CatalogType::Hive);
    assert!(config.catalog.hive.is_some());
    let hive = config.catalog.hive.unwrap();
    assert_eq!(hive.uri, "thrift://hive-metastore:9083");
}

#[test]
fn test_iceberg_config_with_partitioning() {
    let yaml = r#"
        catalog:
          type: memory
          warehouse: /tmp/warehouse
        namespace: test
        table: partitioned_events
        partitioning: identity
        partition_fields:
          - region
          - date
    "#;

    let config: IcebergSinkConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.partitioning, PartitionStrategy::Identity);
    assert_eq!(config.partition_fields, vec!["region", "date"]);
}

#[test]
fn test_iceberg_config_with_bucket_partitioning() {
    let yaml = r#"
        catalog:
          type: memory
          warehouse: /tmp/warehouse
        namespace: test
        table: bucketed_events
        partitioning: bucket
        partition_fields:
          - user_id
        num_buckets: 32
    "#;

    let config: IcebergSinkConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.partitioning, PartitionStrategy::Bucket);
    assert_eq!(config.num_buckets, 32);
}

#[test]
fn test_iceberg_config_with_s3_storage() {
    let yaml = r#"
        catalog:
          type: rest
          rest:
            uri: http://localhost:8181
        namespace: analytics
        table: events
        s3:
          region: eu-west-1
          endpoint: http://minio:9000
          path_style_access: true
    "#;

    let config: IcebergSinkConfig = serde_yaml::from_str(yaml).unwrap();
    assert!(config.s3.is_some());
    let s3 = config.s3.unwrap();
    assert_eq!(s3.region, "eu-west-1");
    assert_eq!(s3.endpoint, Some("http://minio:9000".to_string()));
    assert!(s3.path_style_access);
}

#[test]
fn test_commit_mode_serialization() {
    assert_eq!(
        serde_json::to_string(&CommitMode::Append).unwrap(),
        "\"append\""
    );
    assert_eq!(
        serde_json::to_string(&CommitMode::Overwrite).unwrap(),
        "\"overwrite\""
    );
    assert_eq!(
        serde_json::to_string(&CommitMode::Upsert).unwrap(),
        "\"upsert\""
    );
}

#[test]
fn test_schema_evolution_serialization() {
    assert_eq!(
        serde_json::to_string(&SchemaEvolution::Strict).unwrap(),
        "\"strict\""
    );
    assert_eq!(
        serde_json::to_string(&SchemaEvolution::AddColumns).unwrap(),
        "\"add_columns\""
    );
    assert_eq!(
        serde_json::to_string(&SchemaEvolution::Full).unwrap(),
        "\"full\""
    );
}

#[test]
fn test_compression_codec_serialization() {
    use rivven_connect::connectors::lakehouse::iceberg::CompressionCodec;

    assert_eq!(
        serde_json::to_string(&CompressionCodec::None).unwrap(),
        "\"none\""
    );
    assert_eq!(
        serde_json::to_string(&CompressionCodec::Snappy).unwrap(),
        "\"snappy\""
    );
    assert_eq!(
        serde_json::to_string(&CompressionCodec::Gzip).unwrap(),
        "\"gzip\""
    );
    assert_eq!(
        serde_json::to_string(&CompressionCodec::Lz4).unwrap(),
        "\"lz4\""
    );
    assert_eq!(
        serde_json::to_string(&CompressionCodec::Zstd).unwrap(),
        "\"zstd\""
    );
    assert_eq!(
        serde_json::to_string(&CompressionCodec::Brotli).unwrap(),
        "\"brotli\""
    );

    // Test default is snappy
    assert_eq!(CompressionCodec::default(), CompressionCodec::Snappy);
}

// ============================================================================
// Catalog Properties Tests
// ============================================================================

#[test]
fn test_build_catalog_properties_rest() {
    let config = IcebergSinkConfig {
        catalog: CatalogConfig {
            catalog_type: CatalogType::Rest,
            rest: Some(RestCatalogConfig {
                uri: "http://localhost:8181".to_string(),
                warehouse: Some("s3://bucket/warehouse".to_string()),
                credential: None,
                properties: HashMap::from([("custom.prop".to_string(), "value".to_string())]),
            }),
            ..Default::default()
        },
        namespace: "analytics".to_string(),
        table: "events".to_string(),
        ..Default::default()
    };

    let props = build_catalog_properties(&config);
    assert_eq!(props.get("uri"), Some(&"http://localhost:8181".to_string()));
    assert_eq!(
        props.get("warehouse"),
        Some(&"s3://bucket/warehouse".to_string())
    );
    assert_eq!(props.get("custom.prop"), Some(&"value".to_string()));
}

#[test]
fn test_build_catalog_properties_glue() {
    let config = IcebergSinkConfig {
        catalog: CatalogConfig {
            catalog_type: CatalogType::Glue,
            glue: Some(GlueCatalogConfig {
                region: "us-west-2".to_string(),
                catalog_id: Some("123456789012".to_string()),
                ..Default::default()
            }),
            warehouse: Some("s3://bucket/warehouse".to_string()),
            ..Default::default()
        },
        namespace: "default".to_string(),
        table: "events".to_string(),
        ..Default::default()
    };

    let props = build_catalog_properties(&config);
    assert_eq!(props.get("region"), Some(&"us-west-2".to_string()));
    assert_eq!(props.get("catalog-id"), Some(&"123456789012".to_string()));
    assert_eq!(
        props.get("warehouse"),
        Some(&"s3://bucket/warehouse".to_string())
    );
}

#[test]
fn test_build_catalog_properties_hive() {
    let config = IcebergSinkConfig {
        catalog: CatalogConfig {
            catalog_type: CatalogType::Hive,
            hive: Some(HiveCatalogConfig {
                uri: "thrift://localhost:9083".to_string(),
                warehouse: Some("hdfs:///user/hive/warehouse".to_string()),
            }),
            ..Default::default()
        },
        namespace: "default".to_string(),
        table: "events".to_string(),
        ..Default::default()
    };

    let props = build_catalog_properties(&config);
    assert_eq!(
        props.get("uri"),
        Some(&"thrift://localhost:9083".to_string())
    );
    assert_eq!(
        props.get("warehouse"),
        Some(&"hdfs:///user/hive/warehouse".to_string())
    );
}

#[test]
fn test_build_catalog_properties_with_s3() {
    let config = IcebergSinkConfig {
        catalog: CatalogConfig {
            catalog_type: CatalogType::Memory,
            warehouse: Some("/tmp/warehouse".to_string()),
            ..Default::default()
        },
        namespace: "test".to_string(),
        table: "events".to_string(),
        s3: Some(S3StorageConfig {
            region: "us-east-1".to_string(),
            endpoint: Some("http://localhost:9000".to_string()),
            path_style_access: true,
            ..Default::default()
        }),
        ..Default::default()
    };

    let props = build_catalog_properties(&config);
    assert_eq!(props.get("io-impl"), Some(&"s3".to_string()));
    assert_eq!(props.get("s3.region"), Some(&"us-east-1".to_string()));
    assert_eq!(
        props.get("s3.endpoint"),
        Some(&"http://localhost:9000".to_string())
    );
    assert_eq!(props.get("s3.path-style-access"), Some(&"true".to_string()));
}

// ============================================================================
// Sink Check Tests
// ============================================================================

#[tokio::test]
async fn test_sink_check_valid_rest_config() {
    let sink = IcebergSink::new();
    let config = IcebergSinkConfig {
        catalog: CatalogConfig {
            catalog_type: CatalogType::Rest,
            rest: Some(RestCatalogConfig {
                uri: "http://localhost:8181".to_string(),
                warehouse: Some("s3://bucket/warehouse".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        },
        namespace: "analytics".to_string(),
        table: "events".to_string(),
        ..Default::default()
    };

    let result = sink.check(&config).await.unwrap();
    assert!(
        result.is_success(),
        "Check should pass for valid REST config"
    );
}

#[tokio::test]
async fn test_sink_check_valid_memory_config() {
    let sink = IcebergSink::new();
    let config = IcebergSinkConfig {
        catalog: CatalogConfig {
            catalog_type: CatalogType::Memory,
            warehouse: Some("/tmp/warehouse".to_string()),
            ..Default::default()
        },
        namespace: "test".to_string(),
        table: "events".to_string(),
        ..Default::default()
    };

    let result = sink.check(&config).await.unwrap();
    assert!(
        result.is_success(),
        "Check should pass for valid memory config"
    );
}

#[tokio::test]
async fn test_sink_check_missing_rest_uri() {
    let sink = IcebergSink::new();
    let config = IcebergSinkConfig {
        catalog: CatalogConfig {
            catalog_type: CatalogType::Rest,
            rest: Some(RestCatalogConfig {
                uri: "".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        },
        namespace: "analytics".to_string(),
        table: "events".to_string(),
        ..Default::default()
    };

    let result = sink.check(&config).await.unwrap();
    assert!(!result.is_success(), "Check should fail for missing URI");
}

#[tokio::test]
async fn test_sink_check_missing_namespace() {
    let sink = IcebergSink::new();
    let config = IcebergSinkConfig {
        catalog: CatalogConfig {
            catalog_type: CatalogType::Memory,
            ..Default::default()
        },
        namespace: "".to_string(),
        table: "events".to_string(),
        ..Default::default()
    };

    let result = sink.check(&config).await.unwrap();
    assert!(
        !result.is_success(),
        "Check should fail for missing namespace"
    );
}

#[tokio::test]
async fn test_sink_check_missing_table() {
    let sink = IcebergSink::new();
    let config = IcebergSinkConfig {
        catalog: CatalogConfig {
            catalog_type: CatalogType::Memory,
            ..Default::default()
        },
        namespace: "test".to_string(),
        table: "".to_string(),
        ..Default::default()
    };

    let result = sink.check(&config).await.unwrap();
    assert!(!result.is_success(), "Check should fail for missing table");
}

#[tokio::test]
async fn test_sink_check_missing_partition_fields() {
    let sink = IcebergSink::new();
    let config = IcebergSinkConfig {
        catalog: CatalogConfig {
            catalog_type: CatalogType::Memory,
            warehouse: Some("/tmp/warehouse".to_string()),
            ..Default::default()
        },
        namespace: "test".to_string(),
        table: "events".to_string(),
        partitioning: PartitionStrategy::Identity,
        partition_fields: vec![], // Empty - should fail
        ..Default::default()
    };

    let result = sink.check(&config).await.unwrap();
    assert!(
        !result.is_success(),
        "Check should fail for missing partition fields"
    );
}

#[tokio::test]
async fn test_sink_check_invalid_rest_uri() {
    let sink = IcebergSink::new();
    let config = IcebergSinkConfig {
        catalog: CatalogConfig {
            catalog_type: CatalogType::Rest,
            rest: Some(RestCatalogConfig {
                uri: "not-a-valid-uri".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        },
        namespace: "analytics".to_string(),
        table: "events".to_string(),
        ..Default::default()
    };

    let result = sink.check(&config).await.unwrap();
    assert!(
        !result.is_success(),
        "Check should fail for invalid REST URI"
    );
}

// ============================================================================
// Write Tests (simulated)
// ============================================================================

#[tokio::test]
async fn test_sink_write_empty_stream() {
    let sink = IcebergSink::new();
    let config = IcebergSinkConfig {
        catalog: CatalogConfig {
            catalog_type: CatalogType::Memory,
            warehouse: Some("/tmp/warehouse".to_string()),
            ..Default::default()
        },
        namespace: "test".to_string(),
        table: "events".to_string(),
        ..Default::default()
    };

    let events: Vec<SourceEvent> = vec![];
    let stream = futures::stream::iter(events);

    let result = sink.write(&config, Box::pin(stream)).await.unwrap();

    assert_eq!(result.records_written, 0);
    assert_eq!(result.records_failed, 0);
}

#[tokio::test]
async fn test_sink_write_single_batch() {
    let sink = IcebergSink::new();
    let config = IcebergSinkConfig {
        catalog: CatalogConfig {
            catalog_type: CatalogType::Memory,
            warehouse: Some("file:///tmp/iceberg-test-warehouse".to_string()),
            ..Default::default()
        },
        namespace: "test".to_string(),
        table: "events".to_string(),
        batch_size: 100, // Larger than event count, so flush happens at end
        ..Default::default()
    };

    let events: Vec<SourceEvent> = (0..50)
        .map(|i| {
            SourceEvent::record(
                "test_stream",
                serde_json::json!({
                    "id": i,
                    "name": format!("event_{}", i),
                    "timestamp": "2026-02-03T12:00:00Z"
                }),
            )
        })
        .collect();

    let stream = futures::stream::iter(events);

    let result = sink.write(&config, Box::pin(stream)).await.unwrap();

    // Check that we got some result (may not be 50 if write has issues)
    eprintln!(
        "Write result: records_written={}, records_failed={}",
        result.records_written, result.records_failed
    );
    if !result.errors.is_empty() {
        eprintln!("Errors: {:?}", result.errors);
    }

    // Events should be written on stream close
    assert_eq!(result.records_written, 50);
    assert_eq!(result.records_failed, 0);
}

#[tokio::test]
async fn test_sink_write_multiple_batches() {
    let sink = IcebergSink::new();
    let config = IcebergSinkConfig {
        catalog: CatalogConfig {
            catalog_type: CatalogType::Memory,
            warehouse: Some("/tmp/warehouse".to_string()),
            ..Default::default()
        },
        namespace: "test".to_string(),
        table: "events".to_string(),
        batch_size: 25, // Small batch size to force multiple flushes
        ..Default::default()
    };

    let events: Vec<SourceEvent> = (0..100)
        .map(|i| {
            SourceEvent::record(
                "test_stream",
                serde_json::json!({
                    "id": i,
                    "data": format!("payload_{}", i)
                }),
            )
        })
        .collect();

    let stream = futures::stream::iter(events);

    let result = sink.write(&config, Box::pin(stream)).await.unwrap();

    // Debug output
    eprintln!(
        "Multiple batches result: records_written={}, records_failed={}",
        result.records_written, result.records_failed
    );
    if !result.errors.is_empty() {
        eprintln!("Errors: {:?}", result.errors);
    }

    assert_eq!(result.records_written, 100);
    assert_eq!(result.records_failed, 0);
}

// ============================================================================
// Factory Tests
// ============================================================================

#[test]
fn test_iceberg_factory() {
    use rivven_connect::connectors::lakehouse::iceberg::IcebergSinkFactory;
    use rivven_connect::traits::SinkFactory;

    let factory = IcebergSinkFactory;
    let spec = factory.spec();
    assert_eq!(spec.connector_type, "iceberg");

    let _sink = factory.create().unwrap();
    // Successfully created sink
}

// ============================================================================
// Metrics Tests
// ============================================================================

#[test]
fn test_metrics_default_values() {
    use rivven_connect::connectors::lakehouse::IcebergSinkMetrics;

    let metrics = IcebergSinkMetrics::new();
    assert_eq!(
        metrics
            .records_written
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        metrics
            .commits_success
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(metrics.avg_commit_latency_ms(), 0.0);
    assert_eq!(metrics.success_rate(), 1.0); // 0/0 defaults to 100%
    assert_eq!(metrics.retry_rate(), 0.0);
}

#[test]
fn test_metrics_calculations() {
    use rivven_connect::connectors::lakehouse::IcebergSinkMetrics;
    use std::sync::atomic::Ordering;

    let metrics = IcebergSinkMetrics::new();

    // Simulate some writes
    metrics.records_written.store(900, Ordering::Relaxed);
    metrics.records_failed.store(100, Ordering::Relaxed);
    metrics.commits_success.store(9, Ordering::Relaxed);
    metrics.commits_failed.store(1, Ordering::Relaxed);
    metrics.commit_retries.store(3, Ordering::Relaxed);
    metrics.commit_latency_us.store(9000, Ordering::Relaxed); // 9000us / 9 commits = 1000us = 1ms avg
    metrics.batches_flushed.store(10, Ordering::Relaxed);

    // Verify calculations
    assert_eq!(metrics.success_rate(), 0.9); // 900/1000
    assert_eq!(metrics.retry_rate(), 0.3); // 3 retries / 10 total commits
    assert_eq!(metrics.avg_commit_latency_ms(), 1.0); // 9000us / 9 = 1000us = 1ms
}

#[test]
fn test_metrics_bytes_per_second() {
    use rivven_connect::connectors::lakehouse::IcebergSinkMetrics;
    use std::sync::atomic::Ordering;

    let metrics = IcebergSinkMetrics::new();
    metrics.bytes_written.store(1_000_000, Ordering::Relaxed);

    // 1MB over 2 seconds = 500KB/s
    let throughput = metrics.bytes_per_second(2.0);
    assert_eq!(throughput, 500_000.0);

    // Edge case: 0 elapsed time
    assert_eq!(metrics.bytes_per_second(0.0), 0.0);
    assert_eq!(metrics.bytes_per_second(-1.0), 0.0);
}

#[tokio::test]
async fn test_sink_metrics_after_write() {
    use std::sync::atomic::Ordering;

    let sink = IcebergSink::new();

    // Initial metrics should be zero
    let metrics = sink.metrics();
    assert_eq!(metrics.records_written.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.batches_flushed.load(Ordering::Relaxed), 0);

    // After a successful write, metrics should be updated
    // (Note: We can't actually test this without a real catalog,
    // but we can verify the metrics struct is accessible)
    assert_eq!(metrics.commits_success.load(Ordering::Relaxed), 0);
}

#[test]
fn test_metrics_snapshot() {
    use rivven_connect::connectors::lakehouse::IcebergSinkMetrics;
    use std::sync::atomic::Ordering;

    let metrics = IcebergSinkMetrics::new();

    // Set some values
    metrics.records_written.store(1000, Ordering::Relaxed);
    metrics.bytes_written.store(50000, Ordering::Relaxed);
    metrics.commits_success.store(10, Ordering::Relaxed);
    metrics.batches_flushed.store(10, Ordering::Relaxed);
    metrics.commit_latency_us.store(50000, Ordering::Relaxed); // 5ms avg

    // Take a snapshot
    let snapshot = metrics.snapshot();

    // Verify snapshot captures all values
    assert_eq!(snapshot.records_written, 1000);
    assert_eq!(snapshot.bytes_written, 50000);
    assert_eq!(snapshot.commits_success, 10);
    assert_eq!(snapshot.batches_flushed, 10);
    assert_eq!(snapshot.commit_latency_us, 50000);

    // Verify snapshot methods work
    assert_eq!(snapshot.avg_commit_latency_ms(), 5.0);
    assert_eq!(snapshot.success_rate(), 1.0);
    assert_eq!(snapshot.bytes_per_second(10.0), 5000.0);
    assert_eq!(snapshot.records_per_second(10.0), 100.0);

    // Original metrics should be unchanged
    assert_eq!(metrics.records_written.load(Ordering::Relaxed), 1000);
}

#[test]
fn test_metrics_reset() {
    use rivven_connect::connectors::lakehouse::IcebergSinkMetrics;
    use std::sync::atomic::Ordering;

    let metrics = IcebergSinkMetrics::new();

    // Set some values
    metrics.records_written.store(1000, Ordering::Relaxed);
    metrics.bytes_written.store(50000, Ordering::Relaxed);
    metrics.commits_success.store(10, Ordering::Relaxed);
    metrics.commit_retries.store(5, Ordering::Relaxed);
    metrics.files_created.store(10, Ordering::Relaxed);

    // Reset
    metrics.reset();

    // All values should be zero
    assert_eq!(metrics.records_written.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.bytes_written.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.commits_success.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.commit_retries.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.files_created.load(Ordering::Relaxed), 0);
}

#[test]
fn test_metrics_snapshot_and_reset() {
    use rivven_connect::connectors::lakehouse::IcebergSinkMetrics;
    use std::sync::atomic::Ordering;

    let metrics = IcebergSinkMetrics::new();

    // Set some values
    metrics.records_written.store(1000, Ordering::Relaxed);
    metrics.bytes_written.store(50000, Ordering::Relaxed);
    metrics.commits_success.store(10, Ordering::Relaxed);
    metrics.batches_flushed.store(10, Ordering::Relaxed);

    // Snapshot and reset atomically
    let snapshot = metrics.snapshot_and_reset();

    // Snapshot should have the original values
    assert_eq!(snapshot.records_written, 1000);
    assert_eq!(snapshot.bytes_written, 50000);
    assert_eq!(snapshot.commits_success, 10);
    assert_eq!(snapshot.batches_flushed, 10);

    // Metrics should now be zero
    assert_eq!(metrics.records_written.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.bytes_written.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.commits_success.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.batches_flushed.load(Ordering::Relaxed), 0);
}

#[test]
fn test_metrics_snapshot_clone_and_compare() {
    use rivven_connect::connectors::lakehouse::MetricsSnapshot;

    let snapshot1 = MetricsSnapshot {
        records_written: 100,
        bytes_written: 5000,
        ..Default::default()
    };

    let snapshot2 = snapshot1.clone();
    assert_eq!(snapshot1, snapshot2);

    // Different snapshots are not equal
    let snapshot3 = MetricsSnapshot {
        records_written: 200,
        ..Default::default()
    };
    assert_ne!(snapshot1, snapshot3);
}

#[test]
fn test_metrics_records_per_second() {
    use rivven_connect::connectors::lakehouse::IcebergSinkMetrics;
    use std::sync::atomic::Ordering;

    let metrics = IcebergSinkMetrics::new();
    metrics.records_written.store(10000, Ordering::Relaxed);

    // 10000 records over 10 seconds = 1000 rps
    assert_eq!(metrics.records_per_second(10.0), 1000.0);

    // Edge cases
    assert_eq!(metrics.records_per_second(0.0), 0.0);
    assert_eq!(metrics.records_per_second(-1.0), 0.0);
}
