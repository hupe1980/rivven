//! CDC Latency Benchmarks
//!
//! Measures end-to-end latency characteristics:
//! - Event processing latency (p50, p95, p99)
//! - Pipeline latency with transforms
//! - Metrics collection overhead
//!
//! Run with: cargo bench -p rivven-cdc --bench cdc_latency

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rivven_cdc::common::ExtendedCdcMetrics;
use rivven_cdc::{CdcEvent, CdcFilter, CdcFilterConfig, CdcOp};
use serde_json::json;
use std::time::{Duration, Instant};

/// Benchmark processing time recording (tests internal histogram via metrics API)
fn benchmark_processing_time_recording(c: &mut Criterion) {
    let mut group = c.benchmark_group("processing_time");

    group.bench_function("record_single", |b| {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");
        b.iter(|| {
            metrics.record_processing_time(black_box(Duration::from_micros(50)));
        })
    });

    group.bench_function("record_batch_100", |b| {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");
        let durations: Vec<Duration> = (0..100)
            .map(|i| Duration::from_micros(10 + i * 2))
            .collect();
        b.iter(|| {
            for d in &durations {
                metrics.record_processing_time(black_box(*d));
            }
        })
    });

    group.bench_function("snapshot_with_percentiles", |b| {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");
        // Pre-populate with 10000 samples
        for i in 0..10000 {
            metrics.record_processing_time(Duration::from_micros(10 + (i % 1000)));
        }
        b.iter(|| black_box(metrics.snapshot()))
    });

    group.finish();
}

/// Benchmark ExtendedCdcMetrics operations
fn benchmark_extended_metrics(c: &mut Criterion) {
    let mut group = c.benchmark_group("extended_metrics");

    // Snapshot operations
    group.bench_function("start_snapshot", |b| {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");
        b.iter(|| {
            metrics.start_snapshot(black_box(10));
        })
    });

    group.bench_function("record_rows_scanned", |b| {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");
        metrics.start_snapshot(10);
        metrics.start_table_snapshot("users");
        b.iter(|| {
            metrics.record_rows_scanned(black_box("users"), black_box(1000));
        })
    });

    // Streaming operations
    group.bench_function("record_create_event", |b| {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");
        b.iter(|| {
            metrics.record_create_event();
        })
    });

    group.bench_function("record_update_event", |b| {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");
        b.iter(|| {
            metrics.record_update_event();
        })
    });

    group.bench_function("update_lag", |b| {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");
        b.iter(|| {
            metrics.update_milliseconds_behind_source(black_box(50));
        })
    });

    group.bench_function("record_committed_transaction", |b| {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");
        b.iter(|| {
            metrics.record_committed_transaction(Some(black_box("tx-12345")));
        })
    });

    // Incremental snapshot
    group.bench_function("incremental_snapshot_ops", |b| {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");
        b.iter(|| {
            metrics.start_incremental_snapshot(vec!["orders".to_string()]);
            metrics.open_incremental_snapshot_window("chunk-1");
            metrics.record_incremental_snapshot_chunk(100);
            metrics.close_incremental_snapshot_window();
        })
    });

    // Error tracking
    group.bench_function("record_error", |b| {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");
        b.iter(|| {
            metrics.record_error(black_box("connection"), black_box("Connection reset"));
        })
    });

    // Processing time
    group.bench_function("record_processing_time", |b| {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");
        b.iter(|| {
            metrics.record_processing_time(black_box(Duration::from_micros(50)));
        })
    });

    // Snapshot export
    group.bench_function("snapshot_export", |b| {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");
        // Populate with data
        metrics.start_snapshot(5);
        for i in 0..5 {
            let table = format!("table_{}", i);
            metrics.start_table_snapshot(&table);
            metrics.record_rows_scanned(&table, 10000);
            metrics.complete_table_snapshot(&table, 10000);
        }
        metrics.complete_snapshot();
        for _ in 0..1000 {
            metrics.record_create_event();
        }
        b.iter(|| black_box(metrics.snapshot()))
    });

    // Prometheus format export
    group.bench_function("prometheus_format", |b| {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");
        for _ in 0..1000 {
            metrics.record_create_event();
        }
        let snapshot = metrics.snapshot();
        b.iter(|| black_box(snapshot.to_prometheus()))
    });

    // Health check
    group.bench_function("is_healthy", |b| {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");
        metrics.set_streaming_connected(true);
        b.iter(|| black_box(metrics.is_healthy()))
    });

    group.finish();
}

/// Benchmark event pipeline latency with transforms
fn benchmark_pipeline_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_latency");

    // Create filter with various complexity levels
    let simple_filter = CdcFilter::new(CdcFilterConfig {
        include_tables: vec!["public.*".to_string()],
        ..Default::default()
    })
    .unwrap();

    let complex_filter = CdcFilter::new(CdcFilterConfig {
        include_tables: vec![
            "public.users".to_string(),
            "public.orders".to_string(),
            "sales.*".to_string(),
        ],
        exclude_tables: vec!["*_audit".to_string(), "*_temp".to_string()],
        mask_columns: vec!["password".to_string(), "ssn".to_string()],
        global_exclude_columns: vec!["internal_id".to_string()],
        ..Default::default()
    })
    .unwrap();

    // Test events of varying sizes
    let small_event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        op: CdcOp::Insert,
        before: None,
        after: Some(json!({"id": 1, "name": "Alice", "email": "alice@example.com"})),
        timestamp: 1234567890,
        transaction: None,
    };

    let medium_event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "orders".to_string(),
        op: CdcOp::Update,
        before: Some(json!({
            "id": 1,
            "user_id": 100,
            "status": "pending",
            "total": 99.99,
            "items": [{"sku": "ABC", "qty": 2}],
            "shipping_address": {"street": "123 Main", "city": "NYC"}
        })),
        after: Some(json!({
            "id": 1,
            "user_id": 100,
            "status": "shipped",
            "total": 99.99,
            "items": [{"sku": "ABC", "qty": 2}],
            "shipping_address": {"street": "123 Main", "city": "NYC"}
        })),
        timestamp: 1234567890,
        transaction: None,
    };

    let large_fields: serde_json::Value = (0..100)
        .map(|i| (format!("field_{}", i), json!(format!("value_{}", i))))
        .collect();

    let large_event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "big_table".to_string(),
        op: CdcOp::Update,
        before: Some(large_fields.clone()),
        after: Some(large_fields),
        timestamp: 1234567890,
        transaction: None,
    };

    // Simple filter on different event sizes
    group.bench_function("simple_filter_small_event", |b| {
        b.iter(|| {
            let mut e = small_event.clone();
            black_box(simple_filter.filter_event(&mut e))
        })
    });

    group.bench_function("simple_filter_medium_event", |b| {
        b.iter(|| {
            let mut e = medium_event.clone();
            black_box(simple_filter.filter_event(&mut e))
        })
    });

    group.bench_function("simple_filter_large_event", |b| {
        b.iter(|| {
            let mut e = large_event.clone();
            black_box(simple_filter.filter_event(&mut e))
        })
    });

    // Complex filter on different event sizes
    group.bench_function("complex_filter_small_event", |b| {
        b.iter(|| {
            let mut e = small_event.clone();
            black_box(complex_filter.filter_event(&mut e))
        })
    });

    group.bench_function("complex_filter_medium_event", |b| {
        b.iter(|| {
            let mut e = medium_event.clone();
            black_box(complex_filter.filter_event(&mut e))
        })
    });

    group.bench_function("complex_filter_large_event", |b| {
        b.iter(|| {
            let mut e = large_event.clone();
            black_box(complex_filter.filter_event(&mut e))
        })
    });

    group.finish();
}

/// Benchmark end-to-end event processing simulation
fn benchmark_e2e_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_processing");
    group.sample_size(50); // Reduce samples for longer benchmarks

    let filter = CdcFilter::new(CdcFilterConfig {
        include_tables: vec!["public.*".to_string()],
        mask_columns: vec!["password".to_string()],
        ..Default::default()
    })
    .unwrap();

    let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

    // Simulate processing batches of events
    for batch_size in [10, 100, 500].iter() {
        let events: Vec<CdcEvent> = (0..*batch_size)
            .map(|i| CdcEvent {
                source_type: "postgres".to_string(),
                database: "testdb".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
                op: if i % 3 == 0 {
                    CdcOp::Insert
                } else if i % 3 == 1 {
                    CdcOp::Update
                } else {
                    CdcOp::Delete
                },
                before: if i % 3 != 0 {
                    Some(json!({"id": i, "name": format!("User{}", i), "password": "secret"}))
                } else {
                    None
                },
                after: if i % 3 != 2 {
                    Some(json!({"id": i, "name": format!("User{}", i), "password": "secret"}))
                } else {
                    None
                },
                timestamp: 1234567890 + i,
                transaction: None,
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::new("process_batch_with_metrics", batch_size),
            &events,
            |b, events| {
                b.iter(|| {
                    let start = Instant::now();
                    let mut processed = Vec::with_capacity(events.len());

                    for event in events {
                        let mut e = event.clone();
                        if filter.filter_event(&mut e) {
                            // Record metrics based on operation
                            match e.op {
                                CdcOp::Insert => metrics.record_create_event(),
                                CdcOp::Update => metrics.record_update_event(),
                                CdcOp::Delete => metrics.record_delete_event(),
                                _ => {}
                            }
                            processed.push(e);
                        }
                    }

                    // Record batch processing time
                    metrics.record_processing_time(start.elapsed());
                    metrics.record_batch(processed.len() as u64);

                    processed
                })
            },
        );
    }

    group.finish();
}

/// Memory allocation benchmarks (measures allocations per operation)
fn benchmark_memory_efficiency(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_efficiency");

    // Event cloning cost
    let event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        op: CdcOp::Update,
        before: Some(json!({"id": 1, "name": "Old", "data": "x".repeat(1000)})),
        after: Some(json!({"id": 1, "name": "New", "data": "y".repeat(1000)})),
        timestamp: 1234567890,
        transaction: None,
    };

    group.bench_function("event_clone", |b| b.iter(|| black_box(event.clone())));

    // Serialization allocation
    group.bench_function("event_to_json_bytes", |b| {
        b.iter(|| black_box(serde_json::to_vec(&event)))
    });

    // Metrics snapshot allocation
    let metrics = ExtendedCdcMetrics::new("postgres", "testdb");
    for _ in 0..1000 {
        metrics.record_create_event();
    }

    group.bench_function("metrics_snapshot_allocation", |b| {
        b.iter(|| black_box(metrics.snapshot()))
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_processing_time_recording,
    benchmark_extended_metrics,
    benchmark_pipeline_latency,
    benchmark_e2e_processing,
    benchmark_memory_efficiency,
);

criterion_main!(benches);
