//! CDC Throughput Benchmarks
//!
//! Measures CDC performance characteristics:
//! - Schema inference throughput
//! - Event parsing throughput
//! - Filter evaluation throughput
//!
//! Run with: cargo bench -p rivven-cdc

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rivven_cdc::{CdcEvent, CdcFilter, CdcFilterConfig, CdcOp, PostgresTypeMapper};
use serde_json::json;

fn benchmark_schema_inference(c: &mut Criterion) {
    let mut group = c.benchmark_group("schema_inference");

    // Various table sizes
    for column_count in [5, 10, 20, 50, 100].iter() {
        let columns: Vec<(String, i32, String)> = (0..*column_count)
            .map(|i| {
                let type_oid = match i % 5 {
                    0 => 23,   // int4
                    1 => 25,   // text
                    2 => 1043, // varchar
                    3 => 16,   // bool
                    _ => 1114, // timestamp
                };
                (format!("col_{}", i), type_oid, format!("type_{}", type_oid))
            })
            .collect();

        group.throughput(Throughput::Elements(*column_count as u64));
        group.bench_with_input(
            BenchmarkId::new("generate_avro_schema", column_count),
            &columns,
            |b, cols| {
                b.iter(|| {
                    PostgresTypeMapper::generate_avro_schema(
                        black_box("public"),
                        black_box("test_table"),
                        black_box(cols),
                    )
                })
            },
        );
    }

    group.finish();
}

fn benchmark_type_mapping(c: &mut Criterion) {
    let mut group = c.benchmark_group("type_mapping");

    let test_cases = vec![
        (23, "int4"),
        (25, "text"),
        (1043, "varchar"),
        (16, "bool"),
        (1114, "timestamp"),
        (3802, "jsonb"),
        (2950, "uuid"),
    ];

    for (oid, name) in test_cases {
        group.bench_with_input(
            BenchmarkId::new("pg_type_to_avro", name),
            &(oid, name),
            |b, &(oid, name)| {
                b.iter(|| PostgresTypeMapper::pg_type_to_avro(black_box(oid), black_box(name)))
            },
        );
    }

    group.finish();
}

fn benchmark_event_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_serialization");

    // Small event
    let small_event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        op: CdcOp::Insert,
        before: None,
        after: Some(json!({"id": 1, "name": "Alice"})),
        timestamp: 1234567890,
        transaction: None,
    };

    // Large event
    let large_data: serde_json::Value = (0..50)
        .map(|i| (format!("field_{}", i), json!(format!("value_{}", i))))
        .collect();

    let large_event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "large_table".to_string(),
        op: CdcOp::Update,
        before: Some(large_data.clone()),
        after: Some(large_data),
        timestamp: 1234567890,
        transaction: None,
    };

    group.bench_function("to_message_small", |b| {
        b.iter(|| black_box(&small_event).to_message())
    });

    group.bench_function("to_message_large", |b| {
        b.iter(|| black_box(&large_event).to_message())
    });

    // Deserialize
    let small_bytes = serde_json::to_vec(&small_event).unwrap();
    let large_bytes = serde_json::to_vec(&large_event).unwrap();

    group.throughput(Throughput::Bytes(small_bytes.len() as u64));
    group.bench_function("parse_small", |b| {
        b.iter(|| serde_json::from_slice::<CdcEvent>(black_box(&small_bytes)))
    });

    group.throughput(Throughput::Bytes(large_bytes.len() as u64));
    group.bench_function("parse_large", |b| {
        b.iter(|| serde_json::from_slice::<CdcEvent>(black_box(&large_bytes)))
    });

    group.finish();
}

fn benchmark_filter_evaluation(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_evaluation");

    // Simple filter
    let simple_config = CdcFilterConfig {
        include_tables: vec!["public.*".to_string()],
        exclude_tables: vec![],
        ..Default::default()
    };
    let simple_filter = CdcFilter::new(simple_config).unwrap();

    // Complex filter
    let complex_config = CdcFilterConfig {
        include_tables: vec![
            "public.users".to_string(),
            "public.orders".to_string(),
            "public.products".to_string(),
            "sales.*".to_string(),
        ],
        exclude_tables: vec![
            "*_audit".to_string(),
            "*_log".to_string(),
            "temp_*".to_string(),
        ],
        mask_columns: vec![
            "password".to_string(),
            "ssn".to_string(),
            "credit_card".to_string(),
        ],
        global_exclude_columns: vec!["internal_id".to_string(), "created_by".to_string()],
        ..Default::default()
    };
    let complex_filter = CdcFilter::new(complex_config).unwrap();

    // Test data
    let test_tables = vec![
        ("public", "users"),
        ("public", "orders"),
        ("public", "users_audit"),
        ("sales", "transactions"),
        ("temp", "data"),
    ];

    group.bench_function("simple_filter_table_check", |b| {
        b.iter(|| {
            for (schema, table) in &test_tables {
                black_box(simple_filter.should_include_table(schema, table));
            }
        })
    });

    group.bench_function("complex_filter_table_check", |b| {
        b.iter(|| {
            for (schema, table) in &test_tables {
                black_box(complex_filter.should_include_table(schema, table));
            }
        })
    });

    // Event filtering
    let mut event = CdcEvent {
        source_type: "postgres".to_string(),
        database: "testdb".to_string(),
        schema: "public".to_string(),
        table: "users".to_string(),
        op: CdcOp::Insert,
        before: None,
        after: Some(json!({
            "id": 1,
            "name": "Alice",
            "email": "alice@example.com",
            "password": "secret123",
            "ssn": "123-45-6789",
            "internal_id": "xyz123"
        })),
        timestamp: 1234567890,
        transaction: None,
    };

    group.bench_function("complex_filter_event", |b| {
        b.iter(|| {
            let mut e = event.clone();
            black_box(complex_filter.filter_event(&mut e))
        })
    });

    group.finish();
}

fn benchmark_batch_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_processing");

    // Generate batches of various sizes
    for batch_size in [10, 100, 500, 1000].iter() {
        let events: Vec<CdcEvent> = (0..*batch_size)
            .map(|i| CdcEvent {
                source_type: "postgres".to_string(),
                database: "testdb".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
                op: CdcOp::Insert,
                before: None,
                after: Some(json!({"id": i, "name": format!("User{}", i)})),
                timestamp: 1234567890 + i,
                transaction: None,
            })
            .collect();

        let config = CdcFilterConfig::default();
        let filter = CdcFilter::new(config).unwrap();

        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("filter_batch", batch_size),
            &events,
            |b, events| {
                b.iter(|| {
                    let mut filtered = Vec::with_capacity(events.len());
                    for event in events {
                        let mut e = event.clone();
                        if filter.filter_event(&mut e) {
                            filtered.push(e);
                        }
                    }
                    filtered
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_schema_inference,
    benchmark_type_mapping,
    benchmark_event_serialization,
    benchmark_filter_evaluation,
    benchmark_batch_processing,
);

criterion_main!(benches);
