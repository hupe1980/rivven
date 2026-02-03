use bytes::Bytes;
/// Production Performance Benchmark Suite
///
/// Benchmarks all performance-critical operations to validate optimizations
/// Run with: cargo bench --package rivven-core
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rivven_core::{Config, Message, Partition};
use std::sync::Arc;
use tokio::runtime::Runtime;

fn bench_offset_allocation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("offset_allocation");

    group.bench_function("lock_free_atomic", |b| {
        let config = get_test_config();
        let partition = rt
            .block_on(async { Arc::new(Partition::new(&config, "bench-topic", 0).await.unwrap()) });

        b.to_async(&rt).iter(|| async {
            let msg = create_test_message(100);
            let _ = black_box(partition.append(msg).await);
        });
    });

    group.finish();
}

fn bench_batch_append(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("batch_append");

    for batch_size in [10, 100, 1000, 10_000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &size| {
                let config = get_test_config();
                let partition = rt.block_on(async {
                    Arc::new(Partition::new(&config, "bench-topic", 0).await.unwrap())
                });

                b.to_async(&rt).iter(|| async {
                    let messages: Vec<Message> =
                        (0..size).map(|_| create_test_message(100)).collect();
                    let _ = black_box(partition.append_batch(messages).await);
                });
            },
        );
    }

    group.finish();
}

fn bench_single_vs_batch(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("single_vs_batch");

    let batch_size = 1000;
    group.throughput(Throughput::Elements(batch_size as u64));

    // Single append 1000 times
    group.bench_function("single_append_1000x", |b| {
        let config = get_test_config();
        let partition = rt.block_on(async {
            Arc::new(Partition::new(&config, "bench-single", 0).await.unwrap())
        });

        b.to_async(&rt).iter(|| async {
            for _ in 0..batch_size {
                let msg = create_test_message(100);
                let _ = black_box(partition.append(msg).await);
            }
        });
    });

    // Batch append 1000 messages
    group.bench_function("batch_append_1000", |b| {
        let config = get_test_config();
        let partition = rt
            .block_on(async { Arc::new(Partition::new(&config, "bench-batch", 0).await.unwrap()) });

        b.to_async(&rt).iter(|| async {
            let messages: Vec<Message> =
                (0..batch_size).map(|_| create_test_message(100)).collect();
            let _ = black_box(partition.append_batch(messages).await);
        });
    });

    group.finish();
}

fn bench_message_sizes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("message_sizes");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &msg_size| {
            let config = get_test_config();
            let partition = rt.block_on(async {
                Arc::new(Partition::new(&config, "bench-sizes", 0).await.unwrap())
            });

            b.to_async(&rt).iter(|| async {
                let msg = create_test_message(msg_size);
                let _ = black_box(partition.append(msg).await);
            });
        });
    }

    group.finish();
}

fn bench_concurrent_appends(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("concurrent_appends");

    for num_threads in [1, 2, 4, 8, 16].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_threads),
            num_threads,
            |b, &threads| {
                let config = get_test_config();
                let partition = rt.block_on(async {
                    Arc::new(
                        Partition::new(&config, "bench-concurrent", 0)
                            .await
                            .unwrap(),
                    )
                });
                let partition = Arc::new(partition);

                b.to_async(&rt).iter(|| async {
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let part = Arc::clone(&partition);
                        let handle = tokio::spawn(async move {
                            for _ in 0..100 {
                                let msg = create_test_message(100);
                                let _ = part.append(msg).await;
                            }
                        });
                        handles.push(handle);
                    }

                    for handle in handles {
                        handle.await.unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_read_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("read_performance");

    // Prepare partition with 10K messages
    let config = get_test_config();
    let partition = rt.block_on(async {
        let part = Arc::new(Partition::new(&config, "bench-read", 0).await.unwrap());

        // Populate with messages
        for _ in 0..10_000 {
            let msg = create_test_message(100);
            let _ = part.append(msg).await;
        }

        part
    });

    for batch_size in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let messages = black_box(partition.read(0, size).await.unwrap());
                    assert_eq!(messages.len(), size);
                });
            },
        );
    }

    group.finish();
}

fn bench_metrics_overhead(c: &mut Criterion) {
    use rivven_core::metrics::CoreMetrics;

    let mut group = c.benchmark_group("metrics_overhead");

    group.bench_function("counter_increment", |b| {
        b.iter(|| {
            CoreMetrics::increment_messages_appended();
            black_box(());
        });
    });

    group.bench_function("batch_counter_increment", |b| {
        b.iter(|| {
            CoreMetrics::add_messages_appended(100);
            black_box(());
        });
    });

    group.finish();
}

// Helper functions
fn get_test_config() -> Config {
    Config {
        data_dir: format!("/tmp/rivven-bench-{}", uuid::Uuid::new_v4()),
        ..Default::default()
    }
}

fn create_test_message(payload_size: usize) -> Message {
    let payload = vec![b'x'; payload_size];
    Message::new(Bytes::from(payload))
}

criterion_group!(
    benches,
    bench_offset_allocation,
    bench_batch_append,
    bench_single_vs_batch,
    bench_message_sizes,
    bench_concurrent_appends,
    bench_read_performance,
    bench_metrics_overhead,
);

criterion_main!(benches);
