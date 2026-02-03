# rivven-core

> Core storage engine and types for the Rivven event streaming platform.

## Overview

`rivven-core` is the foundational storage engine that powers Rivven's high-throughput message persistence. It provides append-only log segments, tiered storage, consumer groups, and optimized I/O primitives.

## Features

| Category | Features |
|:---------|:---------|
| **Storage** | Log segments, offset/timestamp indexes, tiered storage |
| **Compression** | LZ4, Zstd, Snappy, Gzip codecs |
| **I/O** | Zero-copy reads, memory-mapped files, buffer pooling |
| **Linux** | io_uring backend for maximum throughput |
| **Consumer** | Consumer groups with offset tracking |

## Installation

```toml
[dependencies]
rivven-core = "0.2"
```

## Architecture

```
rivven-core/
├── storage/          # Log-structured storage engine
│   ├── segment.rs    # Log segment files
│   ├── index.rs      # Offset and timestamp indexes
│   ├── tiered.rs     # Hot/warm/cold tiered storage
│   └── compaction.rs # Log compaction
├── io_uring.rs       # Linux io_uring async I/O
├── zero_copy.rs      # Zero-copy producer/consumer
├── consumer/         # Consumer group coordination
├── compression/      # Codec implementations
├── protocol/         # Wire protocol types
└── metrics/          # Observability
```

## Tiered Storage

Rivven supports automatic data tiering across hot (memory), warm (local disk), and cold (object storage) tiers:

```rust
use rivven_core::{Config, storage::{TieredStorageConfig, ColdStorageConfig}};

// Enable tiered storage with high-performance preset
let config = Config::new()
    .with_tiered_storage(TieredStorageConfig::high_performance());

// Or use cost-optimized preset for archival workloads
let config = Config::new()
    .with_tiered_storage(TieredStorageConfig::cost_optimized());

// Custom configuration
let tiered_config = TieredStorageConfig {
    enabled: true,
    hot_tier_max_bytes: 8 * 1024 * 1024 * 1024, // 8 GB
    hot_tier_max_age_secs: 7200,                 // 2 hours
    warm_tier_path: "/var/lib/rivven/warm".to_string(),
    cold_storage: ColdStorageConfig::S3 {
        endpoint: None,
        bucket: "rivven-archive".to_string(),
        region: "us-east-1".to_string(),
        access_key: None,
        secret_key: None,
        use_path_style: false,
    },
    ..Default::default()
};

let config = Config::new().with_tiered_storage(tiered_config);
```

| Tier | Storage | Latency | Use Case |
|:-----|:--------|:--------|:---------|
| **Hot** | In-memory | < 1ms | Recent data, active consumers |
| **Warm** | Local disk | 1-10ms | Medium-aged data |
| **Cold** | S3/GCS/Azure | 100ms+ | Archival, compliance |

## io_uring Async I/O

For Linux 5.6+, rivven-core provides an io_uring backend that eliminates syscall overhead:

```rust
use rivven_core::io_uring::{IoUringConfig, WalWriter, SegmentReader, IoBatch, BatchExecutor};

// High-throughput WAL writer
let config = IoUringConfig::high_throughput();
let wal = WalWriter::new("/data/wal.log", config)?;

// Direct write (immediate)
let offset = wal.append(b"record data")?;

// Batched writes (queued until flush)
wal.append_batched(b"record1")?;
wal.append_batched(b"record2")?;
wal.flush_batch()?; // Execute all batched writes

// Append with checksum
let offset = wal.append_with_checksum(b"record data")?;
wal.sync()?;

// Read segments
let reader = SegmentReader::open("/data/segment.log", IoUringConfig::default())?;
let messages = reader.read_messages(0, 64 * 1024)?;
```

### Batch Operations

Batched I/O reduces syscall overhead by queueing multiple operations:

```rust
use rivven_core::io_uring::{IoBatch, BatchExecutor, AsyncWriter, IoUringConfig, BatchStats};

// Create a batch of operations
let mut batch = IoBatch::new();
batch.write(0, b"hello".to_vec());
batch.write(5, b"world".to_vec());
batch.read(100, 50);
batch.sync();

// Get batch statistics before execution
let stats: BatchStats = batch.stats();
println!("Batch: {} writes ({} bytes), {} reads ({} bytes), {} syncs",
    stats.write_ops, stats.write_bytes,
    stats.read_ops, stats.read_bytes,
    stats.sync_ops);

// Execute batch
let writer = AsyncWriter::new("/data/file.log", IoUringConfig::default())?;
let executor = BatchExecutor::for_writer(writer);
executor.execute(&mut batch)?;
```

#### Batch Statistics

The `BatchStats` struct provides insight into batch composition:

| Field | Type | Description |
|-------|------|-------------|
| `total_ops` | u64 | Total operations in batch |
| `write_ops` | u64 | Number of write operations |
| `read_ops` | u64 | Number of read operations |
| `sync_ops` | u64 | Number of sync operations |
| `write_bytes` | u64 | Total bytes to be written |
| `read_bytes` | u64 | Total bytes to be read |

### io_uring Performance

| Backend | IOPS (4KB) | Latency p99 | CPU Usage |
|---------|------------|-------------|-----------|
| epoll   | 200K       | 1.5ms       | 80%       |
| io_uring| 800K       | 0.3ms       | 40%       |

## Core Types

```rust
use rivven_core::{
    Record, Topic, Partition, Offset,
    ProducerConfig, ConsumerConfig,
    CompressionCodec,
};

// Create a record
let record = Record::builder()
    .key(b"user-123")
    .value(b"event data")
    .header("source", "api")
    .build();
```

## Storage Engine

The storage engine uses a log-structured design:

```
data/
└── topics/
    └── orders/
        ├── partition-0/
        │   ├── 00000000000000000000.log  # Segment file
        │   ├── 00000000000000000000.idx  # Offset index
        │   └── 00000000000000001000.log  # Next segment
        └── partition-1/
```

## Documentation

- [Architecture](https://rivven.hupe1980.github.io/rivven/docs/architecture)
- [Tiered Storage](https://rivven.hupe1980.github.io/rivven/docs/tiered-storage)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
