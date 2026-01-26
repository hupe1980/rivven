# rivven-core

Core storage engine and types for the Rivven event streaming platform.

## Features

- **Log-Structured Storage** - Append-only log segments for high write throughput
- **Consumer Groups** - Coordinated consumption with offset tracking
- **Compression** - LZ4, Zstd, Snappy, Gzip support
- **Indexing** - Efficient offset and timestamp lookups

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
│   └── compaction.rs # Log compaction
├── consumer/         # Consumer group coordination
├── compression/      # Codec implementations
├── protocol/         # Wire protocol types
└── metrics/          # Observability
```

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

## License

See root [LICENSE](../../LICENSE) file.
