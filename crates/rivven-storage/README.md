# rivven-storage

Object storage connectors for Rivven Connect.

## Features

- **S3** - Amazon S3 and S3-compatible storage (MinIO, Cloudflare R2)
- **GCS** - Google Cloud Storage
- **Azure Blob** - Azure Blob Storage
- **Parquet** - Apache Parquet output format for data lake integration

## Installation

```toml
[dependencies]
# S3 only
rivven-storage = { version = "0.2", features = ["s3"] }

# S3 with Parquet support
rivven-storage = { version = "0.2", features = ["s3", "parquet"] }

# All providers and formats
rivven-storage = { version = "0.2", features = ["full"] }
```

## Usage

```rust
use rivven_connect::SinkRegistry;
use rivven_storage::S3SinkFactory;
use std::sync::Arc;

// Register S3 sink with the registry
let mut sinks = SinkRegistry::new();
sinks.register("s3", Arc::new(S3SinkFactory));

// Or use the convenience function
rivven_storage::register_all(&mut sinks);
```

### Parquet Output

```rust
use rivven_storage::parquet::{ParquetWriter, ParquetWriterConfig, ParquetCompression};
use serde_json::json;

let events = vec![
    json!({"id": 1, "name": "Event 1", "timestamp": "2024-01-15T10:00:00Z"}),
    json!({"id": 2, "name": "Event 2", "timestamp": "2024-01-15T10:01:00Z"}),
];

let config = ParquetWriterConfig {
    compression: ParquetCompression::Snappy,
    row_group_size: 10000,
    ..Default::default()
};

let writer = ParquetWriter::new(config);
let parquet_bytes = writer.write_batch(&events)?;
```

## Configuration

### S3 Sink with JSON

```yaml
sinks:
  - name: s3-export
    type: s3
    config:
      bucket: my-data-lake
      prefix: cdc/postgres
      region: us-west-2
      format: jsonl
      compression: gzip
      batch_size: 1000
      flush_interval_secs: 60
      partition_by: day  # none, hour, day
```

### Parquet Writer Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `compression` | `snappy` | Compression codec: none, snappy, gzip, lz4, zstd, brotli |
| `row_group_size` | `10000` | Number of rows per row group |
| `data_page_size` | `1MB` | Maximum data page size |
| `enable_statistics` | `true` | Write column statistics to footer |

## Features Flags

| Feature | Description | Dependencies |
|---------|-------------|--------------|
| `s3` | Amazon S3 support | aws-sdk-s3 |
| `gcs` | Google Cloud Storage | google-cloud-storage |
| `azure` | Azure Blob Storage | azure_storage, azure_storage_blobs |
| `parquet` | Apache Parquet format | parquet, arrow-array, arrow-schema |
| `full` | All providers + formats | all above |

## Parquet Features

- **Automatic Schema Inference** - Infers Arrow schema from JSON records
- **Type Promotion** - Handles mixed int/float types gracefully
- **Compression Codecs** - Snappy, Gzip, LZ4, Zstd, Brotli
- **Nested Data** - Support for arrays and nested objects
- **Columnar Storage** - Efficient for analytical workloads

## License

See root [LICENSE](../../LICENSE) file.
