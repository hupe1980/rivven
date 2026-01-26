# rivven-storage

Object storage connectors for Rivven Connect.

## Features

- **S3** - Amazon S3 and S3-compatible storage (MinIO, Cloudflare R2)
- **GCS** - Google Cloud Storage (planned)
- **Azure Blob** - Azure Blob Storage (planned)

## Installation

```toml
[dependencies]
rivven-storage = { version = "0.2", features = ["s3"] }
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

## Configuration

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

## Features Flags

| Feature | Description | Dependencies |
|---------|-------------|--------------|
| `s3` | Amazon S3 support | aws-sdk-s3 |
| `gcs` | Google Cloud Storage | (planned) |
| `azure` | Azure Blob Storage | (planned) |
| `full` | All providers | all above |

## License

See root [LICENSE](../../LICENSE) file.
