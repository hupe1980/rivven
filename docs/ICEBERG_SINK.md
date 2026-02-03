# Apache Iceberg Sink

> Write streaming events to Apache Iceberg tables for analytics and lakehouse workloads.

## Overview

The Apache Iceberg sink enables real-time streaming of events from Rivven to Iceberg tables. This connector uses the **official Apache Iceberg Rust SDK** (`iceberg` crate v0.8.0) for catalog operations, providing production-ready table management.

### Features

- **Catalog Management**: REST and Memory catalogs with full Iceberg SDK integration
- **Automatic Table Creation**: Auto-create namespaces and tables with schema inference
- **Transaction Support**: Atomic commits via Iceberg SDK Transaction API
- **Multiple Catalog Types**: REST (Polaris, Tabular, Lakekeeper), Glue, Hive, Memory
- **Storage Backends**: S3, GCS, Azure, Local filesystem

### Iceberg Table Format Benefits

- **ACID Transactions**: Concurrent reads and writes with snapshot isolation
- **Time Travel**: Query data as of any point in time
- **Schema Evolution**: Add, rename, and drop columns without rewriting data
- **Partition Evolution**: Change partitioning without data migration
- **Hidden Partitioning**: Partition without exposing partition columns to users

## Implementation Status

The Iceberg sink is built on the **official Apache Iceberg Rust SDK** for full catalog, table management, and data file writing:

| Feature | Status | Notes |
|---------|--------|-------|
| REST Catalog | ✅ Full | Uses `iceberg-catalog-rest` v0.8.0 |
| Memory Catalog | ✅ Full | For testing and development |
| Namespace Management | ✅ Full | Create, check existence |
| Table Management | ✅ Full | Create, load, check existence |
| Schema Definition | ✅ Full | Iceberg SDK schema types |
| Parquet File Writing | ✅ Full | Full SDK writer stack with atomic commits |
| Transaction API | ✅ Full | Atomic appends via `fast_append()` |
| AWS Glue Catalog | 🔄 Planned | Use REST catalog with Lake Formation |
| Hive Metastore | 🔄 Planned | Use REST catalog with Polaris |

### Writer Stack Architecture

The connector uses the **full Iceberg SDK writer stack** for production-ready data ingestion:

```rust
// Production writer pipeline:
ParquetWriterBuilder::new(writer_props, iceberg_schema)
    → RollingFileWriterBuilder (file size limits)
    → DataFileWriterBuilder (Iceberg data files)
    → Transaction::fast_append() (atomic commit)
```

**Key Features:**
- **Parquet v57.x**: Uses the latest Arrow/Parquet ecosystem (arrow v57, parquet v57)
- **Configurable Compression**: Snappy (default), Gzip, LZ4, Zstd, Brotli, or None
- **Rolling Files**: Automatic file rotation at configurable size limits
- **Iceberg Field IDs**: Proper schema metadata via `schema_to_arrow_schema`
- **Unique File Names**: UUID-suffixed file names for concurrent writes
- **Atomic Commits**: Transaction API ensures data consistency
- **Table Reload**: Automatic table metadata refresh after each commit
- **Structured Logging**: Full observability with tracing integration

## Quick Start

### Prerequisites

1. An Iceberg catalog (REST, AWS Glue, or Hive Metastore)
2. Object storage (S3, GCS, Azure Blob, or local filesystem)
3. Enable the `iceberg` feature in rivven-connect

```bash
# Build with Iceberg support
cargo build -p rivven-connect --features iceberg
```

### Basic Configuration

```yaml
version: "1.0"

sinks:
  lakehouse:
    connector: iceberg
    config:
      catalog:
        type: rest
        rest:
          uri: http://localhost:8181
          warehouse: s3://my-bucket/warehouse
      namespace: analytics
      table: events
```

## Catalog Configuration

### REST Catalog (Polaris, Tabular, Lakekeeper)

The REST catalog is the recommended option for cloud-native deployments. This connector uses `iceberg-catalog-rest` v0.8.0 for full REST catalog protocol support.

```yaml
sinks:
  events:
    connector: iceberg
    config:
      catalog:
        type: rest
        rest:
          uri: http://polaris.example.com:8181
          warehouse: s3://bucket/warehouse
          credential: ${ICEBERG_CATALOG_TOKEN}  # Optional OAuth token
          properties:
            oauth2-server-uri: https://auth.example.com/oauth/token
```

#### Polaris Setup

```yaml
catalog:
  type: rest
  rest:
    uri: http://polaris:8181/api/catalog
    warehouse: s3://polaris-warehouse/data
    credential: ${POLARIS_BEARER_TOKEN}
```

#### Tabular Setup

```yaml
catalog:
  type: rest
  rest:
    uri: https://api.tabular.io
    warehouse: tabular://my-org/my-warehouse
    credential: ${TABULAR_TOKEN}
```

### AWS Glue Catalog

> **Note**: AWS Glue catalog support is planned but not yet fully implemented. Consider using a REST catalog with AWS Lake Formation.

```yaml
sinks:
  events:
    connector: iceberg
    config:
      catalog:
        type: glue
        glue:
          region: us-west-2
          catalog_id: "123456789012"  # Optional: defaults to account ID
          profile: production         # Optional: AWS profile name
        warehouse: s3://my-bucket/warehouse
```

### Hive Metastore

> **Note**: Hive Metastore catalog support is planned but not yet fully implemented. Consider using a REST catalog with Apache Polaris.

```yaml
sinks:
  events:
    connector: iceberg
    config:
      catalog:
        type: hive
        hive:
          uri: thrift://hive-metastore:9083
          warehouse: hdfs:///user/hive/warehouse
```

### Memory Catalog (Testing)

For local development and testing, the Memory catalog is fully supported:

```yaml
sinks:
  events:
    connector: iceberg
    config:
      catalog:
        type: memory
        warehouse: /tmp/iceberg-warehouse
```

## Storage Configuration

### Amazon S3

```yaml
sinks:
  events:
    connector: iceberg
    config:
      catalog:
        type: rest
        rest:
          uri: http://localhost:8181
      namespace: analytics
      table: events
      s3:
        region: us-west-2
        access_key_id: ${AWS_ACCESS_KEY_ID}
        secret_access_key: ${AWS_SECRET_ACCESS_KEY}
        # session_token: ${AWS_SESSION_TOKEN}  # For temporary credentials
```

### MinIO / S3-Compatible Storage

```yaml
sinks:
  events:
    connector: iceberg
    config:
      catalog:
        type: rest
        rest:
          uri: http://localhost:8181
          warehouse: s3://my-bucket/warehouse
      namespace: default
      table: events
      s3:
        region: us-east-1
        endpoint: http://minio:9000
        path_style_access: true
        access_key_id: ${MINIO_ACCESS_KEY}
        secret_access_key: ${MINIO_SECRET_KEY}
```

### Google Cloud Storage

```yaml
sinks:
  events:
    connector: iceberg
    config:
      catalog:
        type: rest
        rest:
          uri: http://localhost:8181
          warehouse: gs://my-bucket/warehouse
      namespace: analytics
      table: events
      gcs:
        project_id: my-gcp-project
        service_account_key: ${GCS_SERVICE_ACCOUNT_JSON}
```

### Azure Blob Storage

```yaml
sinks:
  events:
    connector: iceberg
    config:
      catalog:
        type: rest
        rest:
          uri: http://localhost:8181
          warehouse: abfss://container@account.dfs.core.windows.net/warehouse
      namespace: analytics
      table: events
      azure:
        storage_account: mystorageaccount
        access_key: ${AZURE_STORAGE_KEY}
```

## Batch and Performance Configuration

```yaml
sinks:
  events:
    connector: iceberg
    config:
      catalog:
        type: rest
        rest:
          uri: http://localhost:8181
      namespace: analytics
      table: events
      
      # Batching
      batch_size: 10000           # Events per batch (default: 10000)
      flush_interval_secs: 60    # Max seconds between flushes (default: 60)
      
      # File sizing
      target_file_size_mb: 128   # Target Parquet file size (default: 128)
      
      # Compression
      compression: snappy         # Parquet compression codec (default: snappy)
```

### Compression Codecs

| Codec | Description | Use Case |
|-------|-------------|----------|
| `none` | No compression | Maximum write speed, large files |
| `snappy` | Fast, moderate ratio (default) | General purpose, balanced |
| `gzip` | Slower, better ratio | Storage optimization |
| `lz4` | Very fast, lower ratio | Low latency writes |
| `zstd` | Good balance | Best overall compression |
| `brotli` | Best ratio, slowest | Cold storage, archival |

### Tuning Recommendations

| Workload | batch_size | flush_interval_secs | target_file_size_mb |
|----------|------------|---------------------|---------------------|
| Low latency | 1000 | 10 | 32 |
| Balanced | 10000 | 60 | 128 |
| High throughput | 100000 | 300 | 512 |

## Partitioning

### No Partitioning

```yaml
partitioning: none
```

### Table Default

Use the partition spec defined in the Iceberg table schema:

```yaml
partitioning: table_default
```

### Identity Partitioning

Partition by exact field values:

```yaml
partitioning: identity
partition_fields:
  - region
  - country
```

### Bucket Partitioning

Hash partition for high-cardinality fields:

```yaml
partitioning: bucket
partition_fields:
  - user_id
num_buckets: 64
```

### Time-Based Partitioning

Partition by time (year, month, day, hour):

```yaml
partitioning: time
partition_fields:
  - event_date
time_granularity: day  # year | month | day | hour
```

## Commit Modes

### Append (Default)

Add new records without modifying existing data:

```yaml
commit_mode: append
```

### Overwrite

Replace all data in the target partition(s):

```yaml
commit_mode: overwrite
```

### Upsert (Merge)

Update existing records or insert new ones based on key fields:

```yaml
commit_mode: upsert
key_fields:
  - id
  - event_date
```

## Schema Evolution

Control how schema changes are handled:

```yaml
schema_evolution: add_columns  # strict | add_columns | full
```

| Mode | Description |
|------|-------------|
| `strict` | Error if incoming schema doesn't match table schema |
| `add_columns` | Automatically add new columns (default) |
| `full` | Allow column adds, drops, renames, and type changes |

## Complete Production Example

```yaml
version: "1.0"

sources:
  orders_cdc:
    connector: postgres-cdc
    config:
      host: ${POSTGRES_HOST}
      port: 5432
      database: ecommerce
      user: ${POSTGRES_USER}
      password: ${POSTGRES_PASSWORD}
      slot_name: rivven_orders
      publication: orders_pub
    streams:
      - name: orders
        namespace: public
        sync_mode: incremental

sinks:
  analytics_lake:
    connector: iceberg
    config:
      catalog:
        type: glue
        glue:
          region: us-west-2
        warehouse: s3://analytics-lake/warehouse
      
      namespace: ecommerce
      table: orders
      
      # Performance tuning
      batch_size: 50000
      flush_interval_secs: 120
      target_file_size_mb: 256
      
      # Partitioning
      partitioning: time
      partition_fields:
        - order_date
      time_granularity: day
      
      # Commit mode
      commit_mode: upsert
      key_fields:
        - order_id
      
      # Schema evolution
      schema_evolution: add_columns
      
      # S3 configuration
      s3:
        region: us-west-2

pipelines:
  orders_to_lake:
    source: orders_cdc
    sink: analytics_lake
    enabled: true
```

## Querying Data

After data is written, query it with your favorite engine:

### Spark

```python
df = spark.read.format("iceberg").load("glue_catalog.ecommerce.orders")
df.filter("order_date >= '2024-01-01'").show()

# Time travel
df = spark.read.format("iceberg") \
    .option("as-of-timestamp", "2024-06-15 12:00:00") \
    .load("glue_catalog.ecommerce.orders")
```

### Trino / Presto

```sql
SELECT * FROM iceberg.ecommerce.orders
WHERE order_date >= DATE '2024-01-01';

-- Time travel
SELECT * FROM iceberg.ecommerce.orders 
FOR TIMESTAMP AS OF TIMESTAMP '2024-06-15 12:00:00';
```

### DuckDB

```sql
LOAD iceberg;
SELECT * FROM iceberg_scan('s3://analytics-lake/warehouse/ecommerce/orders');
```

### Polars

```python
import polars as pl

df = pl.scan_iceberg("s3://analytics-lake/warehouse/ecommerce/orders").collect()
```

## Monitoring

### Metrics

The Iceberg sink exposes the following metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `iceberg_records_written_total` | Counter | Total records written |
| `iceberg_batches_committed_total` | Counter | Total batches committed |
| `iceberg_bytes_written_total` | Counter | Total bytes written |
| `iceberg_commit_duration_seconds` | Histogram | Commit latency |
| `iceberg_records_per_batch` | Histogram | Records per batch |

### Health Checks

```bash
# Validate configuration and catalog connectivity
rivven-connect check --config config.yaml --sink analytics_lake
```

## Troubleshooting

### Common Issues

#### Catalog Connection Failures

```
Error: Failed to connect to catalog at http://localhost:8181
```

- Verify catalog is running and accessible
- Check network/firewall rules
- Validate authentication credentials

#### S3 Access Denied

```
Error: Access Denied (Service: S3, Status Code: 403)
```

- Verify AWS credentials are set
- Check IAM permissions for s3:GetObject, s3:PutObject, s3:DeleteObject
- For path-style access (MinIO), set `path_style_access: true`

#### Schema Mismatch

```
Error: Schema evolution not allowed in strict mode
```

- Change `schema_evolution` to `add_columns` or `full`
- Or update the Iceberg table schema to match incoming data

#### Commit Conflicts

```
Error: Commit conflict - table was modified concurrently
```

This can occur with multiple writers. Solutions:
- Use a single writer per table
- Implement retry logic (built-in with exponential backoff)
- Consider partitioning to reduce conflicts

## API Reference

### IcebergSinkConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `catalog` | `CatalogConfig` | required | Catalog configuration |
| `namespace` | `String` | required | Iceberg namespace/database |
| `table` | `String` | required | Table name |
| `batch_size` | `usize` | 10000 | Events per batch |
| `flush_interval_secs` | `u64` | 60 | Max flush interval |
| `target_file_size_mb` | `u64` | 128 | Target Parquet file size |
| `partitioning` | `PartitionStrategy` | `none` | Partition strategy |
| `partition_fields` | `Vec<String>` | `[]` | Fields to partition by |
| `num_buckets` | `u32` | 16 | Buckets for bucket partitioning |
| `time_granularity` | `TimeGranularity` | `day` | Time partition granularity |
| `commit_mode` | `CommitMode` | `append` | Commit mode |
| `key_fields` | `Vec<String>` | `[]` | Key fields for upsert |
| `schema_evolution` | `SchemaEvolution` | `add_columns` | Schema evolution mode |
| `s3` | `S3StorageConfig` | `None` | S3 configuration |
| `gcs` | `GcsStorageConfig` | `None` | GCS configuration |
| `azure` | `AzureStorageConfig` | `None` | Azure configuration |

### CatalogType

| Value | Description |
|-------|-------------|
| `rest` | REST catalog (Polaris, Tabular, Lakekeeper) |
| `glue` | AWS Glue Data Catalog |
| `hive` | Hive Metastore |
| `memory` | In-memory catalog (testing only) |

### PartitionStrategy

| Value | Description |
|-------|-------------|
| `none` | No partitioning |
| `table_default` | Use table's partition spec |
| `identity` | Partition by exact values |
| `bucket` | Hash bucket partitioning |
| `time` | Time-based partitioning |

### CommitMode

| Value | Description |
|-------|-------------|
| `append` | Append records |
| `overwrite` | Replace partition data |
| `upsert` | Merge with key matching |

### SchemaEvolution

| Value | Description |
|-------|-------------|
| `strict` | No schema changes allowed |
| `add_columns` | Allow adding new columns |
| `full` | Allow all schema changes |

## See Also

- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Iceberg Rust SDK (crates.io)](https://crates.io/crates/iceberg)
- [Iceberg Rust GitHub](https://github.com/apache/iceberg-rust)
- [REST Catalog Protocol](https://iceberg.apache.org/spec/#rest-catalog)
- [rivven-connect README](../crates/rivven-connect/README.md)
- [CDC Architecture](CDC_ARCHITECTURE.md)
