---
layout: default
title: Delta Lake Connector
parent: Connectors
nav_order: 11
---

# Delta Lake Connector
{: .no_toc }

Write streaming events to Delta Lake tables with ACID transactions for analytics and lakehouse workloads.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

The Delta Lake connector enables real-time streaming of events from Rivven to Delta Lake tables. This connector uses the **delta-rs** native Rust implementation (`deltalake` crate v0.30) for production-ready table operations — no JVM required.

### Features

- **ACID Transactions**: Every write is atomic with snapshot isolation via the Delta log
- **Automatic Table Creation**: Auto-create tables on first write with default schema
- **Multiple Storage Backends**: Local filesystem, S3, GCS, Azure Blob Storage
- **Batched Writes**: Configurable batch size and flush interval for throughput
- **Commit Retry**: Exponential backoff on transaction conflicts (100ms → 200ms → 400ms)
- **Compression**: Snappy (default), Gzip, LZ4, Zstd, or None
- **Partitioning**: Hive-style partition columns for query optimization
- **Lock-Free Metrics**: Atomic counters for observability (records, bytes, latency)

### Delta Lake Benefits

- **ACID Transactions**: Concurrent reads and writes with snapshot isolation
- **Time Travel**: Query previous versions of the table by version number or timestamp
- **Schema Enforcement**: Prevents writes with incompatible schemas
- **Audit History**: The Delta log records all operations (commits, compactions, etc.)
- **Unified Batch & Streaming**: Same tables for both batch and streaming workloads
- **Wide Ecosystem**: Queryable from Spark, DuckDB, Polars, Trino, Databricks, and more

---

## Implementation Status

The Delta Lake connector is built on the **delta-rs** native Rust implementation:

| Feature | Status | Notes |
|---------|--------|-------|
| Table Creation | ✅ Full | Auto-create with configurable schema |
| Table Loading  | ✅ Full | Open existing tables by URI |
| Parquet Writing | ✅ Full | RecordBatchWriter with compression |
| ACID Commits | ✅ Full | Transactional log with conflict resolution |
| S3 Storage | ✅ Full | Native integration via `deltalake/s3` |
| GCS Storage | ✅ Full | Native integration via `deltalake/gcs` |
| Azure Storage | ✅ Full | Native integration via `deltalake/azure` |
| Local Filesystem | ✅ Full | File-based tables |
| Hive Partitioning | ✅ Full | Partition by event_type, stream |
| Compression | ✅ Full | Snappy, Gzip, LZ4, Zstd, None |
| Commit Retry | ✅ Full | Exponential backoff on conflicts |
| Metrics | ✅ Full | Lock-free atomic counters, Prometheus export |
| Schema Evolution | 🔄 Planned | Append-compatible changes |
| Vacuum/Compact | 🔄 Planned | Table optimization operations |

### Writer Architecture

```text
SourceEvents → Arrow RecordBatch → RecordBatchWriter → Parquet Files → Delta Commit

Delta Table (directory / object store prefix)
  ├── _delta_log/
  │   ├── 00000000000000000000.json  (commit 0 - table creation)
  │   ├── 00000000000000000001.json  (commit 1 - first data)
  │   └── ...
  ├── part-00000-{uuid}.snappy.parquet
  ├── part-00001-{uuid}.snappy.parquet
  └── ...
```

---

## Configuration

### Minimal Configuration

```yaml
sinks:
  events:
    connector: delta-lake
    config:
      table_uri: /data/warehouse/events
```

### Full Configuration

```yaml
sinks:
  events:
    connector: delta-lake
    config:
      # Table location (required)
      table_uri: s3://my-bucket/warehouse/events

      # Auto-create the table if it doesn't exist (default: true)
      auto_create_table: true

      # Write settings
      batch_size: 10000           # Records per batch (default: 10000)
      flush_interval_secs: 60     # Max seconds between flushes (default: 60)
      target_file_size_mb: 128    # Target Parquet file size (default: 128)

      # Write mode: append or overwrite (default: append)
      write_mode: append

      # Compression: snappy, gzip, lz4, zstd, none (default: snappy)
      compression: snappy

      # Commit retry on conflict (default: 3)
      max_commit_retries: 3

      # Optional: partition columns (valid: event_type, stream)
      partition_columns:
        - event_type

      # S3 configuration (when using s3:// URIs)
      s3:
        region: us-east-1
        access_key_id: ${AWS_ACCESS_KEY_ID}
        secret_access_key: ${AWS_SECRET_ACCESS_KEY}

      # Additional table properties (applied on auto-create)
      table_properties:
        delta.logRetentionDuration: "interval 30 days"
```

### Configuration Reference

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `table_uri` | string | *required* | Delta table location (local path, `s3://`, `gs://`, `az://`) |
| `auto_create_table` | bool | `true` | Create the table on first write if it doesn't exist |
| `batch_size` | int | `10000` | Maximum records per batch before flushing |
| `flush_interval_secs` | int | `60` | Maximum seconds between flushes |
| `target_file_size_mb` | int | `128` | Target Parquet file size in MB |
| `write_mode` | string | `append` | Write mode: `append` or `overwrite` |
| `compression` | string | `snappy` | Parquet compression: `snappy`, `gzip`, `lz4`, `zstd`, `none` |
| `max_commit_retries` | int | `3` | Maximum commit retry attempts on conflict |
| `partition_columns` | list | `[]` | Hive-style partition columns |
| `table_properties` | map | `{}` | Delta table properties (applied on auto-create) |
| `storage_options` | map | `{}` | Additional storage options (passed to object store) |

---

## Storage Backends

### Local Filesystem

```yaml
config:
  table_uri: /data/warehouse/events
  # or with file:// scheme
  table_uri: file:///data/warehouse/events
```

### Amazon S3

```yaml
config:
  table_uri: s3://my-bucket/warehouse/events
  s3:
    region: us-east-1
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

Enable the `delta-lake-s3` feature for S3 support:

```toml
rivven-connect = { features = ["delta-lake-s3"] }
```

### S3-Compatible (MinIO, Cloudflare R2)

```yaml
config:
  table_uri: s3://my-bucket/warehouse/events
  s3:
    region: us-east-1
    endpoint: http://minio:9000
    access_key_id: minioadmin
    secret_access_key: minioadmin
    path_style_access: true
    allow_http: true
```

### Google Cloud Storage

```yaml
config:
  table_uri: gs://my-bucket/warehouse/events
  gcs:
    service_account_path: /path/to/service-account.json
```

Enable the `delta-lake-gcs` feature for GCS support:

```toml
rivven-connect = { features = ["delta-lake-gcs"] }
```

### Azure Blob Storage

```yaml
config:
  table_uri: az://my-container/warehouse/events
  azure:
    account: mystorageaccount
    access_key: ${AZURE_STORAGE_KEY}
```

Enable the `delta-lake-azure` feature for Azure support:

```toml
rivven-connect = { features = ["delta-lake-azure"] }
```

---

## Table Schema

The connector writes events using a fixed schema with four columns:

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `data` | String | No | JSON-serialized event payload |
| `event_type` | String | No | Event type: insert, update, delete, state, log |
| `timestamp` | Timestamp (µs, UTC) | No | Event timestamp in microseconds |
| `stream` | String | Yes | Source stream name |

### Example Data

| data | event_type | timestamp | stream |
|------|-----------|-----------|--------|
| `{"id":1,"name":"Alice"}` | insert | 2024-01-15T10:30:00Z | users |
| `{"id":2,"amount":99.99}` | insert | 2024-01-15T10:30:01Z | orders |
| `{"before":null,"after":{"id":1}}` | update | 2024-01-15T10:30:02Z | users |

---

## Querying Delta Tables

### DuckDB

```sql
-- Install the Delta extension
INSTALL delta;
LOAD delta;

-- Query Delta table
SELECT * FROM delta_scan('s3://my-bucket/warehouse/events');

-- Filter by event type
SELECT data, timestamp
FROM delta_scan('/data/warehouse/events')
WHERE event_type = 'insert'
ORDER BY timestamp DESC
LIMIT 100;
```

### Apache Spark

```python
df = spark.read.format("delta").load("s3://my-bucket/warehouse/events")
df.filter(df.event_type == "insert").show()
```

### Polars

```python
import polars as pl

df = pl.read_delta("s3://my-bucket/warehouse/events")
print(df.filter(pl.col("event_type") == "insert"))
```

### Trino / Presto

```sql
SELECT * FROM delta."s3://my-bucket/warehouse"."events"
WHERE event_type = 'insert';
```

### Databricks

```sql
SELECT * FROM delta.`s3://my-bucket/warehouse/events`
WHERE event_type = 'insert';
```

---

## Metrics

The Delta Lake connector provides comprehensive lock-free metrics for observability:

| Metric | Type | Description |
|--------|------|-------------|
| `records_written` | counter | Total records successfully committed |
| `records_failed` | counter | Total records that failed to write |
| `bytes_written` | counter | Estimated total bytes written |
| `commits_success` | counter | Successful Delta log commits |
| `commits_failed` | counter | Failed Delta log commits |
| `commit_retries` | counter | Commit retries due to conflicts |
| `files_created` | counter | Total Parquet data files created |
| `batches_flushed` | counter | Number of batch flushes |
| `commit_latency_us` | counter | Cumulative commit latency (µs) |
| `write_latency_us` | counter | Cumulative write latency (µs) |
| `batch_size_min` | gauge | Minimum batch size seen |
| `batch_size_max` | gauge | Maximum batch size seen |
| `batch_size_sum` | counter | Sum of all batch sizes |

### Computed Metrics

| Metric | Formula |
|--------|---------|
| Avg commit latency | `commit_latency_us / commits_success / 1000` (ms) |
| Avg write latency | `write_latency_us / batches_flushed / 1000` (ms) |
| Records/sec | `records_written / elapsed_secs` |
| Bytes/sec | `bytes_written / elapsed_secs` |
| Avg batch size | `batch_size_sum / batches_flushed` |
| Retry rate | `commit_retries / (commits_success + commits_failed)` |
| Success rate | `commits_success / (commits_success + commits_failed)` |

### Prometheus Export

Metrics are exported in standard Prometheus text exposition format with `# HELP` and `# TYPE` annotations:

```rust
let snapshot = metrics.snapshot();
let prom_text = snapshot.to_prometheus_format("rivven");
```

```text
# HELP rivven_delta_records_written_total Total records successfully written
# TYPE rivven_delta_records_written_total counter
rivven_delta_records_written_total 1000
# HELP rivven_delta_commits_total Total successful Delta commits
# TYPE rivven_delta_commits_total counter
rivven_delta_commits_total 10
# HELP rivven_delta_commit_latency_avg_ms Average commit latency in milliseconds
# TYPE rivven_delta_commit_latency_avg_ms gauge
rivven_delta_commit_latency_avg_ms 12.500
# ...
```

Computed gauges include average commit/write latency, success rate, and average batch size.
Batch size min/max gauges are only emitted when at least one batch has been flushed.

---

## Feature Flags

The Delta Lake connector is optional. Enable it via Cargo features:

| Feature | Description |
|---------|-------------|
| `delta-lake` | Core Delta Lake support (local filesystem) |
| `delta-lake-s3` | Delta Lake + S3 support |
| `delta-lake-gcs` | Delta Lake + GCS support |
| `delta-lake-azure` | Delta Lake + Azure support |
| `lakehouse-full` | All lakehouse formats (Iceberg + Delta Lake) |
| `full` | All connectors including Delta Lake |

### Cargo.toml Example

```toml
[dependencies]
rivven-connect = { version = "0.0.21", features = ["delta-lake-s3"] }
```

---

## Error Handling

### Commit Conflicts

When concurrent writers modify the same Delta table, commit conflicts may occur. The connector automatically retries with exponential backoff:

1. **Attempt 1**: Immediate write
2. **Attempt 2**: 100ms backoff → reload table state → retry
3. **Attempt 3**: 200ms backoff → reload table state → retry
4. **Attempt 4**: 400ms backoff → reload table state → retry

If all retries fail, the batch is recorded as failed in the `WriteResult`.

> **Note on orphan files**: Each retry attempt creates a new Parquet data file because
> Delta-rs's `RecordBatchWriter` produces Parquet files during `write()`. Failed/retried
> attempts leave uncommitted Parquet files on storage. These are harmless—they are never
> referenced by the Delta log—and will be cleaned up by `VACUUM`. The `RecordBatch` itself
> is built once and cheaply cloned (O(num_columns) via `Arc`) across retries.

After each successful commit, the connector reloads the table metadata to keep the internal
table handle fresh for subsequent writes.

### Common Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `Invalid table_uri` | Malformed or unsupported URI | Check URI format (s3://, gs://, az://, local path) |
| `NotATable` | Table doesn't exist | Enable `auto_create_table: true` |
| `Failed to create Delta writer` | Schema mismatch or storage issue | Check table schema matches expected format |
| `Delta commit failed` | Commit conflict or permission error | Increase `max_commit_retries` or check permissions |

---

## Comparison with Iceberg Connector

| Feature | Delta Lake | Iceberg |
|---------|-----------|---------|
| Table Format | Delta Lake (delta-rs) | Apache Iceberg (iceberg-rs) |
| Catalog | No catalog needed | REST, Glue, Hive, Memory |
| Table Location | URI-based (directory) | Catalog-managed |
| Schema Evolution | Additive columns | Full evolution (add, rename, drop) |
| Partition Evolution | Not yet supported | Full partition evolution |
| Time Travel | By version or timestamp | By snapshot ID or timestamp |
| Compression | Snappy, Gzip, LZ4, Zstd | Snappy, Gzip, LZ4, Zstd, Brotli |
| Ecosystem | Spark, DuckDB, Polars, Databricks | Spark, Trino, Flink, Dremio |
| Rust Maturity | delta-rs (3k+ stars) | iceberg-rs (growing) |

**Choose Delta Lake when:**
- You prefer simple URI-based table management (no catalog)
- You're using Databricks or DuckDB as your primary query engine
- You want a simpler operational model

**Choose Iceberg when:**
- You need catalog-managed tables (REST, Glue)
- You need full schema and partition evolution
- You're using Trino, Flink, or Dremio

---

## Examples

### Basic Local Setup

```yaml
# Capture Postgres CDC events to local Delta table
sources:
  postgres:
    connector: postgres-cdc
    config:
      host: localhost
      port: 5432
      database: mydb
      publication: rivven_pub

sinks:
  delta:
    connector: delta-lake
    config:
      table_uri: /tmp/events
      batch_size: 1000
      flush_interval_secs: 10
```

### Production S3 Setup

```yaml
sources:
  postgres:
    connector: postgres-cdc
    config:
      host: db.prod.internal
      port: 5432
      database: production
      publication: rivven_pub

sinks:
  delta:
    connector: delta-lake
    config:
      table_uri: s3://data-lake/warehouse/events
      batch_size: 50000
      flush_interval_secs: 120
      compression: zstd
      partition_columns:
        - event_type
      s3:
        region: us-east-1
      table_properties:
        delta.logRetentionDuration: "interval 30 days"
        delta.deletedFileRetentionDuration: "interval 7 days"
```

---

## Troubleshooting

### Table Not Found

If you see `NotATable` errors, ensure `auto_create_table: true` is set (default), or create the table manually using Delta Lake tools.

### S3 Permission Errors

Ensure the IAM user/role has these S3 permissions:
- `s3:GetObject` — Read Delta log and data files
- `s3:PutObject` — Write data and commit logs
- `s3:DeleteObject` — Needed for Delta log cleanup
- `s3:ListBucket` — List table files

### Slow Writes

- **Increase batch_size**: Larger batches amortize commit overhead
- **Use Snappy or LZ4**: Fastest compression codecs
- **Check network latency**: S3 writes depend on network speed

### Commit Conflicts

If you see frequent commit retries:
- Avoid multiple writers to the same table
- Increase `max_commit_retries` if conflicts are transient
- Consider partitioning to reduce write contention
