# rivven-rdbc: Relational Database Connectivity

Production-grade SQL database connectivity layer for the Rivven event streaming platform.

## Overview

`rivven-rdbc` provides a unified interface for connecting to relational databases with Kafka Connect JDBC and Debezium parity. It's designed for high-throughput CDC sources and exactly-once sinks.

## Features

| Feature | Description |
|---------|-------------|
| **Multi-Database Support** | PostgreSQL, MySQL, MariaDB, SQL Server |
| **Connection Pooling** | High-performance pooling with health checks |
| **SQL Dialect Abstraction** | Vendor-agnostic SQL generation via sea-query |
| **Table Source** | Query-based CDC with incrementing/timestamp modes |
| **Table Sink** | Batch writes with upsert, delete propagation |
| **Schema Discovery** | Automatic schema introspection and evolution |
| **Type System** | 25+ value types with Debezium parity |

## Quick Start

### Dependencies

```toml
[dependencies]
rivven-rdbc = { version = "0.0.8", features = ["postgres", "mysql"] }
```

### Basic Usage

```rust
use rivven_rdbc::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Create connection configuration
    let config = ConnectionConfig::new("postgresql://user:pass@localhost/mydb");
    
    // Connect using the PostgreSQL factory
    let factory = PostgresConnectionFactory;
    let conn = factory.connect(&config).await?;
    
    // Execute queries
    let rows = conn.query(
        "SELECT id, name FROM users WHERE active = $1",
        &[Value::Bool(true)]
    ).await?;
    
    for row in rows {
        let id: i64 = row.get_by_name("id").unwrap().as_i64().unwrap();
        let name: &str = row.get_by_name("name").unwrap().as_str().unwrap();
        println!("User {}: {}", id, name);
    }
    
    Ok(())
}
```

## Supported Databases

### PostgreSQL

```rust
use rivven_rdbc::postgres::PostgresConnectionFactory;

let factory = PostgresConnectionFactory;
let conn = factory.connect(&config).await?;

// PostgreSQL-specific features
// - UPSERT via INSERT ... ON CONFLICT
// - RETURNING clause
// - COPY for bulk loading
// - JSON/JSONB support
// - UUID native type
```

### MySQL

```rust
use rivven_rdbc::mysql::MySqlConnectionFactory;

let factory = MySqlConnectionFactory;
let conn = factory.connect(&config).await?;

// MySQL-specific features
// - UPSERT via INSERT ... ON DUPLICATE KEY UPDATE
// - LOAD DATA INFILE for bulk loading
```

### MariaDB

```rust
// MariaDB uses the same driver as MySQL but with enhanced features
let config = ConnectionConfig::new("mysql://user:pass@localhost/mydb");
let dialect = dialect_for("mariadb");

// MariaDB-specific features (10.5+)
// - RETURNING clause support
// - Native UUID type (10.7+)
// - TRUE/FALSE boolean literals
```

### SQL Server

```rust
use rivven_rdbc::sqlserver::SqlServerConnectionFactory;

let factory = SqlServerConnectionFactory;
let conn = factory.connect(&config).await?;

// SQL Server-specific features
// - MERGE statement for UPSERT
// - OUTPUT clause (similar to RETURNING)
// - Snapshot isolation
```

## Connection Pooling

```rust
use rivven_rdbc::pool::{PoolConfig, PoolBuilder};

// Using builder pattern
let pool_config = PoolBuilder::new("postgres://localhost/mydb")
    .min_size(5)
    .max_size(20)
    .acquire_timeout(Duration::from_secs(30))
    .max_lifetime(Duration::from_secs(3600))
    .idle_timeout(Duration::from_secs(600))
    .test_on_borrow(true)
    .config();

// Pool statistics
let stats = pool.stats();
println!("Connections created: {}", stats.connections_created);
println!("Acquisitions: {}", stats.acquisitions);
println!("Exhausted count: {}", stats.exhausted_count);
```

## Transactions

```rust
// Begin a transaction
let tx = conn.begin().await?;

// With specific isolation level
let tx = conn.begin_with_isolation(IsolationLevel::Serializable).await?;

// Execute within transaction
tx.execute(
    "INSERT INTO users (name) VALUES ($1)", 
    &[Value::String("Alice".into())]
).await?;

// Savepoints for partial rollback
tx.savepoint("my_savepoint").await?;
// ... more operations ...
tx.rollback_to_savepoint("my_savepoint").await?;

// Commit
tx.commit().await?;
```

## Table Source (Query-Based CDC)

For reading data from databases with change tracking:

```rust
use rivven_rdbc::source::{TableSourceConfig, QueryMode};

// Incrementing column mode (for auto-increment IDs)
let config = TableSourceConfig::incrementing("orders", "order_id")
    .with_schema("public")
    .with_batch_size(1000)
    .with_poll_interval(Duration::from_secs(1));

// Timestamp mode (for updated_at columns)
let config = TableSourceConfig::timestamp("events", "updated_at")
    .with_where("status = 'active'")
    .with_columns(vec!["id", "name", "updated_at"]);

// Timestamp + Incrementing (most reliable)
let mode = QueryMode::timestamp_incrementing("updated_at", "id");
```

### Source Offset Tracking

```rust
use rivven_rdbc::source::{SourceOffset, PollResult};

// Resume from a saved offset
let mut offset = SourceOffset::with_incrementing(last_known_id);

// Poll for new data
let result: PollResult = source.poll(&offset).await?;

for record in result.records {
    // Process record
    println!("Key: {:?}, Values: {:?}", record.key, record.values);
}

// Update offset for next poll
offset = result.offset;
```

## Table Sink (Batch Writes)

For writing data to databases with exactly-once semantics:

```rust
use rivven_rdbc::sink::{TableSinkBuilder, WriteMode, BatchConfig};

let config = TableSinkBuilder::new()
    .batch_size(1000)
    .batch_latency(Duration::from_millis(100))
    .write_mode(WriteMode::Upsert)
    .auto_ddl(AutoDdlMode::Create)
    .schema_evolution(SchemaEvolutionMode::AddColumnsOnly)
    .delete_enabled(true)
    .pk_columns(vec!["id".to_string()])
    .build();

// Create records
let record = SinkRecord::upsert(
    Some("public".to_string()),
    "users",
    vec![Value::Int64(1)],  // key
    values,                   // HashMap<String, Value>
);

// Write batch
let result = sink.write_batch(vec![record]).await?;
println!("Written: {}, Failed: {}", result.success_count, result.failure_count);
```

### Write Modes

| Mode | Description |
|------|-------------|
| `Insert` | Insert only, fails on duplicate keys |
| `Update` | Update only, fails if row doesn't exist |
| `Upsert` | Insert or update on conflict (default) |
| `Delete` | Delete records by primary key |

## SQL Dialect Abstraction

rivven-rdbc uses sea-query for portable SQL generation:

```rust
use rivven_rdbc::dialect::{dialect_for, PostgresDialect, MySqlDialect, MariaDbDialect};

// Get dialect from string
let dialect = dialect_for("postgres");  // PostgresDialect
let dialect = dialect_for("mysql");     // MySqlDialect
let dialect = dialect_for("mariadb");   // MariaDbDialect
let dialect = dialect_for("sqlserver"); // SqlServerDialect

// Generate portable SQL
let sql = dialect.upsert_sql(&table_meta, &["id"], &["id", "name", "email"]);

// PostgreSQL: INSERT ... ON CONFLICT (id) DO UPDATE SET ...
// MySQL:      INSERT ... ON DUPLICATE KEY UPDATE ...
// SQL Server: MERGE ... WHEN MATCHED THEN UPDATE ...
```

### Dialect Capabilities

| Feature | PostgreSQL | MySQL | MariaDB | SQL Server |
|---------|:----------:|:-----:|:-------:|:----------:|
| RETURNING | ✅ | ❌ | ✅ | ✅ (OUTPUT) |
| MERGE | ❌ | ❌ | ❌ | ✅ |
| ON CONFLICT | ✅ | ❌ | ❌ | ❌ |
| ON DUPLICATE KEY | ❌ | ✅ | ✅ | ❌ |
| Native UUID | ✅ | ❌ | ✅ (10.7+) | ❌ |
| TRUE/FALSE | ✅ | ❌ | ✅ | ❌ |

## Schema Discovery

```rust
use rivven_rdbc::schema::SchemaProvider;

// List all tables
let tables = provider.list_tables(Some("public")).await?;

// Get table metadata
let table = provider.get_table(Some("public"), "users").await?;
for col in &table.columns {
    println!("{}: {} (nullable: {}, pk: {})", 
        col.name, col.type_name, col.nullable, col.is_primary_key);
}

// Get indexes
let indexes = provider.list_indexes(Some("public"), "users").await?;

// Get foreign keys
let fks = provider.list_foreign_keys(Some("public"), "users").await?;
```

## Schema Evolution

```rust
use rivven_rdbc::schema::{SchemaManager, AutoDdlMode, SchemaEvolutionMode};

// Auto-create tables
let manager = SchemaManager::new(conn, AutoDdlMode::Create);
manager.create_table(&table_metadata).await?;

// Add columns for schema evolution
manager.add_columns(&table_name, &new_columns).await?;
```

## Value Types

rivven-rdbc supports 25+ value types with Debezium parity:

| Category | Types |
|----------|-------|
| **Null** | `Null` |
| **Boolean** | `Bool` |
| **Integer** | `Int8`, `Int16`, `Int32`, `Int64` |
| **Float** | `Float32`, `Float64` |
| **Decimal** | `Decimal` (exact numeric) |
| **String** | `String` |
| **Binary** | `Bytes`, `Bits` |
| **Temporal** | `Date`, `Time`, `DateTime`, `DateTimeWithTz`, `Interval` |
| **Unique ID** | `Uuid` |
| **Structured** | `Json`, `Array` |
| **Spatial** | `Geometry`, `Geography` |
| **Vector** | `FloatVector`, `DoubleVector` (pgvector) |
| **Custom** | `Custom { type_name, data }` |

## Error Handling

```rust
use rivven_rdbc::error::{Error, ErrorCategory};

match result {
    Err(e) if e.is_retriable() => {
        // Retry for: Connection, Timeout, Deadlock, PoolExhausted
        tokio::time::sleep(Duration::from_secs(1)).await;
        retry(operation).await
    }
    Err(Error::Constraint { constraint_name, message }) => {
        // Handle constraint violations (not retriable)
        log::warn!("Constraint violation: {} - {}", constraint_name, message);
    }
    Err(e) => {
        // Log error category for observability
        tracing::error!(
            category = ?e.category(),
            retriable = e.is_retriable(),
            "Database error: {}", e
        );
    }
    Ok(_) => {}
}
```

### Error Categories

| Category | Retriable | Description |
|----------|:---------:|-------------|
| `Connection` | ✅ | Network/connection failures |
| `Timeout` | ✅ | Query or connection timeout |
| `Deadlock` | ✅ | Transaction deadlock detected |
| `PoolExhausted` | ✅ | Connection pool exhausted |
| `Query` | ❌ | SQL syntax or execution error |
| `Constraint` | ❌ | PK/FK/unique constraint violation |
| `TypeConversion` | ❌ | Type mismatch |
| `Authentication` | ❌ | Authentication failed |
| `Configuration` | ❌ | Invalid configuration |

## Performance Considerations

### Connection Pooling
- Use appropriate `min_size` to avoid connection creation latency
- Set `max_size` based on database capacity and concurrent workload
- Enable `test_on_borrow` for reliability (slight latency impact)

### Batch Writes
- Use `write_batch` for bulk operations
- Configure `batch_size` based on row size and network latency
- Set `batch_latency` to balance throughput and latency

### Prepared Statements
- Statements are cached to avoid repeated parsing
- Use `statement_cache_size` to control cache size

### Streaming
- Use `query_stream` for large result sets to reduce memory
- Process records incrementally instead of loading all into memory

## Feature Flags

```toml
[dependencies]
rivven-rdbc = { version = "0.0.8", features = ["postgres", "mysql", "sqlserver", "tls"] }
```

| Feature | Description |
|---------|-------------|
| `postgres` | PostgreSQL support via tokio-postgres |
| `mysql` | MySQL/MariaDB support via mysql_async |
| `sqlserver` | SQL Server support via tiberius |
| `tls` | TLS support for secure connections |
| `full` | All features enabled |

## Integration with rivven-connect

rivven-rdbc is the foundation for rivven-connect's database connectors:

### RDBC Source

Query-based polling source with incrementing/timestamp tracking:

```yaml
sources:
  users:
    connector: rdbc-source
    topic: user-events              # Required - destination topic
    config:
      connection_url: postgres://user:pass@localhost/db
      table: users
      mode: incrementing            # bulk | incrementing | timestamp | timestamp_incrementing
      incrementing_column: id
      poll_interval_ms: 1000
      batch_size: 1000
      pool_size: 1                  # Optional - connection pool size (default: 1)
```

### RDBC Sink

High-performance batch sink with connection pooling and hot path optimizations:

```yaml
sinks:
  users_copy:
    connector: rdbc-sink
    topics: [user-events]           # Required - topics to consume
    consumer_group: rdbc-warehouse  # Required - for offset tracking
    config:
      connection_url: postgres://user:pass@localhost/warehouse
      table: users_snapshot
      write_mode: upsert            # insert | update | upsert
      pk_columns: [id]
      batch_size: 1000
      pool_size: 4                  # Optional - connection pool size (default: 4)
      transactional: true           # Optional - exactly-once semantics
```

### Configuration Reference

| Config | Source | Sink | Default | Description |
|--------|--------|------|---------|-------------|
| `connection_url` | ✓ | ✓ | - | Database connection URL |
| `table` | ✓ | ✓ | - | Target table name |
| `schema` | ✓ | ✓ | - | Database schema (e.g., "public") |
| `batch_size` | ✓ | ✓ | 1000 | Records per batch |
| `pool_size` | ✓ | ✓ | 1/4 | Connection pool size (source: 1, sink: 4) |
| `mode` | ✓ | - | bulk | Query mode (bulk/incrementing/timestamp) |
| `write_mode` | - | ✓ | insert | Write mode (insert/update/upsert) |
| `pk_columns` | - | ✓ | - | Primary key columns for upsert/update |
| `transactional` | - | ✓ | false | Wrap batches in transactions |
| `poll_interval_ms` | ✓ | - | 5000 | Polling interval |
| `batch_timeout_ms` | - | ✓ | 5000 | Batch flush timeout |

### Feature Flags

```toml
# Enable database-specific backends
rivven-connect = { version = "0.0.8", features = ["rdbc-postgres"] }
rivven-connect = { version = "0.0.8", features = ["rdbc-mysql"] }
rivven-connect = { version = "0.0.8", features = ["rdbc-sqlserver"] }
rivven-connect = { version = "0.0.8", features = ["rdbc-full"] }  # All databases
```

## Testing

### Unit Tests (190+)

Run the unit tests:

```bash
cargo test -p rivven-rdbc
```

### Integration Tests (20)

The RDBC connectors have comprehensive integration tests using testcontainers:

```bash
# Run all RDBC integration tests
cargo test -p rivven-integration-tests --test rdbc_connectors -- --nocapture

# Run specific tests
cargo test -p rivven-integration-tests --test rdbc_connectors test_rdbc_sink_upsert
```

**Test Coverage:**

| Category | Tests |
|----------|-------|
| **Infrastructure** | Container startup, table setup |
| **Source Modes** | Bulk, Incrementing, Timestamp, State resume |
| **Sink Modes** | Insert, Upsert, Update, Delete, Transactional |
| **Pool Features** | Pool sizing, connection reuse, stress test |
| **Advanced** | Schema qualification, error handling, large batches |
| **E2E** | Full source-to-sink pipeline |

**Performance Benchmarks:**

The stress test verifies **500 records in ~180ms** (2,700+ records/sec) using batch execution with connection pooling.
