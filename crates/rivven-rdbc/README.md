# rivven-rdbc

Production-grade relational database connectivity layer for the rivven event streaming platform.

## Overview

rivven-rdbc provides a unified abstraction over multiple SQL database backends with:

- **Async-first design** using tokio
- **Type-safe queries** with sea-query SQL generation
- **Connection pooling** with health checking and metrics
- **Transaction support** including isolation levels and savepoints
- **Schema introspection** and evolution capabilities
- **Prepared statement caching**
- **SQL injection prevention** — identifier, type name, and string literal validation on all DDL/introspection paths; WHERE clause deny-list with word-boundary matching to prevent false positives; covers stored procedures (DECLARE, CALL), PostgreSQL file-access functions (PG_READ_FILE, PG_LS_DIR, PG_READ_BINARY_FILE), timing attacks, and privilege escalation
- **Deferred rollback** — rollback operations avoid `block_in_place`, preventing Tokio runtime stalls
- **MAX_STREAM_ROWS safeguard** — streaming queries enforce a row limit to prevent unbounded memory growth
- **Range literal serialization** — PostgreSQL `Range` types are correctly serialized as SQL literals

## Supported Databases

| Backend | Driver | Dialect | Features |
|---------|--------|---------|----------|
| PostgreSQL | `tokio-postgres` | `PostgresDialect` | RETURNING, ON CONFLICT, streaming, prepared statements |
| MySQL | `mysql_async` | `MySqlDialect` | ON DUPLICATE KEY UPDATE, streaming, prepared statements |
| MariaDB | `mysql_async` | `MariaDbDialect` | RETURNING (10.5+), native UUID (10.7+), streaming, prepared statements |
| SQL Server | `tiberius` | `SqlServerDialect` | MERGE, OUTPUT clause, streaming, prepared statements |

## Quick Start

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

## Transactions

```rust
// Begin a transaction
let tx = conn.begin().await?;

// With specific isolation level
let tx = conn.begin_with_isolation(IsolationLevel::Serializable).await?;

// Execute within transaction
tx.execute("INSERT INTO users (name) VALUES ($1)", &[Value::String("Alice".into())]).await?;

// Savepoints for partial rollback
tx.savepoint("my_savepoint").await?;
// ... more operations ...
tx.rollback_to_savepoint("my_savepoint").await?;

// Commit
tx.commit().await?;
```

## Connection Pooling

```rust
use rivven_rdbc::pool::{PoolConfig, BasicConnectionPool};

let pool_config = PoolConfig {
    connection: ConnectionConfig::new("postgresql://localhost/mydb"),
    min_size: 5,
    max_size: 20,
    acquire_timeout: Duration::from_secs(30),
    max_lifetime: Duration::from_secs(3600),
    idle_timeout: Duration::from_secs(600),
    ..Default::default()
};

let pool = BasicConnectionPool::new(pool_config, PostgresConnectionFactory).await?;

// Get a connection from pool
let conn = pool.get().await?;

// Connection is automatically returned when dropped
```

### Connection Lifecycle Tracking

The pool tracks connection lifecycle to enable accurate recycling and observability:

```rust
use rivven_rdbc::prelude::*;

// Get a pooled connection
let conn = pool.get().await?;

// Check connection age and expiry status
println!("Connection age: {:?}", conn.age());
println!("Is expired: {}", conn.is_expired(Duration::from_secs(1800)));

// Check how long the connection has been in use (borrowed)
println!("Time in use: {:?}", conn.time_in_use());

// The pool tracks recycling reasons for observability
let stats = pool.stats();
println!("Connections expired by lifetime: {}", stats.lifetime_expired_count);
println!("Connections expired by idle timeout: {}", stats.idle_expired_count);
println!("Connections reused: {}", stats.reused_count);
println!("Fresh connections: {}", stats.fresh_count);

// Use helper methods for common metrics
println!("Reuse rate: {:.1}%", stats.reuse_rate() * 100.0);
println!("Total recycled: {}", stats.recycled_total());
println!("Avg wait time: {:.2}ms", stats.avg_wait_time_ms());
println!("Health failure rate: {:.1}%", stats.health_failure_rate() * 100.0);
println!("Health checks performed: {}", stats.health_checks_performed);
println!("Active connections: {}", stats.active_connections());
```

Connections implement the `ConnectionLifecycle` trait for detailed lifecycle introspection:

```rust
use rivven_rdbc::connection::ConnectionLifecycle;

// Get creation time and idle duration
let created_at = conn.created_at();
let idle = conn.idle_time().await;

// Check against timeouts
if conn.is_idle_expired(Duration::from_secs(600)).await {
    // Connection has been idle too long
}
```

## SQL Dialect Abstraction

rivven-rdbc uses sea-query for portable SQL generation:

```rust
use rivven_rdbc::dialect::{PostgresDialect, MySqlDialect, MariaDbDialect, SqlDialect, dialect_for};

// Get dialect from string
let dialect = dialect_for("postgres");   // PostgresDialect
let dialect = dialect_for("mysql");      // MySqlDialect
let dialect = dialect_for("mariadb");    // MariaDbDialect (with RETURNING support)
let dialect = dialect_for("sqlserver");  // SqlServerDialect

// Generate portable SQL
let sql = dialect.upsert_sql(&table_meta, &["id"], &["id", "name", "email"]);
// PostgreSQL: INSERT ... ON CONFLICT (id) DO UPDATE SET ...
// MySQL:      INSERT ... ON DUPLICATE KEY UPDATE ...
// MariaDB:    INSERT ... ON DUPLICATE KEY UPDATE ... (with RETURNING available)
// SQL Server: MERGE ... WHEN MATCHED THEN UPDATE ...
```

## Schema Introspection

```rust
use rivven_rdbc::schema::SchemaProvider;

// List all tables in a schema
let tables = provider.list_tables(Some("public")).await?;

// Get table metadata including columns
let table = provider.get_table(Some("public"), "users").await?;
for col in &table.columns {
    println!("{}: {} (nullable: {})", col.name, col.type_name, col.nullable);
}

// Get indexes
let indexes = provider.list_indexes(Some("public"), "users").await?;

// Get foreign keys
let fks = provider.list_foreign_keys(Some("public"), "users").await?;
```

## Feature Flags

```toml
[dependencies]
rivven-rdbc = { version = "0.0.20", features = ["postgres", "mysql"] }
```

| Feature | Description |
|---------|-------------|
| `postgres` | PostgreSQL support via tokio-postgres |
| `mysql` | MySQL/MariaDB support via mysql_async |
| `sqlserver` | SQL Server support via tiberius |
| `tls` | TLS support for all backends |
| `full` | All features enabled |

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  rivven-rdbc                        │
├─────────────────────────────────────────────────────┤
│  Connection Trait  │  Transaction  │  RowStream    │
├─────────────────────────────────────────────────────┤
│           SQL Dialect Abstraction (sea-query)       │
├─────────────────────────────────────────────────────┤
│  Connection Pool  │  Health Check  │  Metrics      │
├─────────────┬────────────────┬─────────────────────┤
│  PostgreSQL │     MySQL      │    SQL Server       │
│  (tokio-pg) │ (mysql_async)  │    (tiberius)       │
└─────────────┴────────────────┴─────────────────────┘
```

## Error Handling

```rust
use rivven_rdbc::error::{Error, ErrorCategory};

match result {
    Err(e) if e.is_retriable() => {
        // Retry for connection, timeout, deadlock errors
    }
    Err(Error::Constraint { constraint_name, message }) => {
        // Handle constraint violations
    }
    Err(e) => {
        // Log error category for observability
        tracing::error!(category = ?e.category(), "Database error: {}", e);
    }
    Ok(_) => {}
}
```

## Performance Considerations

- **Statement Caching**: Prepared statements are cached to avoid repeated parsing
- **Connection Pooling**: Reuse connections to avoid connection overhead
- **Async Streaming**: All backends support `query_stream` for memory-efficient result processing. PostgreSQL uses true incremental streaming via `query_raw`, while MySQL and SQL Server fetch results then stream them through an async iterator.
- **Batch Operations**: Use transactions for bulk inserts/updates
- **Metrics**: Built-in metrics for monitoring pool health and query performance

## Testing

```bash
# Run unit tests (190+)
cargo test -p rivven-rdbc

# Run integration tests (20 tests with testcontainers)
cargo test -p rivven-integration-tests --test rdbc_connectors
```

**Integration Test Coverage:**
- Source modes: Bulk, Incrementing, Timestamp, State resume
- Sink modes: Insert, Upsert, Update, Delete, Transactional
- Pool features: Connection pooling, stress testing (500 records)
- Advanced: Schema qualification, error handling, E2E pipeline

## License

Apache-2.0. See [LICENSE](../../LICENSE).
