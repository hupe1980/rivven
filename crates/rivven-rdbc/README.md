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

## Supported Databases

| Backend | Driver | Dialect | Features |
|---------|--------|---------|----------|
| PostgreSQL | `tokio-postgres` | `PostgresDialect` | RETURNING, ON CONFLICT, streaming |
| MySQL | `mysql_async` | `MySqlDialect` | ON DUPLICATE KEY UPDATE |
| MariaDB | `mysql_async` | `MariaDbDialect` | RETURNING (10.5+), native UUID (10.7+) |
| SQL Server | `tiberius` | `SqlServerDialect` | MERGE, OUTPUT clause |

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
rivven-rdbc = { version = "0.0.8", features = ["postgres", "mysql"] }
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
- **Async Streaming**: Large result sets can be streamed to reduce memory
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

LApache-2.0. See [LICENSE](../../LICENSE).
