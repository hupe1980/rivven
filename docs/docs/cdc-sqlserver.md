# SQL Server CDC Guide

This guide covers setting up Change Data Capture (CDC) for Microsoft SQL Server with rivven-cdc.

## Overview

SQL Server CDC uses a **poll-based** approach, unlike PostgreSQL and MySQL which stream changes via WAL/binlog replication. SQL Server's native CDC feature creates change tables that rivven-cdc queries periodically.

### How It Works

```text
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   SQL Server    │     │   CDC Tables     │     │   rivven-cdc    │
│   Transaction   │ ──► │ (change history) │ ◄── │ (polling loop)  │
│      Log        │     │ fn_cdc_get_all_  │     │                 │
│                 │     │    changes_*     │     │                 │
└─────────────────┘     └──────────────────┘     └─────────────────┘
        ↑                        ↓                        ↓
  SQL Server Agent       LSN Positioning          CdcEvent Stream
   (capture job)        (commit + change)
```

### Key Differences from PostgreSQL/MySQL

| Feature | PostgreSQL/MySQL | SQL Server |
|---------|------------------|------------|
| Mechanism | Stream WAL/binlog | Poll CDC change tables |
| Connection | Persistent replication | Query-based polling |
| Real-time | Sub-second | Configurable polling interval |
| Complexity | Protocol parsing | SQL queries |

### Checkpoint Strategy

SQL Server CDC defers LSN checkpoint commits to the **next poll cycle** rather than checkpointing immediately after processing events. This ensures events are fully delivered to downstream consumers before the position is advanced, providing **at-least-once delivery semantics** — on crash, at most one batch of events may be re-sent, but events are never lost.

An **initial checkpoint** is written with the starting position before the first poll, preventing data loss if a crash occurs during the first poll cycle. All checkpoint writes use an atomic write→`fsync`→rename pattern for crash durability — without `fsync`, the kernel could reorder operations on power loss, leaving a valid filename pointing at an empty file.
| Dependencies | None | SQL Server Agent |

## Prerequisites

### Supported Versions

| Version | Status | Notes |
|---------|--------|-------|
| SQL Server 2016 SP1+ | ✅ Supported | CDC introduced |
| SQL Server 2019 | ✅ Recommended | Enhanced performance |
| SQL Server 2022 | ✅ Supported | Latest features |
| Azure SQL Database | ✅ Supported | Managed CDC |
| Azure SQL Managed Instance | ✅ Supported | Full CDC support |

### Requirements

1. **SQL Server Agent** must be running (except Azure SQL Database which manages this automatically)
2. **sysadmin** or **db_owner** role for enabling CDC
3. Database must not be in Simple recovery mode (Full or Bulk-Logged required)

## Database Setup

### 1. Enable CDC on Database

```sql
USE your_database;
GO

-- Enable CDC on the database
EXEC sys.sp_cdc_enable_db;
GO

-- Verify CDC is enabled
SELECT name, is_cdc_enabled 
FROM sys.databases 
WHERE name = 'your_database';
```

### 2. Enable CDC on Tables

```sql
-- Enable CDC on a specific table
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = 'users',
    @role_name = NULL,  -- No additional role restrictions
    @supports_net_changes = 1;  -- Enable net changes view
GO

-- Verify table CDC is enabled
SELECT * FROM cdc.change_tables;
```

### 3. Configure Capture Job (Optional)

```sql
-- View current capture job settings
EXEC sys.sp_cdc_help_jobs;
GO

-- Adjust capture frequency (default: 5 seconds)
EXEC sys.sp_cdc_change_job
    @job_type = N'capture',
    @pollinginterval = 1;  -- Poll every 1 second
GO
```

### 4. Create a Dedicated User (Recommended)

```sql
-- Create login and user for CDC
CREATE LOGIN rivven_cdc WITH PASSWORD = 'YourStrong@Passw0rd';
GO

USE your_database;
GO

CREATE USER rivven_cdc FOR LOGIN rivven_cdc;
GO

-- Grant necessary permissions
EXEC sp_addrolemember 'db_datareader', 'rivven_cdc';
GRANT SELECT ON SCHEMA::cdc TO rivven_cdc;
GRANT EXECUTE ON sys.sp_cdc_help_change_data_capture TO rivven_cdc;
GRANT EXECUTE ON sys.fn_cdc_get_all_changes_dbo_users TO rivven_cdc;
-- Repeat for each enabled table
GO
```

## Quick Start

### Basic Configuration

```rust
use rivven_cdc::sqlserver::{SqlServerCdc, SqlServerCdcConfig, SnapshotMode};
use rivven_cdc::CdcSource;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = SqlServerCdcConfig::builder()
        .host("localhost")
        .port(1433)
        .username("rivven_cdc")
        .password("YourStrong@Passw0rd")
        .database("your_database")
        .poll_interval_ms(100)
        .snapshot_mode(SnapshotMode::Initial)
        .include_table("dbo", "users")
        .include_table("dbo", "orders")
        .build()?;

    let mut cdc = SqlServerCdc::new(config);
    let rx = cdc.take_event_receiver().expect("receiver already taken");
    
    // Start CDC in background
    cdc.start().await?;

    // Access metrics at any time
    let metrics = cdc.metrics();
    println!("Events captured: {}", metrics.events_captured);

    // Process events
    while let Some(event) = rx.recv().await {
        println!("Database: {}", event.database);
        println!("Table: {}.{}", event.schema, event.table);
        println!("Operation: {:?}", event.op);
        println!("Data: {:?}", event.after);
    }

    Ok(())
}
```

### With TLS

```rust
use rivven_cdc::sqlserver::{SqlServerCdc, SqlServerCdcConfig};

let config = SqlServerCdcConfig::builder()
    .host("your-server.database.windows.net")  // Azure SQL
    .port(1433)
    .username("your_user")
    .password("your_password")
    .database("your_database")
    .use_tls(true)
    .trust_server_certificate(false)  // Verify certificate
    .build()?;
```

### Table Filtering

```rust
use rivven_cdc::sqlserver::SqlServerCdcConfig;

let config = SqlServerCdcConfig::builder()
    .host("localhost")
    .username("sa")
    .password("YourStrong@Passw0rd")
    .database("mydb")
    // Include specific tables
    .include_table("dbo", "users")
    .include_table("dbo", "orders")
    // Or use patterns for all tables in a schema
    .include_table("sales", ".*")  // All tables in sales schema
    // Exclude specific tables
    .exclude_table("dbo", "audit_.*")  // Exclude audit tables
    .build()?;
```

## Configuration Reference

### SqlServerCdcConfig

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `host` | String | Required | SQL Server hostname |
| `port` | u16 | 1433 | SQL Server port |
| `username` | String | Required | SQL Server username |
| `password` | String | Required | SQL Server password |
| `database` | String | Required | Target database |
| `poll_interval` | Duration | 100ms | CDC table polling interval |
| `snapshot_mode` | SnapshotMode | Initial | Initial snapshot behavior |
| `batch_size` | usize | 1000 | Max rows per polling batch |
| `use_tls` | bool | false | Enable TLS encryption |
| `trust_server_certificate` | bool | false | Trust self-signed certs |
| `application_intent` | String | None | "ReadOnly" for Always On |

### Snapshot Modes

All standard snapshot modes are supported:

| Mode | Description |
|------|-------------|
| `initial` | Snapshot on first run, then stream changes (default) |
| `always` | Full snapshot on every restart |
| `never` | Skip snapshot, stream changes only |
| `when_needed` | Snapshot if no valid LSN position exists |
| `initial_only` | Snapshot and stop (for data migration) |
| `no_data` | Capture schema only, skip data (alias: `schema_only`) |
| `recovery` | Rebuild schema history after corruption |

## LSN Positioning

SQL Server CDC uses Log Sequence Numbers (LSN) for positioning:

```rust
use rivven_cdc::sqlserver::{Lsn, CdcPosition};

// LSN is a 10-byte binary value
let lsn = Lsn::from_hex("00000025:00000F48:0003")?;

// Position includes both commit and change LSN
let position = CdcPosition {
    commit_lsn: lsn.clone(),
    change_lsn: lsn,
    capture_instance: "dbo_users".to_string(),
};

// Resume from a specific position
let config = SqlServerCdcConfig::builder()
    .host("localhost")
    .username("sa")
    .password("password")
    .database("mydb")
    .start_position(position)
    .build()
    .await?;
```

## Azure SQL Database

Azure SQL Database has automatic CDC management without SQL Server Agent:

```sql
-- Enable CDC (Azure SQL Database)
EXEC sys.sp_cdc_enable_db;

-- Enable on table
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = 'orders',
    @role_name = NULL;

-- No need to configure capture job - Azure manages it
```

```rust
let config = SqlServerCdcConfig::builder()
    .host("your-server.database.windows.net")
    .port(1433)
    .username("your_user@your-server")
    .password("your_password")
    .database("your_database")
    .use_tls(true)
    .build()
    .await?;
```

## Always On Availability Groups

For read-only replicas in Always On setups:

```rust
let config = SqlServerCdcConfig::builder()
    .host("ag-listener.example.com")
    .port(1433)
    .username("cdc_user")
    .password("password")
    .database("mydb")
    .application_intent("ReadOnly")  // Route to secondary
    .build()
    .await?;
```

## Security Considerations

### Identifier Validation

All table, schema, and capture instance identifiers are validated via `Validator::validate_identifier()`, enforcing `^[a-zA-Z_][a-zA-Z0-9_]{0,254}$`. Invalid identifiers are rejected at `TableSpec::new()` before any SQL is constructed.

### SQL Injection Prevention

Snapshot queries use bracket-escaping (`]` → `]]`) for SQL Server identifiers. CDC system function calls (`fn_cdc_get_all_changes_*`, `fn_cdc_get_min_lsn`) validate capture instance names before interpolation.

---

## Troubleshooting

### CDC Not Capturing Changes

1. **SQL Server Agent not running:**
   ```sql
   -- Check Agent status
   EXEC xp_servicecontrol 'QueryState', 'SQLServerAgent';
   
   -- Start Agent
   EXEC xp_servicecontrol 'Start', 'SQLServerAgent';
   ```

2. **CDC job not running:**
   ```sql
   -- Check CDC jobs
   EXEC sys.sp_cdc_help_jobs;
   
   -- Start capture job manually
   EXEC sys.sp_cdc_start_job @job_type = 'capture';
   ```

3. **Transaction log truncation:**
   ```sql
   -- Check if log has been truncated past CDC position
   SELECT * FROM sys.fn_cdc_get_min_lsn('dbo_users');
   ```

### High Latency

1. **Reduce polling interval:**
   ```rust
   .poll_interval(Duration::from_millis(50))  // More frequent polling
   ```

2. **Increase capture job frequency:**
   ```sql
   EXEC sys.sp_cdc_change_job
       @job_type = N'capture',
       @pollinginterval = 1;  -- 1 second
   ```

### Missing Tables

```sql
-- List all CDC-enabled tables
SELECT 
    t.name AS table_name,
    ct.capture_instance,
    ct.start_lsn
FROM sys.tables t
JOIN cdc.change_tables ct ON t.object_id = ct.source_object_id;
```

## Performance Tuning

### Batch Size

```rust
// Larger batches = fewer queries, higher latency
.batch_size(5000)

// Smaller batches = more queries, lower latency
.batch_size(100)
```

### Connection Pooling

For high-throughput scenarios, consider connection pooling:

```rust
// The SqlServerCdc handles connection management internally
// but you can tune connection settings:
let config = SqlServerCdcConfig::builder()
    .host("localhost")
    .username("sa")
    .password("password")
    .database("mydb")
    .connection_timeout(Duration::from_secs(30))
    .build()
    .await?;
```

## Metrics

SQL Server CDC provides comprehensive observability through `SqlServerMetrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `events_captured` | Counter | Total CDC events captured |
| `poll_cycles` | Counter | Total polling iterations |
| `empty_polls` | Counter | Polls with no changes (efficiency metric) |
| `avg_poll_time_ms` | Gauge | Average time per polling cycle |
| `last_poll_duration_ms` | Gauge | Most recent poll duration |
| `capture_instances` | Gauge | Number of active capture instances |

### Accessing Metrics

```rust
use rivven_cdc::sqlserver::{SqlServerCdc, SqlServerCdcConfig};
use rivven_cdc::CdcSource;

let mut cdc = SqlServerCdc::new(config);
cdc.start().await?;

// Get metrics snapshot at any time
let metrics = cdc.metrics();
println!("Events captured: {}", metrics.events_captured);
println!("Poll cycles: {}", metrics.poll_cycles);
println!("Empty polls: {}", metrics.empty_polls);
println!("Avg poll time: {}ms", metrics.avg_poll_time_ms);
println!("Last poll: {}ms", metrics.last_poll_duration_ms);
println!("Capture instances: {}", metrics.capture_instances);

// Calculate efficiency (lower is better - fewer empty polls)
let efficiency = if metrics.poll_cycles > 0 {
    100.0 * (1.0 - metrics.empty_polls as f64 / metrics.poll_cycles as f64)
} else {
    100.0
};
println!("Polling efficiency: {:.1}%", efficiency);
```

## rivven-connect Integration

SQL Server CDC is fully integrated with rivven-connect for YAML-based configuration and deployment.

### YAML Configuration

```yaml
# pipeline.yaml
sources:
  sqlserver_db:
    connector: sqlserver-cdc
    config:
      host: localhost
      port: 1433
      database: mydb
      username: rivven_cdc
      password: ${MSSQL_PASSWORD}
      schema: dbo
      poll_interval_ms: 500
      snapshot_mode: initial  # initial, always, never, when_needed
      encrypt: false
      trust_server_certificate: true
      heartbeat_interval_secs: 10
      connect_timeout_secs: 30
      max_retries: 3
      include_tables:
        - users
        - orders
      transforms:
        - type: flatten
          config:
            separator: "_"
    streams:
      - name: users
        namespace: dbo
        sync_mode: incremental
      - name: orders
        namespace: dbo
        sync_mode: incremental

sinks:
  rivven:
    connector: rivven
    topics: [cdc.sqlserver.events]
    config:
      address: localhost:9092

pipelines:
  sqlserver_cdc:
    source: sqlserver_db
    sink: rivven
    enabled: true
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `host` | String | Required | SQL Server hostname |
| `port` | u16 | 1433 | SQL Server port |
| `database` | String | Required | Target database |
| `username` | String | Required | SQL Server username |
| `password` | String | Required | SQL Server password (supports `${ENV_VAR}`) |
| `schema` | String | dbo | Default schema for table filtering |
| `poll_interval_ms` | u64 | 500 | CDC table polling interval in milliseconds |
| `snapshot_mode` | String | initial | Snapshot modes: initial, always, never, when_needed, initial_only, schema_only, recovery |
| `encrypt` | bool | false | Enable TLS encryption |
| `trust_server_certificate` | bool | false | Trust self-signed certificates |
| `heartbeat_interval_secs` | u32 | 10 | Heartbeat interval for lag monitoring |
| `connect_timeout_secs` | u32 | 30 | Connection timeout |
| `max_retries` | u32 | 3 | Maximum connection retry attempts |
| `include_tables` | list | [] | Tables to include (empty = all CDC-enabled) |
| `exclude_tables` | list | [] | Tables to exclude |

### Topic Routing

Use dynamic topic routing for per-table topics:

```yaml
sources:
  sqlserver_db:
    connector: sqlserver-cdc
    config:
      # ... connection config ...
      topic_routing: "cdc.{database}.{schema}.{table}"
      # Results in: cdc.mydb.dbo.users, cdc.mydb.dbo.orders
```

### Running with rivven-connect

```bash
# Check configuration
rivven-connect check --config pipeline.yaml

# Run the pipeline
rivven-connect run --config pipeline.yaml
```

## See Also

- [CDC Overview](./cdc.md)
- [PostgreSQL CDC](./cdc-postgres.md)
- [MySQL/MariaDB CDC](./cdc-mysql.md)
- [Configuration Reference](./cdc-configuration.md)
