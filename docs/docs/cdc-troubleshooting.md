---
layout: default
title: CDC Troubleshooting
parent: Change Data Capture
nav_order: 3
---

# CDC Troubleshooting Guide
{: .no_toc }

Diagnose and resolve common CDC issues.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Diagnostic Commands

### Check Connector Status

```rust
use rivven_cdc::CdcSource;

// Get current state
let state = cdc.state();
println!("Status: {:?}", state.status);
println!("Position: {:?}", state.position);
println!("Lag: {:?}", state.lag);
```

### View Extended Metrics

```rust
use rivven_cdc::common::ExtendedCdcMetrics;

let metrics = cdc.extended_metrics();

// Check streaming status
println!("Connected: {}", metrics.is_connected());
println!("Last event: {:?}", metrics.last_event_time());
println!("Events/sec: {}", metrics.events_per_second());

// Check latency
println!("Capture to emit: {:?}", metrics.capture_to_emit_latency_p99());
println!("Transaction latency: {:?}", metrics.committed_transaction_latency_p99());
```

### Export Prometheus Metrics

```rust
// Get all metrics in Prometheus format
let prometheus_output = metrics.to_prometheus();
println!("{}", prometheus_output);
```

---

## Common Issues

### Connection Problems

#### "Connection refused"

**Symptoms:**
- Connector fails to start
- Error: `Connection refused (os error 111)`

**Causes:**
1. Database server not running
2. Wrong host/port
3. Firewall blocking connection

**Solutions:**

```bash
# Test connectivity
nc -zv localhost 5432

# Check database is running (PostgreSQL)
pg_isready -h localhost -p 5432

# Check database is running (MySQL)
mysqladmin ping -h localhost
```

#### "Authentication failed"

**Symptoms:**
- Error: `password authentication failed for user`
- Error: `Access denied for user`

**Solutions:**

```sql
-- PostgreSQL: Check user exists
SELECT usename, passwd IS NOT NULL as has_password 
FROM pg_shadow WHERE usename = 'rivven';

-- MySQL: Check user exists
SELECT User, Host FROM mysql.user WHERE User = 'rivven';

-- Reset password
ALTER USER 'rivven' WITH PASSWORD 'new_password';
```

#### "TLS/SSL errors"

**Symptoms:**
- Error: `certificate verify failed`
- Error: `unknown CA`

**Solutions:**

```yaml
# Use correct CA certificate
tls:
  mode: verify-ca
  ca_cert_path: /correct/path/to/ca.pem

# For testing only (not production!)
tls:
  mode: require  # Encrypted but no verification
```

---

### Replication Problems

#### "Replication slot does not exist" (PostgreSQL)

**Symptoms:**
- Error: `replication slot "rivven_slot" does not exist`

**Solutions:**

```sql
-- Create slot manually
SELECT pg_create_logical_replication_slot('rivven_slot', 'pgoutput');

-- List existing slots
SELECT slot_name, plugin, database FROM pg_replication_slots;

-- Drop stale slot
SELECT pg_drop_replication_slot('old_slot_name');
```

#### "Replication slot is active" (PostgreSQL)

**Symptoms:**
- Error: `replication slot is already active`

**Solutions:**

```sql
-- Find active connection
SELECT * FROM pg_stat_replication;

-- Terminate (if safe)
SELECT pg_terminate_backend(pid) 
FROM pg_stat_activity 
WHERE usename = 'rivven' AND application_name LIKE 'rivven%';

-- Then restart CDC connector
```

#### "Server ID conflict" (MySQL)

**Symptoms:**
- Error: `A slave with the same server_uuid/server_id`

**Solutions:**

Use a unique `server_id` for each CDC instance:

```yaml
config:
  server_id: 12346  # Different from other replicas
```

#### "WAL/Binlog purged"

**Symptoms:**
- PostgreSQL: `requested WAL segment has already been removed`
- MySQL: `Could not find first log file name in binary log index file`

**Causes:**
- CDC connector was offline too long
- WAL/binlog retention too short

**Solutions:**

```sql
-- PostgreSQL: Increase retention
ALTER SYSTEM SET wal_keep_size = '16GB';
SELECT pg_reload_conf();

-- MySQL: Increase retention (my.cnf)
-- expire_logs_days = 14
```

**Recovery:**
```yaml
# Force new snapshot
snapshot_mode: always
```

---

### Schema Problems

#### "Column not found in schema"

**Symptoms:**
- Error: `column "new_column" not found in schema`

**Causes:**
- Schema changed but CDC has cached old schema

**Solutions:**

```rust
// Force schema refresh
cdc.refresh_schema().await?;
```

Or restart the CDC connector with fresh cache.

#### "Schema evolution not supported"

**Symptoms:**
- Error after `DROP COLUMN` or type change

**Solutions:**

Use schema evolution settings:

```yaml
config:
  schema_evolution:
    mode: compatible  # or: full, none
    allow_column_drops: true
```

---

### Performance Problems

#### High Replication Lag

**Symptoms:**
- `rivven_cdc_lag_milliseconds` consistently high
- Events arriving late

**Diagnosis:**

```rust
let metrics = cdc.extended_metrics();
println!("Source lag: {:?}", metrics.source_lag());
println!("Emit lag: {:?}", metrics.emit_lag());
println!("Pending events: {}", metrics.pending_events());
```

**Solutions:**

1. **Scale workers:**
   ```yaml
   config:
     workers: 8
   ```

2. **Increase batch size:**
   ```yaml
   config:
     batch_size: 10000
   ```

3. **Check bottlenecks:**
   - Network bandwidth
   - Downstream sink throughput
   - CPU usage on CDC host

#### Memory Growth

**Symptoms:**
- Memory usage keeps increasing
- OOM errors

**Diagnosis:**

```rust
let metrics = cdc.extended_metrics();
println!("Memory usage: {} bytes", metrics.memory_usage_bytes());
println!("Queue size: {}", metrics.queue_size());
```

**Solutions:**

1. **Limit queue size:**
   ```yaml
   config:
     max_queue_size: 10000
     queue_timeout: 30s
   ```

2. **Enable backpressure:**
   ```yaml
   config:
     backpressure:
       enabled: true
       high_watermark: 8000
       low_watermark: 2000
   ```

#### Snapshot Taking Too Long

**Symptoms:**
- Initial snapshot runs for hours
- Database load spikes during snapshot

**Solutions:**

1. **Use parallel snapshot:**
   ```yaml
   config:
     snapshot_parallel_tables: 4
   ```

2. **Filter tables:**
   ```yaml
   config:
     tables:
       - app.critical_table
     exclude_tables:
       - "*_archive"
       - "*_backup"
   ```

3. **Use incremental snapshot (PostgreSQL):**
   ```yaml
   config:
     snapshot_mode: incremental
     signal_table: rivven.signals
   ```

---

### Data Issues

#### Missing Events

**Symptoms:**
- Source has more changes than CDC emitted
- Gaps in data

**Diagnosis:**

```rust
// Check for skipped events
let metrics = cdc.extended_metrics();
println!("Filtered events: {}", metrics.filtered_events());
println!("Skipped events: {}", metrics.skipped_events());
println!("Error events: {}", metrics.errored_events());
```

**Common causes:**
1. Table not in inclusion list
2. Filter rules excluding events
3. Connector crashed and restarted without checkpointing

**Solutions:**

1. **Check table filters:**
   ```yaml
   config:
     tables:
       - schema.table_name  # Ensure table is included
   ```

2. **Disable aggressive filtering:**
   ```yaml
   config:
     filter:
       include_all_dml: true
   ```

3. **Force re-snapshot:**
   ```yaml
   config:
     snapshot_mode: always
   ```

#### Duplicate Events

**Symptoms:**
- Same event received multiple times
- Idempotency errors downstream

**Causes:**
- At-least-once delivery (expected)
- Checkpoint failed during processing

**Solutions:**

1. **Enable exactly-once (if available):**
   ```yaml
   config:
     delivery: exactly_once
   ```

2. **Use event IDs for deduplication:**
   ```rust
   let event_id = event.metadata().event_id();
   if !seen.contains(&event_id) {
       process(event)?;
       seen.insert(event_id);
   }
   ```

#### Incorrect Data Types

**Symptoms:**
- Numeric precision lost
- Timestamps incorrectly parsed

**Solutions:**

```yaml
config:
  # Preserve decimal precision
  decimal_handling: string  # or: precise, double
  
  # Timestamp handling
  time_precision: microseconds
  timezone: UTC
```

---

## Health Checks

### Basic Health Check

```rust
pub async fn health_check(cdc: &impl CdcSource) -> Result<(), Error> {
    // Check connection
    if !cdc.is_connected() {
        return Err(Error::NotConnected);
    }
    
    // Check lag
    let metrics = cdc.extended_metrics();
    if metrics.lag_milliseconds() > 60_000 {
        return Err(Error::HighLag(metrics.lag_milliseconds()));
    }
    
    // Check recent activity
    if let Some(last_event) = metrics.last_event_time() {
        if last_event.elapsed() > Duration::from_secs(300) {
            return Err(Error::Stale);
        }
    }
    
    Ok(())
}
```

### Kubernetes Liveness Probe

```yaml
livenessProbe:
  httpGet:
    path: /health/live
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 10
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /health/ready
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 5
  failureThreshold: 3
```

### Alert Rules

```yaml
groups:
- name: rivven-cdc-alerts
  rules:
  - alert: CdcHighLag
    expr: rivven_cdc_lag_milliseconds > 60000
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "CDC lag is high"
      description: "CDC connector {{ $labels.connector }} has {{ $value }}ms lag"
      
  - alert: CdcDisconnected
    expr: rivven_cdc_connected == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "CDC connector disconnected"
      description: "CDC connector {{ $labels.connector }} lost connection"
      
  - alert: CdcErrorRate
    expr: rate(rivven_cdc_errors_total[5m]) > 0.1
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "CDC error rate elevated"
```

---

## Recovery Procedures

### Force Reconnection

```rust
// Graceful reconnect
cdc.disconnect().await?;
cdc.connect().await?;

// Or force reset
cdc.reset().await?;
```

### Reset Offset

```rust
// Reset to beginning (re-snapshot)
cdc.reset_offset(Offset::Beginning).await?;

// Reset to specific position (PostgreSQL)
cdc.reset_offset(Offset::Lsn("0/1234567".parse()?)).await?;

// Reset to timestamp
cdc.reset_offset(Offset::Timestamp(some_time)).await?;
```

### Emergency Cleanup (PostgreSQL)

```sql
-- List all Rivven slots
SELECT slot_name, active, restart_lsn 
FROM pg_replication_slots 
WHERE slot_name LIKE 'rivven%';

-- Drop inactive slots
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE slot_name LIKE 'rivven%' AND NOT active;

-- Terminate stuck connections
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE usename = 'rivven' AND state = 'idle';
```

### Emergency Cleanup (MySQL)

```sql
-- Show replication connections
SHOW PROCESSLIST;

-- Kill stuck connection
KILL <process_id>;

-- Reset binary log position (careful!)
RESET MASTER;
```

---

## Debug Logging

### Enable Verbose Logging

```rust
// Rust
use tracing_subscriber::EnvFilter;

tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::new("rivven_cdc=debug"))
    .init();
```

Or via environment:
```bash
export RUST_LOG=rivven_cdc=debug
```

### Log Levels

| Level | Use Case |
|:------|:---------|
| `error` | Failures only |
| `warn` | Recoverable issues |
| `info` | Normal operations (default) |
| `debug` | Detailed diagnostics |
| `trace` | All messages including raw data |

### Event Tracing

```rust
let config = CdcConfig::builder()
    .trace_events(true)
    .build();

// Events will include trace IDs
let event = cdc.next().await?;
println!("Trace ID: {:?}", event.trace_id());
```

---

## Getting Help

### Information to Collect

When reporting issues, include:

1. **Configuration** (redact passwords)
2. **Error message** and stack trace
3. **Relevant metrics** from Prometheus
4. **Database version** and settings
5. **Steps to reproduce**

### Debug Report

```rust
// Generate debug report
let report = cdc.debug_report().await?;
println!("{}", report);
```

This includes:
- Connection status
- Current position
- Recent errors
- Performance metrics
- Configuration (sanitized)

---

## Next Steps

- [PostgreSQL CDC Guide](cdc-postgres) - PostgreSQL setup
- [MySQL CDC Guide](cdc-mysql) - MySQL/MariaDB setup
- [CDC Configuration Reference](cdc-configuration) - Full reference
