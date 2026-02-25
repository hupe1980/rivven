---
layout: default
title: MySQL/MariaDB CDC Guide
parent: Change Data Capture
nav_order: 2
---

# MySQL/MariaDB CDC Guide
{: .no_toc }

Complete guide to Change Data Capture from MySQL and MariaDB databases.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Rivven CDC for MySQL/MariaDB uses **binary log replication** to stream row-level changes in real-time. This guide covers:

- Prerequisites and database setup
- Configuration options
- GTID vs position-based replication
- MariaDB-specific features
- Full JSON type support
- Automatic reconnection
- Troubleshooting

---

## Prerequisites

### Version Requirements

#### MySQL

| Version | Status | EOL | Notes |
|:--------|:-------|:----|:------|
| 5.7.x | ⚠️ | Oct 2023 | End of life, not recommended |
| 8.0.x | ✅ Tested | Apr 2026 | GTID, caching_sha2_password |
| 8.4.x | ✅ **Recommended** | Apr 2032 | LTS, enhanced replication |
| 9.0.x | ✅ Tested | TBD | Innovation release (latest) |

{: .note }
> We test against MySQL 8.0, 8.4, and 9.0 in CI. MySQL 8.4 LTS is our recommended version for production deployments.

#### MariaDB

| Version | Status | EOL | Notes |
|:--------|:-------|:----|:------|
| 10.5.x | ⚠️ | Jun 2025 | Approaching EOL |
| 10.6.x | ✅ Tested | Jul 2026 | LTS, GTID improvements |
| 10.11.x | ✅ **Recommended** | Feb 2028 | LTS, enhanced JSON |
| 11.4.x | ✅ Tested | May 2029 | LTS, latest features |

{: .note }
> We test against MariaDB 10.6, 10.11, and 11.4 in CI. MariaDB 10.11 LTS is our recommended version for production deployments.

### Binary Log Configuration

Edit `my.cnf` (MySQL) or `mariadb.cnf`:

```ini
[mysqld]
# Required: Enable binary logging
log-bin = mysql-bin
binlog_format = ROW           # Must be ROW for CDC
binlog_row_image = FULL       # Capture all columns

# GTID (recommended)
gtid_mode = ON                # MySQL GTID
enforce_gtid_consistency = ON # MySQL only

# MariaDB GTID (different syntax)
# gtid_strict_mode = ON       # MariaDB

# Recommended settings
server-id = 1                 # Unique server ID
expire_logs_days = 7          # Retain binlogs
max_binlog_size = 1G          # Max file size
sync_binlog = 1               # Durability
```

Restart MySQL/MariaDB after changes.

### User Permissions

```sql
-- Create replication user
CREATE USER 'rivven'@'%' IDENTIFIED BY 'secure_password';

-- Grant replication privileges
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'rivven'@'%';

-- Grant SELECT for snapshot
GRANT SELECT ON *.* TO 'rivven'@'%';

-- For schema history (optional)
GRANT RELOAD ON *.* TO 'rivven'@'%';

FLUSH PRIVILEGES;
```

---

## Basic Configuration

### Minimal Setup (Rust)

```rust
use rivven_cdc::mysql::{MySqlCdc, MySqlCdcConfig};
use rivven_cdc::CdcSource;

let config = MySqlCdcConfig::new("localhost", "rivven")
    .with_password("password")
    .with_database("mydb")
    .with_server_id(12345);  // Must be unique

let mut cdc = MySqlCdc::new(config);

while let Some(event) = cdc.next().await? {
    println!("Event: {:?}", event);
}
```

### YAML Configuration

**Single topic (all tables → one topic):**

```yaml
version: "1.0"

sources:
  mysql_cdc:
    connector: mysql-cdc
    topic: cdc.mysql  # Fallback topic
    config:
      host: localhost
      port: 3306
      database: shop
      user: rivven
      password: ${MYSQL_PASSWORD}
      server_id: 12345
      tables:
        - shop.orders
        - shop.customers
```

**Dynamic topic routing (per-table topics):**

```yaml
version: "1.0"

sources:
  mysql_cdc:
    connector: mysql-cdc
    topic: cdc.default               # Fallback topic
    config:
      host: localhost
      port: 3306
      database: shop
      user: rivven
      password: ${MYSQL_PASSWORD}
      server_id: 12345
      tables:
        - shop.orders      # → cdc.shop.orders
        - shop.customers   # → cdc.shop.customers
      topic_routing: "cdc.{database}.{table}"  # Dynamic routing
```

{: .note }
> Topic routing supports placeholders: `{database}`, `{schema}`, `{table}`. See [CDC Configuration Reference](cdc-configuration.md#topic-routing) for details.

---

## Configuration Reference

### Connection Settings

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `host` | string | `localhost` | MySQL/MariaDB host |
| `port` | u16 | `3306` | Port |
| `database` | string | required | Database name |
| `user` | string | required | Username |
| `password` | string | required | Password |
| `connect_timeout` | duration | `30s` | Connection timeout |
| `read_timeout` | duration | `60s` | Read timeout |

### Replication Settings

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `server_id` | u32 | required | **Unique** server ID for this client |
| `gtid_mode` | bool | `true` | Use GTID positioning |
| `gtid_set` | string | - | Starting GTID set (MySQL format) |
| `binlog_filename` | string | - | Starting binlog file (position mode) |
| `binlog_position` | u64 | - | Starting position (position mode) |

### Table Selection

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `tables` | list | `[]` | Tables to include (empty = all) |
| `exclude_tables` | list | `[]` | Tables to exclude |
| `database_filter` | regex | - | Database name pattern |

### Snapshot Settings

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `snapshot_mode` | enum | `initial` | Snapshot behavior |
| `snapshot_batch_size` | u64 | `10000` | Rows per batch |
| `snapshot_lock_timeout` | duration | `30s` | Lock timeout |
| `snapshot_isolation_level` | enum | `repeatable_read` | Transaction isolation |

#### Snapshot Modes

All standard snapshot modes are supported:

| Mode | Description |
|------|-------------|
| `initial` | Snapshot on first run, then stream changes (default) |
| `always` | Full snapshot on every restart |
| `never` | Skip snapshot, stream changes only |
| `when_needed` | Snapshot if no valid binlog position exists |
| `initial_only` | Snapshot and stop (for data migration) |
| `no_data` | Capture schema only, skip data (alias: `schema_only`) |
| `recovery` | Rebuild schema history after corruption |

### TLS Settings

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `tls.mode` | enum | `prefer` | TLS mode |
| `tls.ca_cert_path` | path | - | CA certificate |
| `tls.client_cert_path` | path | - | Client cert (mTLS) |
| `tls.client_key_path` | path | - | Client key (mTLS) |
| `tls.verify_identity` | bool | `true` | Verify server identity |

---

## Authentication

Rivven CDC supports all common MySQL and MariaDB authentication plugins:

### mysql_native_password (MySQL 5.x default)

Traditional SHA1-based authentication. Works with all MySQL versions:

```sql
-- Create user with mysql_native_password
CREATE USER 'rivven'@'%' IDENTIFIED WITH mysql_native_password BY 'password';
```

### caching_sha2_password (MySQL 8.0+ default)

Modern SHA256-based authentication with caching for performance. Rivven fully supports this plugin including:

- **Fast authentication**: When the server has cached the password hash
- **Full authentication over TLS**: Password sent in cleartext over encrypted connection
- **Full authentication with RSA**: Password encrypted with server's public key (non-TLS)

```sql
-- Create user with caching_sha2_password (MySQL 8.0+ default)
CREATE USER 'rivven'@'%' IDENTIFIED BY 'password';

-- Or explicitly specify the plugin
CREATE USER 'rivven'@'%' IDENTIFIED WITH caching_sha2_password BY 'password';
```

{: .note }
For `caching_sha2_password` without TLS, the server's RSA public key is automatically fetched and used to encrypt the password. This is secure but TLS is still recommended for production.

### sha256_password

Legacy SHA256 authentication (predecessor to caching_sha2_password):

```sql
CREATE USER 'rivven'@'%' IDENTIFIED WITH sha256_password BY 'password';
```

### client_ed25519 (MariaDB)

MariaDB's Ed25519 authentication plugin uses modern elliptic curve cryptography:

```sql
-- MariaDB only
INSTALL SONAME 'auth_ed25519';
CREATE USER 'rivven'@'%' IDENTIFIED VIA ed25519 USING PASSWORD('password');
```

{: .note }
Ed25519 provides strong security with shorter signatures. The client derives an Ed25519 keypair from SHA-512(password) and signs the server's challenge.

### Checking User Authentication Plugin

```sql
-- MySQL
SELECT user, host, plugin FROM mysql.user WHERE user = 'rivven';

-- MariaDB
SELECT user, host, plugin FROM mysql.user WHERE user = 'rivven';
```

### Changing Authentication Plugin

```sql
-- Switch to mysql_native_password (for legacy compatibility)
ALTER USER 'rivven'@'%' IDENTIFIED WITH mysql_native_password BY 'password';

-- Switch to caching_sha2_password (recommended for MySQL 8.0+)
ALTER USER 'rivven'@'%' IDENTIFIED WITH caching_sha2_password BY 'password';
```

---

## GTID vs Position-Based Replication

### GTID Mode (Recommended)

GTID provides:
- Automatic position tracking
- Easier failover handling
- No binlog filename/position management

```yaml
config:
  gtid_mode: true
  # Optional: Start from specific GTID
  gtid_set: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
```

### Position-Based Mode

For databases without GTID:

```yaml
config:
  gtid_mode: false
  binlog_filename: mysql-bin.000001
  binlog_position: 4
```

**Position Tracking**: In position-based mode, the binlog position is advanced after every decoded event (using `EventHeader.next_position`) rather than only on Rotate events. The committed position is only updated after a full transaction (Xid event) is successfully delivered to the channel, providing at-least-once delivery at the transaction level. On reconnect, replay starts from the last committed transaction boundary.

> **Note**: Binlog positions exceeding 4 GB (u32::MAX) are clamped with a warning. This is a MySQL protocol limitation — `EventHeader.next_position` is a 32-bit field.

**Finding Current Position:**
```sql
SHOW MASTER STATUS;
-- Output: mysql-bin.000042, 12345
```

---

## MariaDB-Specific Configuration

MariaDB uses a different GTID format than MySQL.

### MariaDB GTID

```yaml
config:
  # MariaDB GTID format: domain-server_id-sequence
  mariadb_gtid: true
  gtid_set: "0-1-100"  # Domain 0, Server 1, Sequence 100
```

### MariaDB Server Configuration

```ini
[mysqld]
# MariaDB GTID
gtid_strict_mode = ON
gtid_domain_id = 0
server_id = 1
```

### Differences from MySQL

| Feature | MySQL | MariaDB |
|:--------|:------|:--------|
| GTID Format | UUID:transaction_id | domain-server_id-sequence |
| GTID Variable | `gtid_executed` | `gtid_current_pos` |
| Binlog Events | MySQL-specific | MariaDB extensions |

---

## Binlog Checksum Handling

MySQL 5.6.2+ and MariaDB 10.2+ support binlog checksums (CRC32) for data integrity. Rivven automatically handles checksum negotiation and verification.

### How It Works

1. **MySQL 8.0+**: Rivven sets `@source_binlog_checksum = @@global.binlog_checksum` to tell the server we understand checksums
2. **MySQL 5.6-8.0**: Falls back to `@master_binlog_checksum` variable name
3. **MariaDB 10+**: Uses explicit `SET @master_binlog_checksum = 'CRC32'` and `@mariadb_slave_capability=5`
4. **CRC32 Stripping**: 4-byte checksums are automatically stripped from event payloads

### Configuration

Binlog checksums are handled automatically—no configuration needed. Ensure your server has checksums enabled (default for modern versions):

```sql
-- Check current checksum setting
SHOW VARIABLES LIKE 'binlog_checksum';
-- Should return: CRC32

-- Enable checksums if disabled
SET GLOBAL binlog_checksum = 'CRC32';
```

{: .note }
Rivven gracefully handles servers with checksums disabled. No errors will occur if `binlog_checksum = NONE`.

---

## JSON Type Support

Rivven includes a **full MySQL binary JSON decoder** that converts MySQL's internal `MYSQL_TYPE_JSON` representation into structured JSON values. All JSON subtypes are supported:

| JSON Subtype | Decoded As |
|:-------------|:-----------|
| Small/large JSON object | Nested `serde_json::Value::Object` |
| Small/large JSON array | `serde_json::Value::Array` |
| String literal | `serde_json::Value::String` |
| Integer (int16/32/64, uint16/32/64) | `serde_json::Value::Number` |
| Double | `serde_json::Value::Number` |
| Boolean (true/false literals) | `serde_json::Value::Bool` |
| Null literal | `serde_json::Value::Null` |
| Opaque types (date, time, datetime, decimal) | Opaque binary preserved |

{: .note }
> Rivven decodes JSON columns into structured values, enabling downstream consumers to process JSON data without additional parsing.

---

## Automatic Reconnection

The MySQL CDC connector automatically reconnects to the binlog stream on transient failures with **exponential backoff and jitter**:

- Initial backoff delay with configurable parameters
- Exponential increase with randomized jitter to prevent thundering herd
- Automatic GTID position recovery after reconnection
- Connection failures are logged and exposed via Prometheus metrics

This ensures CDC pipelines survive temporary network partitions, MySQL restarts, and failover events without manual intervention.

---

## Schema Metadata

Rivven automatically resolves column names from the database schema, providing human-readable field names in CDC events instead of generic `col0`, `col1` placeholders.

### How It Works

1. On each `TABLE_MAP` event, Rivven queries `INFORMATION_SCHEMA.COLUMNS` 
2. Column names are cached in memory per (schema, table) pair
3. Cached names are used for all subsequent row events

### Event Output

**With Schema Metadata (default):**
```json
{
  "before": null,
  "after": {
    "id": 1,
    "name": "John Doe",
    "email": "john@example.com",
    "created_at": "2024-01-15T10:30:00"
  },
  "op": "c"
}
```

**Without Schema Metadata (fallback):**
```json
{
  "before": null,
  "after": {
    "col0": 1,
    "col1": "John Doe",
    "col2": "john@example.com",
    "col3": "2024-01-15T10:30:00"
  },
  "op": "c"
}
```

### Required Permissions

The replication user needs `SELECT` on `INFORMATION_SCHEMA`:

```sql
-- Already included in basic GRANT SELECT ON *.*
GRANT SELECT ON INFORMATION_SCHEMA.* TO 'rivven'@'%';
```

---

## TLS Configuration

### Basic TLS

```yaml
config:
  tls:
    mode: require
    ca_cert_path: /etc/ssl/certs/mysql-ca.pem
```

### AWS RDS MySQL

```yaml
config:
  tls:
    mode: verify-identity
    ca_cert_path: /etc/ssl/certs/rds-combined-ca-bundle.pem
```

Download RDS CA:
```bash
curl -o /etc/ssl/certs/rds-combined-ca-bundle.pem \
  https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
```

### Google Cloud SQL

```yaml
config:
  tls:
    mode: verify-identity
    ca_cert_path: /etc/ssl/certs/server-ca.pem
    client_cert_path: /etc/ssl/certs/client-cert.pem
    client_key_path: /etc/ssl/private/client-key.pem
```

---

## Monitoring

### Prometheus Metrics

```
# Event counters
rivven_cdc_events_total{connector="mysql_cdc",op="insert"}
rivven_cdc_events_total{connector="mysql_cdc",op="update"}
rivven_cdc_events_total{connector="mysql_cdc",op="delete"}

# Lag
rivven_cdc_lag_milliseconds{connector="mysql_cdc"}

# Connection
rivven_cdc_connected{connector="mysql_cdc"}

# GTID position
rivven_cdc_gtid_position{connector="mysql_cdc"}
```

### Health Check Query

```sql
-- Check binlog status
SHOW MASTER STATUS;

-- Check replication connections
SHOW PROCESSLIST;

-- Check GTID (MySQL)
SELECT @@gtid_executed;

-- Check GTID (MariaDB)
SELECT @@gtid_current_pos;
```

---

## Security Considerations

### Identifier Validation

All table, schema, and column identifiers used in snapshot queries are validated via `Validator::validate_identifier()`, which enforces `^[a-zA-Z_][a-zA-Z0-9_]{0,254}$`. Invalid identifiers are rejected at `TableSpec::new()` before any SQL is constructed.

### SQL Injection Prevention

Snapshot `SELECT` queries apply defense-in-depth backtick-doubling (`` ` `` → ` `` ``) on all identifiers, preventing injection even in the unlikely event that validation is bypassed via internal API misuse. Keyset pagination values are always passed as parameterized query parameters.

---

## Troubleshooting

### Common Issues

#### "Access denied; you need REPLICATION SLAVE privilege"

**Cause:** User lacks replication privileges.

**Solution:**
```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'rivven'@'%';
FLUSH PRIVILEGES;
```

#### "Binlog not enabled" / "Binary logging not enabled"

**Cause:** Binary logging is disabled.

**Solution:** Enable in `my.cnf`:
```ini
[mysqld]
log-bin = mysql-bin
binlog_format = ROW
```

#### "Server ID conflict"

**Cause:** Another client is using the same server_id.

**Solution:** Use a unique server_id for each CDC connector:
```yaml
config:
  server_id: 12346  # Different from other replicators
```

#### "GTID mode OFF"

**Cause:** GTID is disabled but `gtid_mode: true` is configured.

**Solution:** Either enable GTID on the server:
```ini
[mysqld]
gtid_mode = ON
enforce_gtid_consistency = ON
```

Or disable GTID in configuration:
```yaml
config:
  gtid_mode: false
  binlog_filename: mysql-bin.000001
  binlog_position: 4
```

#### "Binlog file has been purged"

**Cause:** CDC fell behind and binlogs were deleted.

**Solution:**
1. Increase `expire_logs_days`:
   ```ini
   expire_logs_days = 14
   ```
2. Use larger `max_binlog_size`:
   ```ini
   max_binlog_size = 2G
   ```
3. Use GTID mode (auto-recovers with new snapshot)

#### "Row image not FULL"

**Cause:** `binlog_row_image` is not set to FULL.

**Solution:**
```ini
[mysqld]
binlog_row_image = FULL
```

---

## Best Practices

### Production Checklist

- [ ] Dedicated replication user
- [ ] GTID enabled (recommended)
- [ ] `binlog_format = ROW`
- [ ] `binlog_row_image = FULL`
- [ ] Unique `server_id` per connector
- [ ] TLS enabled
- [ ] Monitoring configured
- [ ] Binlog retention sized appropriately

### Performance Tuning

```yaml
config:
  # Larger batches
  snapshot_batch_size: 50000
  
  # Reduce network roundtrips
  binlog_batch_size: 4096
  
  # Connection pooling
  connection_pool_size: 4
```

### High Availability

1. **GTID auto-positioning:** Use GTID for automatic position recovery after failover.

2. **Read replica CDC:** Connect to read replica to reduce primary load.

3. **Binlog retention:** Ensure binlogs are retained long enough for recovery.

---

## Examples

### Full Production Configuration

```yaml
version: "1.0"

sources:
  production_mysql:
    connector: mysql-cdc
    topic: cdc.production
    config:
      # Connection
      host: mysql-primary.internal
      port: 3306
      database: production
      user: rivven_cdc
      password: ${MYSQL_CDC_PASSWORD}
      connect_timeout: 30s
      read_timeout: 60s
      
      # TLS
      tls:
        mode: verify-identity
        ca_cert_path: /etc/ssl/certs/mysql-ca.pem
      
      # Replication
      server_id: 12345
      gtid_mode: true
      
      # Tables
      tables:
        - production.orders
        - production.order_items
        - production.customers
      exclude_tables:
        - "*_backup"
        - "*_archive"
      
      # Snapshot
      snapshot_mode: initial
      snapshot_batch_size: 50000
      snapshot_isolation_level: repeatable_read
      
      # Filtering
      column_masks:
        - production.customers.ssn
        - production.customers.credit_card
```

### MariaDB Configuration

```yaml
version: "1.0"

sources:
  mariadb_cdc:
    connector: mariadb-cdc
    topic: cdc.mariadb
    config:
      host: mariadb.internal
      port: 3306
      database: mydb
      user: rivven
      password: ${MARIADB_PASSWORD}
      
      # MariaDB-specific
      server_id: 12345
      mariadb_gtid: true
      
      # TLS
      tls:
        mode: require
```

---

## Next Steps

- [PostgreSQL CDC Guide](cdc-postgres) - PostgreSQL setup
- [CDC Troubleshooting](cdc-troubleshooting) - Debug common issues
- [CDC Configuration Reference](cdc-configuration) - All parameters
