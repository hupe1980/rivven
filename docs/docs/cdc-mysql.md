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
- Troubleshooting

---

## Prerequisites

### Version Requirements

| Database | Minimum Version | Recommended | Notes |
|:---------|:----------------|:------------|:------|
| MySQL | 5.7 | 8.0+ | GTID available in 5.6+ |
| MariaDB | 10.2 | 10.6+ | MariaDB GTID differs from MySQL |

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

```yaml
version: "1.0"

sources:
  mysql_cdc:
    connector: mysql-cdc
    topic: cdc.mysql
    config:
      # Connection
      host: localhost
      port: 3306
      database: shop
      user: rivven
      password: ${MYSQL_PASSWORD}
      
      # Server ID (must be unique across all replicating clients)
      server_id: 12345
      
      # Tables
      tables:
        - shop.orders
        - shop.customers
```

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

### TLS Settings

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `tls.mode` | enum | `prefer` | TLS mode |
| `tls.ca_cert_path` | path | - | CA certificate |
| `tls.client_cert_path` | path | - | Client cert (mTLS) |
| `tls.client_key_path` | path | - | Client key (mTLS) |
| `tls.verify_identity` | bool | `true` | Verify server identity |

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
