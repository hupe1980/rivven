# Rivven CDC

Native Change Data Capture for PostgreSQL, MySQL, and MariaDB.

## Features

- 🚀 **Native Implementation** - Direct TCP connections, no external dependencies
- 🐘 **PostgreSQL** - Logical replication via pgoutput plugin (v10+)
- 🐬 **MySQL/MariaDB** - Binlog replication with GTID support (MySQL 5.7+, MariaDB 10.2+)
- 📦 **Zero-Copy** - Efficient binary protocol parsing
- ⚡ **Async** - Built on Tokio for high-performance streaming

## Quick Start

### PostgreSQL

```rust
use rivven_cdc::postgres::{PostgresCdc, PostgresCdcConfig};
use rivven_cdc::CdcSource;

let config = PostgresCdcConfig::builder()
    .connection_string("postgres://user:pass@localhost/mydb")
    .slot_name("rivven_slot")
    .publication_name("rivven_pub")
    .build()?;

let mut cdc = PostgresCdc::new(config);
cdc.start().await?;
```

### MySQL / MariaDB

```rust
use rivven_cdc::mysql::{MySqlCdc, MySqlCdcConfig};

let config = MySqlCdcConfig::new("localhost", "root")
    .with_password("password")
    .with_database("mydb")
    .with_server_id(1001);

let cdc = MySqlCdc::new(config);
```

## Documentation

- [CDC Architecture](../../docs/CDC_ARCHITECTURE.md) - Protocol deep-dive and internals
- [CDC Quickstart](../../docs/CDC_QUICKSTART.md) - Setup guides for each database
- [CDC Production](../../docs/CDC_PRODUCTION.md) - Performance tuning and monitoring

## License

See root [LICENSE](../../LICENSE) file.
