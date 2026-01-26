# rivven-warehouse

Data warehouse connectors for Rivven Connect.

## Features

- **Snowflake** - Snowpipe Streaming API for low-latency ingestion
- **BigQuery** - Google BigQuery (planned)
- **Redshift** - AWS Redshift (planned)

## Installation

```toml
[dependencies]
rivven-warehouse = { version = "0.2", features = ["snowflake"] }
```

## Usage

```rust
use rivven_connect::SinkRegistry;
use rivven_warehouse::SnowflakeSinkFactory;
use std::sync::Arc;

// Register Snowflake sink with the registry
let mut sinks = SinkRegistry::new();
sinks.register("snowflake", Arc::new(SnowflakeSinkFactory));

// Or use the convenience function
rivven_warehouse::register_all(&mut sinks);
```

## Snowflake Configuration

### Prerequisites

1. Generate an RSA key pair:
   ```bash
   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
   ```

2. Register the public key with Snowflake:
   ```sql
   ALTER USER RIVVEN_USER SET RSA_PUBLIC_KEY='MIIBIjAN...';
   ```

### Configuration

```yaml
sinks:
  - name: snowflake-cdc
    type: snowflake
    config:
      account: myorg-account123
      user: RIVVEN_USER
      private_key_path: /secrets/rsa_key.p8
      database: ANALYTICS
      schema: CDC
      table: EVENTS
      channel: rivven-channel-1
      batch_size: 1000
      flush_interval_secs: 1
```

## Features Flags

| Feature | Description | Dependencies |
|---------|-------------|--------------|
| `snowflake` | Snowflake support | reqwest, jsonwebtoken, rsa |
| `bigquery` | Google BigQuery | (planned) |
| `redshift` | AWS Redshift | (planned) |
| `full` | All providers | all above |

## License

See root [LICENSE](../../LICENSE) file.
