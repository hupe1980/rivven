# rivven-warehouse

Data warehouse connectors for Rivven Connect.

## Features

- **Snowflake** - Snowpipe Streaming API for low-latency ingestion
- **BigQuery** - Google BigQuery insertAll API for streaming inserts
- **Redshift** - Amazon Redshift via PostgreSQL protocol with SSL/TLS

## Installation

```toml
[dependencies]
# Snowflake only
rivven-warehouse = { version = "0.2", features = ["snowflake"] }

# BigQuery only
rivven-warehouse = { version = "0.2", features = ["bigquery"] }

# Redshift only
rivven-warehouse = { version = "0.2", features = ["redshift"] }

# All providers
rivven-warehouse = { version = "0.2", features = ["full"] }
```

## Usage

```rust
use rivven_connect::SinkRegistry;
use rivven_warehouse::{SnowflakeSinkFactory, BigQuerySinkFactory, RedshiftSinkFactory};
use std::sync::Arc;

// Register individual sinks
let mut sinks = SinkRegistry::new();
sinks.register("snowflake", Arc::new(SnowflakeSinkFactory));
sinks.register("bigquery", Arc::new(BigQuerySinkFactory));
sinks.register("redshift", Arc::new(RedshiftSinkFactory));

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

## BigQuery Configuration

### Prerequisites

1. Create a BigQuery table with appropriate schema
2. Set up authentication (ADC or service account)

### Configuration

```yaml
sinks:
  - name: bigquery-events
    type: bigquery
    config:
      project_id: my-gcp-project
      dataset_id: my_dataset
      table_id: events
      # Optional: service account credentials
      credentials_file: /secrets/gcp-sa.json
      batch_size: 500
      flush_interval_secs: 5
      skip_invalid_rows: false
      ignore_unknown_values: true
```

### Authentication Methods

1. **Application Default Credentials (ADC)** - Default, uses `GOOGLE_APPLICATION_CREDENTIALS`
2. **Service Account Key File** - Via `credentials_file` config
3. **Service Account JSON** - Via `credentials_json` config

## Redshift Configuration

### Prerequisites

1. Create a Redshift cluster and database
2. Create a target table with appropriate schema

### Configuration

```yaml
sinks:
  - name: redshift-events
    type: redshift
    config:
      host: my-cluster.region.redshift.amazonaws.com
      port: 5439
      database: mydb
      username: admin
      password: ${REDSHIFT_PASSWORD}
      schema: public
      table: events
      ssl_mode: require
      batch_size: 500
      flush_interval_secs: 5
```

### SSL Modes

- `disable` - No SSL (not recommended)
- `prefer` - Prefer SSL (default)
- `require` - Require SSL
- `verify-ca` - Verify server certificate
- `verify-full` - Verify certificate and hostname

## Features Flags

| Feature | Description | Dependencies |
|---------|-------------|--------------|
| `snowflake` | Snowflake support | reqwest, jsonwebtoken, rsa |
| `bigquery` | Google BigQuery | gcp-bigquery-client, google-cloud-auth |
| `redshift` | AWS Redshift | tokio-postgres, native-tls, aws-sdk-redshift |
| `full` | All providers | all above |

## License

See root [LICENSE](../../LICENSE) file.
