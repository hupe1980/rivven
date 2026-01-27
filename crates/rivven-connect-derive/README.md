# rivven-connect-derive

Procedural macros for Rivven connector development.

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
rivven-connect-derive = "0.0.1"
```

## Available Macros

### SourceConfig

Derive macro for source connector configurations:

```rust
use rivven_connect_derive::SourceConfig;
use serde::Deserialize;
use schemars::JsonSchema;
use validator::Validate;

#[derive(Debug, Deserialize, Validate, JsonSchema, SourceConfig)]
#[source(name = "postgres-cdc", version = "1.0.0", description = "PostgreSQL CDC connector")]
pub struct PostgresCdcConfig {
    pub connection_string: String,
    pub slot_name: String,
}

// Generates: PostgresCdcConfigSpec with spec(), name(), version() methods
```

### SinkConfig

Derive macro for sink connector configurations:

```rust
use rivven_connect_derive::SinkConfig;
use serde::Deserialize;
use schemars::JsonSchema;
use validator::Validate;

#[derive(Debug, Deserialize, Validate, JsonSchema, SinkConfig)]
#[sink(name = "s3-sink", version = "1.0.0", batching, batch_size = 1000)]
pub struct S3SinkConfig {
    pub bucket: String,
    pub prefix: Option<String>,
}

// Generates: S3SinkConfigSpec with spec(), name(), version(), batch_config() methods
```

### TransformConfig

Derive macro for transform configurations:

```rust
use rivven_connect_derive::TransformConfig;
use serde::Deserialize;
use schemars::JsonSchema;
use validator::Validate;

#[derive(Debug, Deserialize, Validate, JsonSchema, TransformConfig)]
#[transform(name = "json-filter", version = "1.0.0")]
pub struct JsonFilterConfig {
    pub field: String,
    pub pattern: String,
}

// Generates: JsonFilterConfigSpec with spec(), name(), version() methods
```

### connector_spec (attribute macro)

Define connector specifications inline:

```rust
use rivven_connect_derive::connector_spec;

#[connector_spec(
    name = "my-connector",
    version = "1.0.0",
    description = "A custom connector"
)]
pub mod my_connector {
    // Connector implementation
}

// Generates: connector_spec() function returning ConnectorSpec
```

## Attributes

### Source Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `name` | string | Connector name (default: struct name in lowercase) |
| `version` | string | Connector version (default: "0.1.0") |
| `description` | string | Connector description |
| `documentation_url` | string | Documentation URL |

### Sink Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `name` | string | Connector name (default: struct name in lowercase) |
| `version` | string | Connector version (default: "0.1.0") |
| `description` | string | Connector description |
| `documentation_url` | string | Documentation URL |
| `batching` | flag | Enable batch configuration |
| `batch_size` | usize | Default batch size (default: 10,000) |

## License

MIT OR Apache-2.0
