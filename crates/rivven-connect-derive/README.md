# rivven-connect-derive

Procedural macros for Rivven connector development. Reduces boilerplate when building custom connectors with automatic config schema generation.

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
rivven-connect = "0.0.12"  # Re-exports derive macros
# Or directly:
# rivven-connect-derive = "0.0.12"
```

## Available Macros

### SourceConfig

Derive macro for source connector configurations. Generates a `*Spec` struct with `spec()`, `name()`, and `version()` methods, including automatic JSON Schema generation.

```rust
use rivven_connect::prelude::*;
use rivven_connect::SourceConfigDerive;

#[derive(Debug, Deserialize, Validate, JsonSchema, SourceConfigDerive)]
#[source(
    name = "postgres-cdc",
    version = "1.0.0",
    description = "PostgreSQL CDC connector using logical replication",
    author = "Rivven Team",
    license = "Apache-2.0",
    documentation_url = "https://rivven.dev/docs/connectors/postgres-cdc",
    incremental
)]
pub struct PostgresCdcConfig {
    pub connection_string: String,
    pub slot_name: String,
}

// Generates: PostgresCdcConfigSpec with:
// - spec() -> ConnectorSpec (with config_schema)
// - name() -> &'static str
// - version() -> &'static str
```

### SinkConfig

Derive macro for sink connector configurations:

```rust
use rivven_connect::prelude::*;
use rivven_connect::SinkConfigDerive;

#[derive(Debug, Deserialize, Validate, JsonSchema, SinkConfigDerive)]
#[sink(
    name = "s3",
    version = "1.0.0",
    description = "Amazon S3 storage sink with Parquet support",
    author = "Rivven Team",
    license = "Apache-2.0",
    batching,
    batch_size = 1000
)]
pub struct S3SinkConfig {
    pub bucket: String,
    pub prefix: Option<String>,
}

// Generates: S3SinkConfigSpec with:
// - spec() -> ConnectorSpec (with config_schema)
// - name() -> &'static str
// - version() -> &'static str
// - batch_config() -> BatchConfig (when batching = true)
```

### TransformConfig

Derive macro for transform configurations:

```rust
use rivven_connect::prelude::*;
use rivven_connect::TransformConfigDerive;

#[derive(Debug, Deserialize, Validate, JsonSchema, TransformConfigDerive)]
#[transform(name = "json-filter", version = "1.0.0", description = "Filter events by JSON path")]
pub struct JsonFilterConfig {
    pub field: String,
    pub pattern: String,
}

// Generates: JsonFilterConfigSpec with spec(), name(), version() methods
```

### connector_spec (attribute macro)

Define connector specifications inline for modules:

```rust
use rivven_connect::connector_spec;

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

## Attributes Reference

### Source Attributes

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | string | struct name (lowercase, -config removed) | Connector identifier |
| `version` | string | "0.0.1" | Semantic version |
| `description` | string | - | Human-readable description |
| `author` | string | - | Author or maintainer |
| `license` | string | - | License identifier (e.g., "Apache-2.0") |
| `documentation_url` | string | - | Documentation URL |
| `incremental` | flag | false | Supports incremental sync |

### Sink Attributes

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | string | struct name (lowercase, -config removed) | Connector identifier |
| `version` | string | "0.0.1" | Semantic version |
| `description` | string | - | Human-readable description |
| `author` | string | - | Author or maintainer |
| `license` | string | - | License identifier |
| `documentation_url` | string | - | Documentation URL |
| `batching` | flag | false | Enable batch_config() method |
| `batch_size` | usize | 10,000 | Default batch size |

## Using with Source/Sink Implementations

```rust
use rivven_connect::prelude::*;
use rivven_connect::SourceConfigDerive;

#[derive(Debug, Deserialize, Validate, JsonSchema, SourceConfigDerive)]
#[source(name = "my-source", version = "1.0.0")]
pub struct MySourceConfig {
    pub endpoint: String,
}

pub struct MySource;

#[async_trait]
impl Source for MySource {
    type Config = MySourceConfig;

    fn spec() -> ConnectorSpec {
        MySourceConfigSpec::spec()  // Use generated spec
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        Ok(CheckResult::success())
    }

    async fn discover(&self, _config: &Self::Config) -> Result<Catalog> {
        Ok(Catalog::default())
    }

    async fn read(
        &self,
        _config: &Self::Config,
        _catalog: &ConfiguredCatalog,
        _state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        todo!()
    }
}
```

## License

See root [LICENSE](../../LICENSE) file.
