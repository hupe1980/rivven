# Connector Development

This guide covers building custom connectors for Rivven using the SDK and derive macros.

## Overview

Rivven connectors are Rust types that implement the `Source`, `Sink`, or `Transform` traits. The SDK provides derive macros to reduce boilerplate and ensure consistency.

## Prerequisites

```toml
[dependencies]
rivven-connect = "0.0.1"
serde = { version = "1.0", features = ["derive"] }
schemars = "0.8"
validator = { version = "0.20", features = ["derive"] }
async-trait = "0.1"
```

## Source Connectors

### Basic Structure

```rust
use rivven_connect::prelude::*;
use rivven_connect::SourceConfigDerive;

// Configuration with derive macro
#[derive(Debug, Deserialize, Validate, JsonSchema, SourceConfigDerive)]
#[source(name = "my-source", version = "1.0.0", description = "Custom data source")]
pub struct MySourceConfig {
    #[validate(url)]
    pub endpoint: String,
    
    #[validate(range(min = 1, max = 1000))]
    pub batch_size: usize,
    
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}

fn default_timeout() -> u64 { 30_000 }

// The derive macro generates: MySourceConfigSpec
// with spec(), name(), version() methods

pub struct MySource;

#[async_trait]
impl Source for MySource {
    type Config = MySourceConfig;

    fn spec() -> ConnectorSpec {
        MySourceConfigSpec::spec()
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        // Validate connectivity
        CheckResult::builder()
            .check_if("endpoint_reachable", || {
                // Test connectivity
                Ok(())
            })
            .build()
    }

    async fn discover(&self, config: &Self::Config) -> Result<Catalog> {
        // Return available streams
        Ok(Catalog::default())
    }

    async fn read(
        &self,
        config: &Self::Config,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        // Return event stream
        todo!()
    }
}
```

### Registering the Source

```rust
// In your connector crate's lib.rs
pub fn register(registry: &mut SourceRegistry) {
    registry.register::<MySource, MySourceConfig>("my-source");
}
```

## Sink Connectors

### Basic Structure

```rust
use rivven_connect::prelude::*;
use rivven_connect::SinkConfigDerive;

#[derive(Debug, Deserialize, Validate, JsonSchema, SinkConfigDerive)]
#[sink(name = "my-sink", version = "1.0.0", batching, batch_size = 1000)]
pub struct MySinkConfig {
    pub destination: String,
    
    #[serde(default)]
    pub compress: bool,
}

// The derive macro generates: MySinkConfigSpec
// with spec(), name(), version(), batch_config() methods

pub struct MySink;

#[async_trait]
impl Sink for MySink {
    type Config = MySinkConfig;

    fn spec() -> ConnectorSpec {
        MySinkConfigSpec::spec()
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        CheckResult::builder()
            .check_if("destination_writable", || {
                // Test write permissions
                Ok(())
            })
            .build()
    }

    async fn write(
        &self,
        config: &Self::Config,
        events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        use futures::StreamExt;
        
        let mut result = WriteResult::new();
        let mut events = std::pin::pin!(events);
        
        while let Some(event) = events.next().await {
            // Process event
            result.add_success(1, event.data.len() as u64);
        }
        
        Ok(result)
    }
}
```

### Batch Sink Implementation

For sinks that benefit from batching:

```rust
#[async_trait]
impl BatchSink for MySink {
    fn batch_config(&self, _config: &Self::Config) -> BatchConfig {
        MySinkConfigSpec::batch_config()
    }

    async fn write_batch(
        &self,
        config: &Self::Config,
        events: Vec<SourceEvent>,
    ) -> Result<WriteResult> {
        let mut result = WriteResult::new();
        
        // Process batch efficiently
        let total_bytes: u64 = events.iter()
            .map(|e| e.data.len() as u64)
            .sum();
        
        result.add_success(events.len() as u64, total_bytes);
        Ok(result)
    }
}
```

## Transform Connectors

### Basic Structure

```rust
use rivven_connect::prelude::*;
use rivven_connect::TransformConfigDerive;

#[derive(Debug, Deserialize, Validate, JsonSchema, TransformConfigDerive)]
#[transform(name = "filter-transform", version = "1.0.0")]
pub struct FilterConfig {
    pub field: String,
    pub pattern: String,
}

// Generates: FilterConfigSpec with spec(), name(), version()

pub struct FilterTransform;

#[async_trait]
impl Transform for FilterTransform {
    type Config = FilterConfig;

    fn spec() -> ConnectorSpec {
        FilterConfigSpec::spec()
    }

    async fn transform(
        &self,
        config: &Self::Config,
        event: SourceEvent,
    ) -> Result<TransformOutput> {
        // Apply transformation
        let matches = check_pattern(&event, &config.field, &config.pattern);
        
        if matches {
            Ok(TransformOutput::Emit(event))
        } else {
            Ok(TransformOutput::Drop)
        }
    }
}
```

## Derive Macro Attributes

### Source Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `name` | string | Connector name (default: struct name in lowercase) |
| `version` | string | Connector version (default: "0.0.1") |
| `description` | string | Human-readable description |
| `documentation_url` | string | Link to documentation |

### Sink Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `name` | string | Connector name |
| `version` | string | Connector version |
| `description` | string | Human-readable description |
| `documentation_url` | string | Link to documentation |
| `batching` | flag | Enable batch processing |
| `batch_size` | usize | Default max batch size (default: 10,000) |

### Transform Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `name` | string | Transform name |
| `version` | string | Transform version |
| `description` | string | Human-readable description |

## Configuration Validation

Use the `validator` crate for configuration validation:

```rust
#[derive(Debug, Deserialize, Validate, JsonSchema, SourceConfigDerive)]
#[source(name = "validated-source", version = "1.0.0")]
pub struct ValidatedConfig {
    #[validate(url)]
    pub endpoint: String,
    
    #[validate(email)]
    pub notification_email: Option<String>,
    
    #[validate(range(min = 1, max = 100))]
    pub concurrency: usize,
    
    #[validate(length(min = 1, max = 256))]
    pub table_name: String,
    
    #[validate(custom(function = "validate_cron"))]
    pub schedule: Option<String>,
}

fn validate_cron(cron: &str) -> Result<(), validator::ValidationError> {
    // Custom validation logic
    Ok(())
}
```

## Error Handling

Use the SDK's error types:

```rust
use rivven_connect::{ConnectorError, Result};

async fn my_operation() -> Result<()> {
    // Connectivity errors
    Err(ConnectorError::connection("Failed to connect to endpoint"))?;
    
    // Configuration errors
    Err(ConnectorError::configuration("Invalid batch size"))?;
    
    // Data errors
    Err(ConnectorError::data("Malformed JSON record"))?;
    
    Ok(())
}
```

## State Management

Implement stateful sources for incremental sync:

```rust
async fn read(
    &self,
    config: &Self::Config,
    catalog: &ConfiguredCatalog,
    state: Option<State>,
) -> Result<BoxStream<'static, Result<SourceEvent>>> {
    // Resume from previous state
    let cursor = state
        .and_then(|s| s.get_stream_state("my_stream"))
        .and_then(|ss| ss.get::<String>("cursor").ok())
        .unwrap_or_default();
    
    let stream = async_stream::stream! {
        let mut current_cursor = cursor;
        
        loop {
            let records = fetch_records(&current_cursor).await?;
            
            for record in records {
                current_cursor = record.id.clone();
                
                // Emit record with state
                yield Ok(SourceEvent::builder()
                    .stream("my_stream")
                    .data(record.data)
                    .state("cursor", &current_cursor)
                    .build());
            }
        }
    };
    
    Ok(Box::pin(stream))
}
```

## Testing

Use the SDK's test harness:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use rivven_connect::prelude::*;

    #[tokio::test]
    async fn test_source_check() {
        let source = MySource;
        let config = MySourceConfig {
            endpoint: "https://api.example.com".to_string(),
            batch_size: 100,
            timeout_ms: 30_000,
        };
        
        let result = source.check(&config).await.unwrap();
        assert!(result.is_success());
    }

    #[tokio::test]
    async fn test_sink_write() {
        let harness = TestHarness::new();
        let sink = MySink;
        let config = MySinkConfig {
            destination: "/tmp/test".to_string(),
            compress: false,
        };
        
        let events = harness.create_events(vec![
            events::record("test", json!({"id": 1})),
            events::record("test", json!({"id": 2})),
        ]);
        
        let result = sink.write(&config, events).await.unwrap();
        assert_eq!(result.records_written, 2);
    }
}
```

## Best Practices

### 1. Configuration Defaults

Provide sensible defaults:

```rust
impl Default for MySourceConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            batch_size: 100,
            timeout_ms: 30_000,
        }
    }
}
```

### 2. Comprehensive Health Checks

Validate all aspects of connectivity:

```rust
async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
    CheckResult::builder()
        .check_if("endpoint_reachable", || test_endpoint(config))
        .check_if("auth_valid", || test_auth(config))
        .check_if("permissions", || test_permissions(config))
        .build()
}
```

### 3. Graceful Error Recovery

Handle transient failures:

```rust
use rivven_connect::{retry, RetryConfig};

async fn fetch_with_retry(&self) -> Result<Data> {
    retry(
        RetryConfig::default()
            .max_retries(3)
            .initial_backoff_ms(100),
        || async { self.fetch_data().await }
    ).await
}
```

### 4. Metrics and Observability

Emit metrics for monitoring:

```rust
async fn write(
    &self,
    config: &Self::Config,
    events: BoxStream<'static, SourceEvent>,
) -> Result<WriteResult> {
    let metrics = Metrics::new();
    let timer = metrics.start_timer("write_duration");
    
    // ... write logic ...
    
    timer.stop();
    metrics.counter("records_written", result.records_written);
    
    Ok(result)
}
```

## Example: HTTP Webhook Sink

Complete example of a production-ready sink:

```rust
use rivven_connect::prelude::*;
use rivven_connect::SinkConfigDerive;
use reqwest::Client;

#[derive(Debug, Deserialize, Validate, JsonSchema, SinkConfigDerive)]
#[sink(name = "http-webhook", version = "1.0.0", batching, batch_size = 100)]
pub struct WebhookSinkConfig {
    #[validate(url)]
    pub url: String,
    
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
    
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
    
    #[serde(default)]
    pub retry_count: usize,
}

fn default_timeout() -> u64 { 10_000 }

pub struct WebhookSink {
    client: Client,
}

impl WebhookSink {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }
}

#[async_trait]
impl Sink for WebhookSink {
    type Config = WebhookSinkConfig;

    fn spec() -> ConnectorSpec {
        WebhookSinkConfigSpec::spec()
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        let response = self.client
            .head(&config.url)
            .timeout(std::time::Duration::from_millis(config.timeout_ms))
            .send()
            .await;
        
        match response {
            Ok(r) if r.status().is_success() => Ok(CheckResult::success()),
            Ok(r) => Ok(CheckResult::failure(format!("HTTP {}", r.status()))),
            Err(e) => Ok(CheckResult::failure(e.to_string())),
        }
    }

    async fn write(
        &self,
        config: &Self::Config,
        events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        use futures::StreamExt;
        
        let mut result = WriteResult::new();
        let mut events = std::pin::pin!(events);
        
        while let Some(event) = events.next().await {
            let response = self.client
                .post(&config.url)
                .json(&event.data)
                .timeout(std::time::Duration::from_millis(config.timeout_ms))
                .send()
                .await;
            
            match response {
                Ok(r) if r.status().is_success() => {
                    result.add_success(1, event.data.len() as u64);
                }
                Ok(r) => {
                    result.add_failure(1, format!("HTTP {}", r.status()));
                }
                Err(e) => {
                    result.add_failure(1, e.to_string());
                }
            }
        }
        
        Ok(result)
    }
}
```

## See Also

- [Connector Architecture](connectors.md)
- [Getting Started](getting-started.md)
- [Admin API](admin-api.md)
