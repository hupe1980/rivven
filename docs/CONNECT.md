# Rivven Connect Architecture

## Overview

Rivven Connect is a lightweight, embeddable data integration framework for building real-time streaming pipelines. It provides a unified runtime for connectors (sources, sinks, and transforms) that move data to and from Rivven topics.

**Key Principles:**
- **Minimal Core**: `rivven-connect` has zero heavy dependencies (~8MB binary)
- **Opt-In Complexity**: Add `rivven-cdc`, `rivven-storage` only when needed
- **Library-First**: Embed in your app or use the CLI binary
- **Production-Ready**: Built-in resilience, observability, and graceful shutdown

## Table of Contents

- [Quick Start](#quick-start)
- [Architecture](#architecture-diagram)
- [Design Goals](#design-goals)
- [Crate Structure](#rivven-crate-structure)
- [Core Traits](#core-traits)
- [SDK Ergonomics](#sdk-ergonomics)
  - [Builder Patterns](#builder-patterns)
  - [Convenience Macros](#convenience-macros)
  - [Transform Helpers](#transform-helpers)
  - [Retry Utilities](#retry-utilities)
  - [Typed State Access](#typed-state-access)
  - [Metrics & Observability](#metrics--observability)
  - [Testing Utilities](#testing-utilities)
  - [Circuit Breaker](#circuit-breaker)
  - [Batch Processing](#batch-processing)
- [Library Usage](#library-usage)
- [Configuration](#configuration)
- [Runtime Features](#runtime-features)
- [Offset Management](#offset-management--checkpointing)
- [Schema Registry](#schema-registry)
- [WASM Plugins](#wasm-plugins)
- [Distributed Mode](#distributed-mode)
- [Security](#security)
- [Observability](#observability)
- [CLI Commands](#cli-commands)
- [Comparison](#comparison-with-alternatives)
- [Roadmap](#roadmap)
- [Migration Guide](#migration-guide)

### Quick Start

```toml
# Pick only what you need:
rivven-connect = "0.2"                               # Minimal (datagen, stdout, file)
rivven-connect = { version = "0.2", features = ["http"] }  # + HTTP (webhook, REST)
rivven-cdc = { version = "0.2", features = ["postgres"] }  # + CDC
rivven-storage = { version = "0.2", features = ["s3"] }    # + Object storage
rivven-warehouse = { version = "0.2", features = ["snowflake"] }  # + Data warehouses
```

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Rivven Connect                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │   Sources    │    │  Transforms  │    │    Sinks     │              │
│  ├──────────────┤    ├──────────────┤    ├──────────────┤              │
│  │ • Datagen    │    │ • Filter     │    │ • Stdout     │              │
│  │ • Postgres   │    │ • Map        │    │ • File       │              │
│  │ • MySQL      │    │ • Enrich     │    │ • Postgres   │              │
│  │ • HTTP       │    │ • Aggregate  │    │ • S3         │              │
│  │ • File       │    │ • Custom     │    │ • HTTP       │              │
│  │ • Custom     │    └──────────────┘    │ • Custom     │              │
│  └──────────────┘                         └──────────────┘              │
│         │                                        ▲                      │
│         ▼                                        │                      │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      Connect Runtime                             │   │
│  ├─────────────────────────────────────────────────────────────────┤   │
│  │ • Source Runner (backpressure, batching, offset tracking)       │   │
│  │ • Sink Runner (delivery guarantees, retries, DLQ)               │   │
│  │ • Broker Client (failover, reconnection, topic management)      │   │
│  │ • Configuration (YAML, validation, schema inference)            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│         │                                        ▲                      │
│         ▼                                        │                      │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                       Rivven Cluster                             │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Design Goals

1. **Minimal Core, Extensible Plugins**: Core runtime has zero heavy dependencies; connectors are optional
2. **Library-First Design**: `rivven-connect` is both a library (embed in your app) and a CLI binary
3. **Grouped Connector Crates**: Related connectors share dependencies (CDC, Cloud, Warehouse)
4. **Best-in-Class Resilience**: Automatic failover, reconnection, and delivery guarantees
5. **Production Ready**: Built-in observability, health checks, and graceful shutdown
6. **Future: WASM Plugins**: Load custom connectors at runtime without recompilation

## Rivven Crate Structure

```
rivven/
├── crates/
│   │
│   │  ── Core Platform ─────────────────────────────────────────────────
│   │
│   ├── rivven-core/           # Core types, traits, and utilities
│   │   └── (Record, Topic, Partition, Error types, serialization)
│   │
│   ├── rivven-cluster/        # Distributed cluster coordination
│   │   └── (Raft consensus, membership, partitioning, QUIC transport)
│   │
│   ├── rivven-server/         # Broker server binary
│   │   └── (gRPC API, storage engine, replication, HTTP gateway)
│   │
│   │  ── Clients ───────────────────────────────────────────────────────
│   │
│   ├── rivven-client/         # Rust client library
│   │   └── (Producer, Consumer, Admin APIs)
│   │
│   ├── rivven-cli/            # CLI tool (rivven)
│   │   └── (topic, produce, consume, cluster commands)
│   │
│   ├── rivven-python/         # Python bindings (PyO3)
│   │   └── (pip install rivven)
│   │
│   │  ── Connect Framework ─────────────────────────────────────────────
│   │
│   ├── rivven-connect/        # Connect core (lib + bin)
│   │   ├── traits/            # Source, Sink, Transform traits (minimal deps)
│   │   ├── runtime/           # SourceRunner, SinkRunner, BrokerClient
│   │   ├── registry/          # Dynamic connector registration
│   │   └── connectors/        # Built-in connectors
│   │       ├── datagen        # Test data generator (zero deps)
│   │       ├── stdout         # Console output (zero deps)
│   │       ├── file           # File I/O (zero deps)
│   │       └── http/          # HTTP webhook/REST (feature = "http")
│   │           ├── webhook    # HTTP POST sink
│   │           └── rest       # REST API polling source
│   │
│   │  ── Connector Crates (Optional, Heavy Dependencies) ───────────────
│   │
│   ├── rivven-cdc/            # Change Data Capture connectors
│   │   ├── postgres/          # PostgreSQL logical replication
│   │   ├── mysql/             # MySQL binlog
│   │   └── (future: oracle, sqlserver, mongodb)
│   │
│   ├── rivven-storage/        # Object storage connectors
│   │   ├── s3/                # AWS S3 / MinIO / R2
│   │   ├── gcs/               # Google Cloud Storage
│   │   ├── azure-blob/        # Azure Blob Storage
│   │   └── hdfs/              # Hadoop (future)
│   │
│   ├── rivven-warehouse/      # Data warehouse connectors
│   │   ├── snowflake/         # Snowflake Snowpipe
│   │   ├── bigquery/          # Google BigQuery
│   │   └── redshift/          # AWS Redshift
│   │
│   │  ── Future Connector Crates ───────────────────────────────────────
│   │
│   ├── rivven-queue/          # Message queue connectors (future)
│   │   ├── sqs/               # AWS SQS
│   │   ├── pubsub/            # Google Pub/Sub
│   │   ├── rabbitmq/          # RabbitMQ
│   │   └── nats/              # NATS
│   │
│   ├── rivven-search/         # Search engine connectors (future)
│   │   ├── elasticsearch/     # Elasticsearch / OpenSearch
│   │   └── solr/              # Apache Solr
│   │
│   ├── rivven-nosql/          # NoSQL database connectors (future)
│   │   ├── redis/             # Redis
│   │   ├── cassandra/         # Apache Cassandra
│   │   └── dynamodb/          # AWS DynamoDB
│   │
│   ├── rivven-saas/           # SaaS API connectors (future)
│   │   ├── salesforce/        # Salesforce
│   │   ├── hubspot/           # HubSpot
│   │   └── stripe/            # Stripe
│   │
│   │  ── Operations ────────────────────────────────────────────────────
│   │
│   └── rivven-operator/       # Kubernetes operator
│       └── (CRDs, auto-scaling, rolling upgrades)
│
├── scripts/
├── docs/
└── target/
```

### Connector Category Strategy

Our naming follows a **category-based** approach that scales to 50+ connectors:

| Crate | Purpose | Connectors |
|-------|---------|------------|
| `rivven-cdc` | Change Data Capture | Postgres, MySQL, Oracle, MongoDB, SQL Server |
| `rivven-storage` | Object/Blob Storage | S3, GCS, Azure Blob, MinIO, HDFS |
| `rivven-warehouse` | Data Warehouses | Snowflake, BigQuery, Redshift, Databricks |
| `rivven-queue` | Message Queues | SQS, Pub/Sub, RabbitMQ, NATS, Pulsar |
| `rivven-search` | Search Engines | Elasticsearch, OpenSearch, Solr |
| `rivven-nosql` | NoSQL Databases | Redis, Cassandra, DynamoDB |
| `rivven-saas` | SaaS APIs | Salesforce, HubSpot, Stripe |

**Why not vendor-based naming?** (`rivven-aws`, `rivven-gcp`)
- Mixed-vendor pipelines are common (Postgres CDC → BigQuery)
- Would create awkward choices for vendor-neutral tools (Elasticsearch)
- Category names are more intuitive for users

### Why This Structure?

| Approach | Compile Time | Deps | Versioning | Maintenance |
|----------|--------------|------|------------|-------------|
| One giant crate | ❌ Slow | ❌ Conflicts | ✅ Simple | ❌ Hard |
| Crate per connector | ❌ N×M matrix | ✅ Isolated | ❌ Complex | ❌ Hard |
| **Grouped crates** ✅ | ✅ Fast | ✅ Grouped | ✅ Manageable | ✅ Clear |

**Our choice: Grouped connector crates + feature flags**

```toml
# Core Connect (fast compile, zero heavy deps)
rivven-connect = "0.2"

# Add HTTP support (webhook, REST - pulls in reqwest/hyper)
rivven-connect = { version = "0.2", features = ["http"] }

# Add CDC support (pulls in postgres/mysql drivers)
rivven-cdc = "0.2"

# Add object storage (pulls in AWS/GCP SDKs)
rivven-storage = { version = "0.2", features = ["s3"] }

# Or use the convenience meta-crate (pulls everything)
rivven-connect-full = "0.2"
```

### Building the Binary

```bash
# Minimal binary (datagen, stdout, file only) - compiles in ~30s
cargo build -p rivven-connect --release

# With HTTP (webhook, REST) - adds ~1min compile
cargo build -p rivven-connect --release --features http

# With CDC (adds ~2min compile, ~15MB binary size)
cargo build -p rivven-connect --release --features cdc

# With object storage connectors (adds ~3min compile for AWS SDK)
cargo build -p rivven-connect --release --features storage

# Full build (all connectors) - compiles in ~8min
cargo build -p rivven-connect --release --features full
```

### Dependency Isolation

Each connector group has isolated dependencies:

```
rivven-connect (core, zero heavy deps)
├── tokio, serde, tracing (shared)
└── connectors: datagen, stdout, file

rivven-connect[http] (feature flag)
├── reqwest (~15MB with TLS)
└── connectors: webhook, rest

rivven-cdc (separate crate)
├── rivven-connect (core traits)
├── tokio-postgres (~10MB)
└── mysql_async (~8MB)

rivven-storage (separate crate)
├── rivven-connect (core traits)
├── aws-sdk-s3 (~25MB)
└── google-cloud-storage (~20MB)

rivven-warehouse (separate crate)
├── rivven-connect (core traits)
├── snowflake-api (custom)
└── bigquery (via google SDK)
```

## Rivven Connect Crate Structure

```
rivven-connect/
├── Cargo.toml
├── src/
│   ├── lib.rs              # Library entry point
│   ├── main.rs             # CLI binary entry point
│   │
│   ├── traits/             # Core connector traits (minimal deps)
│   │   ├── mod.rs
│   │   ├── source.rs       # Source trait
│   │   ├── sink.rs         # Sink trait
│   │   ├── transform.rs    # Transform trait
│   │   ├── event.rs        # SourceEvent type
│   │   ├── catalog.rs      # Schema catalog
│   │   └── state.rs        # State management
│   │
│   ├── runtime/            # Connect runtime
│   │   ├── mod.rs
│   │   ├── source_runner.rs
│   │   ├── sink_runner.rs
│   │   └── transform_runner.rs
│   │
│   ├── broker/             # Broker client
│   │   ├── mod.rs
│   │   └── client.rs       # Resilient broker client
│   │
│   ├── config/             # Configuration
│   │   ├── mod.rs
│   │   ├── connect.rs      # ConnectConfig
│   │   ├── broker.rs       # BrokerConfig
│   │   └── topics.rs       # TopicSettings
│   │
│   ├── registry/           # Connector registry
│   │   ├── mod.rs
│   │   └── factory.rs      # Factory pattern
│   │
│   └── connectors/         # Lightweight built-in connectors only
│       ├── mod.rs
│       ├── datagen/        # Test data generator
│       ├── stdout/         # Console output
│       └── file/           # File source/sink
```

## Core Traits

### Source Trait

Sources produce events from external systems:

```rust
use rivven_connect::{Source, SourceEvent, Catalog, ConfiguredCatalog, CheckResult, State, ConnectorSpec, ConnectError};
use async_trait::async_trait;
use futures::stream::BoxStream;

#[async_trait]
pub trait Source: Send + Sync {
    /// Configuration type for this source (must implement Deserialize, Validate, JsonSchema)
    type Config: SourceConfig;
    
    /// Return the connector specification (name, version, capabilities)
    fn spec() -> ConnectorSpec;
    
    /// Check connectivity and prerequisites
    async fn check(&self, config: &Self::Config) -> Result<CheckResult, ConnectError>;
    
    /// Discover available streams/tables
    async fn discover(&self, config: &Self::Config) -> Result<Catalog, ConnectError>;
    
    /// Read events as a stream (called once, runs until completion)
    async fn read(
        &self,
        config: &Self::Config,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent, ConnectError>>, ConnectError>;
}
```

### Sink Trait

Sinks consume events and write to external systems:

```rust
use rivven_connect::{Sink, SourceEvent, CheckResult, WriteResult, ConnectorSpec, ConnectError};
use async_trait::async_trait;
use futures::stream::BoxStream;

#[async_trait]
pub trait Sink: Send + Sync {
    /// Configuration type for this sink
    type Config: SinkConfig;
    
    /// Return the connector specification
    fn spec() -> ConnectorSpec;
    
    /// Check connectivity and prerequisites
    async fn check(&self, config: &Self::Config) -> Result<CheckResult, ConnectError>;
    
    /// Write events from a stream
    async fn write(
        &self,
        config: &Self::Config,
        events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult, ConnectError>;
}

/// For sinks that prefer batch operations
#[async_trait]
pub trait BatchSink: Sink {
    /// Get batch configuration
    fn batch_config(&self, config: &Self::Config) -> BatchConfig;
    
    /// Write a batch of events
    async fn write_batch(
        &self,
        config: &Self::Config,
        events: Vec<SourceEvent>,
    ) -> Result<WriteResult, ConnectError>;
}
```

### Transform Trait

Transforms modify events in-flight:

```rust
use rivven_connect::{Transform, Event};
use async_trait::async_trait;

#[async_trait]
pub trait Transform: Send + Sync {
    /// Human-readable name
    fn name(&self) -> &str;
    
    /// Transform a single event (may return 0, 1, or many events)
    async fn transform(&mut self, event: Event) -> Result<Vec<Event>, TransformError>;
}
```

## SDK Ergonomics

### Builder Patterns

The SDK provides fluent builder patterns for constructing events, check results, and state:

```rust
use rivven_connect::prelude::*;
use serde_json::json;

// SourceEvent Builder
let event = SourceEvent::builder()
    .stream("users")
    .data(json!({"id": 1, "name": "Alice"}))
    .with_metadata("source", json!("postgres"))
    .build();

// CDC Event shortcuts
let insert = SourceEvent::cdc_insert("users", json!({"id": 1}));
let update = SourceEvent::cdc_update("users", json!({"id": 1}), json!({"id": 1, "name": "Updated"}));
let delete = SourceEvent::cdc_delete("users", json!({"id": 1}));

// CheckResult Builder for detailed health checks
let check_result = CheckResult::builder()
    .check_passed("connectivity")
    .check_passed("authentication")
    .check_failed("permissions", "missing SELECT on users table")
    .build();

// Conditional checks
let result = CheckResult::builder()
    .check_if("port_valid", || {
        if port > 0 && port < 65536 { Ok(()) } 
        else { Err("invalid port".to_string()) }
    })
    .build();

// State Builder for incremental sync
let state = State::builder()
    .stream_cursor("users", "updated_at", json!("2026-01-25T10:00:00Z"))
    .stream_cursor("orders", "id", json!(12345))
    .global("last_sync", json!("2026-01-25"))
    .build();
```

### Convenience Macros

Create events quickly with macros:

```rust
use rivven_connect::{record, state, log_event, prelude::*};

// Quick record event
let event = record!("users", {"id": 1, "name": "Alice"});

// Quick state event  
let state_event = state!("users", {"cursor": "abc123"});

// Quick log event
let log = log_event!(LogLevel::Info, "Sync started");
```

### Transform Helpers

Powerful field manipulation utilities:

```rust
use rivven_connect::prelude::*;
use serde_json::json;

let mut event = SourceEvent::record("test", json!({"name": "Alice", "age": 30, "secret": "xxx"}));

// Field operations
transforms::rename_field(&mut event, "name", "full_name");
transforms::remove_fields(&mut event, &["secret"]);
transforms::add_field(&mut event, "processed", json!(true));

// Nested field access
let city = transforms::get_nested_field(&event, "address.city");

// Bulk operations
transforms::keep_only_fields(&mut event, &["id", "name"]);

// Field predicates for filtering
if transforms::field_contains(&event, "email", "@example.com") {
    // Process event
}
if transforms::field_starts_with(&event, "status", "active") {
    // Process event
}

// TransformOutput composition
let output = TransformOutput::single(event)
    .map(|e| { transforms::add_field(&mut e.clone(), "tag", json!("processed")); e })
    .filter(|e| transforms::has_field(e, "id"));
```

### Retry Utilities

Built-in retry with exponential backoff:

```rust
use rivven_connect::prelude::*;
use std::time::Duration;

// Configure retry behavior
let config = RetryConfig::new()
    .with_max_retries(5)
    .with_initial_delay(Duration::from_millis(100))
    .with_max_delay(Duration::from_secs(30))
    .with_backoff_multiplier(2.0)
    .with_jitter(0.1);

// Execute with automatic retry
let result = retry(&config, || async {
    make_http_request().await
}).await;

if result.is_success() {
    println!("Success after {} attempts", result.attempts);
}

// Or use the simpler wrapper
let value = retry_result(&config, || async {
    fetch_data().await
}).await?;

// Manual retry control
let mut guard = RetryGuard::new(config);
while guard.should_retry() {
    match try_operation().await {
        Ok(v) => break,
        Err(e) if e.is_retryable() => {
            if let Some(delay) = guard.record_attempt() {
                tokio::time::sleep(delay).await;
            }
        }
        Err(e) => return Err(e),
    }
}
```

### Typed State Access

State management with typed accessors:

```rust
use rivven_connect::prelude::*;
use serde_json::json;

let mut state = State::new();

// Update cursor
state.update_cursor("users", "id", json!(1000));

// Get stream state with typed access
if let Some(stream) = state.get_stream("users") {
    let cursor_id: Option<i64> = stream.cursor_as_i64();
    let cursor_str: Option<&str> = stream.cursor_as_str();
}

// Get or create stream state
let stream = state.get_or_create_stream("new_stream");
stream.set_cursor("offset", json!(0));
stream.set_data("partition", json!(3));

// Typed global access
state.set_global("retry_count", json!(5));
let count: Option<i64> = state.get_global_as("retry_count");
```

### Metrics & Observability

Track connector performance with the built-in metrics system:

```rust
use rivven_connect::prelude::*;
use std::time::Duration;

// Create a metrics collector
let metrics = MetricsCollector::new();

// Counters
metrics.increment("events_processed");
metrics.increment_by("bytes_written", 1024);

// Gauges (for current values)
metrics.gauge("queue_depth", 42);

// Histograms with automatic percentile calculation
metrics.histogram("event_latency_ms", 15.5);
metrics.histogram("event_latency_ms", 23.2);
metrics.histogram("event_latency_ms", 8.1);

// Time operations automatically
{
    let _timer = metrics.start_timer("process_batch");
    // ... do work ...
    // Timer records duration when dropped
}

// Labeled metrics for dimensions
let labels = Labels::new()
    .add("connector", "postgres-cdc")
    .add("stream", "users");
metrics.increment_with_labels("events", &labels);

// Get snapshot for export
let snapshot = metrics.snapshot();
for (name, value) in &snapshot.counters {
    println!("{}: {}", name, value);
}
for (name, hist) in &snapshot.histograms {
    println!("{}: p50={}, p95={}, p99={}", 
        name, hist.p50(), hist.p95(), hist.p99());
}

// Implement Metrics trait for custom collectors
struct PrometheusMetrics { /* ... */ }

impl Metrics for PrometheusMetrics {
    fn increment(&self, name: &str) { /* push to prometheus */ }
    fn histogram(&self, name: &str, value: f64) { /* ... */ }
    // ...
}
```

### Testing Utilities

Comprehensive testing support with mocks and helpers:

```rust
use rivven_connect::prelude::*;
use serde_json::json;

// Mock source for testing sinks
let source = MockSource::new()
    .with_events(vec![
        SourceEvent::record("users", json!({"id": 1, "name": "Alice"})),
        SourceEvent::record("users", json!({"id": 2, "name": "Bob"})),
    ]);

// Mock sink for testing sources
let sink = MockSink::new();

// Mock with deliberate failures
let failing_source = MockSource::new()
    .fail_with("connection refused");

let failing_sink = MockSink::new()
    .fail_after(100);  // Fail after 100 events

// Test harness for integration tests
let harness = TestHarness::new(source, sink);
let result = harness.run_pipeline().await?;

assert!(result.success);
assert_eq!(result.events_written, 2);
assert!(result.duration < Duration::from_secs(1));

// Verify sink received correct events
let written = harness.sink().written_events();
assert_eq!(written.len(), 2);

// Quick event generation
use rivven_connect::traits::testing::events;

// Generate numbered events
let events = events::with_ids("orders", 0..100);

// Generate events with custom data
let events = events::with_generator("users", 10, |i| {
    json!({"id": i, "name": format!("User {}", i)})
});

// Generate CDC events
let inserts = events::cdc_inserts("products", 50);

// Assertions for common patterns
use rivven_connect::traits::testing::assertions;

assert!(assertions::all_records(&events));
assert!(assertions::all_have_field(&events, "id"));
assert!(assertions::ordered_by::<i64>(&events, "id"));
```

### Circuit Breaker

Protect your connectors from cascading failures:

```rust
use rivven_connect::prelude::*;
use std::time::Duration;

// Configure circuit breaker
let config = CircuitBreakerConfig {
    failure_threshold: 5,           // Open after 5 failures
    success_threshold: 3,           // Close after 3 successes in half-open
    open_duration: Duration::from_secs(30),  // Stay open for 30s
};

let circuit = CircuitBreaker::new(config);

// Wrap operations with circuit breaker
match circuit.call(|| async {
    make_database_query().await
}).await {
    Ok(result) => process(result),
    Err(CircuitBreakerError::Open) => {
        // Circuit is open, skip operation
        log::warn!("Circuit open, using cached data");
        use_fallback();
    }
    Err(CircuitBreakerError::OperationFailed(e)) => {
        // Operation failed, circuit may have opened
        log::error!("Operation failed: {}", e);
    }
}

// Check circuit state
match circuit.state() {
    CircuitState::Closed => println!("Healthy"),
    CircuitState::Open => println!("Failing, rejecting calls"),
    CircuitState::HalfOpen => println!("Testing recovery"),
}

// Manual control
circuit.trip();   // Force open
circuit.reset();  // Force closed

// Share across tasks with SharedCircuitBreaker
let shared = SharedCircuitBreaker::new(config);
let circuit_clone = shared.clone();

tokio::spawn(async move {
    circuit_clone.call(|| async { task_work().await }).await
});

// Get statistics
let stats = circuit.stats();
println!("Failures: {}, Successes: {}", stats.failures, stats.successes);
```

### Batch Processing

Efficient batching for high-throughput sinks:

```rust
use rivven_connect::prelude::*;
use std::time::Duration;

// Configure batcher
let config = BatcherConfig {
    max_size: 1000,                          // Max events per batch
    max_bytes: 10 * 1024 * 1024,             // Max 10MB per batch  
    max_wait: Duration::from_secs(5),        // Flush every 5s
    ..Default::default()
};

let mut batcher = Batcher::new(config);

// Add events one at a time
for event in incoming_events {
    batcher.add(event);
    
    // Check if batch is ready
    if batcher.is_ready() {
        let batch = batcher.flush();
        write_batch(&batch.events).await?;
    }
}

// Force flush remaining events
if !batcher.is_empty() {
    let batch = batcher.flush();
    write_batch(&batch.events).await?;
}

// Async batcher with timeout support
let mut async_batcher = AsyncBatcher::new(config);

loop {
    tokio::select! {
        Some(event) = event_stream.next() => {
            async_batcher.add(event);
            if async_batcher.is_ready() {
                let batch = async_batcher.flush();
                write_batch(&batch.events).await?;
            }
        }
        _ = async_batcher.wait_for_timeout() => {
            if !async_batcher.is_empty() {
                let batch = async_batcher.flush();
                write_batch(&batch.events).await?;
            }
        }
    }
}

// Utility functions for batch processing
use rivven_connect::traits::batch::{chunk_events, partition_events};

// Split into fixed-size chunks
let chunks = chunk_events(events, 100);
for chunk in chunks {
    process_batch(&chunk).await?;
}

// Partition by key
let partitions = partition_events(events, |e| e.stream.clone());
for (stream, events) in partitions {
    println!("Stream {}: {} events", stream, events.len());
}
```

## Library Usage

### Embedding in Your Application

```rust
use rivven_connect::{
    ConnectConfig, SourceRunner, SinkRunner, BrokerClient,
    SourceRegistry, SinkRegistry,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let config = ConnectConfig::from_file("config.yaml")?;
    
    // Create broker client with automatic failover
    let broker = BrokerClient::connect(&config.broker).await?;
    
    // Get built-in connectors (or create your own registries)
    let sources = SourceRegistry::builtin();
    let sinks = SinkRegistry::builtin();
    
    // Run sources and sinks
    let handles = rivven_connect::start(&config, &sources, &sinks, broker).await?;
    
    // Wait for completion or signal
    handles.join().await?;
    
    Ok(())
}
```

### Building a Custom Source

```rust
use rivven_connect::{Source, SourceEvent, Catalog, ConfiguredCatalog, CheckResult, State, ConnectorSpec};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use schemars::JsonSchema;
use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Deserialize, Validate, JsonSchema)]
pub struct MySourceConfig {
    #[validate(url)]
    pub endpoint: String,
    pub api_key: String,
    #[serde(default = "default_poll_interval")]
    pub poll_interval_ms: u64,
}

fn default_poll_interval() -> u64 { 1000 }

pub struct MySource;

#[async_trait]
impl Source for MySource {
    type Config = MySourceConfig;
    
    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("my-source", "1.0.0")
            .description("Reads events from My API")
            .build()
    }
    
    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        let client = reqwest::Client::new();
        match client.head(&config.endpoint).send().await {
            Ok(resp) if resp.status().is_success() => Ok(CheckResult::success()),
            Ok(resp) => Ok(CheckResult::failure(format!("HTTP {}", resp.status()))),
            Err(e) => Ok(CheckResult::failure(e.to_string())),
        }
    }
    
    async fn discover(&self, _config: &Self::Config) -> Result<Catalog> {
        Ok(Catalog::default())
    }
    
    async fn read(
        &self,
        config: &Self::Config,
        _catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        let client = reqwest::Client::new();
        let endpoint = config.endpoint.clone();
        let api_key = config.api_key.clone();
        let poll_interval = config.poll_interval_ms;
        let mut offset = state.and_then(|s| s.get::<u64>("offset")).unwrap_or(0);
        
        let stream = async_stream::stream! {
            loop {
                let response = client
                    .get(&format!("{}?offset={}", endpoint, offset))
                    .header("Authorization", format!("Bearer {}", api_key))
                    .send()
                    .await;
                
                match response {
                    Ok(resp) => {
                        if let Ok(data) = resp.json::<Vec<serde_json::Value>>().await {
                            for (i, value) in data.into_iter().enumerate() {
                                yield Ok(SourceEvent::builder()
                                    .stream("events")
                                    .data(value)
                                    .build());
                                offset += 1;
                            }
                        }
                    }
                    Err(e) => {
                        yield Err(e.into());
                    }
                }
                
                tokio::time::sleep(std::time::Duration::from_millis(poll_interval)).await;
            }
        };
        
        Ok(Box::pin(stream))
    }
}
```

### Building a Custom Sink

```rust
use rivven_connect::{BatchSink, Sink, SourceEvent, CheckResult, WriteResult, BatchConfig, ConnectorSpec};
use async_trait::async_trait;
use futures::stream::BoxStream;
use schemars::JsonSchema;
use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Deserialize, Validate, JsonSchema)]
pub struct MySinkConfig {
    #[validate(url)]
    pub endpoint: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize { 1000 }

pub struct MySink;

#[async_trait]
impl Sink for MySink {
    type Config = MySinkConfig;
    
    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("my-sink", "1.0.0")
            .description("Writes events to My API")
            .build()
    }
    
    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        let client = reqwest::Client::new();
        match client.head(&config.endpoint).send().await {
            Ok(_) => Ok(CheckResult::success()),
            Err(e) => Ok(CheckResult::failure(e.to_string())),
        }
    }
    
    async fn write(
        &self,
        config: &Self::Config,
        events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        use futures::StreamExt;
        
        let client = reqwest::Client::new();
        let mut result = WriteResult::new();
        let mut buffer = Vec::with_capacity(config.batch_size);
        
        futures::pin_mut!(events);
        
        while let Some(event) = events.next().await {
            buffer.push(event);
            
            if buffer.len() >= config.batch_size {
                match client.post(&config.endpoint).json(&buffer).send().await {
                    Ok(_) => result.add_success(buffer.len() as u64, 0),
                    Err(e) => result.add_failure(buffer.len() as u64, e.to_string()),
                }
                buffer.clear();
            }
        }
        
        // Flush remaining
        if !buffer.is_empty() {
            match client.post(&config.endpoint).json(&buffer).send().await {
                Ok(_) => result.add_success(buffer.len() as u64, 0),
                Err(e) => result.add_failure(buffer.len() as u64, e.to_string()),
            }
        }
        
        Ok(result)
    }
}
```

### Creating a Custom Binary

Build your own `rivven-connect` binary with custom connectors:

```rust
use rivven_connect::{
    SourceRegistry, SinkRegistry, SourceFactory, SinkFactory,
    AnySource, AnySink, ConnectorSpec,
};
use std::sync::Arc;

mod my_source;
mod my_sink;

use my_source::{MySource, MySourceConfig};
use my_sink::{MySink, MySinkConfig};

// Factory for MySource
pub struct MySourceFactory;

impl SourceFactory for MySourceFactory {
    fn spec(&self) -> ConnectorSpec {
        MySource::spec()
    }
    
    fn create(&self) -> Box<dyn AnySource> {
        Box::new(TypedSourceAdapter::<MySource, MySourceConfig>::new())
    }
}

// Factory for MySink
pub struct MySinkFactory;

impl SinkFactory for MySinkFactory {
    fn spec(&self) -> ConnectorSpec {
        MySink::spec()
    }
    
    fn create(&self) -> Box<dyn AnySink> {
        Box::new(TypedSinkAdapter::<MySink, MySinkConfig>::new())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create registries
    let mut sources = SourceRegistry::new();
    sources.register("my-source", Arc::new(MySourceFactory));
    
    let mut sinks = SinkRegistry::new();
    sinks.register("my-sink", Arc::new(MySinkFactory));
    
    // Run Connect with custom registries
    rivven_connect::run(sources, sinks).await
}
```

## Configuration

### Full Configuration Reference

```yaml
# rivven-connect configuration

# Broker connection settings
broker:
  # Multiple brokers for failover (required)
  bootstrap_servers:
    - "broker1.example.com:9092"
    - "broker2.example.com:9092"
    - "broker3.example.com:9092"
  
  # Connection timeouts
  connection_timeout_ms: 10000    # Per-server connection timeout
  request_timeout_ms: 30000       # Request timeout
  metadata_refresh_ms: 300000     # Metadata cache refresh interval
  
  # TLS configuration (optional)
  tls:
    enabled: true
    ca_cert: "/path/to/ca.crt"
    client_cert: "/path/to/client.crt"
    client_key: "/path/to/client.key"

# Topic settings
topics:
  auto_create: true               # Auto-create topics if missing
  default_partitions: 3           # Default partition count
  default_replication_factor: 1   # Default replication factor

# Sources configuration
sources:
  - name: "user-events"
    type: "datagen"
    topic: "users"
    topic_config:                  # Per-source topic overrides
      partitions: 6
      replication_factor: 3
    config:
      format: "json"
      schema:
        type: "object"
        properties:
          id: { type: "integer", minimum: 1, maximum: 1000000 }
          name: { type: "string", generator: "name" }
          email: { type: "string", generator: "email" }
      events_per_second: 100
      max_events: 10000

# Sinks configuration
sinks:
  - name: "console-output"
    type: "stdout"
    topic: "users"
    config:
      format: "pretty"
  
  # HTTP webhook sink (requires "http" feature)
  - name: "slack-alerts"
    type: "webhook"
    topic: "alerts"
    config:
      url: "https://hooks.slack.com/services/XXX/YYY/ZZZ"
      method: "POST"
      headers:
        Content-Type: "application/json"
      body_template: |
        {"text": "Alert: {{ .message }}"}
      retry:
        max_attempts: 3
        backoff_ms: 1000

# Transforms configuration (optional)
transforms:
  - name: "filter-adults"
    type: "filter"
    config:
      condition: "age >= 18"
```

## Runtime Features

### Automatic Failover

The broker client automatically tries multiple bootstrap servers:

```
Connecting to Rivven cluster...
  Trying broker1.example.com:9092... timeout after 10s
  Trying broker2.example.com:9092... timeout after 10s
  Trying broker3.example.com:9092... connected!
```

### Auto-Create Topics

Topics are automatically created with configurable settings:

```
Topic 'users' does not exist, creating with 6 partitions...
Topic 'users' created successfully
```

### Backpressure Handling

Sources respect backpressure from the broker:

- Adaptive batching based on throughput
- Configurable batch size limits
- Automatic slowdown when broker is overloaded

### Exactly-Once Delivery (Sinks)

Sinks support configurable delivery guarantees:

```yaml
sinks:
  - name: "important-data"
    type: "postgres"
    config:
      delivery: "exactly_once"  # Options: at_most_once, at_least_once, exactly_once
      idempotency_key: "event_id"
```

### Dead Letter Queue (DLQ)

Failed events are routed to a DLQ for later processing:

```yaml
sinks:
  - name: "api-sink"
    type: "http"
    config:
      dlq_topic: "api-sink-dlq"
      max_retries: 3
      retry_backoff_ms: 1000
```

### Health Checks

Built-in health check endpoint:

```bash
curl http://localhost:8083/health
```

```json
{
  "status": "healthy",
  "sources": [
    { "name": "user-events", "status": "running", "events_produced": 15234 }
  ],
  "sinks": [
    { "name": "console-output", "status": "running", "events_consumed": 15200 }
  ]
}
```

### Graceful Shutdown

Connect handles shutdown signals (SIGINT, SIGTERM) gracefully:

1. **Stop accepting new events** from sources
2. **Drain in-flight batches** (configurable timeout)
3. **Commit final offsets** to broker
4. **Close connections** cleanly

```yaml
# Global shutdown configuration
shutdown:
  timeout_ms: 30000              # Max time to drain (default: 30s)
  commit_offsets: true           # Commit offsets before exit (default: true)
```

```bash
# Graceful shutdown via signal
kill -SIGTERM $(pgrep rivven-connect)

# Or via CLI (if running in background)
rivven-connect stop
```

## Offset Management & Checkpointing

### Source Offsets

Sources track their position to enable:
- **Resume after restart**: Continue from last committed offset
- **Exactly-once processing**: Combine with idempotent sinks
- **Parallel processing**: Per-partition offset tracking

```yaml
sources:
  - name: "postgres-cdc"
    type: "postgres"
    config:
      # Offset storage options
      offset_storage: "broker"      # Store in __connect_offsets topic (default)
      # offset_storage: "file"      # Store in local file
      # offset_storage: "custom"    # Implement your own
      
      checkpoint_interval_ms: 5000  # Commit offsets every 5s
      checkpoint_on_batch: true     # Also commit after each batch
```

### Sink Offsets (Consumer Groups)

Sinks use consumer groups for offset management:

```yaml
sinks:
  - name: "s3-export"
    type: "s3"
    topic: "events"
    config:
      consumer_group: "s3-export-group"
      auto_offset_reset: "earliest"  # earliest | latest | none
      enable_auto_commit: false      # Manual commit after successful write
```

## Schema Registry

Rivven Connect includes a built-in schema registry with two deployment modes:

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Schema Registry                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────────┐         ┌──────────────────────┐             │
│  │   Embedded Mode      │         │   External Mode      │             │
│  │   (_schemas topic)   │         │   (Confluent API)    │             │
│  └──────────────────────┘         └──────────────────────┘             │
│            │                                │                           │
│            └────────────┬───────────────────┘                           │
│                         ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                   Unified SchemaRegistry Client                  │   │
│  │  • register(subject, schema) → SchemaId                         │   │
│  │  • get_by_id(id) → Schema                                       │   │
│  │  • get_latest(subject) → Schema                                 │   │
│  │  • check_compatibility(subject, schema) → bool                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│            │                                                            │
│            ▼                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                  Compatibility Checker                           │   │
│  │  • Backward: new schema can read old data                       │   │
│  │  • Forward: old schema can read new data                        │   │
│  │  • Full: both directions compatible                             │   │
│  │  • None: no compatibility checking                              │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Embedded Mode (Default)

Schemas are stored in Rivven topics (`_schemas`), requiring no external infrastructure:

```yaml
schema_registry:
  mode: embedded
  topic: _schemas              # Topic for schema storage
  compatibility: backward      # Default compatibility level
  cache_size: 1000            # LRU cache size
```

**Benefits:**
- Zero additional infrastructure
- Schemas replicated with your data
- Automatic failover with Rivven cluster

### External Mode (Confluent-Compatible)

Connect to external schema registries (Confluent, Apicurio, etc.):

```yaml
schema_registry:
  mode: external
  url: "http://schema-registry:8081"
  auth:
    type: basic
    username: "${SCHEMA_REGISTRY_USER}"
    password: "${SCHEMA_REGISTRY_PASSWORD}"
  timeout_ms: 5000
  cache_size: 1000
```

### Schema Types

| Format | Status | Description |
|--------|--------|-------------|
| JSON Schema | ✅ Supported | Native JSON Schema draft-07 |
| Avro | 🔄 Planned | Apache Avro schemas |
| Protobuf | 🔄 Planned | Protocol Buffers schemas |

### Schema Inference

Automatically infer schemas from data samples:

```rust
use rivven_connect::schema::{SchemaInference, InferredSchema};

let samples = vec![
    json!({"id": 1, "name": "Alice", "active": true}),
    json!({"id": 2, "name": "Bob", "age": 30}),
];

let inference = SchemaInference::new();
let schema = inference.infer_from_samples(&samples)?;

// Result: JSON Schema with merged properties
// { "type": "object", "properties": { "id": {"type": "integer"}, ... }}
```

### Compatibility Checking

```rust
use rivven_connect::schema::{CompatibilityChecker, CompatibilityLevel};

let checker = CompatibilityChecker::new(CompatibilityLevel::Backward);

// Old schema: {"type": "object", "properties": {"id": {"type": "integer"}}}
// New schema: adds optional "name" field
let result = checker.check(&old_schema, &new_schema)?;

assert!(result.is_compatible);  // ✅ Backward compatible
```

### Schema Evolution

| Strategy | Add Field | Remove Field | Change Type |
|----------|-----------|--------------|-------------|
| `backward` | ✅ (optional only) | ✅ | ❌ |
| `forward` | ✅ | ✅ (optional only) | ❌ |
| `full` | ✅ (optional only) | ✅ (optional only) | ❌ |
| `none` | ✅ | ✅ | ✅ |

### Configuration Reference

```yaml
sources:
  - name: "postgres-cdc"
    type: "postgres"
    config:
      schema:
        subject_strategy: "topic_name"  # topic_name | record_name | topic_record_name
        compatibility: "backward"       # backward | forward | full | none
        auto_register: true             # Auto-register inferred schemas
        validate: true                  # Validate events against schema
```

## WASM Plugins

Load custom connectors at runtime via WebAssembly without recompiling Rivven Connect.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        WASM Plugin Runtime                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────────┐    ┌──────────────────────┐                  │
│  │    Plugin Loader     │    │    Plugin Store      │                  │
│  │  • Load .wasm files  │    │  • Manifest cache    │                  │
│  │  • Validate ABI      │    │  • Version tracking  │                  │
│  │  • Parse exports     │    │  • Hot reload        │                  │
│  └──────────────────────┘    └──────────────────────┘                  │
│            │                           │                                │
│            └───────────┬───────────────┘                                │
│                        ▼                                                │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                     Sandbox (per-plugin)                         │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │   │
│  │  │ Memory Limit│  │ CPU Limit   │  │ Capabilities│              │   │
│  │  │ (64MB-1GB)  │  │ (fuel-based)│  │ (net, fs)   │              │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘              │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                        │                                                │
│                        ▼                                                │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                     Host Functions                               │   │
│  │  • rivven_log(level, msg)        - Logging                      │   │
│  │  • rivven_get_config(key)        - Config access                │   │
│  │  • rivven_get_state(key)         - State management             │   │
│  │  • rivven_set_state(key, val)    - State persistence            │   │
│  │  • rivven_http_request(req)      - HTTP calls (if allowed)      │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Plugin Types

| Type | Purpose | Required Exports |
|------|---------|------------------|
| Source | Produce events | `rivven_poll` |
| Sink | Consume events | `rivven_write` |
| Transform | Modify events | `rivven_transform` |

### Configuration

```yaml
plugins:
  # Load a custom source plugin
  my-api-source:
    path: /plugins/my-api-source.wasm
    type: source
    config:
      api_key: "${MY_API_KEY}"
      endpoint: "https://api.example.com/events"
    
    # Sandbox configuration
    sandbox:
      memory_limit_mb: 128
      execution_timeout_ms: 30000
      capabilities:
        network: true       # Allow HTTP calls
        filesystem: false   # No filesystem access
        environment: false  # No env var access

sources:
  - name: "custom-events"
    type: "plugin:my-api-source"   # Reference plugin
    topic: "custom-events"
```

### Plugin ABI (v1.0)

Plugins must export these functions:

```rust
// Required: Return plugin manifest as JSON
#[no_mangle]
pub extern "C" fn rivven_get_manifest() -> i64;

// Required: Initialize plugin with config
#[no_mangle]
pub extern "C" fn rivven_init(config_ptr: u32, config_len: u32) -> i32;

// Required: Health check
#[no_mangle]
pub extern "C" fn rivven_check() -> i32;

// Source plugins: Poll for records
#[no_mangle]
pub extern "C" fn rivven_poll(buf_ptr: u32, buf_len: u32) -> i64;

// Sink plugins: Write records
#[no_mangle]
pub extern "C" fn rivven_write(records_ptr: u32, records_len: u32) -> i32;

// Transform plugins: Transform a record
#[no_mangle]
pub extern "C" fn rivven_transform(record_ptr: u32, record_len: u32, out_ptr: u32, out_len: u32) -> i64;
```

### Host Functions Available to Plugins

```rust
// Logging
extern "C" fn rivven_log(level: i32, msg_ptr: u32, msg_len: u32);

// Configuration
extern "C" fn rivven_get_config(key_ptr: u32, key_len: u32, out_ptr: u32, out_len: u32) -> i64;

// State management (persisted across restarts)
extern "C" fn rivven_get_state(key_ptr: u32, key_len: u32, out_ptr: u32, out_len: u32) -> i64;
extern "C" fn rivven_set_state(key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32) -> i32;

// HTTP requests (if capability granted)
extern "C" fn rivven_http_request(req_ptr: u32, req_len: u32, resp_ptr: u32, resp_len: u32) -> i64;
```

### Building Plugins

```bash
# Build a Rust plugin targeting WASM
cargo build --target wasm32-wasi --release

# Plugin manifest (embedded in WASM or sidecar JSON)
{
  "name": "my-api-source",
  "version": "1.0.0",
  "abi_version": "1.0",
  "type": "source",
  "description": "Fetch events from My API",
  "config_schema": { ... }
}
```

## Distributed Mode

Run Rivven Connect across multiple nodes for scalability and high availability.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     Distributed Connect Cluster                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │                    Coordinator (Leader)                         │     │
│  │  • Membership management (nodes join/leave)                    │     │
│  │  • Task assignment (which node runs what)                      │     │
│  │  • Rebalancing on topology changes                             │     │
│  │  • Singleton enforcement for CDC connectors                    │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                              │                                           │
│              ┌───────────────┼───────────────┐                          │
│              ▼               ▼               ▼                          │
│  ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐        │
│  │   Worker Node 1  │ │   Worker Node 2  │ │   Worker Node 3  │        │
│  │  ┌────────────┐  │ │  ┌────────────┐  │ │  ┌────────────┐  │        │
│  │  │ CDC Source │  │ │  │ HTTP Sink  │  │ │  │ HTTP Sink  │  │        │
│  │  │ (Leader)   │  │ │  │ (Task 1)   │  │ │  │ (Task 2)   │  │        │
│  │  └────────────┘  │ │  └────────────┘  │ │  └────────────┘  │        │
│  │  ┌────────────┐  │ │  ┌────────────┐  │ │  ┌────────────┐  │        │
│  │  │ CDC Source │  │ │  │ S3 Sink    │  │ │  │ S3 Sink    │  │        │
│  │  │ (Standby)  │  │ │  │ (Task 1)   │  │ │  │ (Task 2)   │  │        │
│  │  └────────────┘  │ │  └────────────┘  │ │  └────────────┘  │        │
│  └──────────────────┘ └──────────────────┘ └──────────────────┘        │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Connector Modes

| Mode | Description | Use Case | Scaling |
|------|-------------|----------|----------|
| **Singleton** | Only one active instance | CDC connectors | Failover only |
| **Scalable** | Multiple instances, load balanced | HTTP sinks, file sources | Horizontal |
| **Partitioned** | One instance per partition | Topic consumers | Partition-based |

### Singleton Enforcement (CDC)

CDC connectors **cannot run in parallel** because database replication slots allow only one active consumer. Rivven Connect enforces this automatically:

```yaml
sources:
  - name: "postgres-cdc"
    type: "postgres"
    mode: singleton              # Automatic for CDC, explicit for others
    failover:
      enabled: true
      heartbeat_interval_ms: 1000
      failure_timeout_ms: 10000
      min_failover_interval_ms: 30000
```

**How it works:**

1. **Leader Election**: When a CDC connector starts, the coordinator assigns it to exactly one node (the leader)
2. **Standby Nodes**: Other nodes with the connector configured become standbys
3. **Heartbeat Monitoring**: Leader sends heartbeats; coordinator monitors health
4. **Automatic Failover**: If leader fails (no heartbeat for `failure_timeout_ms`), coordinator promotes the first standby

```
Normal Operation:
  Node-1 (Leader)  ◄── CDC running, heartbeats every 1s
  Node-2 (Standby) ◄── Ready to take over
  Node-3 (Standby) ◄── Ready to take over

Failover Scenario:
  Node-1 (Leader)  ──✗ Dies
  Node-2 (Standby) ──► Promoted to Leader (within 10s)
  Node-3 (Standby) ◄── Remains standby
```

### Scalable Connectors

Connectors that can run in parallel get automatic load balancing:

```yaml
sinks:
  - name: "http-webhook"
    type: "http"
    mode: scalable
    tasks_max: 4                 # Run up to 4 parallel tasks
    config:
      url: "https://api.example.com/webhook"
```

The coordinator distributes tasks across nodes using configurable strategies:

| Strategy | Description |
|----------|-------------|
| `round_robin` | Equal distribution |
| `least_loaded` | Prefer nodes with fewer tasks (default) |
| `rack_aware` | Distribute across availability zones |
| `sticky` | Keep tasks on same node when possible |

### Configuration

```yaml
distributed:
  enabled: true
  node_id: "${HOSTNAME}"         # Unique node identifier
  address: "0.0.0.0:9093"        # Address for node-to-node communication
  
  # Coordination settings
  coordination:
    cluster_topic: _connect_status   # Topic for coordination messages
    heartbeat_interval_ms: 1000
    session_timeout_ms: 30000
    rebalance_delay_ms: 3000        # Wait before rebalancing
  
  # Task assignment
  assignment:
    strategy: least_loaded          # round_robin | least_loaded | sticky | rack_aware
    max_tasks_per_node: 100
  
  # Failover settings (for singleton connectors)
  failover:
    heartbeat_interval_ms: 1000
    failure_timeout_ms: 10000
    min_failover_interval_ms: 30000
```

### Protocol Messages

Nodes communicate via the `_connect_status` topic:

| Message | Purpose |
|---------|----------|
| `JoinRequest` | Node joining cluster |
| `Heartbeat` | Health check + task status |
| `TaskAssignment` | Coordinator assigns task to node |
| `LeaderElection` | Singleton leader changes |
| `RebalanceTrigger` | Initiate rebalance |

### Observability

```yaml
# Prometheus metrics for distributed mode
rivven_connect_cluster_nodes_total 3
rivven_connect_cluster_tasks_total 12
rivven_connect_cluster_rebalances_total 2
rivven_connect_singleton_leader{connector="postgres-cdc"} "node-1"
rivven_connect_singleton_failovers_total{connector="postgres-cdc"} 1
```

## Security

### Authentication

```yaml
broker:
  bootstrap_servers:
    - "broker1.example.com:9092"
  
  # SASL authentication
  auth:
    mechanism: "SCRAM-SHA-256"      # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
    username: "${RIVVEN_USERNAME}"
    password: "${RIVVEN_PASSWORD}"
```

### TLS/mTLS

```yaml
broker:
  tls:
    enabled: true
    ca_cert: "/etc/rivven/ca.crt"
    # For mutual TLS (mTLS)
    client_cert: "/etc/rivven/client.crt"
    client_key: "/etc/rivven/client.key"
    # Optional: skip verification (dev only)
    # insecure_skip_verify: true
```

### Secrets Management

Connect supports multiple secret backends:

```yaml
# Environment variables (default)
broker:
  auth:
    password: "${RIVVEN_PASSWORD}"

# File-based secrets
broker:
  auth:
    password: "file:///run/secrets/rivven-password"

# HashiCorp Vault (planned)
# broker:
#   auth:
#     password: "vault://secret/data/rivven#password"
```

### Connector-Specific Credentials

```yaml
sinks:
  - name: "s3-sink"
    type: "s3"
    config:
      # AWS credential chain (recommended)
      credentials: "default"        # Uses AWS_* env vars, IAM roles, etc.
      
      # Or explicit (not recommended for production)
      # access_key_id: "${AWS_ACCESS_KEY_ID}"
      # secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
```

## Observability

### Metrics

Connect exposes Prometheus-compatible metrics:

```bash
curl http://localhost:8083/metrics
```

```
# Source metrics
rivven_connect_source_events_total{source="user-events"} 15234
rivven_connect_source_bytes_total{source="user-events"} 1523400
rivven_connect_source_lag_seconds{source="user-events"} 0.5

# Sink metrics
rivven_connect_sink_events_total{sink="s3-export"} 15200
rivven_connect_sink_errors_total{sink="s3-export"} 3
rivven_connect_sink_latency_seconds{sink="s3-export",quantile="0.99"} 0.25

# Runtime metrics
rivven_connect_broker_reconnects_total 2
rivven_connect_uptime_seconds 3600
```

### Structured Logging

```yaml
logging:
  level: "info"                     # trace | debug | info | warn | error
  format: "json"                    # json | pretty
  
  # Per-component levels
  components:
    source_runner: "debug"
    broker_client: "warn"
```

### Distributed Tracing (OpenTelemetry)

```yaml
tracing:
  enabled: true
  exporter: "otlp"
  endpoint: "http://jaeger:4317"
  service_name: "rivven-connect"
  sample_rate: 0.1                  # Sample 10% of traces
```

## CLI Commands

```bash
# Validate configuration
rivven-connect validate -c config.yaml

# Run connectors
rivven-connect run -c config.yaml

# List available connector types
rivven-connect connectors list

# Check connector status (when running)
rivven-connect status

# Generate sample configuration
rivven-connect init > config.yaml

# Show connector schema (for config validation)
rivven-connect schema postgres

# Dry-run mode (validate + check connectivity)
rivven-connect run -c config.yaml --dry-run
```

## Why Grouped Connector Crates?

We evaluated three approaches:

### ❌ Approach 1: Everything in One Crate

```toml
rivven-connect = { features = ["postgres", "mysql", "s3", "snowflake", ...] }
```

**Problems:**
- 50+ feature flags become unmanageable
- AWS SDK + GCP SDK + Azure SDK = dependency hell
- CI needs every external service running
- One bug in any connector blocks releases
- Compile time: **15+ minutes** for full build

### ❌ Approach 2: One Crate Per Connector

```toml
rivven-connect = "0.2"
rivven-connector-postgres = "0.2"
rivven-connector-mysql = "0.1.8"  # Version mismatch!
rivven-connector-s3 = "0.3"
```

**Problems:**
- N connectors × M versions = compatibility matrix hell
- "rivven-connector-s3 0.3 requires rivven-connect 0.2.5" issues
- 30+ Cargo.toml files to maintain
- Users confused about versions

### ✅ Approach 3: Grouped Crates + Feature Flags (Our Choice)

```toml
rivven-connect = "0.2"              # Minimal (datagen, stdout, file)
rivven-connect = { features = ["http"] }  # + HTTP (webhook, REST)
rivven-cdc = "0.2"                  # + Databases (postgres, mysql)
rivven-storage = "0.2"              # + Object storage (s3, gcs, azure)
rivven-warehouse = "0.2"            # + Warehouses (snowflake, bigquery)
```

**Benefits:**
- **Logical grouping**: Databases together, cloud together
- **Feature flags**: HTTP in core crate, opt-in via feature
- **Shared dependencies**: postgres + mysql share connection pool code
- **Atomic releases**: All DB connectors tested together
- **Fast iteration**: Change S3? Only rebuild cloud crate
- **Clear ownership**: Team A owns CDC, Team B owns cloud
- **Idiomatic naming**: Matches `sqlx-*`, `aws-sdk-*` conventions

### Compile Time Comparison

| Build | Crates/Features | Time | Binary Size |
|-------|-----------------|------|-------------|
| Minimal | rivven-connect | 30s | 8MB |
| + HTTP | + `http` feature | +1min | +5MB |
| + CDC | + rivven-cdc | +2min | +12MB |
| + Cloud | + rivven-cloud | +3min | +18MB |
| Full | all | 8min | 45MB |

### Usage Examples

```toml
# Minimal: Just need datagen for testing
[dependencies]
rivven-connect = "0.2"

# Webhook alerts: Events → Slack/PagerDuty
[dependencies]
rivven-connect = { version = "0.2", features = ["http"] }

# CDC pipeline: Postgres → Rivven
[dependencies]
rivven-connect = "0.2"
rivven-cdc = { version = "0.2", features = ["postgres"] }

# Data lake: Rivven → S3
[dependencies]
rivven-connect = "0.2"
rivven-storage = { version = "0.2", features = ["s3"] }

# Full ETL: Postgres → Rivven → Snowflake
[dependencies]
rivven-connect = "0.2"
rivven-cdc = { version = "0.2", features = ["postgres"] }
rivven-warehouse = { version = "0.2", features = ["snowflake"] }
```

## Comparison with Alternatives

| Feature | Rivven Connect | Kafka Connect | Debezium | Benthos |
|---------|---------------|---------------|----------|---------|
| **Language** | Rust | Java | Java | Go |
| **Single Binary** | ✅ | ❌ (JVM) | ❌ (JVM) | ✅ |
| **Embeddable** | ✅ | ❌ | ❌ | ✅ |
| **Memory Footprint** | ~10MB | ~500MB | ~500MB | ~30MB |
| **Startup Time** | <1s | 10-30s | 10-30s | <1s |
| **Custom Connectors** | Rust | Java | Java | Go/WASM |
| **CDC Support** | ✅ | Via Debezium | ✅ | Limited |
| **Schema Registry** | ✅ | ✅ | ✅ | ❌ |
| **Exactly-Once** | ✅ | ✅ | ✅ | ❌ |
| **Distributed Mode** | ✅ | ✅ | ✅ | ❌ |
| **WASM Plugins** | ✅ | ❌ | ❌ | ✅ |

## Roadmap

### v0.2 (Q1 2026) ✅ COMPLETE
- [x] Core traits (Source, Sink, Transform)
- [x] Built-in connectors (datagen, stdout, file)
- [x] Bootstrap server failover
- [x] Auto-create topics
- [x] Consolidate rivven-connect-sdk into rivven-connect
- [x] HTTP connector as feature flag in rivven-connect
- [x] Keep CDC in dedicated rivven-cdc crate
- [x] SourceEvent builder pattern and fluent API
- [x] CheckResult validation builder for detailed health checks
- [x] Transform composability (map, filter operations)
- [x] Enhanced field helpers (nested access, bulk operations)
- [x] State management typed accessors and builder
- [x] Retry utilities with exponential backoff
- [x] Convenience macros (record!, state!, log_event!)
- [x] Comprehensive prelude with 50+ re-exports
- [x] Group storage connectors into rivven-storage
- [x] Group warehouse connectors into rivven-warehouse
- [x] Remove deprecated rivven-s3 and rivven-snowflake crates

### v0.3 (Q2 2026) ✅ COMPLETE
- [x] Schema Registry integration (embedded + external)
- [x] Offset checkpointing to broker
- [x] Prometheus metrics endpoint
- [x] OpenTelemetry tracing
- [x] Crate READMEs for all packages
- [ ] Add Oracle CDC support to rivven-cdc
- [ ] Add GCS and Azure Blob to rivven-storage
- [ ] Connector derive macros (#[derive(Source)], #[derive(Sink)])

### v0.4 (Q3 2026) ✅ COMPLETE
- [x] WASM plugin support (load custom connectors at runtime)
- [x] Distributed mode (run Connect across multiple nodes)
- [x] Singleton enforcement for CDC connectors
- [x] Dead code cleanup and warning fixes
- [ ] Web UI for pipeline management
- [ ] Add MongoDB CDC support to rivven-cdc
- [ ] Add BigQuery and Redshift to rivven-warehouse

### v1.0 (Q4 2026)
- [ ] Production-hardened with extensive testing
- [ ] Complete connector ecosystem (20+ connectors)
- [ ] Enterprise features (RBAC, audit logging)
- [ ] Certified connectors program

## Migration Guide

### From rivven-connect-sdk (v0.1)

The SDK has been merged into rivven-connect. Update your imports:

```rust
// Before (v0.1)
use rivven_connect_sdk::{Source, Sink, SourceEvent};

// After (v0.2+)
use rivven_connect::{Source, Sink, SourceEvent};
// Or use the prelude for convenience:
use rivven_connect::prelude::*;
```

### From Separate Connector Crates

```toml
# Before (old structure v0.1)
[dependencies]
rivven-connect = "0.1"
rivven-connect-sdk = "0.1"  # No longer needed, merged into rivven-connect
rivven-http = "0.1"         # Now a feature in rivven-connect

# After (v0.2+ structure)
[dependencies]
rivven-connect = { version = "0.2", features = ["http"] }  # HTTP now a feature
rivven-storage = { version = "0.2", features = ["s3"] }     # Object storage crate
rivven-warehouse = { version = "0.2", features = ["snowflake"] }  # Warehouse crate
```

### From CDC in rivven-connect

```toml
# Before (CDC as feature flag)
[dependencies]
rivven-connect = { version = "0.1", features = ["postgres"] }

# After (CDC as separate crate)
[dependencies]
rivven-connect = "0.2"
rivven-cdc = { version = "0.2", features = ["postgres"] }
```

## See Also

- [Getting Started](docs/getting-started.md) - Quick start guide
- [Architecture](docs/architecture.md) - Rivven platform architecture
- [CDC Guide](docs/cdc.md) - Change Data Capture details
- [Connectors](docs/connectors.md) - Available connectors reference
- [Security](docs/security.md) - Authentication and encryption
- [Kubernetes](docs/kubernetes.md) - Deploying on K8s
