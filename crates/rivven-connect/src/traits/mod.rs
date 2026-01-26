//! Core connector traits and types
//!
//! This module provides the fundamental building blocks for Rivven Connect:
//! - `Source` - Read data from external systems
//! - `Sink` - Write data to external systems
//! - `Transform` - Modify events in-flight
//! - `CircuitBreaker` - Resilience pattern for connections
//! - `Batcher` - Efficient batch processing
//! - `Metrics` - Observability for connectors
//! - `testing` - Mock sources/sinks for testing
//! - `retry` - Retry utilities for resilient operations
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::prelude::*;
//!
//! #[derive(Debug, Deserialize, Validate, JsonSchema)]
//! pub struct MySourceConfig {
//!     #[validate(url)]
//!     pub endpoint: String,
//! }
//!
//! pub struct MySource;
//!
//! #[async_trait]
//! impl Source for MySource {
//!     type Config = MySourceConfig;
//!     // ...
//! }
//! ```

pub mod batch;
pub mod catalog;
pub mod circuit_breaker;
pub mod event;
pub mod metrics;
pub mod registry;
pub mod retry;
pub mod sink;
pub mod source;
pub mod spec;
pub mod state;
pub mod testing;
pub mod transform;

// Re-export source types
pub use source::{CheckResult, CheckResultBuilder, CheckDetail, Source, SourceConfig, SourceExt};

// Re-export sink types
pub use sink::{BatchConfig, BatchSink, Sink, SinkConfig, WriteResult};

// Re-export transform types
pub use transform::{Transform, TransformConfig, TransformOutput};

// Re-export event types
pub use event::{EventMetadata, LogLevel, SourceEvent, SourceEventBuilder, SourceEventType};

// Re-export catalog types
pub use catalog::{Catalog, ConfiguredCatalog, ConfiguredStream, DestinationSyncMode, Stream, SyncMode};

// Re-export state types
pub use state::{State, StateBuilder, StreamState};

// Re-export spec types
pub use spec::{ConnectorSpec, ConnectorSpecBuilder, SyncModeSpec};

// Re-export retry types
pub use retry::{RetryConfig, RetryGuard, RetryResult, retry, retry_result};

// Re-export registry types
pub use registry::{
    AnySource, AnySink, AnyTransform,
    SourceFactory, SinkFactory, TransformFactory,
    SourceRegistry, SinkRegistry, TransformRegistry,
};

// Re-export batch types
pub use batch::{Batch, Batcher, BatcherConfig, AsyncBatcher, chunk_events, partition_events};

// Re-export circuit breaker types
pub use circuit_breaker::{
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerError, 
    CircuitState, SharedCircuitBreaker,
};

// Re-export metrics types
pub use metrics::{
    Metrics, MetricsCollector, MetricsSnapshot, MetricValue,
    HistogramSnapshot, Label, Timer, NoopMetrics, metric_names,
};

// Re-export testing utilities
pub use testing::{
    MockSource, MockSourceConfig, MockSink, MockSinkConfig, MockTransform,
    TestHarness, TestResult, events, assertions,
};
