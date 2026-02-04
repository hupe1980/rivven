//! rivven-connect - Connector SDK and runtime for Rivven
//!
//! This crate provides both the SDK (traits for building connectors) and the
//! runtime (CLI and execution engine) for Rivven's connector framework.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    rivven-connect (SDK + Runtime)               │
//! │  Source, Sink, Transform, Event, Registry, State, Catalog       │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                    Built-in Connectors                          │
//! │  ├── Database CDC (postgres, mysql)                             │
//! │  ├── Messaging (kafka, mqtt, sqs, pubsub)                       │
//! │  ├── Storage (s3, gcs, azure)                                   │
//! │  └── Warehouse (snowflake, bigquery, redshift)                  │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                    External Crates                              │
//! │  └── rivven-cdc (optional, reusable CDC primitives)             │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # SDK Usage (Library)
//!
//! ```rust,ignore
//! use rivven_connect::{
//!     Source, Sink, Transform, SourceEvent, ConnectorResult,
//!     SourceFactory, SinkFactory, SourceRegistry, SinkRegistry,
//! };
//!
//! // Implement a custom source
//! #[async_trait::async_trait]
//! impl Source for MySource {
//!     type Config = MySourceConfig;
//!     
//!     async fn read(&self, config: &Self::Config, catalog: &ConfiguredCatalog, state: Option<State>)
//!         -> ConnectorResult<BoxStream<'static, ConnectorResult<SourceEvent>>> {
//!         // ...
//!     }
//! }
//! ```
//!
//! # CLI Usage (Binary)
//!
//! ```bash
//! # Run all connectors
//! rivven-connect -c connect.yaml
//!
//! # Run only sources
//! rivven-connect -c connect.yaml sources
//!
//! # Run only sinks
//! rivven-connect -c connect.yaml sinks
//!
//! # Validate configuration
//! rivven-connect -c connect.yaml validate
//! ```

// Core SDK traits (merged from rivven-connect-sdk)
pub mod traits;

// Common types (SensitiveString, etc.)
pub mod types;

// Error types
pub mod error;

// Runtime modules
pub mod broker_client;
pub mod config;
pub mod connectors;
pub mod health;
pub mod metrics;
pub mod rate_limiter;
pub mod sink_runner;
pub mod source_runner;
pub mod telemetry;
pub mod topic_resolver;

// Distributed coordination
pub mod distributed;

// Schema registry
pub mod schema;

// Output format writers (JSON, JSONL, Parquet, Avro)
pub mod format;

// Re-export SensitiveString at crate root for convenience
pub use types::SensitiveString;

// Re-export TopicResolver types for CDC topic routing
pub use topic_resolver::{
    max_topic_length, valid_placeholders, validate_topic_routing, CaseConversion,
    NormalizationConfig, TopicMetadata, TopicPatternError, TopicResolver, TopicResolverBuilder,
};

// Re-export core traits at crate root for ergonomic use
pub use traits::{
    chunk_events,
    metric_names,
    partition_events,
    retry,
    retry_result,
    AnySink,
    // Registry
    AnySource,
    AnyTransform,
    AsyncBatcher,
    // Batch processing
    Batch,
    BatchConfig,
    BatchSink,
    Batcher,
    BatcherConfig,
    // Catalog
    Catalog,
    CheckDetail,
    CheckResult,
    CheckResultBuilder,
    // Circuit breaker
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerError,
    CircuitState,
    ConfiguredCatalog,
    ConfiguredStream,
    // Spec
    ConnectorSpec,
    ConnectorSpecBuilder,
    DestinationSyncMode,
    EventMetadata,
    HistogramSnapshot,
    Label,
    LogLevel,
    MetricValue,
    // Metrics
    Metrics,
    MetricsCollector,
    MetricsSnapshot,
    MockSink,
    MockSinkConfig,
    // Testing utilities
    MockSource,
    MockSourceConfig,
    MockTransform,
    NoopMetrics,
    // Retry utilities
    RetryConfig,
    RetryGuard,
    RetryResult,
    SharedCircuitBreaker,
    Sink,
    SinkConfig,
    SinkFactory,
    SinkRegistry,
    // Core connector traits
    Source,
    SourceConfig,
    // Event types
    SourceEvent,
    SourceEventBuilder,
    SourceEventType,
    SourceExt,
    SourceFactory,
    SourceRegistry,
    // State management
    State,
    StateBuilder,
    Stream,
    StreamState,
    SyncMode,
    SyncModeSpec,
    TestHarness,
    TestResult,
    Timer,
    Transform,
    TransformConfig,
    TransformFactory,
    TransformOutput,
    TransformRegistry,
    WriteResult,
};

// Re-export error types
pub use error::{ConnectError, ConnectorError, ConnectorResult, ConnectorStatus, Result};

// Re-export config types
pub use config::ConnectConfig;

// Re-export health types
pub use health::{ConnectorHealth, HealthState, SharedHealthState};

// Re-export broker client
pub use broker_client::BrokerClient;

// Re-export commonly used dependencies for connector implementations
pub use async_trait::async_trait;
pub use futures::stream::BoxStream;
pub use serde::{Deserialize, Serialize};
pub use serde_json::Value as JsonValue;

// Re-export derive macros for connector development
pub use rivven_connect_derive::{
    connector_spec, SinkConfig as SinkConfigDerive, SourceConfig as SourceConfigDerive,
    TransformConfig as TransformConfigDerive,
};

/// Prelude module for convenient imports
pub mod prelude {
    pub use crate::{
        // Re-exports
        async_trait,
        chunk_events,
        metric_names,
        partition_events,
        retry,
        retry_result,
        AnySink,
        // Registry
        AnySource,
        AnyTransform,
        AsyncBatcher,
        // Batch processing
        Batch,
        BatchConfig,
        BatchSink,
        Batcher,
        BatcherConfig,
        BoxStream,
        // Catalog & State
        Catalog,
        CheckDetail,
        CheckResult,
        CheckResultBuilder,
        // Circuit breaker
        CircuitBreaker,
        CircuitBreakerConfig,
        CircuitBreakerError,
        CircuitState,
        ConfiguredCatalog,
        ConfiguredStream,
        ConnectError,
        // Errors
        ConnectorError,
        ConnectorResult,
        // Spec
        ConnectorSpec,
        ConnectorSpecBuilder,
        Deserialize,
        DestinationSyncMode,
        EventMetadata,
        JsonValue,
        Label,
        LogLevel,
        MetricValue,
        // Metrics
        Metrics,
        MetricsCollector,
        MetricsSnapshot,
        MockSink,
        MockSinkConfig,
        // Testing
        MockSource,
        MockSourceConfig,
        MockTransform,
        NoopMetrics,
        Result,
        // Retry
        RetryConfig,
        RetryGuard,
        Serialize,
        SharedCircuitBreaker,
        Sink,
        SinkConfig,
        SinkFactory,
        SinkRegistry,
        // Core traits
        Source,
        SourceConfig,
        // Events
        SourceEvent,
        SourceEventBuilder,
        SourceEventType,
        SourceExt,
        SourceFactory,
        SourceRegistry,
        State,
        StateBuilder,
        Stream,
        StreamState,
        SyncMode,
        SyncModeSpec,
        TestHarness,
        TestResult,
        Timer,
        Transform,
        TransformConfig,
        TransformFactory,
        TransformOutput,
        TransformRegistry,
        WriteResult,
    };

    // Re-export validation and schema traits
    pub use schemars::JsonSchema;
    pub use validator::Validate;

    // Re-export transform helpers
    pub use crate::traits::transform::transforms;

    // Re-export testing helpers
    pub use crate::traits::testing::{assertions, events};
}

/// Convenience macro for creating a record event
///
/// # Example
/// ```rust,ignore
/// use rivven_connect::{record, prelude::*};
///
/// let event = record!("users", {"id": 1, "name": "Alice"});
/// ```
#[macro_export]
macro_rules! record {
    ($stream:expr, $data:tt) => {
        $crate::SourceEvent::record($stream, serde_json::json!($data))
    };
}

/// Convenience macro for creating a state event
///
/// # Example
/// ```rust,ignore
/// use rivven_connect::{state, prelude::*};
///
/// let event = state!("users", {"cursor": "abc123"});
/// ```
#[macro_export]
macro_rules! state {
    ($stream:expr, $data:tt) => {
        $crate::SourceEvent::state($stream, serde_json::json!($data))
    };
}

/// Convenience macro for creating a log event
///
/// # Example
/// ```rust,ignore
/// use rivven_connect::{log_event, prelude::*, LogLevel};
///
/// let event = log_event!(LogLevel::Info, "Starting sync");
/// ```
#[macro_export]
macro_rules! log_event {
    ($level:expr, $message:expr) => {
        $crate::SourceEvent::log($level, $message)
    };
}
