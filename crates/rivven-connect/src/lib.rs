//! rivven-connect - Connector SDK and runtime for Rivven
//!
//! This crate provides both the SDK (traits for building connectors) and the
//! runtime (CLI and execution engine) for Rivven's connector framework.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    rivven-connect (SDK)                         │
//! │  Source, Sink, Transform, Event, Registry, State, Catalog       │
//! └─────────────────────────────────────────────────────────────────┘
//!         ↑ implement
//! ┌─────────────────┬─────────────────┬─────────────────────────────┐
//! │ rivven-cdc      │ rivven-storage  │ rivven-warehouse            │
//! │ (CDC sources)   │ (S3/GCS sinks)  │ (Snowflake sinks)           │
//! └─────────────────┴─────────────────┴─────────────────────────────┘
//!         ↑ compose
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    rivven-connect (CLI)                         │
//! │  Composes connectors, runs pipelines, config-driven             │
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

// Distributed coordination
pub mod distributed;

// Schema registry
pub mod schema;

// WASM plugin support
pub mod plugin;

// Re-export core traits at crate root for ergonomic use
pub use traits::{
    // Core connector traits
    Source, SourceConfig, SourceExt, CheckResult, CheckResultBuilder, CheckDetail,
    Sink, SinkConfig, BatchSink, BatchConfig, WriteResult,
    Transform, TransformConfig, TransformOutput,
    // Event types
    SourceEvent, SourceEventBuilder, SourceEventType, EventMetadata, LogLevel,
    // Registry
    AnySource, AnySink, AnyTransform,
    SourceFactory, SinkFactory, TransformFactory,
    SourceRegistry, SinkRegistry, TransformRegistry,
    // Catalog
    Catalog, ConfiguredCatalog, ConfiguredStream, Stream, SyncMode, DestinationSyncMode,
    // State management
    State, StateBuilder, StreamState,
    // Spec
    ConnectorSpec, ConnectorSpecBuilder, SyncModeSpec,
    // Retry utilities
    RetryConfig, RetryGuard, RetryResult, retry, retry_result,
    // Batch processing
    Batch, Batcher, BatcherConfig, AsyncBatcher, chunk_events, partition_events,
    // Circuit breaker
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerError, CircuitState, SharedCircuitBreaker,
    // Metrics
    Metrics, MetricsCollector, MetricsSnapshot, MetricValue, HistogramSnapshot, Label, Timer, NoopMetrics, metric_names,
    // Testing utilities
    MockSource, MockSourceConfig, MockSink, MockSinkConfig, MockTransform, TestHarness, TestResult,
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

/// Prelude module for convenient imports
pub mod prelude {
    pub use crate::{
        // Core traits
        Source, SourceConfig, SourceExt, CheckResult, CheckResultBuilder, CheckDetail,
        Sink, SinkConfig, BatchSink, BatchConfig, WriteResult,
        Transform, TransformConfig, TransformOutput,
        // Events
        SourceEvent, SourceEventBuilder, SourceEventType, EventMetadata, LogLevel,
        // Registry
        AnySource, AnySink, AnyTransform,
        SourceFactory, SinkFactory, TransformFactory,
        SourceRegistry, SinkRegistry, TransformRegistry,
        // Catalog & State
        Catalog, ConfiguredCatalog, ConfiguredStream, Stream, State, StateBuilder, StreamState, 
        SyncMode, DestinationSyncMode,
        // Spec
        ConnectorSpec, ConnectorSpecBuilder, SyncModeSpec,
        // Retry
        RetryConfig, RetryGuard, retry, retry_result,
        // Batch processing
        Batch, Batcher, BatcherConfig, AsyncBatcher, chunk_events, partition_events,
        // Circuit breaker
        CircuitBreaker, CircuitBreakerConfig, CircuitBreakerError, CircuitState, SharedCircuitBreaker,
        // Metrics
        Metrics, MetricsCollector, MetricsSnapshot, MetricValue, Label, Timer, NoopMetrics, metric_names,
        // Testing
        MockSource, MockSourceConfig, MockSink, MockSinkConfig, MockTransform, TestHarness, TestResult,
        // Errors
        ConnectorError, ConnectorResult, ConnectError, Result,
        // Re-exports
        async_trait, BoxStream, Deserialize, Serialize, JsonValue,
    };
    
    // Re-export validation and schema traits
    pub use validator::Validate;
    pub use schemars::JsonSchema;
    
    // Re-export transform helpers
    pub use crate::traits::transform::transforms;
    
    // Re-export testing helpers
    pub use crate::traits::testing::{events, assertions};
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
