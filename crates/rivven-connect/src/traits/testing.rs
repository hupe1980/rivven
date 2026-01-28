//! Testing utilities for connectors
//!
//! This module provides mock implementations and test harnesses for
//! testing connectors without external dependencies.
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::testing::*;
//!
//! #[tokio::test]
//! async fn test_my_transform() {
//!     let source = MockSource::new()
//!         .with_events(vec![
//!             SourceEvent::record("test", json!({"id": 1})),
//!             SourceEvent::record("test", json!({"id": 2})),
//!         ]);
//!     
//!     let sink = MockSink::new();
//!     
//!     let harness = TestHarness::new(source, sink)
//!         .with_transform(|e| vec![e]);
//!     
//!     let result = harness.run().await.unwrap();
//!     
//!     assert_eq!(result.records_written, 2);
//!     assert_eq!(sink.written_events().len(), 2);
//! }
//! ```

use super::catalog::{Catalog, ConfiguredCatalog};
use super::event::SourceEvent;
use super::sink::{Sink, WriteResult};
use super::source::{CheckResult, Source};
use super::spec::ConnectorSpec;
use super::state::State;
use crate::error::{ConnectError, Result};
use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;
use validator::Validate;

// ============================================================================
// Mock Source
// ============================================================================

/// Configuration for MockSource
#[derive(Debug, Clone, Default, Deserialize, Validate, JsonSchema)]
pub struct MockSourceConfig {
    /// Name of the mock source
    pub name: Option<String>,
}

/// A mock source for testing
#[derive(Debug)]
pub struct MockSource {
    events: Arc<Mutex<Vec<SourceEvent>>>,
    check_result: Arc<Mutex<CheckResult>>,
    catalog: Arc<Mutex<Catalog>>,
    should_fail: Arc<Mutex<bool>>,
    fail_message: Arc<Mutex<Option<String>>>,
}

impl Default for MockSource {
    fn default() -> Self {
        Self::new()
    }
}

impl MockSource {
    /// Create a new mock source
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            check_result: Arc::new(Mutex::new(CheckResult::success())),
            catalog: Arc::new(Mutex::new(Catalog::default())),
            should_fail: Arc::new(Mutex::new(false)),
            fail_message: Arc::new(Mutex::new(None)),
        }
    }

    /// Set the events to emit
    pub fn with_events(self, events: Vec<SourceEvent>) -> Self {
        *self.events.lock() = events;
        self
    }

    /// Add a single event
    pub fn add_event(self, event: SourceEvent) -> Self {
        self.events.lock().push(event);
        self
    }

    /// Set the check result
    pub fn with_check_result(self, result: CheckResult) -> Self {
        *self.check_result.lock() = result;
        self
    }

    /// Set the catalog
    pub fn with_catalog(self, catalog: Catalog) -> Self {
        *self.catalog.lock() = catalog;
        self
    }

    /// Make the source fail with an error
    pub fn fail_with(self, message: impl Into<String>) -> Self {
        *self.should_fail.lock() = true;
        *self.fail_message.lock() = Some(message.into());
        self
    }
}

#[async_trait]
impl Source for MockSource {
    type Config = MockSourceConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("mock-source", "0.0.1").build()
    }

    async fn check(&self, _config: &Self::Config) -> Result<CheckResult> {
        if *self.should_fail.lock() {
            let msg = self
                .fail_message
                .lock()
                .clone()
                .unwrap_or_else(|| "mock error".into());
            return Ok(CheckResult::failure(msg));
        }
        Ok(self.check_result.lock().clone())
    }

    async fn discover(&self, _config: &Self::Config) -> Result<Catalog> {
        if *self.should_fail.lock() {
            let msg = self
                .fail_message
                .lock()
                .clone()
                .unwrap_or_else(|| "mock error".into());
            return Err(ConnectError::Source {
                name: "mock-source".to_string(),
                message: msg,
            });
        }
        Ok(self.catalog.lock().clone())
    }

    async fn read(
        &self,
        _config: &Self::Config,
        _catalog: &ConfiguredCatalog,
        _state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        if *self.should_fail.lock() {
            let msg = self
                .fail_message
                .lock()
                .clone()
                .unwrap_or_else(|| "mock error".into());
            return Err(ConnectError::Source {
                name: "mock-source".to_string(),
                message: msg,
            });
        }
        let events = self.events.lock().clone();
        Ok(Box::pin(stream::iter(events.into_iter().map(Ok))))
    }
}

// ============================================================================
// Mock Sink
// ============================================================================

/// Configuration for MockSink
#[derive(Debug, Clone, Default, Deserialize, Validate, JsonSchema)]
pub struct MockSinkConfig {
    /// Name of the mock sink
    pub name: Option<String>,
}

/// A mock sink for testing
#[derive(Debug)]
pub struct MockSink {
    written: Arc<Mutex<Vec<SourceEvent>>>,
    check_result: Arc<Mutex<CheckResult>>,
    should_fail: Arc<Mutex<bool>>,
    fail_message: Arc<Mutex<Option<String>>>,
    fail_after: Arc<Mutex<Option<usize>>>,
}

impl Default for MockSink {
    fn default() -> Self {
        Self::new()
    }
}

impl MockSink {
    /// Create a new mock sink
    pub fn new() -> Self {
        Self {
            written: Arc::new(Mutex::new(Vec::new())),
            check_result: Arc::new(Mutex::new(CheckResult::success())),
            should_fail: Arc::new(Mutex::new(false)),
            fail_message: Arc::new(Mutex::new(None)),
            fail_after: Arc::new(Mutex::new(None)),
        }
    }

    /// Set the check result
    pub fn with_check_result(self, result: CheckResult) -> Self {
        *self.check_result.lock() = result;
        self
    }

    /// Make the sink fail with an error
    pub fn fail_with(self, message: impl Into<String>) -> Self {
        *self.should_fail.lock() = true;
        *self.fail_message.lock() = Some(message.into());
        self
    }

    /// Make the sink fail after N events
    pub fn fail_after(self, n: usize, message: impl Into<String>) -> Self {
        *self.fail_after.lock() = Some(n);
        *self.fail_message.lock() = Some(message.into());
        self
    }

    /// Get the written events
    pub fn written_events(&self) -> Vec<SourceEvent> {
        self.written.lock().clone()
    }

    /// Get the number of written events
    pub fn written_count(&self) -> usize {
        self.written.lock().len()
    }

    /// Clear the written events
    pub fn clear(&self) {
        self.written.lock().clear();
    }
}

#[async_trait]
impl Sink for MockSink {
    type Config = MockSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("mock-sink", "0.0.1").build()
    }

    async fn check(&self, _config: &Self::Config) -> Result<CheckResult> {
        if *self.should_fail.lock() {
            let msg = self
                .fail_message
                .lock()
                .clone()
                .unwrap_or_else(|| "mock error".into());
            return Ok(CheckResult::failure(msg));
        }
        Ok(self.check_result.lock().clone())
    }

    async fn write(
        &self,
        _config: &Self::Config,
        events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        if *self.should_fail.lock() {
            let msg = self
                .fail_message
                .lock()
                .clone()
                .unwrap_or_else(|| "mock error".into());
            return Err(ConnectError::Sink {
                name: "mock-sink".to_string(),
                message: msg,
            });
        }

        let fail_after = *self.fail_after.lock();
        let mut result = WriteResult::new();
        let mut count = 0;

        futures::pin_mut!(events);

        while let Some(event) = events.next().await {
            if let Some(n) = fail_after {
                if count >= n {
                    let msg = self
                        .fail_message
                        .lock()
                        .clone()
                        .unwrap_or_else(|| "mock error".into());
                    result.add_failure(1, msg);
                    continue;
                }
            }

            self.written.lock().push(event);
            result.add_success(1, 0);
            count += 1;
        }

        Ok(result)
    }
}

// ============================================================================
// Mock Transform
// ============================================================================

/// A simple transform function type for testing
pub type TransformFn = Box<dyn Fn(SourceEvent) -> Vec<SourceEvent> + Send + Sync>;

/// A mock transform for testing
pub struct MockTransform {
    name: String,
    transform_fn: TransformFn,
}

impl MockTransform {
    /// Create a new mock transform
    pub fn new<F>(name: impl Into<String>, transform_fn: F) -> Self
    where
        F: Fn(SourceEvent) -> Vec<SourceEvent> + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            transform_fn: Box::new(transform_fn),
        }
    }

    /// Get the transform name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Apply the transform
    pub fn apply(&self, event: SourceEvent) -> Vec<SourceEvent> {
        (self.transform_fn)(event)
    }
}

// ============================================================================
// Test Harness
// ============================================================================

/// Test harness for running pipeline tests
pub struct TestHarness<So: Source, Si: Sink> {
    source: So,
    source_config: So::Config,
    sink: Si,
    sink_config: Si::Config,
    transforms: Vec<MockTransform>,
    state: Option<State>,
}

impl<So, Si> TestHarness<So, Si>
where
    So: Source + 'static,
    Si: Sink + 'static,
    So::Config: Default,
    Si::Config: Default,
{
    /// Create a new test harness with default configs
    pub fn new(source: So, sink: Si) -> Self {
        Self {
            source,
            source_config: Default::default(),
            sink,
            sink_config: Default::default(),
            transforms: Vec::new(),
            state: None,
        }
    }
}

impl<So, Si> TestHarness<So, Si>
where
    So: Source + 'static,
    Si: Sink + 'static,
{
    /// Create a test harness with explicit configs
    pub fn with_configs(
        source: So,
        source_config: So::Config,
        sink: Si,
        sink_config: Si::Config,
    ) -> Self {
        Self {
            source,
            source_config,
            sink,
            sink_config,
            transforms: Vec::new(),
            state: None,
        }
    }

    /// Add a transform function to the pipeline
    pub fn with_transform<F>(mut self, name: impl Into<String>, transform_fn: F) -> Self
    where
        F: Fn(SourceEvent) -> Vec<SourceEvent> + Send + Sync + 'static,
    {
        self.transforms.push(MockTransform::new(name, transform_fn));
        self
    }

    /// Set the initial state
    pub fn with_state(mut self, state: State) -> Self {
        self.state = Some(state);
        self
    }

    /// Run the test pipeline
    pub async fn run(self) -> Result<TestResult<Si>> {
        // Check source
        let source_check = self.source.check(&self.source_config).await?;
        if !source_check.is_success() {
            return Err(ConnectError::Source {
                name: "test-source".to_string(),
                message: source_check
                    .message
                    .unwrap_or_else(|| "unknown".to_string()),
            });
        }

        // Check sink
        let sink_check = self.sink.check(&self.sink_config).await?;
        if !sink_check.is_success() {
            return Err(ConnectError::Sink {
                name: "test-sink".to_string(),
                message: sink_check.message.unwrap_or_else(|| "unknown".to_string()),
            });
        }

        // Discover catalog
        let catalog = self.source.discover(&self.source_config).await?;
        let configured = ConfiguredCatalog::from_catalog(&catalog);

        // Read events
        let events = self
            .source
            .read(&self.source_config, &configured, self.state)
            .await?;

        // Apply transforms
        let transformed = if self.transforms.is_empty() {
            Box::pin(events.filter_map(|r| async move { r.ok() }))
                as BoxStream<'static, SourceEvent>
        } else {
            let events_vec: Vec<_> = events.filter_map(|r| async move { r.ok() }).collect().await;

            let mut result = events_vec;
            for transform in &self.transforms {
                let mut new_result = Vec::new();
                for event in result {
                    new_result.extend(transform.apply(event));
                }
                result = new_result;
            }

            Box::pin(stream::iter(result)) as BoxStream<'static, SourceEvent>
        };

        // Write to sink
        let write_result = self.sink.write(&self.sink_config, transformed).await?;

        Ok(TestResult {
            records_read: write_result.records_written + write_result.records_failed,
            records_written: write_result.records_written,
            records_failed: write_result.records_failed,
            sink: self.sink,
        })
    }
}

/// Result of running a test harness
pub struct TestResult<Si> {
    /// Number of records read from source
    pub records_read: u64,
    /// Number of records written to sink
    pub records_written: u64,
    /// Number of records that failed
    pub records_failed: u64,
    /// The sink (for inspection)
    pub sink: Si,
}

// ============================================================================
// Event Builder Helpers for Tests
// ============================================================================

/// Create test events quickly
pub mod events {
    use super::*;
    use serde_json::json;

    /// Create a sequence of record events
    pub fn records(stream: &str, count: usize) -> Vec<SourceEvent> {
        (0..count)
            .map(|i| SourceEvent::record(stream, json!({"id": i})))
            .collect()
    }

    /// Create a sequence of record events with a field generator
    pub fn records_with<F>(stream: &str, count: usize, generator: F) -> Vec<SourceEvent>
    where
        F: Fn(usize) -> serde_json::Value,
    {
        (0..count)
            .map(|i| SourceEvent::record(stream, generator(i)))
            .collect()
    }

    /// Create CDC insert events
    pub fn cdc_inserts(stream: &str, count: usize) -> Vec<SourceEvent> {
        (0..count)
            .map(|i| SourceEvent::insert(stream, json!({"id": i})))
            .collect()
    }
}

/// Assertions for test events
pub mod assertions {
    use super::*;

    /// Assert that all events are records
    pub fn all_records(events: &[SourceEvent]) -> bool {
        events.iter().all(|e| e.is_data())
    }

    /// Assert that all events have a specific field
    pub fn all_have_field(events: &[SourceEvent], field: &str) -> bool {
        events.iter().all(|e| e.data.get(field).is_some())
    }

    /// Assert that events are in order by a field
    pub fn ordered_by<T: Ord + for<'de> Deserialize<'de>>(
        events: &[SourceEvent],
        field: &str,
    ) -> bool {
        let values: Vec<_> = events
            .iter()
            .filter_map(|e| e.data.get(field))
            .filter_map(|v| serde_json::from_value::<T>(v.clone()).ok())
            .collect();

        values.windows(2).all(|w| w[0] <= w[1])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_mock_source() {
        let source = MockSource::new().with_events(vec![
            SourceEvent::record("test", json!({"id": 1})),
            SourceEvent::record("test", json!({"id": 2})),
        ]);

        let config = MockSourceConfig::default();
        let check = source.check(&config).await.unwrap();
        assert!(check.is_success());

        let catalog = source.discover(&config).await.unwrap();
        let configured = ConfiguredCatalog::from_catalog(&catalog);

        let events: Vec<_> = source
            .read(&config, &configured, None)
            .await
            .unwrap()
            .filter_map(|r| async move { r.ok() })
            .collect()
            .await;

        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn test_mock_sink() {
        let sink = MockSink::new();
        let config = MockSinkConfig::default();

        let events = vec![
            SourceEvent::record("test", json!({"id": 1})),
            SourceEvent::record("test", json!({"id": 2})),
        ];

        let stream = Box::pin(stream::iter(events));
        let result = sink.write(&config, stream).await.unwrap();

        assert_eq!(result.records_written, 2);
        assert_eq!(sink.written_count(), 2);
    }

    #[tokio::test]
    async fn test_mock_source_failure() {
        let source = MockSource::new().fail_with("connection refused");
        let config = MockSourceConfig::default();

        let check = source.check(&config).await.unwrap();
        assert!(!check.is_success());
    }

    #[tokio::test]
    async fn test_mock_sink_fail_after() {
        let sink = MockSink::new().fail_after(2, "quota exceeded");
        let config = MockSinkConfig::default();

        let events = vec![
            SourceEvent::record("test", json!({"id": 1})),
            SourceEvent::record("test", json!({"id": 2})),
            SourceEvent::record("test", json!({"id": 3})),
            SourceEvent::record("test", json!({"id": 4})),
        ];

        let stream = Box::pin(stream::iter(events));
        let result = sink.write(&config, stream).await.unwrap();

        assert_eq!(result.records_written, 2);
        assert_eq!(result.records_failed, 2);
        assert_eq!(sink.written_count(), 2);
    }

    #[test]
    fn test_events_helper() {
        let events = events::records("test", 5);
        assert_eq!(events.len(), 5);
        assert!(assertions::all_records(&events));
        assert!(assertions::all_have_field(&events, "id"));
    }

    #[test]
    fn test_events_with_generator() {
        let events = events::records_with("users", 3, |i| {
            json!({
                "id": i,
                "name": format!("User {}", i),
            })
        });

        assert_eq!(events.len(), 3);
        assert!(assertions::all_have_field(&events, "name"));
    }
}
