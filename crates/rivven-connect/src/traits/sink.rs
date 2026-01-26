//! Sink connector trait

use super::event::SourceEvent;
use super::source::CheckResult;
use super::spec::ConnectorSpec;
use crate::error::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use validator::Validate;

/// Trait for sink connector configuration
pub trait SinkConfig: DeserializeOwned + Validate + JsonSchema + Send + Sync {}

// Blanket implementation
impl<T> SinkConfig for T where T: DeserializeOwned + Validate + JsonSchema + Send + Sync {}

/// Result of a write operation
#[derive(Debug, Clone, Default)]
pub struct WriteResult {
    /// Number of records written
    pub records_written: u64,
    /// Number of bytes written
    pub bytes_written: u64,
    /// Number of records that failed
    pub records_failed: u64,
    /// Error messages for failed records
    pub errors: Vec<String>,
}

impl WriteResult {
    /// Create a new write result
    pub fn new() -> Self {
        Self::default()
    }

    /// Add successful records
    pub fn add_success(&mut self, records: u64, bytes: u64) {
        self.records_written += records;
        self.bytes_written += bytes;
    }

    /// Add failed records
    pub fn add_failure(&mut self, records: u64, error: impl Into<String>) {
        self.records_failed += records;
        self.errors.push(error.into());
    }

    /// Check if there were any failures
    pub fn has_failures(&self) -> bool {
        self.records_failed > 0
    }
}

/// Trait for sink connectors
///
/// Sink connectors consume events and write them to external systems.
///
/// # Example
///
/// ```rust,ignore
/// use rivven_connect::prelude::*;
///
/// #[derive(Debug, Deserialize, Validate, JsonSchema)]
/// pub struct MySinkConfig {
///     pub bucket: String,
/// }
///
/// pub struct MySink;
///
/// #[async_trait]
/// impl Sink for MySink {
///     type Config = MySinkConfig;
///
///     fn spec() -> ConnectorSpec {
///         ConnectorSpec::new("my-sink", "1.0.0").build()
///     }
///
///     async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
///         Ok(CheckResult::success())
///     }
///
///     async fn write(
///         &self,
///         config: &Self::Config,
///         events: BoxStream<'static, SourceEvent>,
///     ) -> Result<WriteResult> {
///         todo!()
///     }
/// }
/// ```
#[async_trait]
pub trait Sink: Send + Sync {
    /// Configuration type for this sink
    type Config: SinkConfig;

    /// Return the connector specification
    fn spec() -> ConnectorSpec;

    /// Check connectivity and configuration
    async fn check(&self, config: &Self::Config) -> Result<CheckResult>;

    /// Write events to the destination
    ///
    /// # Arguments
    ///
    /// * `config` - Sink configuration
    /// * `events` - Stream of events to write
    async fn write(
        &self,
        config: &Self::Config,
        events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult>;
}

/// Batching configuration for sinks
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum number of records per batch
    pub max_records: usize,
    /// Maximum bytes per batch
    pub max_bytes: usize,
    /// Maximum time to wait before flushing (milliseconds)
    pub max_wait_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_records: 10_000,
            max_bytes: 10 * 1024 * 1024, // 10 MB
            max_wait_ms: 5_000,          // 5 seconds
        }
    }
}

/// Trait for sinks that support batching
#[async_trait]
pub trait BatchSink: Sink {
    /// Get batch configuration
    fn batch_config(&self, _config: &Self::Config) -> BatchConfig {
        BatchConfig::default()
    }

    /// Write a batch of events
    async fn write_batch(
        &self,
        config: &Self::Config,
        events: Vec<SourceEvent>,
    ) -> Result<WriteResult>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_result() {
        let mut result = WriteResult::new();
        result.add_success(100, 1024);
        result.add_success(50, 512);

        assert_eq!(result.records_written, 150);
        assert_eq!(result.bytes_written, 1536);
        assert!(!result.has_failures());

        result.add_failure(5, "write timeout");
        assert!(result.has_failures());
        assert_eq!(result.records_failed, 5);
    }

    #[test]
    fn test_batch_config_default() {
        let config = BatchConfig::default();
        assert_eq!(config.max_records, 10_000);
        assert_eq!(config.max_bytes, 10 * 1024 * 1024);
        assert_eq!(config.max_wait_ms, 5_000);
    }
}
