//! Source connector trait
//!
//! This module provides the core `Source` trait for implementing data source connectors.

use super::catalog::{Catalog, ConfiguredCatalog};
use super::event::SourceEvent;
use super::spec::ConnectorSpec;
use super::state::State;
use crate::error::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use std::fmt;
use validator::Validate;

/// Trait for source connector configuration
pub trait SourceConfig: DeserializeOwned + Validate + JsonSchema + Send + Sync {}

// Blanket implementation
impl<T> SourceConfig for T where T: DeserializeOwned + Validate + JsonSchema + Send + Sync {}

/// Result of a connection check with detailed status information
#[derive(Debug, Clone)]
pub struct CheckResult {
    /// Whether the check succeeded
    pub success: bool,
    /// Error message if failed
    pub message: Option<String>,
    /// Individual check details
    pub checks: Vec<CheckDetail>,
}

/// A single check detail
#[derive(Debug, Clone)]
pub struct CheckDetail {
    /// Name of the check (e.g., "connectivity", "auth", "permissions")
    pub name: String,
    /// Whether this check passed
    pub passed: bool,
    /// Description or error message
    pub message: Option<String>,
    /// Duration of the check in milliseconds
    pub duration_ms: Option<u64>,
}

impl CheckDetail {
    /// Create a passed check
    pub fn passed(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            passed: true,
            message: None,
            duration_ms: None,
        }
    }

    /// Create a failed check
    pub fn failed(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            passed: false,
            message: Some(message.into()),
            duration_ms: None,
        }
    }

    /// Add a message to this check
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Add duration to this check
    pub fn with_duration_ms(mut self, duration_ms: u64) -> Self {
        self.duration_ms = Some(duration_ms);
        self
    }
}

impl CheckResult {
    /// Create a successful check result
    pub fn success() -> Self {
        Self {
            success: true,
            message: None,
            checks: Vec::new(),
        }
    }

    /// Create a failed check result
    pub fn failure(message: impl Into<String>) -> Self {
        Self {
            success: false,
            message: Some(message.into()),
            checks: Vec::new(),
        }
    }

    /// Check if successful
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Create a builder for detailed checks
    pub fn builder() -> CheckResultBuilder {
        CheckResultBuilder::new()
    }

    /// Add a check detail
    pub fn with_check(mut self, check: CheckDetail) -> Self {
        self.checks.push(check);
        self
    }

    /// Get all failed checks
    pub fn failed_checks(&self) -> impl Iterator<Item = &CheckDetail> {
        self.checks.iter().filter(|c| !c.passed)
    }
}

impl fmt::Display for CheckResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.success {
            write!(f, "✓ Connection check passed")?;
        } else {
            write!(f, "✗ Connection check failed")?;
            if let Some(ref msg) = self.message {
                write!(f, ": {}", msg)?;
            }
        }

        if !self.checks.is_empty() {
            writeln!(f)?;
            for check in &self.checks {
                let status = if check.passed { "✓" } else { "✗" };
                write!(f, "  {} {}", status, check.name)?;
                if let Some(ref msg) = check.message {
                    write!(f, ": {}", msg)?;
                }
                if let Some(ms) = check.duration_ms {
                    write!(f, " ({}ms)", ms)?;
                }
                writeln!(f)?;
            }
        }
        Ok(())
    }
}

/// Builder for creating CheckResult with multiple validation checks
pub struct CheckResultBuilder {
    checks: Vec<CheckDetail>,
}

impl CheckResultBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self { checks: Vec::new() }
    }

    /// Add a passed check
    pub fn check_passed(mut self, name: impl Into<String>) -> Self {
        self.checks.push(CheckDetail::passed(name));
        self
    }

    /// Add a failed check
    pub fn check_failed(mut self, name: impl Into<String>, message: impl Into<String>) -> Self {
        self.checks.push(CheckDetail::failed(name, message));
        self
    }

    /// Add a check detail
    pub fn check(mut self, detail: CheckDetail) -> Self {
        self.checks.push(detail);
        self
    }

    /// Add a conditional check
    pub fn check_if<F>(mut self, name: impl Into<String>, condition: F) -> Self
    where
        F: FnOnce() -> std::result::Result<(), String>,
    {
        let name = name.into();
        match condition() {
            Ok(()) => self.checks.push(CheckDetail::passed(name)),
            Err(msg) => self.checks.push(CheckDetail::failed(name, msg)),
        }
        self
    }

    /// Build the final CheckResult
    pub fn build(self) -> CheckResult {
        let all_passed = self.checks.iter().all(|c| c.passed);
        let message = if all_passed {
            None
        } else {
            let failed: Vec<_> = self
                .checks
                .iter()
                .filter(|c| !c.passed)
                .filter_map(|c| c.message.clone())
                .collect();
            if failed.is_empty() {
                Some("One or more checks failed".to_string())
            } else {
                Some(failed.join("; "))
            }
        };

        CheckResult {
            success: all_passed,
            message,
            checks: self.checks,
        }
    }
}

impl Default for CheckResultBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for source connectors
///
/// Source connectors read data from external systems and produce events.
///
/// # Example
///
/// ```rust,ignore
/// use rivven_connect::prelude::*;
///
/// #[derive(Debug, Deserialize, Validate, JsonSchema)]
/// pub struct MySourceConfig {
///     #[validate(url)]
///     pub endpoint: String,
/// }
///
/// pub struct MySource;
///
/// #[async_trait]
/// impl Source for MySource {
///     type Config = MySourceConfig;
///
///     fn spec() -> ConnectorSpec {
///         ConnectorSpec::new("my-source", "1.0.0").build()
///     }
///
///     async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
///         Ok(CheckResult::success())
///     }
///
///     async fn discover(&self, config: &Self::Config) -> Result<Catalog> {
///         Ok(Catalog::default())
///     }
///
///     async fn read(
///         &self,
///         config: &Self::Config,
///         catalog: &ConfiguredCatalog,
///         state: Option<State>,
///     ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
///         todo!()
///     }
/// }
/// ```
#[async_trait]
pub trait Source: Send + Sync {
    /// Configuration type for this source
    type Config: SourceConfig;

    /// Return the connector specification
    ///
    /// This describes the connector's capabilities and configuration schema.
    fn spec() -> ConnectorSpec;

    /// Check connectivity and configuration
    ///
    /// This should validate:
    /// - Configuration values are valid
    /// - Connection to the external system works
    /// - Required permissions are available
    async fn check(&self, config: &Self::Config) -> Result<CheckResult>;

    /// Discover available streams
    ///
    /// This should return a catalog of all streams (tables, endpoints, etc.)
    /// that are available to sync from the source.
    async fn discover(&self, config: &Self::Config) -> Result<Catalog>;

    /// Read data from the source
    ///
    /// Returns a stream of events. For incremental sync, use the provided
    /// state to resume from the last position.
    ///
    /// # Arguments
    ///
    /// * `config` - Source configuration
    /// * `catalog` - User's configured streams and sync modes
    /// * `state` - Previous state for incremental sync (None for full refresh)
    async fn read(
        &self,
        config: &Self::Config,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>>;
}

/// Extension trait for source operations
#[async_trait]
pub trait SourceExt: Source {
    /// Validate configuration
    fn validate_config(config: &Self::Config) -> crate::error::ConnectorResult<()> {
        config
            .validate()
            .map_err(|e| crate::error::ConnectorError::Config(e.to_string()))
    }

    /// Get configuration schema as JSON
    fn config_schema_json() -> serde_json::Value {
        let schema = schemars::schema_for!(Self::Config);
        serde_json::to_value(schema).unwrap_or_default()
    }
}

// Blanket implementation
impl<T: Source> SourceExt for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_result() {
        let success = CheckResult::success();
        assert!(success.is_success());
        assert!(success.message.is_none());

        let failure = CheckResult::failure("connection refused");
        assert!(!failure.is_success());
        assert_eq!(failure.message, Some("connection refused".to_string()));
    }

    #[test]
    fn test_check_result_builder() {
        let result = CheckResult::builder()
            .check_passed("connectivity")
            .check_passed("authentication")
            .check_failed("permissions", "missing SELECT permission on table users")
            .build();

        assert!(!result.is_success());
        assert!(result.message.is_some());
        assert_eq!(result.checks.len(), 3);

        let failed: Vec<_> = result.failed_checks().collect();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].name, "permissions");
    }

    #[test]
    fn test_check_result_builder_all_passed() {
        let result = CheckResult::builder()
            .check_passed("connectivity")
            .check_passed("authentication")
            .check_passed("permissions")
            .build();

        assert!(result.is_success());
        assert!(result.message.is_none());
    }

    #[test]
    fn test_check_detail() {
        let detail = CheckDetail::passed("test")
            .with_message("all good")
            .with_duration_ms(150);

        assert!(detail.passed);
        assert_eq!(detail.message, Some("all good".to_string()));
        assert_eq!(detail.duration_ms, Some(150));
    }

    #[test]
    fn test_check_result_display() {
        let result = CheckResult::builder()
            .check_passed("connectivity")
            .check_failed("auth", "invalid credentials")
            .build();

        let display = format!("{}", result);
        assert!(display.contains("Connection check failed"));
        assert!(display.contains("connectivity"));
        assert!(display.contains("auth"));
    }

    #[test]
    fn test_check_if() {
        let result = CheckResult::builder()
            .check_if("port_valid", || {
                let port = 5432;
                if port > 0 && port < 65536 {
                    Ok(())
                } else {
                    Err("port out of range".to_string())
                }
            })
            .check_if("host_present", || {
                let host = "";
                if host.is_empty() {
                    Err("host is required".to_string())
                } else {
                    Ok(())
                }
            })
            .build();

        assert!(!result.is_success());
        assert_eq!(result.checks.len(), 2);
        assert!(result.checks[0].passed); // port_valid
        assert!(!result.checks[1].passed); // host_present
    }
}
