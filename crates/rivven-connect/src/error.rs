//! Error types for rivven-connect
//!
//! Provides structured error handling for both the Connect runtime and connectors.

use std::fmt;
use thiserror::Error;

/// Result type alias for rivven-connect runtime
pub type Result<T> = std::result::Result<T, ConnectError>;

/// Result type alias for connector operations
pub type ConnectorResult<T> = std::result::Result<T, ConnectorError>;

/// Main error type for rivven-connect runtime
#[derive(Error, Debug)]
pub enum ConnectError {
    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Broker connection error
    #[error("Broker connection error: {0}")]
    BrokerConnection(String),

    /// Source connector error
    #[error("Source '{name}' error: {message}")]
    Source { name: String, message: String },

    /// Sink connector error
    #[error("Sink '{name}' error: {message}")]
    Sink { name: String, message: String },

    /// Topic error
    #[error("Topic error: {0}")]
    Topic(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Shutdown requested
    #[error("Shutdown requested")]
    Shutdown,

    /// Retryable error (can be retried after backoff)
    #[error("Retryable error: {message} (attempt {attempt}/{max_attempts})")]
    Retryable {
        message: String,
        attempt: u32,
        max_attempts: u32,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Permanent error (should not be retried)
    #[error("Permanent error: {0}")]
    Permanent(String),

    /// Connector error (from individual connectors)
    #[error(transparent)]
    Connector(#[from] ConnectorError),
}

/// Errors that can occur in connector operations
#[derive(Debug, Error)]
pub enum ConnectorError {
    /// Configuration validation failed
    #[error("configuration error: {0}")]
    Config(String),

    /// Connection to external system failed
    #[error("connection error: {0}")]
    Connection(String),

    /// Authentication failed
    #[error("authentication error: {0}")]
    Auth(String),

    /// Permission denied
    #[error("permission denied: {0}")]
    Permission(String),

    /// Resource not found
    #[error("not found: {0}")]
    NotFound(String),

    /// Rate limited by external system
    #[error("rate limited: {0}")]
    RateLimited(String),

    /// Timeout waiting for response
    #[error("timeout: {0}")]
    Timeout(String),

    /// Data serialization/deserialization error
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Schema mismatch or validation error
    #[error("schema error: {0}")]
    Schema(String),

    /// State management error
    #[error("state error: {0}")]
    State(String),

    /// Transient error that may succeed on retry
    #[error("transient error (retryable): {0}")]
    Transient(String),

    /// Fatal error that will not succeed on retry
    #[error("fatal error: {0}")]
    Fatal(String),

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),

    /// IO error
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON error
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// YAML error
    #[error("yaml error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    /// Generic error
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

impl ConnectorError {
    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Connection(_) | Self::RateLimited(_) | Self::Timeout(_) | Self::Transient(_)
        )
    }

    /// Create a configuration error
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    /// Create a connection error
    pub fn connection(msg: impl Into<String>) -> Self {
        Self::Connection(msg.into())
    }

    /// Create a transient error
    pub fn transient(msg: impl Into<String>) -> Self {
        Self::Transient(msg.into())
    }

    /// Create a fatal error
    pub fn fatal(msg: impl Into<String>) -> Self {
        Self::Fatal(msg.into())
    }
}

impl ConnectError {
    /// Create a config error
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    /// Create a broker connection error
    pub fn broker(msg: impl Into<String>) -> Self {
        Self::BrokerConnection(msg.into())
    }

    /// Create a source error
    pub fn source(name: impl Into<String>, msg: impl Into<String>) -> Self {
        Self::Source {
            name: name.into(),
            message: msg.into(),
        }
    }

    /// Create a sink error
    pub fn sink(name: impl Into<String>, msg: impl Into<String>) -> Self {
        Self::Sink {
            name: name.into(),
            message: msg.into(),
        }
    }

    /// Create a retryable error
    pub fn retryable(msg: impl Into<String>, attempt: u32, max_attempts: u32) -> Self {
        Self::Retryable {
            message: msg.into(),
            attempt,
            max_attempts,
            source: None,
        }
    }

    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable { .. } | Self::BrokerConnection(_))
    }

    /// Check if this is a shutdown error
    pub fn is_shutdown(&self) -> bool {
        matches!(self, Self::Shutdown)
    }
}

/// Connector status for health checks
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorStatus {
    /// Starting up
    Starting,
    /// Running normally
    Running,
    /// Temporarily unhealthy (reconnecting)
    Unhealthy,
    /// Stopped
    Stopped,
    /// Failed permanently
    Failed,
}

impl fmt::Display for ConnectorStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Starting => write!(f, "starting"),
            Self::Running => write!(f, "running"),
            Self::Unhealthy => write!(f, "unhealthy"),
            Self::Stopped => write!(f, "stopped"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = ConnectError::source("my-source", "connection timeout");
        assert_eq!(
            err.to_string(),
            "Source 'my-source' error: connection timeout"
        );
    }

    #[test]
    fn test_retryable_check() {
        assert!(ConnectError::broker("timeout").is_retryable());
        assert!(ConnectError::retryable("network error", 1, 3).is_retryable());
        assert!(!ConnectError::config("bad config").is_retryable());
    }

    #[test]
    fn test_shutdown_check() {
        assert!(ConnectError::Shutdown.is_shutdown());
        assert!(!ConnectError::broker("test").is_shutdown());
    }

    #[test]
    fn test_connector_error_retryable() {
        assert!(ConnectorError::Connection("timeout".to_string()).is_retryable());
        assert!(ConnectorError::RateLimited("slow down".to_string()).is_retryable());
        assert!(ConnectorError::Timeout("5s".to_string()).is_retryable());
        assert!(ConnectorError::Transient("temp failure".to_string()).is_retryable());
        assert!(!ConnectorError::Config("bad config".to_string()).is_retryable());
        assert!(!ConnectorError::Fatal("unrecoverable".to_string()).is_retryable());
    }
}
