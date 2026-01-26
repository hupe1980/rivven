//! Error types for CDC operations
//!
//! Feature-gated error variants for database-specific errors.

use thiserror::Error;

/// CDC-specific errors
#[derive(Error, Debug)]
pub enum CdcError {
    /// PostgreSQL connection error
    #[cfg(feature = "postgres")]
    #[error("PostgreSQL error: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    /// MySQL protocol error
    #[cfg(feature = "mysql")]
    #[error("MySQL error: {0}")]
    MySql(String),

    /// Replication protocol error
    #[error("Replication error: {0}")]
    Replication(String),

    /// Schema inference error
    #[error("Schema error: {0}")]
    Schema(String),

    /// Topic/routing error
    #[error("Topic error: {0}")]
    Topic(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Filter error
    #[error("Filter error: {0}")]
    Filter(String),

    /// Core error from rivven-core
    #[error("Core error: {0}")]
    Core(#[from] rivven_core::Error),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// JSON serialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// I/O error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Timeout error
    #[error("Timeout: {0}")]
    Timeout(String),

    /// Connection closed
    #[error("Connection closed")]
    ConnectionClosed,

    /// Invalid state
    #[error("Invalid state: {0}")]
    InvalidState(String),

    /// Generic error
    #[error("{0}")]
    Other(String),
}

impl CdcError {
    /// Create a new MySQL error
    #[cfg(feature = "mysql")]
    pub fn mysql(msg: impl Into<String>) -> Self {
        Self::MySql(msg.into())
    }

    /// Create a new replication error
    pub fn replication(msg: impl Into<String>) -> Self {
        Self::Replication(msg.into())
    }

    /// Create a new schema error
    pub fn schema(msg: impl Into<String>) -> Self {
        Self::Schema(msg.into())
    }

    /// Create a new topic error
    pub fn topic(msg: impl Into<String>) -> Self {
        Self::Topic(msg.into())
    }

    /// Create a new config error
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    /// Create a new serialization error
    pub fn serialization(msg: impl Into<String>) -> Self {
        Self::Serialization(msg.into())
    }

    /// Create a timeout error
    pub fn timeout(msg: impl Into<String>) -> Self {
        Self::Timeout(msg.into())
    }

    /// Create a generic error
    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}

/// Result type for CDC operations
pub type Result<T> = std::result::Result<T, CdcError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = CdcError::replication("Connection lost");
        assert!(err.to_string().contains("Replication error"));
        assert!(err.to_string().contains("Connection lost"));
    }

    #[test]
    fn test_error_constructors() {
        let _ = CdcError::schema("Invalid type");
        let _ = CdcError::config("Missing option");
        let _ = CdcError::timeout("5 seconds");
        let _ = CdcError::other("Unknown error");
    }
}
