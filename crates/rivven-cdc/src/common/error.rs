//! Error types for CDC operations
//!
//! Feature-gated error variants for database-specific errors.
//! Includes error classification for intelligent retry and alerting.

use crate::common::resilience::RetriableErrorType;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Error categories for metrics and alerting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    /// Database-specific errors (connection, query, protocol)
    Database,
    /// Replication protocol errors (WAL, binlog)
    Replication,
    /// Schema-related errors (DDL, type mapping)
    Schema,
    /// Configuration errors (invalid settings)
    Configuration,
    /// Network errors (connection, timeout)
    Network,
    /// Serialization errors (JSON, Avro)
    Serialization,
    /// Other/unknown errors
    Other,
}

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

    /// SQL Server protocol error
    #[cfg(feature = "sqlserver")]
    #[error("SQL Server error: {0}")]
    SqlServer(String),

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

    /// Connection refused
    #[error("Connection refused: {0}")]
    ConnectionRefused(String),

    /// Invalid state
    #[error("Invalid state: {0}")]
    InvalidState(String),

    /// Replication slot in use
    #[error("Replication slot in use: {0}")]
    ReplicationSlotInUse(String),

    /// Deadlock detected
    #[error("Deadlock detected: {0}")]
    DeadlockDetected(String),

    /// Snapshot operation failed
    #[error("Snapshot failed: {0}")]
    SnapshotFailed(String),

    /// SMT transform error
    #[error("Transform error: {0}")]
    Transform(String),

    /// Checkpoint/offset store error
    #[error("Checkpoint error: {0}")]
    Checkpoint(String),

    /// Encryption/decryption error
    #[error("Encryption error: {0}")]
    Encryption(String),

    /// Rate limit exceeded
    #[error("Rate limit exceeded: {0}")]
    RateLimitExceeded(String),

    /// Authentication error
    #[error("Authentication failed: {0}")]
    Authentication(String),

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

    /// Create a connection refused error
    pub fn connection_refused(msg: impl Into<String>) -> Self {
        Self::ConnectionRefused(msg.into())
    }

    /// Create a replication slot in use error
    pub fn replication_slot_in_use(msg: impl Into<String>) -> Self {
        Self::ReplicationSlotInUse(msg.into())
    }

    /// Create a deadlock detected error
    pub fn deadlock_detected(msg: impl Into<String>) -> Self {
        Self::DeadlockDetected(msg.into())
    }

    /// Create a snapshot failed error
    pub fn snapshot_failed(msg: impl Into<String>) -> Self {
        Self::SnapshotFailed(msg.into())
    }

    /// Create a transform error
    pub fn transform(msg: impl Into<String>) -> Self {
        Self::Transform(msg.into())
    }

    /// Create a checkpoint error
    pub fn checkpoint(msg: impl Into<String>) -> Self {
        Self::Checkpoint(msg.into())
    }

    /// Create an encryption error
    pub fn encryption(msg: impl Into<String>) -> Self {
        Self::Encryption(msg.into())
    }

    /// Create a rate limit exceeded error
    pub fn rate_limit_exceeded(msg: impl Into<String>) -> Self {
        Self::RateLimitExceeded(msg.into())
    }

    /// Create an authentication error
    pub fn authentication(msg: impl Into<String>) -> Self {
        Self::Authentication(msg.into())
    }

    /// Create a SQL Server error
    #[cfg(feature = "sqlserver")]
    pub fn sqlserver(msg: impl Into<String>) -> Self {
        Self::SqlServer(msg.into())
    }

    /// Create a generic error
    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }

    /// Check if this error is retriable.
    ///
    /// Returns true for transient errors that may succeed on retry.
    pub fn is_retriable(&self) -> bool {
        match self {
            // Always retriable
            Self::ConnectionClosed => true,
            Self::ConnectionRefused(_) => true,
            Self::Timeout(_) => true,
            Self::DeadlockDetected(_) => true,
            Self::ReplicationSlotInUse(_) => true,

            // Replication errors may be retriable
            Self::Replication(msg) => {
                msg.contains("temporarily")
                    || msg.contains("connection reset")
                    || msg.contains("connection lost")
            }

            // PostgreSQL transient errors
            #[cfg(feature = "postgres")]
            Self::Postgres(e) => is_transient_pg_error(e),

            // MySQL transient errors
            #[cfg(feature = "mysql")]
            Self::MySql(msg) => {
                msg.contains("Lost connection")
                    || msg.contains("Deadlock")
                    || msg.contains("Lock wait timeout")
            }

            // I/O errors may be retriable
            Self::Io(e) => {
                use std::io::ErrorKind;
                matches!(
                    e.kind(),
                    ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::TimedOut
                        | ErrorKind::Interrupted
                )
            }

            // SQL Server transient errors
            #[cfg(feature = "sqlserver")]
            Self::SqlServer(msg) => {
                msg.contains("connection") || msg.contains("timeout") || msg.contains("deadlock")
            }

            // Rate limiting - always retriable after delay
            Self::RateLimitExceeded(_) => true,

            // Snapshot failures may be retriable (e.g., lock timeouts)
            Self::SnapshotFailed(msg) => {
                msg.contains("timeout") || msg.contains("lock") || msg.contains("temporarily")
            }

            // Non-retriable
            Self::Schema(_)
            | Self::Config(_)
            | Self::Filter(_)
            | Self::Topic(_)
            | Self::Serialization(_)
            | Self::Json(_)
            | Self::InvalidState(_)
            | Self::Transform(_)
            | Self::Checkpoint(_)
            | Self::Encryption(_)
            | Self::Authentication(_)
            | Self::Other(_) => false,
        }
    }

    /// Get the retriable error type, if applicable.
    pub fn retriable_error_type(&self) -> Option<RetriableErrorType> {
        match self {
            Self::ConnectionClosed => Some(RetriableErrorType::ConnectionLost),
            Self::ConnectionRefused(_) => Some(RetriableErrorType::ConnectionRefused),
            Self::Timeout(_) => Some(RetriableErrorType::Timeout),
            Self::DeadlockDetected(_) => Some(RetriableErrorType::DeadlockDetected),
            Self::ReplicationSlotInUse(_) => Some(RetriableErrorType::ReplicationSlotInUse),
            Self::RateLimitExceeded(_) => Some(RetriableErrorType::TemporaryFailure),
            Self::Replication(msg) if msg.contains("temporarily") => {
                Some(RetriableErrorType::TemporaryFailure)
            }
            _ if self.is_retriable() => Some(RetriableErrorType::TemporaryFailure),
            _ => None,
        }
    }

    /// Get the error category for metrics and alerting.
    pub fn category(&self) -> ErrorCategory {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => ErrorCategory::Database,
            #[cfg(feature = "mysql")]
            Self::MySql(_) => ErrorCategory::Database,
            #[cfg(feature = "sqlserver")]
            Self::SqlServer(_) => ErrorCategory::Database,
            Self::Replication(_) => ErrorCategory::Replication,
            Self::ReplicationSlotInUse(_) => ErrorCategory::Replication,
            Self::SnapshotFailed(_) => ErrorCategory::Database,
            Self::Schema(_) => ErrorCategory::Schema,
            Self::Config(_) => ErrorCategory::Configuration,
            Self::Filter(_) => ErrorCategory::Configuration,
            Self::Topic(_) => ErrorCategory::Configuration,
            Self::Timeout(_) => ErrorCategory::Network,
            Self::ConnectionClosed => ErrorCategory::Network,
            Self::ConnectionRefused(_) => ErrorCategory::Network,
            Self::Io(_) => ErrorCategory::Network,
            Self::RateLimitExceeded(_) => ErrorCategory::Network,
            Self::Serialization(_) => ErrorCategory::Serialization,
            Self::Json(_) => ErrorCategory::Serialization,
            Self::Transform(_) => ErrorCategory::Serialization,
            Self::DeadlockDetected(_) => ErrorCategory::Database,
            Self::Checkpoint(_) => ErrorCategory::Database,
            Self::Encryption(_) => ErrorCategory::Serialization,
            Self::Authentication(_) => ErrorCategory::Database,
            Self::InvalidState(_) => ErrorCategory::Other,
            Self::Other(_) => ErrorCategory::Other,
        }
    }

    /// Get a metric-safe error code.
    pub fn error_code(&self) -> &'static str {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => "postgres_error",
            #[cfg(feature = "mysql")]
            Self::MySql(_) => "mysql_error",
            #[cfg(feature = "sqlserver")]
            Self::SqlServer(_) => "sqlserver_error",
            Self::Replication(_) => "replication_error",
            Self::ReplicationSlotInUse(_) => "slot_in_use",
            Self::SnapshotFailed(_) => "snapshot_failed",
            Self::Schema(_) => "schema_error",
            Self::Config(_) => "config_error",
            Self::Filter(_) => "filter_error",
            Self::Topic(_) => "topic_error",
            Self::Timeout(_) => "timeout",
            Self::ConnectionClosed => "connection_closed",
            Self::ConnectionRefused(_) => "connection_refused",
            Self::Io(_) => "io_error",
            Self::RateLimitExceeded(_) => "rate_limit_exceeded",
            Self::Serialization(_) => "serialization_error",
            Self::Json(_) => "json_error",
            Self::Transform(_) => "transform_error",
            Self::DeadlockDetected(_) => "deadlock",
            Self::Checkpoint(_) => "checkpoint_error",
            Self::Encryption(_) => "encryption_error",
            Self::Authentication(_) => "auth_error",
            Self::InvalidState(_) => "invalid_state",
            Self::Other(_) => "unknown",
        }
    }
}

/// Check if a PostgreSQL error is transient.
#[cfg(feature = "postgres")]
fn is_transient_pg_error(e: &tokio_postgres::Error) -> bool {
    // Check SQLSTATE codes for transient errors
    if let Some(db_error) = e.as_db_error() {
        let code = db_error.code().code();
        // Connection exception class (08xxx)
        if code.starts_with("08") {
            return true;
        }
        // Transaction rollback class (40xxx)
        if code.starts_with("40") {
            return true;
        }
        // Insufficient resources class (53xxx)
        if code.starts_with("53") {
            return true;
        }
        // Operator intervention class (57xxx) - except query_canceled
        if code.starts_with("57") && code != "57014" {
            return true;
        }
    }

    // Check error message for connection issues
    let msg = e.to_string().to_lowercase();
    msg.contains("connection")
        || msg.contains("closed")
        || msg.contains("timeout")
        || msg.contains("temporarily")
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
        let _ = CdcError::snapshot_failed("Lock timeout");
        let _ = CdcError::transform("Invalid field");
        let _ = CdcError::checkpoint("Write failed");
        let _ = CdcError::encryption("Key not found");
        let _ = CdcError::rate_limit_exceeded("100 req/s");
        let _ = CdcError::authentication("Invalid credentials");
    }

    #[test]
    fn test_error_is_retriable() {
        // Always retriable
        assert!(CdcError::ConnectionClosed.is_retriable());
        assert!(CdcError::connection_refused("host:5432").is_retriable());
        assert!(CdcError::timeout("5s").is_retriable());
        assert!(CdcError::deadlock_detected("txn 123").is_retriable());
        assert!(CdcError::replication_slot_in_use("slot_name").is_retriable());
        assert!(CdcError::rate_limit_exceeded("100/s").is_retriable());

        // Conditionally retriable
        assert!(CdcError::snapshot_failed("lock timeout").is_retriable());
        assert!(!CdcError::snapshot_failed("invalid schema").is_retriable());

        // Never retriable
        assert!(!CdcError::config("bad config").is_retriable());
        assert!(!CdcError::schema("invalid type").is_retriable());
        assert!(!CdcError::other("unknown").is_retriable());
        assert!(!CdcError::transform("bad field").is_retriable());
        assert!(!CdcError::checkpoint("write failed").is_retriable());
        assert!(!CdcError::encryption("key error").is_retriable());
        assert!(!CdcError::authentication("invalid creds").is_retriable());
    }

    #[test]
    fn test_error_category() {
        assert_eq!(
            CdcError::replication("x").category(),
            ErrorCategory::Replication
        );
        assert_eq!(CdcError::schema("x").category(), ErrorCategory::Schema);
        assert_eq!(
            CdcError::config("x").category(),
            ErrorCategory::Configuration
        );
        assert_eq!(CdcError::timeout("x").category(), ErrorCategory::Network);
        assert_eq!(
            CdcError::ConnectionClosed.category(),
            ErrorCategory::Network
        );
        assert_eq!(CdcError::other("x").category(), ErrorCategory::Other);
        assert_eq!(
            CdcError::snapshot_failed("x").category(),
            ErrorCategory::Database
        );
        assert_eq!(
            CdcError::transform("x").category(),
            ErrorCategory::Serialization
        );
        assert_eq!(
            CdcError::checkpoint("x").category(),
            ErrorCategory::Database
        );
        assert_eq!(
            CdcError::encryption("x").category(),
            ErrorCategory::Serialization
        );
        assert_eq!(
            CdcError::rate_limit_exceeded("x").category(),
            ErrorCategory::Network
        );
        assert_eq!(
            CdcError::authentication("x").category(),
            ErrorCategory::Database
        );
    }

    #[test]
    fn test_error_retriable_type() {
        assert_eq!(
            CdcError::ConnectionClosed.retriable_error_type(),
            Some(RetriableErrorType::ConnectionLost)
        );
        assert_eq!(
            CdcError::timeout("x").retriable_error_type(),
            Some(RetriableErrorType::Timeout)
        );
        assert_eq!(
            CdcError::deadlock_detected("x").retriable_error_type(),
            Some(RetriableErrorType::DeadlockDetected)
        );
        assert_eq!(
            CdcError::rate_limit_exceeded("x").retriable_error_type(),
            Some(RetriableErrorType::TemporaryFailure)
        );
        assert_eq!(CdcError::config("x").retriable_error_type(), None);
    }

    #[test]
    fn test_error_code() {
        assert_eq!(CdcError::ConnectionClosed.error_code(), "connection_closed");
        assert_eq!(CdcError::timeout("x").error_code(), "timeout");
        assert_eq!(CdcError::config("x").error_code(), "config_error");
        assert_eq!(
            CdcError::snapshot_failed("x").error_code(),
            "snapshot_failed"
        );
        assert_eq!(CdcError::transform("x").error_code(), "transform_error");
        assert_eq!(CdcError::checkpoint("x").error_code(), "checkpoint_error");
        assert_eq!(CdcError::encryption("x").error_code(), "encryption_error");
        assert_eq!(
            CdcError::rate_limit_exceeded("x").error_code(),
            "rate_limit_exceeded"
        );
        assert_eq!(CdcError::authentication("x").error_code(), "auth_error");
    }
}
