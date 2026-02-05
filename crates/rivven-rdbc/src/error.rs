//! Error types for rivven-rdbc
//!
//! Provides granular error classification for proper retry handling:
//! - Retriable errors (connection, timeout, deadlock)
//! - Non-retriable errors (constraint violations, type errors)

use std::fmt;
use thiserror::Error;

/// Result type for rivven-rdbc operations
pub type Result<T> = std::result::Result<T, Error>;

/// Error categories for classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    /// Connection-related errors (retriable)
    Connection,
    /// Query execution errors
    Query,
    /// Transaction errors
    Transaction,
    /// Constraint violation (not retriable)
    Constraint,
    /// Type conversion errors (not retriable)
    TypeConversion,
    /// Timeout errors (retriable)
    Timeout,
    /// Deadlock detected (retriable)
    Deadlock,
    /// Authentication failure
    Authentication,
    /// Configuration error
    Configuration,
    /// Pool exhausted (retriable with backoff)
    PoolExhausted,
    /// Schema-related errors
    Schema,
    /// Unknown/other errors
    Other,
}

impl ErrorCategory {
    /// Whether errors in this category are generally retriable
    #[inline]
    pub const fn is_retriable(self) -> bool {
        matches!(
            self,
            Self::Connection | Self::Timeout | Self::Deadlock | Self::PoolExhausted
        )
    }
}

/// Main error type for rivven-rdbc
#[derive(Error, Debug)]
#[allow(missing_docs)]
pub enum Error {
    /// Connection failed
    #[error("connection error: {message}")]
    Connection {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Query execution failed
    #[error("query error: {message}")]
    Query {
        message: String,
        sql: Option<String>,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Transaction error
    #[error("transaction error: {message}")]
    Transaction {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Constraint violation (PK, FK, unique, check)
    #[error("constraint violation: {constraint_name} - {message}")]
    Constraint {
        constraint_name: String,
        message: String,
    },

    /// Type conversion failed
    #[error("type conversion error: {message}")]
    TypeConversion { message: String },

    /// Operation timed out
    #[error("timeout: {message}")]
    Timeout { message: String },

    /// Deadlock detected
    #[error("deadlock detected")]
    Deadlock,

    /// Authentication failed
    #[error("authentication failed: {message}")]
    Authentication { message: String },

    /// Configuration error
    #[error("configuration error: {message}")]
    Configuration { message: String },

    /// Connection pool exhausted
    #[error("pool exhausted: {message}")]
    PoolExhausted { message: String },

    /// Schema error (table not found, column mismatch)
    #[error("schema error: {message}")]
    Schema { message: String },

    /// Prepared statement not found
    #[error("prepared statement not found: {name}")]
    PreparedStatementNotFound { name: String },

    /// Table not found
    #[error("table not found: {table}")]
    TableNotFound { table: String },

    /// Column not found
    #[error("column not found: {column} in table {table}")]
    ColumnNotFound { table: String, column: String },

    /// Unsupported operation for this backend
    #[error("unsupported: {message}")]
    Unsupported { message: String },

    /// Internal error
    #[error("internal error: {message}")]
    Internal { message: String },
}

impl Error {
    /// Get the error category
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::Connection { .. } => ErrorCategory::Connection,
            Self::Query { .. } => ErrorCategory::Query,
            Self::Transaction { .. } => ErrorCategory::Transaction,
            Self::Constraint { .. } => ErrorCategory::Constraint,
            Self::TypeConversion { .. } => ErrorCategory::TypeConversion,
            Self::Timeout { .. } => ErrorCategory::Timeout,
            Self::Deadlock => ErrorCategory::Deadlock,
            Self::Authentication { .. } => ErrorCategory::Authentication,
            Self::Configuration { .. } => ErrorCategory::Configuration,
            Self::PoolExhausted { .. } => ErrorCategory::PoolExhausted,
            Self::Schema { .. } | Self::TableNotFound { .. } | Self::ColumnNotFound { .. } => {
                ErrorCategory::Schema
            }
            Self::PreparedStatementNotFound { .. } => ErrorCategory::Query,
            Self::Unsupported { .. } | Self::Internal { .. } => ErrorCategory::Other,
        }
    }

    /// Whether this error is retriable
    #[inline]
    pub fn is_retriable(&self) -> bool {
        self.category().is_retriable()
    }

    /// Create a connection error
    pub fn connection(message: impl Into<String>) -> Self {
        Self::Connection {
            message: message.into(),
            source: None,
        }
    }

    /// Create a connection error with source
    pub fn connection_with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Connection {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Create a query error
    pub fn query(message: impl Into<String>) -> Self {
        Self::Query {
            message: message.into(),
            sql: None,
            source: None,
        }
    }

    /// Create a query error with SQL
    pub fn query_with_sql(message: impl Into<String>, sql: impl Into<String>) -> Self {
        Self::Query {
            message: message.into(),
            sql: Some(sql.into()),
            source: None,
        }
    }

    /// Create a timeout error
    pub fn timeout(message: impl Into<String>) -> Self {
        Self::Timeout {
            message: message.into(),
        }
    }

    /// Create a configuration error
    pub fn config(message: impl Into<String>) -> Self {
        Self::Configuration {
            message: message.into(),
        }
    }

    /// Create a type conversion error
    pub fn type_conversion(message: impl Into<String>) -> Self {
        Self::TypeConversion {
            message: message.into(),
        }
    }

    /// Create a schema error
    pub fn schema(message: impl Into<String>) -> Self {
        Self::Schema {
            message: message.into(),
        }
    }

    /// Create a transaction error
    pub fn transaction(message: impl Into<String>) -> Self {
        Self::Transaction {
            message: message.into(),
            source: None,
        }
    }

    /// Create an execution error (alias for query)
    pub fn execution(message: impl Into<String>) -> Self {
        Self::Query {
            message: message.into(),
            sql: None,
            source: None,
        }
    }

    /// Create an unsupported operation error
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::Unsupported {
            message: message.into(),
        }
    }
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connection => write!(f, "connection"),
            Self::Query => write!(f, "query"),
            Self::Transaction => write!(f, "transaction"),
            Self::Constraint => write!(f, "constraint"),
            Self::TypeConversion => write!(f, "type_conversion"),
            Self::Timeout => write!(f, "timeout"),
            Self::Deadlock => write!(f, "deadlock"),
            Self::Authentication => write!(f, "authentication"),
            Self::Configuration => write!(f, "configuration"),
            Self::PoolExhausted => write!(f, "pool_exhausted"),
            Self::Schema => write!(f, "schema"),
            Self::Other => write!(f, "other"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_category_retriable() {
        assert!(ErrorCategory::Connection.is_retriable());
        assert!(ErrorCategory::Timeout.is_retriable());
        assert!(ErrorCategory::Deadlock.is_retriable());
        assert!(ErrorCategory::PoolExhausted.is_retriable());

        assert!(!ErrorCategory::Constraint.is_retriable());
        assert!(!ErrorCategory::TypeConversion.is_retriable());
        assert!(!ErrorCategory::Query.is_retriable());
    }

    #[test]
    fn test_error_is_retriable() {
        assert!(Error::connection("failed").is_retriable());
        assert!(Error::timeout("timed out").is_retriable());
        assert!(Error::Deadlock.is_retriable());

        assert!(!Error::Constraint {
            constraint_name: "pk".into(),
            message: "duplicate".into()
        }
        .is_retriable());
    }

    #[test]
    fn test_error_display() {
        let err = Error::connection("connection refused");
        assert!(err.to_string().contains("connection refused"));

        let err = Error::query_with_sql("syntax error", "SELECT * FORM users");
        assert!(err.to_string().contains("syntax error"));
    }
}
