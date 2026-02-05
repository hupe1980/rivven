//! Unit tests for rivven-rdbc error module

use rivven_rdbc::error::{Error, ErrorCategory};

#[test]
fn test_error_connection() {
    let err = Error::connection("Failed to connect");

    assert_eq!(err.category(), ErrorCategory::Connection);
    assert!(err.to_string().contains("Failed to connect"));
    assert!(err.is_retriable());
}

#[test]
fn test_error_config() {
    let err = Error::config("Invalid URL format");

    assert_eq!(err.category(), ErrorCategory::Configuration);
    assert!(err.to_string().contains("Invalid URL format"));
    assert!(!err.is_retriable());
}

#[test]
fn test_error_query() {
    let err = Error::query("Syntax error in SQL");

    assert_eq!(err.category(), ErrorCategory::Query);
    assert!(err.to_string().contains("Syntax error"));
    assert!(!err.is_retriable());
}

#[test]
fn test_error_transaction() {
    let err = Error::transaction("Transaction aborted");

    assert_eq!(err.category(), ErrorCategory::Transaction);
    assert!(err.to_string().contains("Transaction aborted"));
}

#[test]
fn test_error_execution() {
    let err = Error::execution("Query failed");

    assert_eq!(err.category(), ErrorCategory::Query);
    assert!(err.to_string().contains("Query failed"));
}

#[test]
fn test_error_unsupported() {
    let err = Error::unsupported("Feature X not available");

    assert_eq!(err.category(), ErrorCategory::Other);
    assert!(err.to_string().contains("Feature X"));
}

#[test]
fn test_error_timeout() {
    let err = Error::timeout("Query timeout after 30s");

    assert_eq!(err.category(), ErrorCategory::Timeout);
    assert!(err.to_string().contains("timeout"));
    assert!(err.is_retriable());
}

#[test]
fn test_error_schema() {
    let err = Error::schema("Table not found");

    assert_eq!(err.category(), ErrorCategory::Schema);
    assert!(err.to_string().contains("Table not found"));
}

#[test]
fn test_error_type_conversion() {
    let err = Error::type_conversion("Cannot convert i64 to string");

    assert_eq!(err.category(), ErrorCategory::TypeConversion);
    assert!(err.to_string().contains("Cannot convert"));
}

#[test]
fn test_error_deadlock() {
    let err = Error::Deadlock;

    assert_eq!(err.category(), ErrorCategory::Deadlock);
    assert!(err.is_retriable());
}

#[test]
fn test_error_constraint() {
    let err = Error::Constraint {
        constraint_name: "users_email_key".to_string(),
        message: "duplicate key value".to_string(),
    };

    assert_eq!(err.category(), ErrorCategory::Constraint);
    assert!(!err.is_retriable());
}

#[test]
fn test_error_display() {
    let err = Error::connection("Connection refused: host=localhost port=5432");
    let display = format!("{}", err);

    assert!(display.contains("Connection refused"));
    assert!(display.contains("localhost"));
}

#[test]
fn test_error_debug() {
    let err = Error::query("SELECT * FROM invalid_table");
    let debug = format!("{:?}", err);

    // Debug should include kind and message
    assert!(!debug.is_empty());
}

#[test]
fn test_result_type() {
    fn test_fn() -> rivven_rdbc::error::Result<i32> {
        Ok(42)
    }

    assert_eq!(test_fn().unwrap(), 42);
}

#[test]
fn test_result_error() {
    fn test_fn() -> rivven_rdbc::error::Result<i32> {
        Err(Error::query("Test error"))
    }

    assert!(test_fn().is_err());
}

#[test]
fn test_query_with_sql() {
    let err = Error::query_with_sql("Syntax error", "SELECT * FORM users");

    assert_eq!(err.category(), ErrorCategory::Query);
    assert!(err.to_string().contains("Syntax error"));
}

#[test]
fn test_pool_exhausted() {
    let err = Error::PoolExhausted {
        message: "No connections available".to_string(),
    };

    assert_eq!(err.category(), ErrorCategory::PoolExhausted);
    assert!(err.is_retriable());
}

#[test]
fn test_table_not_found() {
    let err = Error::TableNotFound {
        table: "unknown_table".to_string(),
    };

    assert_eq!(err.category(), ErrorCategory::Schema);
    assert!(err.to_string().contains("unknown_table"));
}

#[test]
fn test_column_not_found() {
    let err = Error::ColumnNotFound {
        table: "users".to_string(),
        column: "unknown_column".to_string(),
    };

    assert_eq!(err.category(), ErrorCategory::Schema);
    assert!(err.to_string().contains("unknown_column"));
    assert!(err.to_string().contains("users"));
}

#[test]
fn test_error_category_is_retriable() {
    assert!(ErrorCategory::Connection.is_retriable());
    assert!(ErrorCategory::Timeout.is_retriable());
    assert!(ErrorCategory::Deadlock.is_retriable());
    assert!(ErrorCategory::PoolExhausted.is_retriable());

    assert!(!ErrorCategory::Query.is_retriable());
    assert!(!ErrorCategory::Transaction.is_retriable());
    assert!(!ErrorCategory::Constraint.is_retriable());
    assert!(!ErrorCategory::TypeConversion.is_retriable());
    assert!(!ErrorCategory::Authentication.is_retriable());
    assert!(!ErrorCategory::Configuration.is_retriable());
    assert!(!ErrorCategory::Schema.is_retriable());
    assert!(!ErrorCategory::Other.is_retriable());
}
