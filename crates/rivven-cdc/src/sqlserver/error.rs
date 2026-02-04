//! SQL Server CDC error types

use crate::common::CdcError;
use thiserror::Error;

/// SQL Server-specific CDC errors
#[derive(Error, Debug)]
pub enum SqlServerError {
    /// TDS protocol error
    #[error("TDS protocol error: {0}")]
    Tds(String),

    /// Authentication failure
    #[error("Authentication failed: {0}")]
    Authentication(String),

    /// CDC not enabled on database
    #[error("CDC not enabled on database '{0}'. Run: EXEC sys.sp_cdc_enable_db")]
    CdcNotEnabled(String),

    /// CDC not enabled on table
    #[error("CDC not enabled on table '{schema}.{table}'. Run: EXEC sys.sp_cdc_enable_table @source_schema=N'{schema}', @source_name=N'{table}', @role_name=NULL")]
    TableNotEnabled { schema: String, table: String },

    /// SQL Server Agent not running (required for CDC capture)
    #[error("SQL Server Agent not running. CDC capture requires SQL Agent to be active")]
    AgentNotRunning,

    /// Invalid LSN format
    #[error("Invalid LSN format: {0}")]
    InvalidLsn(String),

    /// No capture instance found for table
    #[error("No capture instance found for '{schema}.{table}'")]
    NoCaptureInstance { schema: String, table: String },

    /// CDC table query failed
    #[error("CDC table query failed: {0}")]
    QueryFailed(String),

    /// Connection error
    #[error("Connection error: {0}")]
    Connection(String),

    /// Timeout waiting for changes
    #[error("Polling timeout after {0}ms")]
    PollTimeout(u64),

    /// Schema mismatch between CDC table and source
    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    /// Transaction log truncated (LSN no longer available)
    #[error("Transaction log truncated. LSN {0} no longer available. Consider re-snapshotting.")]
    LogTruncated(String),

    /// Maximum LSN query failed
    #[error("Failed to get maximum LSN: {0}")]
    MaxLsnFailed(String),
}

impl From<SqlServerError> for CdcError {
    fn from(err: SqlServerError) -> Self {
        match err {
            SqlServerError::Authentication(msg) => CdcError::ConnectionRefused(msg),
            SqlServerError::Connection(msg) => CdcError::ConnectionRefused(msg),
            SqlServerError::CdcNotEnabled(db) => CdcError::config(format!(
                "CDC not enabled on database '{}'. Run: EXEC sys.sp_cdc_enable_db",
                db
            )),
            SqlServerError::TableNotEnabled { schema, table } => {
                CdcError::config(format!("CDC not enabled on table '{}.{}'", schema, table))
            }
            SqlServerError::InvalidLsn(msg) => CdcError::Replication(msg),
            SqlServerError::QueryFailed(msg) => CdcError::Replication(msg),
            SqlServerError::LogTruncated(lsn) => CdcError::Replication(format!(
                "Transaction log truncated. LSN {} no longer available",
                lsn
            )),
            _ => CdcError::Replication(err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_messages() {
        let err = SqlServerError::CdcNotEnabled("testdb".to_string());
        assert!(err.to_string().contains("sp_cdc_enable_db"));

        let err = SqlServerError::TableNotEnabled {
            schema: "dbo".to_string(),
            table: "users".to_string(),
        };
        assert!(err.to_string().contains("sp_cdc_enable_table"));
        assert!(err.to_string().contains("dbo"));
        assert!(err.to_string().contains("users"));
    }

    #[test]
    fn test_conversion_to_cdc_error() {
        let err = SqlServerError::Connection("network error".to_string());
        let cdc_err: CdcError = err.into();
        assert!(matches!(cdc_err, CdcError::ConnectionRefused(_)));
    }
}
