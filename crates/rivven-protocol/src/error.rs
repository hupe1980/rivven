//! Protocol error types

use thiserror::Error;

/// Protocol error types
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Deserialization error
    #[error("Deserialization error: {0}")]
    Deserialization(String),

    /// Message too large
    #[error("Message size {0} exceeds maximum {1}")]
    MessageTooLarge(usize, usize),

    /// Invalid message format
    #[error("Invalid message format: {0}")]
    InvalidFormat(String),

    /// Protocol version mismatch
    #[error("Protocol version mismatch: expected {expected}, got {actual}")]
    VersionMismatch { expected: u32, actual: u32 },
}

impl From<postcard::Error> for ProtocolError {
    fn from(e: postcard::Error) -> Self {
        ProtocolError::Serialization(e.to_string())
    }
}

/// Result type for protocol operations
pub type Result<T> = std::result::Result<T, ProtocolError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = ProtocolError::Serialization("test".to_string());
        assert_eq!(err.to_string(), "Serialization error: test");

        let err = ProtocolError::Deserialization("bad data".to_string());
        assert_eq!(err.to_string(), "Deserialization error: bad data");

        let err = ProtocolError::MessageTooLarge(1000, 500);
        assert_eq!(err.to_string(), "Message size 1000 exceeds maximum 500");

        let err = ProtocolError::InvalidFormat("missing field".to_string());
        assert_eq!(err.to_string(), "Invalid message format: missing field");

        let err = ProtocolError::VersionMismatch {
            expected: 1,
            actual: 2,
        };
        assert_eq!(
            err.to_string(),
            "Protocol version mismatch: expected 1, got 2"
        );
    }

    #[test]
    fn test_error_debug() {
        let err = ProtocolError::Serialization("test".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Serialization"));
    }

    #[test]
    fn test_result_type() {
        fn returns_ok() -> Result<i32> {
            Ok(42)
        }
        let ok = returns_ok();
        assert!(ok.is_ok());
        assert_eq!(ok.unwrap(), 42);

        fn returns_err() -> Result<i32> {
            Err(ProtocolError::InvalidFormat("test".to_string()))
        }
        let err = returns_err();
        assert!(err.is_err());
    }
}
