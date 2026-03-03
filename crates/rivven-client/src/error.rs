use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("IO error ({0:?}): {1}")]
    IoError(std::io::ErrorKind, String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] postcard::Error),

    #[error("Protocol error: {0}")]
    ProtocolError(#[from] rivven_protocol::ProtocolError),

    #[error("Server error: {0}")]
    ServerError(String),

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Invalid response")]
    InvalidResponse,

    #[error("Response too large: {0} bytes (max: {1})")]
    ResponseTooLarge(usize, usize),

    #[error("Request too large: {0} bytes (max: {1})")]
    RequestTooLarge(usize, usize),

    #[error("Circuit breaker open for server: {0}")]
    CircuitBreakerOpen(String),

    #[error("Connection pool exhausted: {0}")]
    PoolExhausted(String),

    #[error("All servers unavailable")]
    AllServersUnavailable,

    #[error("Request timeout")]
    Timeout,

    #[error("Timeout: {0}")]
    TimeoutWithMessage(String),

    #[error("{0}")]
    Other(String),
}

impl Error {
    /// Classify whether this error is retriable.
    ///
    /// **Permanent errors** (not retriable) include authentication failures,
    /// configuration errors, and server-side rejections for invalid topics,
    /// producer fencing, authorization failures, etc.
    ///
    /// **Transient errors** (retriable) include I/O errors, connection resets,
    /// timeouts, and generic server errors that may resolve after reconnect.
    pub fn is_retriable(&self) -> bool {
        match self {
            // Permanent — retrying won't help
            Error::ConfigError(_)
            | Error::AuthenticationFailed(_)
            | Error::RequestTooLarge(_, _)
            | Error::SerializationError(_) => false,

            // ServerError — classify by known error prefixes.
            // Permanent server-side rejections are not retriable.
            Error::ServerError(msg) => {
                let upper = msg.to_uppercase();
                // Fencing / epoch errors — producer must re-init
                if upper.starts_with("PRODUCER_FENCED")
                    || upper.starts_with("INVALID_PRODUCER_EPOCH")
                    // Authorization failures — credentials won't change on retry
                    || upper.starts_with("TRANSACTIONAL_ID_AUTHORIZATION_FAILED")
                    || upper.starts_with("CLUSTER_AUTHORIZATION_FAILED")
                    || upper.starts_with("TOPIC_AUTHORIZATION_FAILED")
                    || upper.starts_with("GROUP_AUTHORIZATION_FAILED")
                    // Topic/partition validation errors — won't resolve without admin action
                    || upper.starts_with("INVALID_TOPIC")
                    || upper.starts_with("INVALID_PARTITIONS")
                    || upper.starts_with("INVALID_REPLICATION_FACTOR")
                    || upper.starts_with("INVALID_REQUIRED_ACKS")
                    // Version/feature mismatch — won't change on retry
                    || upper.starts_with("UNSUPPORTED_VERSION")
                    || upper.starts_with("UNSUPPORTED_COMPRESSION")
                    || upper.starts_with("UNSUPPORTED_FOR_MESSAGE_FORMAT")
                    // Security errors
                    || upper.starts_with("SECURITY_DISABLED")
                    || upper.starts_with("ILLEGAL_SASL_STATE")
                    // Consumer group state errors — need rejoin, not retry
                    || upper.starts_with("ILLEGAL_GENERATION")
                    || upper.starts_with("UNKNOWN_MEMBER_ID")
                    || upper.starts_with("INVALID_SESSION_TIMEOUT")
                    // Payload errors — won't change on retry
                    || upper.starts_with("RECORD_TOO_LARGE")
                    || upper.starts_with("MESSAGE_TOO_LARGE")
                    // Group / transaction state errors
                    || upper.starts_with("INVALID_GROUP_ID")
                    || upper.starts_with("INVALID_TXN_STATE")
                    || upper.starts_with("INVALID_PRODUCER_ID_MAPPING")
                {
                    false
                } else {
                    true
                }
            }

            // Transient — I/O, connection, pool, timeout errors
            Error::ConnectionError(_)
            | Error::IoError(_, _)
            | Error::ProtocolError(_)
            | Error::InvalidResponse
            | Error::ResponseTooLarge(_, _)
            | Error::CircuitBreakerOpen(_)
            | Error::PoolExhausted(_)
            | Error::AllServersUnavailable
            | Error::Timeout
            | Error::TimeoutWithMessage(_)
            | Error::Other(_) => true,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IoError(err.kind(), err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        assert_eq!(
            Error::ConnectionError("refused".to_string()).to_string(),
            "Connection error: refused"
        );
        assert_eq!(
            Error::ServerError("internal error".to_string()).to_string(),
            "Server error: internal error"
        );
        assert_eq!(
            Error::AuthenticationFailed("bad password".to_string()).to_string(),
            "Authentication failed: bad password"
        );
        assert_eq!(Error::InvalidResponse.to_string(), "Invalid response");
        assert_eq!(
            Error::ResponseTooLarge(1000, 500).to_string(),
            "Response too large: 1000 bytes (max: 500)"
        );
        assert_eq!(
            Error::RequestTooLarge(2000, 1000).to_string(),
            "Request too large: 2000 bytes (max: 1000)"
        );
        assert_eq!(
            Error::CircuitBreakerOpen("server1".to_string()).to_string(),
            "Circuit breaker open for server: server1"
        );
        assert_eq!(
            Error::PoolExhausted("max connections".to_string()).to_string(),
            "Connection pool exhausted: max connections"
        );
        assert_eq!(
            Error::AllServersUnavailable.to_string(),
            "All servers unavailable"
        );
        assert_eq!(Error::Timeout.to_string(), "Request timeout");
        assert_eq!(
            Error::TimeoutWithMessage("connect".to_string()).to_string(),
            "Timeout: connect"
        );
        assert_eq!(
            Error::Other("custom error".to_string()).to_string(),
            "custom error"
        );
    }

    #[test]
    fn test_error_debug() {
        let err = Error::ConnectionError("test".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("ConnectionError"));
        assert!(debug.contains("test"));
    }

    #[test]
    fn test_io_error_from() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: Error = io_err.into();
        match &err {
            Error::IoError(kind, msg) => {
                assert_eq!(*kind, std::io::ErrorKind::NotFound);
                assert!(msg.contains("file not found"));
            }
            _ => panic!("Expected IoError"),
        }
    }

    #[test]
    fn test_postcard_error_from() {
        // Create a postcard deserialization error by trying to deserialize invalid data
        let invalid_data: &[u8] = &[];
        let result: std::result::Result<String, postcard::Error> =
            postcard::from_bytes(invalid_data);
        assert!(result.is_err());
        let postcard_err = result.unwrap_err();
        let err: Error = postcard_err.into();
        assert!(matches!(err, Error::SerializationError(_)));
    }

    #[test]
    fn test_result_type() {
        fn returns_ok() -> Result<i32> {
            Ok(42)
        }

        fn returns_err() -> Result<i32> {
            Err(Error::InvalidResponse)
        }

        assert!(returns_ok().is_ok());
        assert_eq!(returns_ok().unwrap(), 42);
        assert!(returns_err().is_err());
    }

    #[test]
    fn test_is_retriable_permanent_errors() {
        // Config errors — never retriable
        assert!(!Error::ConfigError("bad config".into()).is_retriable());

        // Auth failures — never retriable
        assert!(!Error::AuthenticationFailed("bad password".into()).is_retriable());

        // Request too large — never retriable (payload won't shrink)
        assert!(!Error::RequestTooLarge(1_000_000, 65_536).is_retriable());

        // Server-side permanent rejections
        assert!(!Error::ServerError("PRODUCER_FENCED: epoch mismatch".into()).is_retriable());
        assert!(!Error::ServerError("INVALID_TOPIC: bad name".into()).is_retriable());
        assert!(!Error::ServerError("TOPIC_AUTHORIZATION_FAILED".into()).is_retriable());
        assert!(!Error::ServerError("CLUSTER_AUTHORIZATION_FAILED".into()).is_retriable());
        assert!(!Error::ServerError("TRANSACTIONAL_ID_AUTHORIZATION_FAILED".into()).is_retriable());
        assert!(!Error::ServerError("INVALID_PRODUCER_EPOCH: stale epoch".into()).is_retriable());
        assert!(!Error::ServerError("UNSUPPORTED_VERSION".into()).is_retriable());
        assert!(!Error::ServerError("UNSUPPORTED_COMPRESSION_TYPE".into()).is_retriable());
        assert!(!Error::ServerError("UNSUPPORTED_FOR_MESSAGE_FORMAT".into()).is_retriable());

        // Consumer group errors — need rejoin, not retry
        assert!(!Error::ServerError("ILLEGAL_GENERATION: expected 5, got 3".into()).is_retriable());
        assert!(!Error::ServerError("UNKNOWN_MEMBER_ID: consumer-abc".into()).is_retriable());
        assert!(!Error::ServerError("INVALID_SESSION_TIMEOUT: too large".into()).is_retriable());

        // Payload too large — won't change on retry
        assert!(!Error::ServerError("RECORD_TOO_LARGE: 10485760 bytes".into()).is_retriable());
        assert!(!Error::ServerError("MESSAGE_TOO_LARGE".into()).is_retriable());
        assert!(!Error::ServerError("INVALID_REQUIRED_ACKS".into()).is_retriable());
    }

    #[test]
    fn test_is_retriable_transient_errors() {
        // I/O errors — retriable after reconnect
        assert!(Error::IoError(std::io::ErrorKind::ConnectionReset, "reset".into()).is_retriable());
        assert!(Error::ConnectionError("refused".into()).is_retriable());

        // Timeouts — retriable
        assert!(Error::Timeout.is_retriable());
        assert!(Error::TimeoutWithMessage("connect".into()).is_retriable());

        // Pool/circuit — retriable (transient capacity)
        assert!(Error::CircuitBreakerOpen("server1".into()).is_retriable());
        assert!(Error::PoolExhausted("max conns".into()).is_retriable());
        assert!(Error::AllServersUnavailable.is_retriable());

        // Generic server errors — retriable (might be transient load)
        assert!(Error::ServerError("internal error".into()).is_retriable());
        assert!(Error::ServerError("NOT_LEADER_FOR_PARTITION".into()).is_retriable());

        // UNKNOWN_TOPIC is retriable to support auto-topic-creation
        assert!(Error::ServerError("UNKNOWN_TOPIC: topic-1".into()).is_retriable());

        // Invalid response — retriable (might be corruption)
        assert!(Error::InvalidResponse.is_retriable());
    }
}
