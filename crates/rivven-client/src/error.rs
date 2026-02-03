use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

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
        assert!(matches!(err, Error::IoError(_)));
        assert!(err.to_string().contains("file not found"));
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
}
