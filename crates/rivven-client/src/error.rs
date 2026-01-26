use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),

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

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
