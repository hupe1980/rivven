//! Python error types for Rivven

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use std::fmt;

// Create custom exception types using the create_exception! macro
// This is the correct approach for PyO3 0.27+ and works with ABI3
pyo3::create_exception!(
    rivven,
    RivvenException,
    PyException,
    "Base exception for Rivven errors"
);
pyo3::create_exception!(
    rivven,
    ConnectionException,
    RivvenException,
    "Connection-related errors"
);
pyo3::create_exception!(
    rivven,
    ServerException,
    RivvenException,
    "Server-related errors"
);
pyo3::create_exception!(rivven, TimeoutException, RivvenException, "Timeout errors");
pyo3::create_exception!(
    rivven,
    SerializationException,
    RivvenException,
    "Serialization errors"
);
pyo3::create_exception!(
    rivven,
    ConfigException,
    RivvenException,
    "Configuration errors"
);

/// Error classification for better Python exception handling
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorType {
    Connection,
    Server,
    Timeout,
    InvalidResponse,
    Serialization,
    Io,
    InvalidConfig,
}

impl fmt::Display for ErrorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorType::Connection => write!(f, "ConnectionError"),
            ErrorType::Server => write!(f, "ServerError"),
            ErrorType::Timeout => write!(f, "TimeoutError"),
            ErrorType::InvalidResponse => write!(f, "InvalidResponseError"),
            ErrorType::Serialization => write!(f, "SerializationError"),
            ErrorType::Io => write!(f, "IoError"),
            ErrorType::InvalidConfig => write!(f, "InvalidConfigError"),
        }
    }
}

/// Internal Rust error type for Rivven operations
#[derive(Debug, Clone)]
pub struct RivvenError {
    message: String,
    error_type: ErrorType,
}

impl RivvenError {
    /// Create a new RivvenError
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            error_type: ErrorType::Server,
        }
    }

    /// Get the error message
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Get the error type
    pub fn error_type(&self) -> ErrorType {
        self.error_type
    }

    /// Check if this is a connection error
    pub fn is_connection_error(&self) -> bool {
        self.error_type == ErrorType::Connection
    }

    /// Check if this is a server error
    pub fn is_server_error(&self) -> bool {
        self.error_type == ErrorType::Server
    }

    /// Check if this is a timeout error  
    pub fn is_timeout_error(&self) -> bool {
        self.error_type == ErrorType::Timeout
    }

    /// Create a connection error
    pub fn connection(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
            error_type: ErrorType::Connection,
        }
    }

    /// Create a server error
    pub fn server(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
            error_type: ErrorType::Server,
        }
    }

    /// Create a timeout error
    pub fn timeout(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
            error_type: ErrorType::Timeout,
        }
    }

    /// Create an invalid response error
    pub fn invalid_response() -> Self {
        Self {
            message: "Invalid response from server".to_string(),
            error_type: ErrorType::InvalidResponse,
        }
    }

    /// Create a serialization error
    pub fn serialization(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
            error_type: ErrorType::Serialization,
        }
    }

    /// Create an IO error
    pub fn io(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
            error_type: ErrorType::Io,
        }
    }

    /// Create an invalid config error
    pub fn invalid_config(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
            error_type: ErrorType::InvalidConfig,
        }
    }
}

impl From<rivven_client::Error> for RivvenError {
    fn from(err: rivven_client::Error) -> Self {
        match &err {
            rivven_client::Error::ConnectionError(msg) => Self::connection(msg),
            rivven_client::Error::ServerError(msg) => Self::server(msg),
            rivven_client::Error::InvalidResponse => Self::invalid_response(),
            rivven_client::Error::SerializationError(msg) => Self::serialization(msg.to_string()),
            rivven_client::Error::ProtocolError(e) => Self::serialization(e.to_string()),
            rivven_client::Error::IoError(e) => Self::io(e.to_string()),
            rivven_client::Error::ResponseTooLarge(size, max) => {
                Self::server(format!("Response too large: {} bytes (max: {})", size, max))
            }
            rivven_client::Error::CircuitBreakerOpen(server) => {
                Self::connection(format!("Circuit breaker open for server: {}", server))
            }
            rivven_client::Error::PoolExhausted(msg) => {
                Self::connection(format!("Connection pool exhausted: {}", msg))
            }
            rivven_client::Error::AllServersUnavailable => {
                Self::connection("All servers unavailable")
            }
            rivven_client::Error::Timeout => Self::connection("Request timeout".to_string()),
            rivven_client::Error::TimeoutWithMessage(msg) => {
                Self::connection(format!("Timeout: {}", msg))
            }
            rivven_client::Error::AuthenticationFailed(msg) => {
                Self::server(format!("Authentication failed: {}", msg))
            }
            rivven_client::Error::Other(msg) => Self::server(msg),
        }
    }
}

impl From<RivvenError> for PyErr {
    fn from(err: RivvenError) -> PyErr {
        let msg = format!("{}: {}", err.error_type, err.message);
        match err.error_type {
            ErrorType::Connection => ConnectionException::new_err(msg),
            ErrorType::Server => ServerException::new_err(msg),
            ErrorType::Timeout => TimeoutException::new_err(msg),
            ErrorType::Serialization | ErrorType::InvalidResponse => {
                SerializationException::new_err(msg)
            }
            ErrorType::Io => ConnectionException::new_err(msg),
            ErrorType::InvalidConfig => ConfigException::new_err(msg),
        }
    }
}

/// Extension trait for converting rivven_client errors to PyErr
pub trait IntoPyErr<T> {
    fn into_py_err(self) -> PyResult<T>;
}

impl<T> IntoPyErr<T> for Result<T, rivven_client::Error> {
    fn into_py_err(self) -> PyResult<T> {
        self.map_err(|e| PyErr::from(RivvenError::from(e)))
    }
}

impl std::error::Error for RivvenError {}

impl fmt::Display for RivvenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.error_type, self.message)
    }
}

/// Register exception types in the Python module
pub fn register_exceptions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("RivvenException", m.py().get_type::<RivvenException>())?;
    m.add(
        "ConnectionException",
        m.py().get_type::<ConnectionException>(),
    )?;
    m.add("ServerException", m.py().get_type::<ServerException>())?;
    m.add("TimeoutException", m.py().get_type::<TimeoutException>())?;
    m.add(
        "SerializationException",
        m.py().get_type::<SerializationException>(),
    )?;
    m.add("ConfigException", m.py().get_type::<ConfigException>())?;
    Ok(())
}
