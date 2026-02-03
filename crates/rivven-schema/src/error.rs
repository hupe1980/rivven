//! Schema Registry errors

use thiserror::Error;

/// Confluent-compatible error codes
pub mod error_codes {
    // Subject/schema not found
    pub const SUBJECT_NOT_FOUND: u32 = 40401;
    pub const VERSION_NOT_FOUND: u32 = 40402;
    pub const SCHEMA_NOT_FOUND: u32 = 40403;

    // Invalid schema/compatibility
    pub const INVALID_SCHEMA: u32 = 42201;
    pub const INVALID_VERSION: u32 = 42202;
    pub const INVALID_COMPATIBILITY_LEVEL: u32 = 42203;
    pub const INCOMPATIBLE_SCHEMA: u32 = 409;

    // Validation errors
    pub const VALIDATION_ERROR: u32 = 42204;
    pub const VERSION_DISABLED: u32 = 42205;
    pub const REFERENCE_NOT_FOUND: u32 = 42206;

    // Internal errors
    pub const INTERNAL_ERROR: u32 = 50001;
    pub const STORAGE_ERROR: u32 = 50002;
}

/// Schema Registry error types
#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("Schema not found: {0}")]
    NotFound(String),

    #[error("Subject not found: {0}")]
    SubjectNotFound(String),

    #[error("Version not found: {subject} version {version}")]
    VersionNotFound { subject: String, version: u32 },

    #[error("Version disabled: {subject} version {version}")]
    VersionDisabled { subject: String, version: u32 },

    #[error("Schema reference not found: {name} references {subject} version {version}")]
    ReferenceNotFound {
        name: String,
        subject: String,
        version: u32,
    },

    #[error("Invalid schema: {0}")]
    InvalidSchema(String),

    #[error("Schema parse error: {0}")]
    ParseError(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Incompatible schema: {0}")]
    IncompatibleSchema(String),

    #[error("Schema type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("{0} already exists")]
    AlreadyExists(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl SchemaError {
    /// Get the Confluent-compatible error code
    pub fn error_code(&self) -> u32 {
        match self {
            SchemaError::NotFound(_) => error_codes::SCHEMA_NOT_FOUND,
            SchemaError::SubjectNotFound(_) => error_codes::SUBJECT_NOT_FOUND,
            SchemaError::VersionNotFound { .. } => error_codes::VERSION_NOT_FOUND,
            SchemaError::VersionDisabled { .. } => error_codes::VERSION_DISABLED,
            SchemaError::ReferenceNotFound { .. } => error_codes::REFERENCE_NOT_FOUND,
            SchemaError::InvalidSchema(_) => error_codes::INVALID_SCHEMA,
            SchemaError::ParseError(_) => error_codes::INVALID_SCHEMA,
            SchemaError::Validation(_) => error_codes::VALIDATION_ERROR,
            SchemaError::InvalidInput(_) => error_codes::INVALID_SCHEMA,
            SchemaError::IncompatibleSchema(_) => error_codes::INCOMPATIBLE_SCHEMA,
            SchemaError::TypeMismatch { .. } => error_codes::INVALID_SCHEMA,
            SchemaError::AlreadyExists(_) => error_codes::INVALID_SCHEMA,
            SchemaError::Storage(_) => error_codes::STORAGE_ERROR,
            SchemaError::Config(_) => error_codes::INVALID_COMPATIBILITY_LEVEL,
            SchemaError::Serialization(_) => error_codes::INTERNAL_ERROR,
            SchemaError::Io(_) => error_codes::INTERNAL_ERROR,
            SchemaError::Internal(_) => error_codes::INTERNAL_ERROR,
        }
    }

    /// Get the HTTP status code
    pub fn http_status(&self) -> u16 {
        match self {
            SchemaError::NotFound(_)
            | SchemaError::SubjectNotFound(_)
            | SchemaError::VersionNotFound { .. } => 404,
            SchemaError::VersionDisabled { .. } => 403,
            SchemaError::InvalidSchema(_)
            | SchemaError::ParseError(_)
            | SchemaError::Validation(_)
            | SchemaError::InvalidInput(_)
            | SchemaError::TypeMismatch { .. } => 422,
            SchemaError::IncompatibleSchema(_) => 409,
            SchemaError::AlreadyExists(_) => 409,
            SchemaError::Config(_) => 422,
            _ => 500,
        }
    }
}

/// Result type for schema operations
pub type SchemaResult<T> = Result<T, SchemaError>;

impl From<serde_json::Error> for SchemaError {
    fn from(e: serde_json::Error) -> Self {
        SchemaError::Serialization(e.to_string())
    }
}

#[cfg(feature = "avro")]
impl From<apache_avro::Error> for SchemaError {
    fn from(e: apache_avro::Error) -> Self {
        SchemaError::ParseError(e.to_string())
    }
}
