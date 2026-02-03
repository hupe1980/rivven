//! Schema Registry types and error handling
//!
//! This module re-exports shared types from `rivven-schema` and provides
//! connector-specific configuration types and error handling.
//!
//! # Architecture
//!
//! The broker (rivvend) is **schema-agnostic** — it only handles raw bytes.
//! Schema management is handled externally via:
//!
//! - **rivven-schema**: Standalone Confluent-compatible Schema Registry
//! - **Confluent Schema Registry**: External Kafka-compatible registry
//! - **AWS Glue Schema Registry**: AWS-native schema management
//!
//! Types are sourced from `rivven-schema` to ensure consistency across the platform:
//! - [`SchemaId`], [`SchemaType`], [`Subject`], [`SchemaVersion`] - Core identifiers
//! - [`Schema`], [`SchemaReference`], [`SubjectVersion`] - Schema metadata
//! - [`CompatibilityLevel`] - Schema evolution rules
//!
//! Connector-specific types remain here:
//! - [`SchemaRegistryConfig`] - Backend selection (External, Glue, Disabled)
//! - [`ExternalConfig`] - External registry configuration
//! - [`SchemaRegistryError`] - Error handling for schema operations

use serde::{Deserialize, Serialize};
use thiserror::Error;

// ============================================================================
// Re-export shared types from rivven-schema
// ============================================================================

// Core identifier types
pub use rivven_schema::types::{SchemaId, SchemaType, SchemaVersion, Subject};

// Schema metadata types
pub use rivven_schema::types::{Schema, SchemaReference, SubjectVersion};

// Compatibility types
pub use rivven_schema::CompatibilityLevel;

// ============================================================================
// Connector-specific configuration types
// ============================================================================

/// Schema Registry configuration
///
/// Determines which backend to use for schema storage:
/// - [`External`](SchemaRegistryConfig::External): Confluent-compatible HTTP registry (including rivven-schema)
/// - [`Glue`](SchemaRegistryConfig::Glue): AWS Glue Schema Registry
/// - [`Disabled`](SchemaRegistryConfig::Disabled): No schema registry (default)
///
/// **Note**: The broker (rivvend) is schema-agnostic and only handles raw bytes.
/// Use an external schema registry for schema management.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum SchemaRegistryConfig {
    /// External mode: connect to Confluent-compatible schema registry (including rivven-schema)
    External(ExternalConfig),
    /// AWS Glue mode: connect to AWS Glue Schema Registry
    Glue(crate::schema::glue::GlueConfig),
    /// Disabled: no schema registry (default)
    #[default]
    Disabled,
}

impl SchemaRegistryConfig {
    /// Create external registry config pointing to the given URL
    ///
    /// Use this for rivven-schema, Confluent Schema Registry, or any
    /// Confluent-compatible registry.
    pub fn external(url: impl Into<String>) -> Self {
        Self::External(ExternalConfig {
            url: url.into(),
            ..Default::default()
        })
    }

    /// Create AWS Glue registry config for the given region
    pub fn glue(region: impl Into<String>) -> Self {
        Self::Glue(crate::schema::glue::GlueConfig {
            region: region.into(),
            ..Default::default()
        })
    }

    /// Check if schema registry is enabled
    pub fn is_enabled(&self) -> bool {
        !matches!(self, Self::Disabled)
    }
}

/// External schema registry configuration
///
/// Connects to a Confluent-compatible Schema Registry over HTTP.
/// Use this for rivven-schema, Confluent Schema Registry, or any compatible registry.
///
/// # Authentication Methods (Best-in-Class)
///
/// Supports multiple authentication methods in order of preference:
/// 1. **OAuth2/OIDC** - For Confluent Cloud and enterprise registries
/// 2. **mTLS** - Mutual TLS for zero-trust environments
/// 3. **API Key** - Simple header-based authentication
/// 4. **Bearer Token** - Static or provider-issued tokens
/// 5. **Basic Auth** - Username/password (legacy fallback)
///
/// # Examples
///
/// ```rust
/// use rivven_connect::schema::{ExternalConfig, RegistryAuth};
///
/// // OAuth2 (Confluent Cloud)
/// let config = ExternalConfig {
///     url: "https://psrc-xxxxx.us-east-2.aws.confluent.cloud".into(),
///     auth: RegistryAuth::OAuth2 {
///         client_id: "API_KEY".into(),
///         client_secret: "API_SECRET".into(),
///         token_url: "https://confluent.cloud/oauth/token".into(),
///         scope: Some("registry:read registry:write".into()),
///     },
///     ..Default::default()
/// };
///
/// // mTLS (zero-trust)
/// let config = ExternalConfig {
///     url: "https://registry.internal:8081".into(),
///     auth: RegistryAuth::MTLS {
///         client_cert_path: "/certs/client.crt".into(),
///         client_key_path: "/certs/client.key".into(),
///         ca_cert_path: Some("/certs/ca.crt".into()),
///     },
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalConfig {
    /// Schema registry URL (e.g., `http://localhost:8081`)
    pub url: String,

    /// Authentication configuration
    #[serde(default)]
    pub auth: RegistryAuth,

    /// TLS configuration (for HTTPS connections)
    #[serde(default)]
    pub tls: RegistryTlsConfig,

    /// Request timeout in seconds (default: 30)
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,

    /// Cache TTL in seconds (default: 300)
    #[serde(default = "default_cache_ttl")]
    pub cache_ttl_secs: u64,

    /// Maximum retries for transient failures (default: 3)
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Retry backoff base in milliseconds (default: 100)
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
}

impl Default for ExternalConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:8081".to_string(),
            auth: RegistryAuth::None,
            tls: RegistryTlsConfig::default(),
            timeout_secs: default_timeout(),
            cache_ttl_secs: default_cache_ttl(),
            max_retries: default_max_retries(),
            retry_backoff_ms: default_retry_backoff_ms(),
        }
    }
}

impl ExternalConfig {
    /// Create config with Basic Auth
    pub fn with_basic_auth(
        url: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            url: url.into(),
            auth: RegistryAuth::Basic {
                username: username.into(),
                password: password.into(),
            },
            ..Default::default()
        }
    }

    /// Create config with OAuth2 (Confluent Cloud style)
    pub fn with_oauth2(
        url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        token_url: impl Into<String>,
    ) -> Self {
        Self {
            url: url.into(),
            auth: RegistryAuth::OAuth2 {
                client_id: client_id.into(),
                client_secret: client_secret.into(),
                token_url: token_url.into(),
                scope: None,
            },
            ..Default::default()
        }
    }

    /// Create config with Bearer Token
    pub fn with_bearer_token(url: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            auth: RegistryAuth::Bearer {
                token: token.into(),
            },
            ..Default::default()
        }
    }

    /// Create config with API Key
    pub fn with_api_key(
        url: impl Into<String>,
        api_key: impl Into<String>,
        header_name: Option<String>,
    ) -> Self {
        Self {
            url: url.into(),
            auth: RegistryAuth::ApiKey {
                key: api_key.into(),
                header_name: header_name.unwrap_or_else(|| "X-API-Key".to_string()),
            },
            ..Default::default()
        }
    }
}

/// Authentication methods for external schema registries
///
/// Supports enterprise-grade authentication options compatible with:
/// - Confluent Cloud (OAuth2)
/// - Confluent Platform (Basic Auth, mTLS)
/// - rivven-schema (Basic Auth, Bearer Token)
/// - Enterprise registries (mTLS, API Keys)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RegistryAuth {
    /// No authentication
    #[default]
    None,

    /// HTTP Basic Authentication (username:password)
    ///
    /// Simple but secure over HTTPS. Widely supported.
    Basic { username: String, password: String },

    /// Bearer Token Authentication
    ///
    /// Static token or from external auth provider.
    /// Token is sent as `Authorization: Bearer <token>`
    Bearer { token: String },

    /// API Key Authentication
    ///
    /// Key sent in a custom header (default: `X-API-Key`)
    ApiKey {
        key: String,
        #[serde(default = "default_api_key_header")]
        header_name: String,
    },

    /// OAuth2 Client Credentials Flow
    ///
    /// Used by Confluent Cloud and enterprise OAuth2 providers.
    /// Automatically refreshes tokens before expiry.
    OAuth2 {
        /// OAuth2 client ID (API Key in Confluent Cloud)
        client_id: String,
        /// OAuth2 client secret (API Secret in Confluent Cloud)
        client_secret: String,
        /// Token endpoint URL
        token_url: String,
        /// OAuth2 scopes (space-separated)
        #[serde(default)]
        scope: Option<String>,
    },

    /// Mutual TLS (mTLS) Authentication
    ///
    /// Client certificate authentication for zero-trust environments.
    /// Provides strong identity verification.
    #[serde(rename = "mtls")]
    MTLS {
        /// Path to client certificate (PEM format)
        client_cert_path: String,
        /// Path to client private key (PEM format)
        client_key_path: String,
        /// Optional CA certificate for server verification
        #[serde(default)]
        ca_cert_path: Option<String>,
    },
}

impl RegistryAuth {
    /// Check if authentication is configured
    pub fn is_configured(&self) -> bool {
        !matches!(self, RegistryAuth::None)
    }

    /// Check if this auth method requires TLS
    pub fn requires_tls(&self) -> bool {
        matches!(
            self,
            RegistryAuth::MTLS { .. } | RegistryAuth::OAuth2 { .. }
        )
    }
}

/// TLS configuration for HTTPS connections
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RegistryTlsConfig {
    /// Path to CA certificate for server verification
    #[serde(default)]
    pub ca_cert_path: Option<String>,

    /// Skip server certificate verification (DANGEROUS - testing only)
    #[serde(default)]
    pub insecure_skip_verify: bool,

    /// Minimum TLS version (default: TLS 1.2)
    #[serde(default = "default_min_tls_version")]
    pub min_tls_version: String,
}

fn default_api_key_header() -> String {
    "X-API-Key".to_string()
}

fn default_min_tls_version() -> String {
    "1.2".to_string()
}

fn default_timeout() -> u64 {
    30
}

fn default_cache_ttl() -> u64 {
    300
}

fn default_max_retries() -> u32 {
    3
}

fn default_retry_backoff_ms() -> u64 {
    100
}

// ============================================================================
// Error handling
// ============================================================================

/// Schema registry error types
#[derive(Debug, Error)]
pub enum SchemaRegistryError {
    #[error("Schema not found: {0}")]
    SchemaNotFound(String),

    #[error("Subject not found: {0}")]
    SubjectNotFound(String),

    #[error("Version not found: {0}")]
    VersionNotFound(String),

    #[error("Invalid schema type: {0}")]
    InvalidSchemaType(String),

    #[error("Schema parse error: {0}")]
    ParseError(String),

    #[error("Compatibility check failed: {0}")]
    IncompatibleSchema(String),

    #[error("Compatibility error: {0}")]
    CompatibilityError(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Broker error: {0}")]
    BrokerError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Schema registry is disabled")]
    Disabled,
}

impl From<reqwest::Error> for SchemaRegistryError {
    fn from(err: reqwest::Error) -> Self {
        SchemaRegistryError::NetworkError(err.to_string())
    }
}

impl From<serde_json::Error> for SchemaRegistryError {
    fn from(err: serde_json::Error) -> Self {
        SchemaRegistryError::SerializationError(err.to_string())
    }
}

/// Convert from rivven-schema errors to connector errors
impl From<rivven_schema::SchemaError> for SchemaRegistryError {
    fn from(err: rivven_schema::SchemaError) -> Self {
        match err {
            rivven_schema::SchemaError::NotFound(msg) => SchemaRegistryError::SchemaNotFound(msg),
            rivven_schema::SchemaError::SubjectNotFound(msg) => {
                SchemaRegistryError::SubjectNotFound(msg)
            }
            rivven_schema::SchemaError::VersionNotFound { subject, version } => {
                SchemaRegistryError::VersionNotFound(format!("{} version {}", subject, version))
            }
            rivven_schema::SchemaError::InvalidSchema(msg) => {
                SchemaRegistryError::InvalidSchemaType(msg)
            }
            rivven_schema::SchemaError::ParseError(msg) => SchemaRegistryError::ParseError(msg),
            rivven_schema::SchemaError::IncompatibleSchema(msg) => {
                SchemaRegistryError::IncompatibleSchema(msg)
            }
            rivven_schema::SchemaError::Storage(msg) => SchemaRegistryError::StorageError(msg),
            rivven_schema::SchemaError::Config(msg) => SchemaRegistryError::ConfigError(msg),
            rivven_schema::SchemaError::Serialization(msg) => {
                SchemaRegistryError::SerializationError(msg)
            }
            _ => SchemaRegistryError::StorageError(err.to_string()),
        }
    }
}

pub type SchemaRegistryResult<T> = Result<T, SchemaRegistryError>;
