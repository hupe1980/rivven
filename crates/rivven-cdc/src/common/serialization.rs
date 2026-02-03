//! Serialization format configuration for CDC events
//!
//! CDC events can be serialized in multiple formats:
//! - **JSON**: Human-readable, schema-optional (development/debugging)
//! - **Avro**: Binary with schema evolution (production recommended)
//!
//! # Best Practices
//!
//! In production Rivven deployments, **Avro is the recommended format**:
//! - Schema evolution with compatibility checking
//! - Compact binary encoding (50-70% smaller than JSON)
//! - Schema registry integration for centralized management
//! - Strong typing and validation
//!
//! However, JSON is useful for:
//! - Development and debugging (human-readable)
//! - Systems without Schema Registry
//! - Audit logs requiring human inspection
//! - Interoperability with legacy systems
//!
//! # Example
//!
//! ```rust
//! use rivven_cdc::common::{SerializationConfig, SerializationFormat};
//!
//! // JSON format (default for development)
//! let json_config = SerializationConfig::json();
//!
//! // Avro format with schema registry (production)
//! let avro_config = SerializationConfig::avro()
//!     .with_confluent_wire_format(true);
//! ```

use serde::{Deserialize, Serialize};

/// Serialization format for CDC events
///
/// # Production Recommendations
///
/// | Format | Use Case                        | Schema Registry |
/// |--------|-------------------------------- |-----------------|
/// | JSON   | Development, debugging, logs    | Optional        |
/// | Avro   | Production (recommended)        | Required        |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SerializationFormat {
    /// JSON format - human-readable, no schema required
    ///
    /// Events are serialized as plain JSON objects.
    /// Best for: development, debugging, audit logs
    #[default]
    Json,

    /// Apache Avro - binary format with schema evolution
    ///
    /// Events are serialized using Avro binary encoding with
    /// optional Confluent wire format (5-byte schema ID header).
    /// Best for: production Rivven deployments
    /// Requires: Schema Registry
    Avro,
}

impl SerializationFormat {
    /// Check if this format requires a schema registry
    pub fn requires_schema(&self) -> bool {
        matches!(self, Self::Avro)
    }

    /// Check if this format produces binary output
    pub fn is_binary(&self) -> bool {
        matches!(self, Self::Avro)
    }

    /// Get the content type for this format
    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Json => "application/json",
            Self::Avro => "application/avro",
        }
    }
}

impl std::fmt::Display for SerializationFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json => write!(f, "json"),
            Self::Avro => write!(f, "avro"),
        }
    }
}

impl std::str::FromStr for SerializationFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "avro" => Ok(Self::Avro),
            _ => Err(format!("Unknown serialization format: {}", s)),
        }
    }
}

/// Configuration for CDC event serialization
///
/// Controls how CDC events are serialized before being sent to topics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializationConfig {
    /// Serialization format (json or avro)
    #[serde(default)]
    pub format: SerializationFormat,

    /// Use Confluent wire format for Avro (5-byte header with schema ID)
    ///
    /// Format: `[0x00][schema_id: 4 bytes big-endian][avro_payload]`
    ///
    /// This is required for compatibility with Confluent Schema Registry
    /// and ecosystem tools.
    #[serde(default = "default_true")]
    pub confluent_wire_format: bool,

    /// Auto-register schemas with the schema registry
    ///
    /// When enabled, new schemas are automatically registered.
    /// When disabled, schemas must be pre-registered.
    #[serde(default = "default_true")]
    pub auto_register_schemas: bool,

    /// Pretty-print JSON output (for debugging)
    #[serde(default)]
    pub pretty_json: bool,

    /// Include null fields in JSON output
    #[serde(default = "default_true")]
    pub include_nulls: bool,

    /// Include transaction metadata in events
    #[serde(default = "default_true")]
    pub include_transaction: bool,

    /// Include source metadata (database, schema, table) in events
    #[serde(default = "default_true")]
    pub include_source: bool,
}

fn default_true() -> bool {
    true
}

impl Default for SerializationConfig {
    fn default() -> Self {
        Self {
            format: SerializationFormat::Json,
            confluent_wire_format: true,
            auto_register_schemas: true,
            pretty_json: false,
            include_nulls: true,
            include_transaction: true,
            include_source: true,
        }
    }
}

impl SerializationConfig {
    /// Create JSON serialization config (default for development)
    pub fn json() -> Self {
        Self {
            format: SerializationFormat::Json,
            ..Default::default()
        }
    }

    /// Create Avro serialization config (recommended for production)
    pub fn avro() -> Self {
        Self {
            format: SerializationFormat::Avro,
            confluent_wire_format: true,
            auto_register_schemas: true,
            ..Default::default()
        }
    }

    /// Set Confluent wire format
    pub fn with_confluent_wire_format(mut self, enabled: bool) -> Self {
        self.confluent_wire_format = enabled;
        self
    }

    /// Set auto-register schemas
    pub fn with_auto_register_schemas(mut self, enabled: bool) -> Self {
        self.auto_register_schemas = enabled;
        self
    }

    /// Set pretty JSON output
    pub fn with_pretty_json(mut self, enabled: bool) -> Self {
        self.pretty_json = enabled;
        self
    }

    /// Set include nulls in JSON
    pub fn with_include_nulls(mut self, enabled: bool) -> Self {
        self.include_nulls = enabled;
        self
    }

    /// Set include transaction metadata
    pub fn with_include_transaction(mut self, enabled: bool) -> Self {
        self.include_transaction = enabled;
        self
    }

    /// Set include source metadata
    pub fn with_include_source(mut self, enabled: bool) -> Self {
        self.include_source = enabled;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_parse() {
        assert_eq!(
            "json".parse::<SerializationFormat>().unwrap(),
            SerializationFormat::Json
        );
        assert_eq!(
            "avro".parse::<SerializationFormat>().unwrap(),
            SerializationFormat::Avro
        );
        assert_eq!(
            "JSON".parse::<SerializationFormat>().unwrap(),
            SerializationFormat::Json
        );
        assert_eq!(
            "AVRO".parse::<SerializationFormat>().unwrap(),
            SerializationFormat::Avro
        );
    }

    #[test]
    fn test_format_requires_schema() {
        assert!(!SerializationFormat::Json.requires_schema());
        assert!(SerializationFormat::Avro.requires_schema());
    }

    #[test]
    fn test_format_is_binary() {
        assert!(!SerializationFormat::Json.is_binary());
        assert!(SerializationFormat::Avro.is_binary());
    }

    #[test]
    fn test_config_builders() {
        let json = SerializationConfig::json();
        assert_eq!(json.format, SerializationFormat::Json);

        let avro = SerializationConfig::avro();
        assert_eq!(avro.format, SerializationFormat::Avro);
        assert!(avro.confluent_wire_format);
    }

    #[test]
    fn test_config_defaults() {
        let config = SerializationConfig::default();
        assert_eq!(config.format, SerializationFormat::Json);
        assert!(config.confluent_wire_format);
        assert!(config.auto_register_schemas);
        assert!(!config.pretty_json);
        assert!(config.include_nulls);
    }
}
