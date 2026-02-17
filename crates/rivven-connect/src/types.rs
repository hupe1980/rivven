//! Common types for rivven-connect
//!
//! This module provides shared types used across multiple connectors.

use schemars::JsonSchema;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

/// A wrapper around `SecretString` that provides safe handling of sensitive values.
///
/// This type:
/// - Redacts the value in `Debug` and `Display` output to prevent credential leaks in logs
/// - Serializes as `"***REDACTED***"` to prevent accidental exposure in config dumps
/// - Provides `expose_secret()` method to access the actual value when needed
///
/// # Example
///
/// ```rust
/// use rivven_connect::SensitiveString;
///
/// let secret = SensitiveString::new("my-api-key");
///
/// // Safe to log - shows "[REDACTED]"
/// println!("{:?}", secret);
///
/// // Access the actual value when needed
/// let actual = secret.expose_secret();
/// ```
#[derive(Clone)]
pub struct SensitiveString(SecretString);

impl SensitiveString {
    /// Create a new sensitive string from any string-like value
    pub fn new(value: impl Into<String>) -> Self {
        Self(SecretString::new(value.into().into_boxed_str()))
    }

    /// Expose the secret value.
    ///
    /// Use sparingly - only when the actual value is needed (e.g., for authentication).
    pub fn expose_secret(&self) -> &str {
        self.0.expose_secret()
    }
}

impl std::fmt::Debug for SensitiveString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[REDACTED]")
    }
}

impl std::fmt::Display for SensitiveString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[REDACTED]")
    }
}

impl From<String> for SensitiveString {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for SensitiveString {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

/// Serialize as redacted to prevent accidental exposure in config dumps/logs
impl Serialize for SensitiveString {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str("***REDACTED***")
    }
}

/// Deserialize from the actual string value
impl<'de> Deserialize<'de> for SensitiveString {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Ok(Self::new(value))
    }
}

impl JsonSchema for SensitiveString {
    fn schema_name() -> String {
        "SensitiveString".to_string()
    }

    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        // Schema looks like a normal string but with format hint
        let mut schema = gen.subschema_for::<String>();
        if let schemars::schema::Schema::Object(obj) = &mut schema {
            obj.format = Some("password".to_string());
            obj.metadata().description = Some(
                "Sensitive value (passwords, API keys, etc.). Will be redacted in logs."
                    .to_string(),
            );
        }
        schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sensitive_string_redacted_debug() {
        let secret = SensitiveString::new("my-secret-password");
        let debug_output = format!("{:?}", secret);
        assert_eq!(debug_output, "[REDACTED]");
        assert!(!debug_output.contains("my-secret-password"));
    }

    #[test]
    fn test_sensitive_string_redacted_display() {
        let secret = SensitiveString::new("my-secret-password");
        let display_output = format!("{}", secret);
        assert_eq!(display_output, "[REDACTED]");
    }

    #[test]
    fn test_sensitive_string_expose() {
        let secret = SensitiveString::new("my-secret-password");
        assert_eq!(secret.expose_secret(), "my-secret-password");
        assert_eq!(secret.expose_secret(), "my-secret-password");
    }

    #[test]
    fn test_sensitive_string_serialize() {
        let secret = SensitiveString::new("my-secret-password");
        let serialized = serde_json::to_string(&secret).unwrap();
        assert_eq!(serialized, "\"***REDACTED***\"");
        assert!(!serialized.contains("my-secret-password"));
    }

    #[test]
    fn test_sensitive_string_deserialize() {
        let secret: SensitiveString = serde_json::from_str("\"my-secret-password\"").unwrap();
        assert_eq!(secret.expose_secret(), "my-secret-password");
    }

    #[test]
    fn test_sensitive_string_from_string() {
        let secret = SensitiveString::from("test".to_string());
        assert_eq!(secret.expose_secret(), "test");
    }

    #[test]
    fn test_sensitive_string_from_str() {
        let secret = SensitiveString::from("test");
        assert_eq!(secret.expose_secret(), "test");
    }
}
