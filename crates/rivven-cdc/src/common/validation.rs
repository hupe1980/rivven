//! Input validation for CDC operations
//!
//! Protects against:
//! - SQL injection via identifier validation
//! - Path traversal attacks
//! - Resource exhaustion via size limits

use anyhow::{anyhow, Result};
use once_cell::sync::Lazy;
use regex::Regex;

/// Maximum allowed identifier length (255 for flexibility)
const MAX_IDENTIFIER_LENGTH: usize = 255;

/// Maximum message size (64 MB)
pub const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Maximum connection timeout (30 seconds)
pub const CONNECTION_TIMEOUT_SECS: u64 = 30;

/// Maximum read/write timeout (60 seconds)
pub const IO_TIMEOUT_SECS: u64 = 60;

/// Regex for validating SQL identifiers
static IDENTIFIER_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]{0,254}$").unwrap());

/// Validator for CDC inputs
pub struct Validator;

impl Validator {
    /// Validate a SQL identifier (table name, column name, slot name, etc.)
    ///
    /// # Security
    ///
    /// Prevents SQL injection by rejecting:
    /// - Empty strings
    /// - Strings starting with numbers
    /// - Special characters (quotes, semicolons, etc.)
    /// - Excessively long strings
    pub fn validate_identifier(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(anyhow!("Identifier cannot be empty"));
        }

        if name.len() > MAX_IDENTIFIER_LENGTH {
            return Err(anyhow!(
                "Identifier too long: {} chars (max: {})",
                name.len(),
                MAX_IDENTIFIER_LENGTH
            ));
        }

        if !IDENTIFIER_REGEX.is_match(name) {
            return Err(anyhow!(
                "Invalid identifier '{}': must start with letter/underscore and contain only alphanumeric characters and underscores",
                name
            ));
        }

        Ok(())
    }

    /// Validate message size
    pub fn validate_message_size(size: usize) -> Result<()> {
        if size > MAX_MESSAGE_SIZE {
            return Err(anyhow!(
                "Message size {} bytes exceeds maximum {}",
                size,
                MAX_MESSAGE_SIZE
            ));
        }
        Ok(())
    }

    /// Validate topic name
    ///
    /// Topic format: `cdc.{database}.{schema}.{table}`
    pub fn validate_topic_name(topic: &str) -> Result<()> {
        let parts: Vec<&str> = topic.split('.').collect();

        if parts.len() != 4 {
            return Err(anyhow!(
                "Invalid topic format '{}': expected 'cdc.database.schema.table'",
                topic
            ));
        }

        if parts[0] != "cdc" {
            return Err(anyhow!("Topic must start with 'cdc.', got '{}'", parts[0]));
        }

        // Validate each component
        for (i, part) in parts.iter().enumerate().skip(1) {
            Self::validate_identifier(part)
                .map_err(|e| anyhow!("Invalid topic component {}: {}", i, e))?;
        }

        Ok(())
    }

    /// Validate a connection URL
    pub fn validate_connection_url(url: &str) -> Result<()> {
        // Basic URL validation
        if url.is_empty() {
            return Err(anyhow!("Connection URL cannot be empty"));
        }

        // Check for common URL schemes
        let valid_schemes = ["postgres://", "postgresql://", "mysql://", "mariadb://"];
        if !valid_schemes.iter().any(|s| url.starts_with(s)) {
            return Err(anyhow!(
                "Invalid connection URL scheme. Expected one of: {:?}",
                valid_schemes
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_identifiers() {
        assert!(Validator::validate_identifier("my_table").is_ok());
        assert!(Validator::validate_identifier("MyTable123").is_ok());
        assert!(Validator::validate_identifier("_private").is_ok());
        assert!(Validator::validate_identifier("table_").is_ok());
    }

    #[test]
    fn test_invalid_identifiers() {
        // Empty
        assert!(Validator::validate_identifier("").is_err());

        // Starts with number
        assert!(Validator::validate_identifier("123table").is_err());

        // Contains special characters
        assert!(Validator::validate_identifier("table-name").is_err());
        assert!(Validator::validate_identifier("table.name").is_err());
        assert!(Validator::validate_identifier("table;DROP").is_err());
        assert!(Validator::validate_identifier("table'; DROP TABLE users; --").is_err());

        // Too long
        let long_name = "a".repeat(256);
        assert!(Validator::validate_identifier(&long_name).is_err());
    }

    #[test]
    fn test_sql_injection_attempts() {
        let malicious_inputs = vec![
            "'; DROP TABLE users; --",
            "1' OR '1'='1",
            "admin'--",
            "' UNION SELECT * FROM passwords--",
            "../../../etc/passwd",
            "table; DELETE FROM users",
        ];

        for input in malicious_inputs {
            assert!(
                Validator::validate_identifier(input).is_err(),
                "Should reject malicious input: {}",
                input
            );
        }
    }

    #[test]
    fn test_valid_topic_names() {
        assert!(Validator::validate_topic_name("cdc.mydb.public.users").is_ok());
        assert!(Validator::validate_topic_name("cdc.test_db.my_schema.my_table").is_ok());
    }

    #[test]
    fn test_invalid_topic_names() {
        assert!(Validator::validate_topic_name("invalid").is_err());
        assert!(Validator::validate_topic_name("cdc.db").is_err());
        assert!(Validator::validate_topic_name("other.db.schema.table").is_err());
        assert!(Validator::validate_topic_name("cdc.db.schema.table; DROP TABLE users").is_err());
    }

    #[test]
    fn test_message_size_validation() {
        assert!(Validator::validate_message_size(1024).is_ok());
        assert!(Validator::validate_message_size(64 * 1024 * 1024).is_ok());
        assert!(Validator::validate_message_size(64 * 1024 * 1024 + 1).is_err());
    }

    #[test]
    fn test_connection_url_validation() {
        assert!(Validator::validate_connection_url("postgres://localhost/db").is_ok());
        assert!(Validator::validate_connection_url("postgresql://user:pass@host/db").is_ok());
        assert!(Validator::validate_connection_url("mysql://localhost/db").is_ok());
        assert!(Validator::validate_connection_url("mariadb://localhost/db").is_ok());

        assert!(Validator::validate_connection_url("").is_err());
        assert!(Validator::validate_connection_url("http://localhost").is_err());
        assert!(Validator::validate_connection_url("invalid").is_err());
    }
}
