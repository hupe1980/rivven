//! Input validation for Rivven operations
//!
//! Provides security validation for identifiers, limits, and configuration.
//! Used to prevent injection attacks, path traversal, and resource exhaustion.

use once_cell::sync::Lazy;
use regex::Regex;

/// Maximum allowed identifier length (255 for flexibility, matches PostgreSQL)
pub const MAX_IDENTIFIER_LENGTH: usize = 255;

/// Maximum message size (10 MB default, configurable)
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Minimum topic name length
pub const MIN_TOPIC_NAME_LENGTH: usize = 1;

/// Maximum topic name length
pub const MAX_TOPIC_NAME_LENGTH: usize = 255;

/// Regex for validating SQL-style identifiers
static IDENTIFIER_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]{0,254}$").unwrap());

/// Regex for validating topic names (allows dots, hyphens, underscores)
/// Matches Kafka topic naming conventions
static TOPIC_NAME_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,254}$").unwrap());

/// Validation error type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    /// Identifier is empty
    EmptyIdentifier,
    /// Identifier is too long
    IdentifierTooLong { len: usize, max: usize },
    /// Identifier contains invalid characters
    InvalidIdentifier { name: String, reason: &'static str },
    /// Topic name is empty
    EmptyTopicName,
    /// Topic name is too long
    TopicNameTooLong { len: usize, max: usize },
    /// Topic name contains invalid characters
    InvalidTopicName { name: String, reason: &'static str },
    /// Message size exceeds limit
    MessageTooLarge { size: usize, max: usize },
    /// Partition number is invalid
    InvalidPartition { partition: u32, max: u32 },
    /// Consumer group ID is invalid
    InvalidConsumerGroupId { id: String },
    /// Client ID is invalid
    InvalidClientId { id: String },
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyIdentifier => write!(f, "Identifier cannot be empty"),
            Self::IdentifierTooLong { len, max } => {
                write!(f, "Identifier too long: {} chars (max: {})", len, max)
            }
            Self::InvalidIdentifier { name, reason } => {
                write!(f, "Invalid identifier '{}': {}", name, reason)
            }
            Self::EmptyTopicName => write!(f, "Topic name cannot be empty"),
            Self::TopicNameTooLong { len, max } => {
                write!(f, "Topic name too long: {} chars (max: {})", len, max)
            }
            Self::InvalidTopicName { name, reason } => {
                write!(f, "Invalid topic name '{}': {}", name, reason)
            }
            Self::MessageTooLarge { size, max } => {
                write!(f, "Message size {} exceeds maximum {}", size, max)
            }
            Self::InvalidPartition { partition, max } => {
                write!(f, "Invalid partition {}: must be < {}", partition, max)
            }
            Self::InvalidConsumerGroupId { id } => {
                write!(f, "Invalid consumer group ID: {}", id)
            }
            Self::InvalidClientId { id } => {
                write!(f, "Invalid client ID: {}", id)
            }
        }
    }
}

impl std::error::Error for ValidationError {}

/// Validator for Rivven inputs
pub struct Validator;

impl Validator {
    /// Validate a SQL-style identifier (slot names, internal identifiers)
    ///
    /// # Security
    ///
    /// Prevents SQL injection by rejecting:
    /// - Empty strings
    /// - Strings starting with numbers
    /// - Special characters (quotes, semicolons, etc.)
    /// - Excessively long strings
    pub fn validate_identifier(name: &str) -> Result<(), ValidationError> {
        if name.is_empty() {
            return Err(ValidationError::EmptyIdentifier);
        }

        if name.len() > MAX_IDENTIFIER_LENGTH {
            return Err(ValidationError::IdentifierTooLong {
                len: name.len(),
                max: MAX_IDENTIFIER_LENGTH,
            });
        }

        if !IDENTIFIER_REGEX.is_match(name) {
            return Err(ValidationError::InvalidIdentifier {
                name: name.to_string(),
                reason: "must start with letter/underscore and contain only alphanumeric characters and underscores",
            });
        }

        Ok(())
    }

    /// Validate a topic name
    ///
    /// Topic names follow Kafka conventions:
    /// - Must start with alphanumeric character
    /// - Can contain alphanumeric, dots, hyphens, and underscores
    /// - Maximum 255 characters
    pub fn validate_topic_name(name: &str) -> Result<(), ValidationError> {
        if name.is_empty() {
            return Err(ValidationError::EmptyTopicName);
        }

        if name.len() > MAX_TOPIC_NAME_LENGTH {
            return Err(ValidationError::TopicNameTooLong {
                len: name.len(),
                max: MAX_TOPIC_NAME_LENGTH,
            });
        }

        if !TOPIC_NAME_REGEX.is_match(name) {
            return Err(ValidationError::InvalidTopicName {
                name: name.to_string(),
                reason: "must start with alphanumeric and contain only alphanumeric, dots, hyphens, underscores",
            });
        }

        // Additional security checks for common attack patterns
        if name.contains("..") {
            return Err(ValidationError::InvalidTopicName {
                name: name.to_string(),
                reason: "path traversal patterns not allowed",
            });
        }

        Ok(())
    }

    /// Validate message size
    pub fn validate_message_size(size: usize, max: usize) -> Result<(), ValidationError> {
        if size > max {
            return Err(ValidationError::MessageTooLarge { size, max });
        }
        Ok(())
    }

    /// Validate partition number
    pub fn validate_partition(partition: u32, num_partitions: u32) -> Result<(), ValidationError> {
        if partition >= num_partitions {
            return Err(ValidationError::InvalidPartition {
                partition,
                max: num_partitions,
            });
        }
        Ok(())
    }

    /// Validate consumer group ID
    ///
    /// Uses the same rules as topic names
    pub fn validate_consumer_group_id(id: &str) -> Result<(), ValidationError> {
        if id.is_empty() || id.len() > MAX_IDENTIFIER_LENGTH {
            return Err(ValidationError::InvalidConsumerGroupId { id: id.to_string() });
        }

        if !TOPIC_NAME_REGEX.is_match(id) {
            return Err(ValidationError::InvalidConsumerGroupId { id: id.to_string() });
        }

        Ok(())
    }

    /// Validate client ID
    ///
    /// Uses the same rules as topic names
    pub fn validate_client_id(id: &str) -> Result<(), ValidationError> {
        if id.is_empty() || id.len() > MAX_IDENTIFIER_LENGTH {
            return Err(ValidationError::InvalidClientId { id: id.to_string() });
        }

        if !TOPIC_NAME_REGEX.is_match(id) {
            return Err(ValidationError::InvalidClientId { id: id.to_string() });
        }

        Ok(())
    }

    /// Sanitize a string for safe logging (remove control characters, truncate)
    pub fn sanitize_for_log(s: &str, max_len: usize) -> String {
        let sanitized: String = s
            .chars()
            .filter(|c| !c.is_control())
            .take(max_len)
            .collect();

        if s.len() > max_len {
            format!("{}...", sanitized)
        } else {
            sanitized
        }
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
        assert!(Validator::validate_identifier("a").is_ok());
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
        let long_name = "a".repeat(MAX_IDENTIFIER_LENGTH + 1);
        assert!(Validator::validate_identifier(&long_name).is_err());
    }

    #[test]
    fn test_sql_injection_attempts() {
        let attacks = [
            "table'; DROP TABLE users; --",
            "table\"; DROP TABLE users; --",
            "table`; DROP TABLE users; --",
            "table/**/OR/**/1=1",
            "table%27",
            "table\0",
            "table\n",
            "../../../etc/passwd",
        ];

        for attack in attacks {
            assert!(
                Validator::validate_identifier(attack).is_err(),
                "Should reject SQL injection attempt: {}",
                attack
            );
        }
    }

    #[test]
    fn test_valid_topic_names() {
        assert!(Validator::validate_topic_name("my-topic").is_ok());
        assert!(Validator::validate_topic_name("my_topic").is_ok());
        assert!(Validator::validate_topic_name("my.topic.name").is_ok());
        assert!(Validator::validate_topic_name("MyTopic123").is_ok());
        assert!(Validator::validate_topic_name("topic-with-many-dashes").is_ok());
    }

    #[test]
    fn test_invalid_topic_names() {
        // Empty
        assert!(Validator::validate_topic_name("").is_err());

        // Starts with invalid character
        assert!(Validator::validate_topic_name("-topic").is_err());
        assert!(Validator::validate_topic_name("_topic").is_err());
        assert!(Validator::validate_topic_name(".topic").is_err());

        // Path traversal
        assert!(Validator::validate_topic_name("../etc/passwd").is_err());
        assert!(Validator::validate_topic_name("topic..name").is_err());

        // Special characters
        assert!(Validator::validate_topic_name("topic;DROP").is_err());
    }

    #[test]
    fn test_message_size_validation() {
        let max = 1024;
        assert!(Validator::validate_message_size(100, max).is_ok());
        assert!(Validator::validate_message_size(1024, max).is_ok());
        assert!(Validator::validate_message_size(1025, max).is_err());
    }

    #[test]
    fn test_partition_validation() {
        assert!(Validator::validate_partition(0, 3).is_ok());
        assert!(Validator::validate_partition(2, 3).is_ok());
        assert!(Validator::validate_partition(3, 3).is_err());
        assert!(Validator::validate_partition(10, 3).is_err());
    }

    #[test]
    fn test_sanitize_for_log() {
        // Normal string
        assert_eq!(Validator::sanitize_for_log("hello", 100), "hello");

        // Truncation
        assert_eq!(Validator::sanitize_for_log("hello world", 5), "hello...");

        // Control characters removed
        let with_control = "hello\x00\x01\x02world";
        assert_eq!(Validator::sanitize_for_log(with_control, 100), "helloworld");

        // Newlines removed
        let with_newlines = "hello\nworld";
        assert_eq!(
            Validator::sanitize_for_log(with_newlines, 100),
            "helloworld"
        );
    }
}
