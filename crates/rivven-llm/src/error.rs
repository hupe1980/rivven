//! Error types for rivven-llm

/// Result type alias for LLM operations
pub type LlmResult<T> = std::result::Result<T, LlmError>;

/// Errors that can occur during LLM operations
#[derive(Debug, thiserror::Error)]
pub enum LlmError {
    /// Provider configuration error
    #[error("configuration error: {0}")]
    Config(String),

    /// Authentication failure (invalid API key, expired credentials)
    #[error("authentication error: {0}")]
    Auth(String),

    /// Rate limited by the provider
    #[error("rate limited: {message}")]
    RateLimited {
        message: String,
        /// Retry-after hint in seconds (if provided by the API)
        retry_after_secs: Option<u64>,
    },

    /// Request timeout
    #[error("timeout: {0}")]
    Timeout(String),

    /// Model not found or not available
    #[error("model not found: {0}")]
    ModelNotFound(String),

    /// Content was filtered by safety systems
    #[error("content filtered: {0}")]
    ContentFiltered(String),

    /// Token limit exceeded (input too long)
    #[error("token limit exceeded: {0}")]
    TokenLimitExceeded(String),

    /// Provider returned an error response
    #[error("provider error ({status}): {message}")]
    Provider { status: u16, message: String },

    /// Network / transport error
    #[error("connection error: {0}")]
    Connection(String),

    /// Serialization / deserialization error
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Transient error (can be retried)
    #[error("transient error: {0}")]
    Transient(String),

    /// Internal / unexpected error
    #[error("internal error: {0}")]
    Internal(String),
}

impl LlmError {
    /// Create a timeout error from a Duration
    pub fn timeout(duration: std::time::Duration) -> Self {
        let secs = duration.as_secs();
        let ms = duration.subsec_millis();
        if secs >= 60 {
            Self::Timeout(format!("{}m{}s", secs / 60, secs % 60))
        } else {
            Self::Timeout(format!("{secs}.{ms:03}s"))
        }
    }

    /// Whether this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            LlmError::RateLimited { .. }
                | LlmError::Timeout(_)
                | LlmError::Connection(_)
                | LlmError::Transient(_)
        )
    }

    /// Whether this error is an authentication error
    pub fn is_auth(&self) -> bool {
        matches!(self, LlmError::Auth(_))
    }

    /// Whether this error was rate-limited
    pub fn is_rate_limited(&self) -> bool {
        matches!(self, LlmError::RateLimited { .. })
    }

    /// Get retry-after hint if available
    pub fn retry_after_secs(&self) -> Option<u64> {
        match self {
            LlmError::RateLimited {
                retry_after_secs, ..
            } => *retry_after_secs,
            _ => None,
        }
    }

    /// Truncate error message to a maximum length (for logging safety)
    pub fn truncated_message(&self, max_len: usize) -> String {
        let msg = self.to_string();
        if msg.len() <= max_len {
            msg
        } else {
            // Find the last char boundary at or before max_len to avoid
            // panicking on multi-byte UTF-8 sequences.
            let boundary = msg
                .char_indices()
                .take_while(|(i, _)| *i <= max_len)
                .last()
                .map(|(i, _)| i)
                .unwrap_or(0);
            format!("{}...[truncated]", &msg[..boundary])
        }
    }
}

#[cfg(feature = "openai")]
impl From<reqwest::Error> for LlmError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_timeout() {
            LlmError::Timeout("request timed out".to_string())
        } else if err.is_connect() {
            LlmError::Connection(err.to_string())
        } else if let Some(status) = err.status() {
            match status.as_u16() {
                401 | 403 => LlmError::Auth(err.to_string()),
                429 => LlmError::RateLimited {
                    message: err.to_string(),
                    retry_after_secs: None,
                },
                _ => LlmError::Provider {
                    status: status.as_u16(),
                    message: err.to_string(),
                },
            }
        } else {
            LlmError::Connection(err.to_string())
        }
    }
}

impl From<serde_json::Error> for LlmError {
    fn from(err: serde_json::Error) -> Self {
        LlmError::Serialization(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_retryable() {
        assert!(LlmError::RateLimited {
            message: "slow down".into(),
            retry_after_secs: None,
        }
        .is_retryable());
        assert!(LlmError::Timeout("t".into()).is_retryable());
        assert!(LlmError::Connection("c".into()).is_retryable());
        assert!(LlmError::Transient("t".into()).is_retryable());
        assert!(!LlmError::Auth("a".into()).is_retryable());
        assert!(!LlmError::Config("c".into()).is_retryable());
    }

    #[test]
    fn test_is_auth() {
        assert!(LlmError::Auth("bad key".into()).is_auth());
        assert!(!LlmError::Config("x".into()).is_auth());
    }

    #[test]
    fn test_is_rate_limited() {
        assert!(LlmError::RateLimited {
            message: "x".into(),
            retry_after_secs: Some(5),
        }
        .is_rate_limited());
        assert!(!LlmError::Timeout("x".into()).is_rate_limited());
    }

    #[test]
    fn test_retry_after_secs() {
        let err = LlmError::RateLimited {
            message: "x".into(),
            retry_after_secs: Some(42),
        };
        assert_eq!(err.retry_after_secs(), Some(42));

        let err2 = LlmError::Config("x".into());
        assert_eq!(err2.retry_after_secs(), None);
    }

    #[test]
    fn test_truncated_message_short() {
        let err = LlmError::Config("short".into());
        let msg = err.truncated_message(1000);
        assert_eq!(msg, "configuration error: short");
    }

    #[test]
    fn test_truncated_message_long() {
        let long = "a".repeat(500);
        let err = LlmError::Config(long);
        let msg = err.truncated_message(50);
        assert!(msg.len() < 80); // 50 + "...[truncated]"
        assert!(msg.ends_with("...[truncated]"));
    }

    #[test]
    fn test_truncated_message_utf8_boundary() {
        // Multi-byte chars: each '🔒' is 4 bytes
        let emoji_msg = "🔒".repeat(20); // 80 bytes
        let err = LlmError::Config(emoji_msg);
        // Truncate at byte 22 — must NOT split a 4-byte emoji.
        // The prefix "configuration error: " is 22 bytes, so max_len=26
        // should cleanly cut after the prefix + 1 emoji.
        let msg = err.truncated_message(26);
        assert!(msg.ends_with("...[truncated]"));
        // Must not panic and must be valid UTF-8
        assert!(msg.is_char_boundary(0));
    }

    #[test]
    fn test_truncated_message_zero() {
        let err = LlmError::Config("hello".into());
        let msg = err.truncated_message(0);
        assert!(msg.ends_with("...[truncated]"));
    }

    #[test]
    fn test_timeout_display() {
        let err = LlmError::timeout(std::time::Duration::from_millis(1500));
        assert_eq!(err.to_string(), "timeout: 1.500s");
    }

    #[test]
    fn test_timeout_display_minutes() {
        let err = LlmError::timeout(std::time::Duration::from_secs(125));
        assert_eq!(err.to_string(), "timeout: 2m5s");
    }
}
