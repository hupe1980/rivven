//! Error types for the Rivven Kubernetes Operator

use thiserror::Error;

/// Errors that can occur during operator operations
#[derive(Error, Debug)]
pub enum OperatorError {
    /// Kubernetes API error
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    /// Resource not found
    #[error("Resource not found: {kind}/{name} in namespace {namespace}")]
    NotFound {
        kind: String,
        name: String,
        namespace: String,
    },

    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Reconciliation failed
    #[error("Reconciliation failed: {0}")]
    ReconcileFailed(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    /// YAML serialization error
    #[error("YAML serialization error: {0}")]
    YamlError(#[from] serde_yaml::Error),

    /// Validation error
    #[error("Validation error: {0}")]
    ValidationError(String),

    /// Finalizer error
    #[error("Finalizer error: {0}")]
    FinalizerError(String),

    /// Timeout error
    #[error("Operation timed out: {0}")]
    Timeout(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),

    /// Cluster not found
    #[error("RivvenCluster not found: {0}")]
    ClusterNotFound(String),

    /// Connection to cluster failed
    #[error("Connection to cluster failed: {0}")]
    ConnectionFailed(String),

    /// Cluster operation error
    #[error("Cluster operation error: {0}")]
    ClusterError(String),

    /// Topic operation failed
    #[error("Topic operation failed: {0}")]
    TopicError(String),

    /// Connect operation failed
    #[error("Connect operation failed: {0}")]
    ConnectError(String),
}

/// Result type for operator operations
pub type Result<T> = std::result::Result<T, OperatorError>;

impl OperatorError {
    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            OperatorError::KubeError(_)
                | OperatorError::Timeout(_)
                | OperatorError::ReconcileFailed(_)
        )
    }

    /// Get a suggested requeue delay for retryable errors
    pub fn requeue_delay(&self) -> Option<std::time::Duration> {
        if self.is_retryable() {
            Some(std::time::Duration::from_secs(30))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = OperatorError::NotFound {
            kind: "StatefulSet".to_string(),
            name: "rivven-cluster".to_string(),
            namespace: "default".to_string(),
        };
        assert!(err.to_string().contains("StatefulSet"));
        assert!(err.to_string().contains("rivven-cluster"));
    }

    #[test]
    fn test_retryable_errors() {
        let timeout_err = OperatorError::Timeout("test".to_string());
        assert!(timeout_err.is_retryable());

        let validation_err = OperatorError::ValidationError("test".to_string());
        assert!(!validation_err.is_retryable());
    }

    #[test]
    fn test_requeue_delay() {
        let retryable = OperatorError::Timeout("test".to_string());
        assert!(retryable.requeue_delay().is_some());

        let not_retryable = OperatorError::InvalidConfig("test".to_string());
        assert!(not_retryable.requeue_delay().is_none());
    }
}
