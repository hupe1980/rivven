//! Traits for CDC sources
//!
//! Database-agnostic trait definitions.

use crate::common::Result;
use async_trait::async_trait;

/// Trait for CDC source implementations
///
/// Implement this trait to create a CDC source for a specific database.
#[async_trait]
pub trait CdcSource: Send + Sync {
    /// Start capturing changes
    ///
    /// This begins the CDC process. For streaming sources, this typically
    /// spawns a background task that processes the replication stream.
    async fn start(&mut self) -> Result<()>;

    /// Stop capturing changes
    ///
    /// Gracefully stop the CDC process. This should signal any background
    /// tasks to stop and wait for them to finish.
    async fn stop(&mut self) -> Result<()>;

    /// Check if the source is healthy
    ///
    /// Returns true if the source is connected and processing events.
    async fn is_healthy(&self) -> bool;
}

/// Configuration trait for CDC sources
pub trait CdcConfig: Send + Sync {
    /// Get the source type name (e.g., "postgres", "mysql")
    fn source_type(&self) -> &'static str;

    /// Get the connection string
    fn connection_string(&self) -> &str;

    /// Validate the configuration
    fn validate(&self) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockCdcSource {
        active: bool,
    }

    #[async_trait]
    impl CdcSource for MockCdcSource {
        async fn start(&mut self) -> Result<()> {
            self.active = true;
            Ok(())
        }

        async fn stop(&mut self) -> Result<()> {
            self.active = false;
            Ok(())
        }

        async fn is_healthy(&self) -> bool {
            self.active
        }
    }

    #[tokio::test]
    async fn test_mock_source() {
        let mut source = MockCdcSource { active: false };

        assert!(!source.is_healthy().await);

        source.start().await.unwrap();
        assert!(source.is_healthy().await);

        source.stop().await.unwrap();
        assert!(!source.is_healthy().await);
    }
}
