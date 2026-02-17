//! Schema Registry configuration

use crate::compatibility::CompatibilityLevel;
use serde::{Deserialize, Serialize};

/// Configuration for the Schema Registry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryConfig {
    /// Storage backend configuration
    pub storage: StorageConfig,

    /// Default compatibility level
    #[serde(default)]
    pub compatibility: CompatibilityLevel,

    /// Enable schema normalization before storing
    #[serde(default = "default_true")]
    pub normalize_schemas: bool,

    /// Schema ID generation mode
    #[serde(default)]
    pub id_generation: IdGeneration,
}

fn default_true() -> bool {
    true
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            storage: StorageConfig::Memory,
            compatibility: CompatibilityLevel::default(),
            normalize_schemas: true,
            id_generation: IdGeneration::default(),
        }
    }
}

impl RegistryConfig {
    /// Create config with in-memory storage
    pub fn memory() -> Self {
        Self {
            storage: StorageConfig::Memory,
            ..Default::default()
        }
    }

    /// Create config with broker-backed storage
    pub fn broker(config: BrokerStorageConfig) -> Self {
        Self {
            storage: StorageConfig::Broker(config),
            ..Default::default()
        }
    }

    /// Set compatibility level
    pub fn with_compatibility(mut self, level: CompatibilityLevel) -> Self {
        self.compatibility = level;
        self
    }

    /// Set schema normalization
    pub fn with_normalize(mut self, normalize: bool) -> Self {
        self.normalize_schemas = normalize;
        self
    }
}

/// Storage backend configuration
///
/// The schema registry supports multiple storage backends:
/// - **Memory**: In-memory storage for development and testing
/// - **Broker**: Durable storage in rivven broker topics (recommended for production)
/// - **Glue**: AWS Glue Schema Registry for AWS-native deployments
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageConfig {
    /// In-memory storage (default for development)
    #[default]
    Memory,

    /// Broker-backed storage (durable, replicated)
    ///
    /// Stores schemas in a compacted rivven topic (similar to Kafka's `_schemas` topic).
    /// Provides durability and automatic replication across cluster nodes.
    Broker(BrokerStorageConfig),

    /// AWS Glue Schema Registry (external)
    Glue {
        region: String,
        registry_name: Option<String>,
    },
}

/// Configuration for broker-backed schema storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerStorageConfig {
    /// Broker address(es) to connect to
    /// Format: "host:port" or "host1:port1,host2:port2" for multiple brokers
    pub brokers: String,

    /// Topic name for storing schemas (default: "_schemas")
    #[serde(default = "default_schema_topic")]
    pub topic: String,

    /// Replication factor for the schema topic (default: 3)
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u16,

    /// Number of partitions for the schema topic (default: 1)
    /// Note: Using 1 partition ensures global ordering for schema IDs
    #[serde(default = "default_partitions")]
    pub partitions: u32,

    /// TLS configuration for broker connection
    #[serde(default)]
    pub tls: Option<BrokerTlsConfig>,

    /// Authentication configuration
    #[serde(default)]
    pub auth: Option<BrokerAuthConfig>,

    /// Connection timeout in milliseconds (default: 10000)
    #[serde(default = "default_timeout")]
    pub connect_timeout_ms: u64,

    /// Bootstrap timeout - how long to wait for initial schema load (default: 30000)
    #[serde(default = "default_bootstrap_timeout")]
    pub bootstrap_timeout_ms: u64,
}

fn default_schema_topic() -> String {
    "_schemas".to_string()
}

fn default_replication_factor() -> u16 {
    3
}

fn default_partitions() -> u32 {
    1
}

fn default_timeout() -> u64 {
    10000
}

fn default_bootstrap_timeout() -> u64 {
    30000
}

impl Default for BrokerStorageConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            topic: default_schema_topic(),
            replication_factor: default_replication_factor(),
            partitions: default_partitions(),
            tls: None,
            auth: None,
            connect_timeout_ms: default_timeout(),
            bootstrap_timeout_ms: default_bootstrap_timeout(),
        }
    }
}

impl BrokerStorageConfig {
    /// Create broker storage config with default topic name
    pub fn new(brokers: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            ..Default::default()
        }
    }

    /// Set custom topic name
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = topic.into();
        self
    }

    /// Set replication factor
    pub fn with_replication_factor(mut self, factor: u16) -> Self {
        self.replication_factor = factor;
        self
    }

    /// Enable TLS
    pub fn with_tls(mut self, config: BrokerTlsConfig) -> Self {
        self.tls = Some(config);
        self
    }

    /// Set authentication
    pub fn with_auth(mut self, config: BrokerAuthConfig) -> Self {
        self.auth = Some(config);
        self
    }
}

/// TLS configuration for broker connection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerTlsConfig {
    /// Path to CA certificate file
    pub ca_cert: Option<String>,

    /// Path to client certificate file (for mTLS)
    pub client_cert: Option<String>,

    /// Path to client key file (for mTLS)
    pub client_key: Option<String>,

    /// Skip server certificate verification (NOT recommended for production)
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

/// Authentication configuration for broker connection
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum BrokerAuthConfig {
    /// SASL/PLAIN authentication
    Plain { username: String, password: String },

    /// SASL/SCRAM-SHA-256 authentication
    ScramSha256 { username: String, password: String },

    /// SASL/SCRAM-SHA-512 authentication
    ScramSha512 { username: String, password: String },
}

impl std::fmt::Debug for BrokerAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain { username, .. } => f
                .debug_struct("Plain")
                .field("username", username)
                .field("password", &"[REDACTED]")
                .finish(),
            Self::ScramSha256 { username, .. } => f
                .debug_struct("ScramSha256")
                .field("username", username)
                .field("password", &"[REDACTED]")
                .finish(),
            Self::ScramSha512 { username, .. } => f
                .debug_struct("ScramSha512")
                .field("username", username)
                .field("password", &"[REDACTED]")
                .finish(),
        }
    }
}

/// Schema ID generation mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum IdGeneration {
    /// Auto-increment IDs (default)
    #[default]
    AutoIncrement,

    /// Hash-based IDs (deterministic)
    Hash,

    /// External ID provider
    External,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RegistryConfig::default();
        assert!(matches!(config.storage, StorageConfig::Memory));
        assert_eq!(config.compatibility, CompatibilityLevel::Backward);
        assert!(config.normalize_schemas);
    }

    #[test]
    fn test_memory_config() {
        let config = RegistryConfig::memory();
        assert!(matches!(config.storage, StorageConfig::Memory));
    }

    #[test]
    fn test_broker_config() {
        let broker_config = BrokerStorageConfig::new("localhost:9092")
            .with_topic("my_schemas")
            .with_replication_factor(3);

        let config = RegistryConfig::broker(broker_config);
        if let StorageConfig::Broker(bc) = config.storage {
            assert_eq!(bc.brokers, "localhost:9092");
            assert_eq!(bc.topic, "my_schemas");
            assert_eq!(bc.replication_factor, 3);
        } else {
            panic!("Expected Broker storage config");
        }
    }

    #[test]
    fn test_broker_storage_defaults() {
        let config = BrokerStorageConfig::default();
        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.topic, "_schemas");
        assert_eq!(config.replication_factor, 3);
        assert_eq!(config.partitions, 1);
        assert!(config.tls.is_none());
        assert!(config.auth.is_none());
    }
}
