//! Configuration types for rivven-connect
//!
//! Architecture:
//!   Sources → publish to → Broker Topics
//!   Broker Topics → consumed by → Sinks

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::LazyLock;

/// Pre-compiled regex for environment variable expansion
/// Pattern: ${VAR} or ${VAR:-default}
static ENV_VAR_REGEX: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"\$\{([A-Z_][A-Z0-9_]*)(?::-([^}]*))?\}")
        .expect("env var regex pattern is invalid - this is a bug")
});

/// Root configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConnectConfig {
    /// Configuration version
    #[serde(default = "default_version")]
    pub version: String,

    /// Broker connection configuration
    pub broker: BrokerConfig,

    /// Source connectors (read from external systems, publish to topics)
    #[serde(default)]
    pub sources: HashMap<String, SourceConfig>,

    /// Sink connectors (consume from topics, write to external systems)
    #[serde(default)]
    pub sinks: HashMap<String, SinkConfig>,

    /// Global settings
    #[serde(default)]
    pub settings: GlobalSettings,
}

fn default_version() -> String {
    "1.0".to_string()
}

/// Broker connection configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BrokerConfig {
    /// Bootstrap servers (host:port) - client connects to first available
    /// Supports single server or list for high availability
    pub bootstrap_servers: Vec<String>,

    /// Metadata refresh interval in milliseconds
    /// Periodically refreshes broker list from cluster
    #[serde(default = "default_metadata_refresh_ms")]
    pub metadata_refresh_ms: u64,

    /// Connection timeout in milliseconds
    #[serde(default = "default_connection_timeout_ms")]
    pub connection_timeout_ms: u64,

    /// Request timeout in milliseconds
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,

    /// TLS configuration
    #[serde(default)]
    pub tls: TlsConfig,
}

fn default_metadata_refresh_ms() -> u64 {
    300_000 // 5 minutes
}

fn default_connection_timeout_ms() -> u64 {
    10_000 // 10 seconds
}

fn default_request_timeout_ms() -> u64 {
    30_000 // 30 seconds
}

/// TLS configuration for broker connection
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct TlsConfig {
    /// Enable TLS
    #[serde(default)]
    pub enabled: bool,

    /// Path to client certificate (for mTLS)
    pub cert_path: Option<PathBuf>,

    /// Path to client private key (for mTLS)
    pub key_path: Option<PathBuf>,

    /// Path to CA certificate for server verification
    pub ca_path: Option<PathBuf>,

    /// Skip server certificate verification (DANGEROUS - testing only)
    #[serde(default)]
    pub insecure: bool,

    /// Server name for SNI
    pub server_name: Option<String>,
}

/// Source connector configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceConfig {
    /// Connector type (e.g., "postgres-cdc", "mysql-cdc", "http")
    pub connector: String,

    /// Topic to publish events to
    pub topic: String,

    /// Topic creation settings (overrides global settings.topic)
    #[serde(default)]
    pub topic_config: Option<SourceTopicConfig>,

    /// Connector-specific configuration
    #[serde(default)]
    pub config: serde_yaml::Value,

    /// Tables/streams to capture (connector-specific)
    #[serde(default)]
    pub tables: Vec<TableConfig>,

    /// Topic routing pattern (optional)
    /// Supports placeholders: {schema}, {table}, {database}
    pub topic_routing: Option<String>,

    /// Whether this source is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,
}

/// Per-source topic configuration (overrides global defaults)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceTopicConfig {
    /// Number of partitions (overrides settings.topic.default_partitions)
    pub partitions: Option<u32>,

    /// Replication factor (overrides settings.topic.default_replication_factor)
    pub replication_factor: Option<u16>,

    /// Disable auto-create for this specific source
    pub auto_create: Option<bool>,
}

/// Table/stream configuration for sources
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TableConfig {
    /// Schema/namespace
    #[serde(default)]
    pub schema: Option<String>,

    /// Table name
    pub table: String,

    /// Override topic for this table
    pub topic: Option<String>,
}

/// Sink connector configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SinkConfig {
    /// Connector type (e.g., "s3", "elasticsearch", "http")
    pub connector: String,

    /// Topics to consume from (supports wildcards like "cdc.*")
    pub topics: Vec<String>,

    /// Consumer group for offset tracking
    pub consumer_group: String,

    /// Connector-specific configuration
    #[serde(default)]
    pub config: serde_yaml::Value,

    /// Whether this sink is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Starting offset (earliest, latest, or specific)
    #[serde(default)]
    pub start_offset: StartOffset,

    /// Rate limiting configuration
    #[serde(default)]
    pub rate_limit: SinkRateLimitConfig,
}

/// Rate limiting configuration for sinks
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SinkRateLimitConfig {
    /// Maximum events per second (0 = unlimited)
    #[serde(default)]
    pub events_per_second: u64,

    /// Burst capacity (extra events allowed above steady rate)
    /// Default: 10% of events_per_second or minimum 10
    #[serde(default)]
    pub burst_capacity: Option<u64>,
}

impl SinkRateLimitConfig {
    /// Check if rate limiting is enabled
    pub fn is_enabled(&self) -> bool {
        self.events_per_second > 0
    }

    /// Convert to rate limiter config
    pub fn to_rate_limiter_config(&self) -> crate::rate_limiter::RateLimitConfig {
        if !self.is_enabled() {
            crate::rate_limiter::RateLimitConfig::unlimited()
        } else if let Some(burst) = self.burst_capacity {
            crate::rate_limiter::RateLimitConfig::with_burst(self.events_per_second, burst)
        } else {
            crate::rate_limiter::RateLimitConfig::new(self.events_per_second)
        }
    }
}

/// Starting offset for sink consumers
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum StartOffset {
    /// Start from the earliest available offset
    Earliest,
    /// Start from the latest offset (new messages only)
    #[default]
    Latest,
    /// Start from a specific timestamp (ISO 8601)
    Timestamp(String),
}

/// Global settings
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GlobalSettings {
    /// Directory for connector state (offsets, checkpoints)
    #[serde(default = "default_state_dir")]
    pub state_dir: PathBuf,

    /// Topic auto-creation settings
    #[serde(default)]
    pub topic: TopicSettings,

    /// Retry configuration for broker connections
    #[serde(default)]
    pub retry: RetryConfig,

    /// Health check configuration
    #[serde(default)]
    pub health: HealthConfig,

    /// Metrics configuration
    #[serde(default)]
    pub metrics: MetricsConfig,

    /// OpenTelemetry tracing configuration
    #[serde(default)]
    pub telemetry: crate::telemetry::TelemetryConfig,

    /// Log level
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

/// Topic creation and management settings
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TopicSettings {
    /// Enable automatic topic creation for sources
    /// When true, topics are created before starting connectors
    #[serde(default = "default_true")]
    pub auto_create: bool,

    /// Default number of partitions for auto-created topics
    #[serde(default = "default_partitions")]
    pub default_partitions: u32,

    /// Default replication factor for auto-created topics
    /// Only used in cluster mode (ignored for single-node)
    #[serde(default = "default_replication_factor")]
    pub default_replication_factor: u16,

    /// Fail if topic doesn't exist and auto_create is false
    /// When false, connector waits for topic to be created
    #[serde(default = "default_true")]
    pub require_topic_exists: bool,

    /// Validate topic configuration matches expected settings
    /// Warns if existing topic has different partition count
    #[serde(default)]
    pub validate_existing: bool,
}

impl Default for TopicSettings {
    fn default() -> Self {
        Self {
            auto_create: true,
            default_partitions: 1,
            default_replication_factor: 1,
            require_topic_exists: true,
            validate_existing: false,
        }
    }
}

fn default_partitions() -> u32 {
    1
}

fn default_replication_factor() -> u16 {
    1
}

impl Default for GlobalSettings {
    fn default() -> Self {
        Self {
            state_dir: default_state_dir(),
            topic: TopicSettings::default(),
            retry: RetryConfig::default(),
            health: HealthConfig::default(),
            metrics: MetricsConfig::default(),
            telemetry: crate::telemetry::TelemetryConfig::default(),
            log_level: default_log_level(),
        }
    }
}

/// Retry configuration for transient failures
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Initial backoff in milliseconds
    #[serde(default = "default_initial_backoff")]
    pub initial_backoff_ms: u64,

    /// Maximum backoff in milliseconds
    #[serde(default = "default_max_backoff")]
    pub max_backoff_ms: u64,

    /// Backoff multiplier
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            initial_backoff_ms: default_initial_backoff(),
            max_backoff_ms: default_max_backoff(),
            backoff_multiplier: default_backoff_multiplier(),
        }
    }
}

fn default_max_retries() -> u32 {
    10
}
fn default_initial_backoff() -> u64 {
    100
}
fn default_max_backoff() -> u64 {
    30_000
}
fn default_backoff_multiplier() -> f64 {
    2.0
}

/// Health check endpoint configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HealthConfig {
    /// Enable health check HTTP endpoint
    #[serde(default)]
    pub enabled: bool,

    /// Port for health check endpoint
    #[serde(default = "default_health_port")]
    pub port: u16,

    /// Health check endpoint path
    #[serde(default = "default_health_path")]
    pub path: String,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: default_health_port(),
            path: default_health_path(),
        }
    }
}

fn default_health_port() -> u16 {
    8080
}
fn default_health_path() -> String {
    "/health".to_string()
}

fn default_state_dir() -> PathBuf {
    PathBuf::from("./data/connect-state")
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_true() -> bool {
    true
}

/// Metrics configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MetricsConfig {
    /// Enable metrics endpoint
    #[serde(default)]
    pub enabled: bool,

    /// Metrics port
    #[serde(default = "default_metrics_port")]
    pub port: u16,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: default_metrics_port(),
        }
    }
}

fn default_metrics_port() -> u16 {
    9091
}

impl ConnectConfig {
    /// Load configuration from a YAML file
    pub fn from_file(path: &PathBuf) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Failed to read config file: {}", e))?;

        // Expand environment variables
        let expanded = Self::expand_env_vars(&content);

        let config: Self = serde_yaml::from_str(&expanded)
            .map_err(|e| anyhow::anyhow!("Failed to parse config: {}", e))?;

        config.validate()?;
        Ok(config)
    }

    /// Expand environment variables in the format ${VAR} or ${VAR:-default}
    fn expand_env_vars(content: &str) -> String {
        ENV_VAR_REGEX
            .replace_all(content, |caps: &regex::Captures| {
                let var_name = &caps[1];
                let default = caps.get(2).map(|m| m.as_str());

                std::env::var(var_name).unwrap_or_else(|_| default.unwrap_or("").to_string())
            })
            .to_string()
    }

    /// Validate configuration
    pub fn validate(&self) -> anyhow::Result<()> {
        // Validate sources
        for (name, source) in &self.sources {
            if source.topic.is_empty() && source.topic_routing.is_none() {
                anyhow::bail!(
                    "Source '{}' must have either 'topic' or 'topic_routing'",
                    name
                );
            }
        }

        // Validate sinks
        for (name, sink) in &self.sinks {
            if sink.topics.is_empty() {
                anyhow::bail!("Sink '{}' must have at least one topic", name);
            }
            if sink.consumer_group.is_empty() {
                anyhow::bail!("Sink '{}' must have a consumer_group", name);
            }
        }

        // Validate connector-specific configs
        self.validate_connector_configs()?;

        Ok(())
    }

    /// Validate connector-specific configurations using SDK types
    fn validate_connector_configs(&self) -> anyhow::Result<()> {
        use crate::connectors::postgres_cdc::PostgresCdcConfig;
        use crate::connectors::stdout::StdoutSinkConfig;
        use validator::Validate;

        // Validate source configs
        for (name, source) in &self.sources {
            match source.connector.as_str() {
                "postgres-cdc" => {
                    let pg_config: PostgresCdcConfig =
                        serde_yaml::from_value(source.config.clone()).map_err(|e| {
                            anyhow::anyhow!("Source '{}': invalid postgres-cdc config: {}", name, e)
                        })?;
                    pg_config.validate().map_err(|e| {
                        anyhow::anyhow!("Source '{}': config validation failed: {}", name, e)
                    })?;
                }
                "http" => {
                    // Basic validation - http connector not fully implemented yet
                }
                unknown => {
                    // Unknown connectors - warn but don't fail (may be custom)
                    tracing::warn!(
                        "Source '{}': unknown connector type '{}', skipping config validation",
                        name,
                        unknown
                    );
                }
            }
        }

        // Validate sink configs
        for (name, sink) in &self.sinks {
            match sink.connector.as_str() {
                "stdout" => {
                    // StdoutSinkConfig has sensible defaults, parse to validate
                    let _: StdoutSinkConfig =
                        serde_yaml::from_value(sink.config.clone()).unwrap_or_default();
                }
                "s3" | "http" => {
                    // Not fully implemented yet - basic validation only
                }
                unknown => {
                    tracing::warn!(
                        "Sink '{}': unknown connector type '{}', skipping config validation",
                        name,
                        unknown
                    );
                }
            }
        }

        Ok(())
    }

    /// Get enabled sources
    pub fn enabled_sources(&self) -> impl Iterator<Item = (&String, &SourceConfig)> {
        self.sources.iter().filter(|(_, s)| s.enabled)
    }

    /// Get enabled sinks
    pub fn enabled_sinks(&self) -> impl Iterator<Item = (&String, &SinkConfig)> {
        self.sinks.iter().filter(|(_, s)| s.enabled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_var_expansion() {
        std::env::set_var("TEST_VAR", "hello");
        let content = "value: ${TEST_VAR}";
        let expanded = ConnectConfig::expand_env_vars(content);
        assert_eq!(expanded, "value: hello");
    }

    #[test]
    fn test_env_var_with_default() {
        std::env::remove_var("MISSING_VAR");
        let content = "value: ${MISSING_VAR:-default_value}";
        let expanded = ConnectConfig::expand_env_vars(content);
        assert_eq!(expanded, "value: default_value");
    }

    #[test]
    fn test_parse_config() {
        let yaml = r#"
version: "1.0"
broker:
  bootstrap_servers:
    - localhost:9092
sources:
  test:
    connector: postgres-cdc
    topic: test.events
    config:
      host: localhost
sinks:
  debug:
    connector: stdout
    topics: [test.events]
    consumer_group: debug
"#;
        let config: ConnectConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.broker.bootstrap_servers, vec!["localhost:9092"]);
        assert!(config.sources.contains_key("test"));
        assert!(config.sinks.contains_key("debug"));
    }

    #[test]
    fn test_validate_postgres_config_missing_required() {
        // Config missing required fields should fail validation
        let yaml = r#"
version: "1.0"
broker:
  bootstrap_servers:
    - localhost:9092
sources:
  pg:
    connector: postgres-cdc
    topic: test.events
    config:
      host: localhost
      # Missing: database, user, password
"#;
        let config: ConnectConfig = serde_yaml::from_str(yaml).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("postgres-cdc"),
            "Error should mention connector: {}",
            err
        );
    }

    #[test]
    fn test_validate_postgres_config_empty_host() {
        // Config with empty host should fail validation
        let yaml = r#"
version: "1.0"
broker:
  bootstrap_servers:
    - localhost:9092
sources:
  pg:
    connector: postgres-cdc
    topic: test.events
    config:
      host: ""
      port: 5432
      database: test
      user: test
      password: test
"#;
        let config: ConnectConfig = serde_yaml::from_str(yaml).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("validation failed") || err.contains("invalid"),
            "Error should mention validation: {}",
            err
        );
    }

    #[test]
    fn test_validate_valid_config() {
        // Valid config should pass
        let yaml = r#"
version: "1.0"
broker:
  bootstrap_servers:
    - localhost:9092
sources:
  pg:
    connector: postgres-cdc
    topic: test.events
    config:
      host: localhost
      port: 5432
      database: test
      user: test
      password: secret
sinks:
  debug:
    connector: stdout
    topics: [test.events]
    consumer_group: debug
"#;
        let config: ConnectConfig = serde_yaml::from_str(yaml).unwrap();
        let result = config.validate();
        assert!(result.is_ok(), "Valid config should pass: {:?}", result);
    }
}
