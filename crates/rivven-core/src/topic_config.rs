//! Topic Configuration Management
//!
//! Provides configurable topic settings for production deployments:
//!
//! | Config Key | Type | Default | Description |
//! |------------|------|---------|-------------|
//! | `retention.ms` | i64 | 604800000 | Message retention time (7 days) |
//! | `retention.bytes` | i64 | -1 | Max bytes per partition (-1 = unlimited) |
//! | `max.message.bytes` | i64 | 1048576 | Max message size (1 MB) |
//! | `segment.bytes` | i64 | 1073741824 | Segment file size (1 GB) |
//! | `segment.ms` | i64 | 604800000 | Segment roll time (7 days) |
//! | `cleanup.policy` | string | "delete" | "delete" or "compact" |
//! | `min.insync.replicas` | i32 | 1 | Min replicas for ack |
//! | `compression.type` | string | "producer" | Compression algorithm |

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::broadcast;

/// Default retention time (7 days in milliseconds)
pub const DEFAULT_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1000;

/// Default retention bytes (-1 = unlimited)
pub const DEFAULT_RETENTION_BYTES: i64 = -1;

/// Default max message size (1 MB)
pub const DEFAULT_MAX_MESSAGE_BYTES: i64 = 1024 * 1024;

/// Default segment size (1 GB)
pub const DEFAULT_SEGMENT_BYTES: i64 = 1024 * 1024 * 1024;

/// Default segment roll time (7 days in milliseconds)
pub const DEFAULT_SEGMENT_MS: i64 = 7 * 24 * 60 * 60 * 1000;

/// Cleanup policy options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CleanupPolicy {
    /// Delete old segments based on retention
    #[default]
    Delete,
    /// Compact log keeping only latest value per key
    Compact,
    /// Both delete and compact
    CompactDelete,
}

impl std::fmt::Display for CleanupPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CleanupPolicy::Delete => write!(f, "delete"),
            CleanupPolicy::Compact => write!(f, "compact"),
            CleanupPolicy::CompactDelete => write!(f, "compact,delete"),
        }
    }
}

impl std::str::FromStr for CleanupPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "delete" => Ok(CleanupPolicy::Delete),
            "compact" => Ok(CleanupPolicy::Compact),
            "compact,delete" | "delete,compact" => Ok(CleanupPolicy::CompactDelete),
            _ => Err(format!("Invalid cleanup policy: {}", s)),
        }
    }
}

/// Compression type options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CompressionType {
    /// No compression
    None,
    /// Use producer's compression
    #[default]
    Producer,
    /// LZ4 compression
    Lz4,
    /// Zstd compression
    Zstd,
    /// Snappy compression
    Snappy,
    /// Gzip compression
    Gzip,
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionType::None => write!(f, "none"),
            CompressionType::Producer => write!(f, "producer"),
            CompressionType::Lz4 => write!(f, "lz4"),
            CompressionType::Zstd => write!(f, "zstd"),
            CompressionType::Snappy => write!(f, "snappy"),
            CompressionType::Gzip => write!(f, "gzip"),
        }
    }
}

impl std::str::FromStr for CompressionType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(CompressionType::None),
            "producer" => Ok(CompressionType::Producer),
            "lz4" => Ok(CompressionType::Lz4),
            "zstd" => Ok(CompressionType::Zstd),
            "snappy" => Ok(CompressionType::Snappy),
            "gzip" => Ok(CompressionType::Gzip),
            _ => Err(format!("Invalid compression type: {}", s)),
        }
    }
}

/// Topic configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    /// Message retention time in milliseconds
    pub retention_ms: i64,

    /// Max bytes to retain per partition (-1 = unlimited)
    pub retention_bytes: i64,

    /// Maximum message size in bytes
    pub max_message_bytes: i64,

    /// Segment file size in bytes
    pub segment_bytes: i64,

    /// Segment roll time in milliseconds
    pub segment_ms: i64,

    /// Cleanup policy
    pub cleanup_policy: CleanupPolicy,

    /// Minimum in-sync replicas for ack
    pub min_insync_replicas: i32,

    /// Compression type
    pub compression_type: CompressionType,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            retention_ms: DEFAULT_RETENTION_MS,
            retention_bytes: DEFAULT_RETENTION_BYTES,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            segment_bytes: DEFAULT_SEGMENT_BYTES,
            segment_ms: DEFAULT_SEGMENT_MS,
            cleanup_policy: CleanupPolicy::default(),
            min_insync_replicas: 1,
            compression_type: CompressionType::default(),
        }
    }
}

impl TopicConfig {
    /// Create new topic config with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Get retention duration
    pub fn retention_duration(&self) -> Duration {
        Duration::from_millis(self.retention_ms as u64)
    }

    /// Get segment roll duration
    pub fn segment_roll_duration(&self) -> Duration {
        Duration::from_millis(self.segment_ms as u64)
    }

    /// Convert to HashMap for describe response
    pub fn to_map(&self) -> HashMap<String, ConfigValue> {
        let mut map = HashMap::new();

        map.insert(
            "retention.ms".to_string(),
            ConfigValue {
                value: self.retention_ms.to_string(),
                is_default: self.retention_ms == DEFAULT_RETENTION_MS,
                is_read_only: false,
                is_sensitive: false,
            },
        );

        map.insert(
            "retention.bytes".to_string(),
            ConfigValue {
                value: self.retention_bytes.to_string(),
                is_default: self.retention_bytes == DEFAULT_RETENTION_BYTES,
                is_read_only: false,
                is_sensitive: false,
            },
        );

        map.insert(
            "max.message.bytes".to_string(),
            ConfigValue {
                value: self.max_message_bytes.to_string(),
                is_default: self.max_message_bytes == DEFAULT_MAX_MESSAGE_BYTES,
                is_read_only: false,
                is_sensitive: false,
            },
        );

        map.insert(
            "segment.bytes".to_string(),
            ConfigValue {
                value: self.segment_bytes.to_string(),
                is_default: self.segment_bytes == DEFAULT_SEGMENT_BYTES,
                is_read_only: false,
                is_sensitive: false,
            },
        );

        map.insert(
            "segment.ms".to_string(),
            ConfigValue {
                value: self.segment_ms.to_string(),
                is_default: self.segment_ms == DEFAULT_SEGMENT_MS,
                is_read_only: false,
                is_sensitive: false,
            },
        );

        map.insert(
            "cleanup.policy".to_string(),
            ConfigValue {
                value: self.cleanup_policy.to_string(),
                is_default: self.cleanup_policy == CleanupPolicy::default(),
                is_read_only: false,
                is_sensitive: false,
            },
        );

        map.insert(
            "min.insync.replicas".to_string(),
            ConfigValue {
                value: self.min_insync_replicas.to_string(),
                is_default: self.min_insync_replicas == 1,
                is_read_only: false,
                is_sensitive: false,
            },
        );

        map.insert(
            "compression.type".to_string(),
            ConfigValue {
                value: self.compression_type.to_string(),
                is_default: self.compression_type == CompressionType::default(),
                is_read_only: false,
                is_sensitive: false,
            },
        );

        map
    }

    /// Apply a configuration change
    pub fn apply(&mut self, key: &str, value: Option<&str>) -> Result<(), String> {
        match key {
            "retention.ms" => {
                let val: i64 = match value {
                    Some(v) => v
                        .parse()
                        .map_err(|e| format!("Invalid retention.ms: {}", e))?,
                    None => DEFAULT_RETENTION_MS,
                };
                if val < -1 {
                    return Err("retention.ms must be >= -1 (-1 = infinite)".into());
                }
                self.retention_ms = val;
            }
            "retention.bytes" => {
                self.retention_bytes = match value {
                    Some(v) => v
                        .parse()
                        .map_err(|e| format!("Invalid retention.bytes: {}", e))?,
                    None => DEFAULT_RETENTION_BYTES,
                };
            }
            "max.message.bytes" => {
                let val: i64 = match value {
                    Some(v) => v
                        .parse()
                        .map_err(|e| format!("Invalid max.message.bytes: {}", e))?,
                    None => DEFAULT_MAX_MESSAGE_BYTES,
                };
                if val <= 0 {
                    return Err("max.message.bytes must be > 0".into());
                }
                self.max_message_bytes = val;
            }
            "segment.bytes" => {
                let val: i64 = match value {
                    Some(v) => v
                        .parse()
                        .map_err(|e| format!("Invalid segment.bytes: {}", e))?,
                    None => DEFAULT_SEGMENT_BYTES,
                };
                if val < 1024 {
                    return Err("segment.bytes must be >= 1024".into());
                }
                self.segment_bytes = val;
            }
            "segment.ms" => {
                self.segment_ms = match value {
                    Some(v) => v
                        .parse()
                        .map_err(|e| format!("Invalid segment.ms: {}", e))?,
                    None => DEFAULT_SEGMENT_MS,
                };
            }
            "cleanup.policy" => {
                self.cleanup_policy = match value {
                    Some(v) => v.parse()?,
                    None => CleanupPolicy::default(),
                };
            }
            "min.insync.replicas" => {
                let val: i32 = match value {
                    Some(v) => v
                        .parse()
                        .map_err(|e| format!("Invalid min.insync.replicas: {}", e))?,
                    None => 1,
                };
                if val <= 0 {
                    return Err("min.insync.replicas must be > 0".into());
                }
                self.min_insync_replicas = val;
            }
            "compression.type" => {
                self.compression_type = match value {
                    Some(v) => v.parse()?,
                    None => CompressionType::default(),
                };
            }
            _ => {
                return Err(format!("Unknown configuration key: {}", key));
            }
        }
        Ok(())
    }
}

/// Configuration value with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigValue {
    /// Current value
    pub value: String,
    /// Whether this is the default value
    pub is_default: bool,
    /// Whether this config is read-only
    pub is_read_only: bool,
    /// Whether this config is sensitive
    pub is_sensitive: bool,
}

/// Notification about a config change
#[derive(Debug, Clone)]
pub struct ConfigChangeEvent {
    /// Topic name
    pub topic: String,
    /// Changed keys
    pub changed_keys: Vec<String>,
}

/// Topic configuration manager
///
/// Manages per-topic configurations with thread-safe access.
/// Supports change notifications via a broadcast channel.
pub struct TopicConfigManager {
    /// Per-topic configurations
    configs: RwLock<HashMap<String, TopicConfig>>,
    /// Change notification sender
    change_tx: broadcast::Sender<ConfigChangeEvent>,
}

impl Default for TopicConfigManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicConfigManager {
    /// Create a new topic config manager
    pub fn new() -> Self {
        let (change_tx, _) = broadcast::channel(256);
        Self {
            configs: RwLock::new(HashMap::new()),
            change_tx,
        }
    }

    /// Subscribe to config change notifications
    pub fn subscribe(&self) -> broadcast::Receiver<ConfigChangeEvent> {
        self.change_tx.subscribe()
    }

    /// Get or create config for a topic
    pub fn get_or_default(&self, topic: &str) -> TopicConfig {
        let configs = self.configs.read();
        configs.get(topic).cloned().unwrap_or_default()
    }

    /// Get config for a topic
    pub fn get(&self, topic: &str) -> Option<TopicConfig> {
        let configs = self.configs.read();
        configs.get(topic).cloned()
    }

    /// Set config for a topic
    pub fn set(&self, topic: &str, config: TopicConfig) {
        let keys: Vec<String> = config.to_map().keys().cloned().collect();
        {
            let mut configs = self.configs.write();
            configs.insert(topic.to_string(), config);
        }
        let _ = self.change_tx.send(ConfigChangeEvent {
            topic: topic.to_string(),
            changed_keys: keys,
        });
    }

    /// Apply configuration changes to a topic
    pub fn apply_changes(
        &self,
        topic: &str,
        changes: &[(String, Option<String>)],
    ) -> Result<usize, String> {
        let changed_keys: Vec<String> = changes.iter().map(|(k, _)| k.clone()).collect();
        let changed = {
            let mut configs = self.configs.write();
            let config = configs.entry(topic.to_string()).or_default();

            let mut changed = 0;
            for (key, value) in changes {
                config.apply(key, value.as_deref())?;
                changed += 1;
            }
            changed
        };

        if changed > 0 {
            let _ = self.change_tx.send(ConfigChangeEvent {
                topic: topic.to_string(),
                changed_keys,
            });
        }

        Ok(changed)
    }

    /// Remove config for a topic (reverts to defaults)
    pub fn remove(&self, topic: &str) {
        let mut configs = self.configs.write();
        configs.remove(topic);
    }

    /// List all configured topics
    pub fn list_topics(&self) -> Vec<String> {
        let configs = self.configs.read();
        configs.keys().cloned().collect()
    }

    /// Describe configurations for topics
    pub fn describe(&self, topics: &[String]) -> Vec<(String, HashMap<String, ConfigValue>)> {
        let configs = self.configs.read();

        if topics.is_empty() {
            // Return all topics
            configs
                .iter()
                .map(|(name, config)| (name.clone(), config.to_map()))
                .collect()
        } else {
            // Return specific topics
            topics
                .iter()
                .map(|name| {
                    let config = configs.get(name).cloned().unwrap_or_default();
                    (name.clone(), config.to_map())
                })
                .collect()
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_config_defaults() {
        let config = TopicConfig::default();
        assert_eq!(config.retention_ms, DEFAULT_RETENTION_MS);
        assert_eq!(config.retention_bytes, DEFAULT_RETENTION_BYTES);
        assert_eq!(config.max_message_bytes, DEFAULT_MAX_MESSAGE_BYTES);
        assert_eq!(config.cleanup_policy, CleanupPolicy::Delete);
    }

    #[test]
    fn test_cleanup_policy_parse() {
        assert_eq!(
            "delete".parse::<CleanupPolicy>().unwrap(),
            CleanupPolicy::Delete
        );
        assert_eq!(
            "compact".parse::<CleanupPolicy>().unwrap(),
            CleanupPolicy::Compact
        );
        assert_eq!(
            "compact,delete".parse::<CleanupPolicy>().unwrap(),
            CleanupPolicy::CompactDelete
        );
    }

    #[test]
    fn test_compression_type_parse() {
        assert_eq!(
            "lz4".parse::<CompressionType>().unwrap(),
            CompressionType::Lz4
        );
        assert_eq!(
            "zstd".parse::<CompressionType>().unwrap(),
            CompressionType::Zstd
        );
        assert_eq!(
            "producer".parse::<CompressionType>().unwrap(),
            CompressionType::Producer
        );
    }

    #[test]
    fn test_apply_config_changes() {
        let mut config = TopicConfig::default();

        config.apply("retention.ms", Some("86400000")).unwrap();
        assert_eq!(config.retention_ms, 86400000);

        config.apply("cleanup.policy", Some("compact")).unwrap();
        assert_eq!(config.cleanup_policy, CleanupPolicy::Compact);

        config.apply("compression.type", Some("lz4")).unwrap();
        assert_eq!(config.compression_type, CompressionType::Lz4);
    }

    #[test]
    fn test_apply_reset_to_default() {
        let mut config = TopicConfig {
            retention_ms: 123456,
            ..Default::default()
        };

        config.apply("retention.ms", None).unwrap();
        assert_eq!(config.retention_ms, DEFAULT_RETENTION_MS);
    }

    #[test]
    fn test_invalid_config_key() {
        let mut config = TopicConfig::default();
        let result = config.apply("invalid.key", Some("value"));
        assert!(result.is_err());
    }

    #[test]
    fn test_config_to_map() {
        let config = TopicConfig::default();
        let map = config.to_map();

        assert!(map.contains_key("retention.ms"));
        assert!(map.contains_key("cleanup.policy"));
        assert!(map.get("retention.ms").unwrap().is_default);
    }

    #[test]
    fn test_topic_config_manager() {
        let manager = TopicConfigManager::new();

        // Get default config for non-existent topic
        let config = manager.get_or_default("test-topic");
        assert_eq!(config.retention_ms, DEFAULT_RETENTION_MS);

        // Apply changes
        let changes = vec![
            ("retention.ms".to_string(), Some("3600000".to_string())),
            ("cleanup.policy".to_string(), Some("compact".to_string())),
        ];
        let changed = manager.apply_changes("test-topic", &changes).unwrap();
        assert_eq!(changed, 2);

        // Verify changes
        let config = manager.get("test-topic").unwrap();
        assert_eq!(config.retention_ms, 3600000);
        assert_eq!(config.cleanup_policy, CleanupPolicy::Compact);
    }

    #[test]
    fn test_describe_configs() {
        let manager = TopicConfigManager::new();

        // Set custom config
        let config = TopicConfig {
            retention_ms: 86400000,
            ..Default::default()
        };
        manager.set("topic-a", config);

        // Describe specific topic
        let descriptions = manager.describe(&["topic-a".to_string()]);
        assert_eq!(descriptions.len(), 1);
        assert_eq!(descriptions[0].0, "topic-a");
        assert_eq!(
            descriptions[0].1.get("retention.ms").unwrap().value,
            "86400000"
        );
        assert!(!descriptions[0].1.get("retention.ms").unwrap().is_default);
    }

    #[test]
    fn test_retention_duration() {
        let config = TopicConfig::default();
        let duration = config.retention_duration();
        assert_eq!(duration, Duration::from_millis(DEFAULT_RETENTION_MS as u64));
    }

    #[test]
    fn test_config_change_notification() {
        let manager = TopicConfigManager::new();
        let mut rx = manager.subscribe();

        // Apply changes should send notification
        let changes = vec![("retention.ms".to_string(), Some("3600000".to_string()))];
        manager.apply_changes("my-topic", &changes).unwrap();

        let event = rx.try_recv().unwrap();
        assert_eq!(event.topic, "my-topic");
        assert!(event.changed_keys.contains(&"retention.ms".to_string()));
    }

    #[test]
    fn test_config_set_notification() {
        let manager = TopicConfigManager::new();
        let mut rx = manager.subscribe();

        let config = TopicConfig {
            retention_ms: 86400000,
            ..Default::default()
        };
        manager.set("topic-b", config);

        let event = rx.try_recv().unwrap();
        assert_eq!(event.topic, "topic-b");
        assert!(!event.changed_keys.is_empty());
    }
}
