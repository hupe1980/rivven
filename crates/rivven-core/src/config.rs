use serde::{Deserialize, Serialize};

/// Configuration for Rivven
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Server bind address
    pub bind_address: String,
    
    /// Server port
    pub port: u16,
    
    /// Default number of partitions for new topics
    pub default_partitions: u32,
    
    /// Enable disk persistence
    pub enable_persistence: bool,
    
    /// Data directory for persistence
    pub data_dir: String,

    /// Maximum segment size in bytes
    pub max_segment_size: u64,
    
    /// Log level
    pub log_level: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1".to_string(),
            port: 9092,
            default_partitions: 3,
            enable_persistence: false,
            data_dir: "./data".to_string(),
            max_segment_size: 1024 * 1024 * 1024, // 1GB
            log_level: "info".to_string(),
        }
    }
}

impl Config {
    /// Create a new configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the bind address
    pub fn with_bind_address(mut self, address: String) -> Self {
        self.bind_address = address;
        self
    }

    /// Set the port
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the default partitions
    pub fn with_default_partitions(mut self, partitions: u32) -> Self {
        self.default_partitions = partitions;
        self
    }

    /// Enable or disable persistence
    pub fn with_persistence(mut self, enabled: bool) -> Self {
        self.enable_persistence = enabled;
        self
    }

    /// Set the data directory
    pub fn with_data_dir(mut self, data_dir: String) -> Self {
        self.data_dir = data_dir;
        self
    }

    /// Get the server address
    pub fn server_address(&self) -> String {
        format!("{}:{}", self.bind_address, self.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.bind_address, "127.0.0.1");
        assert_eq!(config.port, 9092);
        assert_eq!(config.default_partitions, 3);
        assert!(!config.enable_persistence);
        assert_eq!(config.data_dir, "./data");
        assert_eq!(config.max_segment_size, 1024 * 1024 * 1024);
        assert_eq!(config.log_level, "info");
    }

    #[test]
    fn test_builder_pattern() {
        let config = Config::new()
            .with_bind_address("0.0.0.0".to_string())
            .with_port(9093)
            .with_default_partitions(6)
            .with_persistence(true)
            .with_data_dir("/var/lib/rivven".to_string());
        
        assert_eq!(config.bind_address, "0.0.0.0");
        assert_eq!(config.port, 9093);
        assert_eq!(config.default_partitions, 6);
        assert!(config.enable_persistence);
        assert_eq!(config.data_dir, "/var/lib/rivven");
    }

    #[test]
    fn test_server_address() {
        let config = Config::default();
        assert_eq!(config.server_address(), "127.0.0.1:9092");
        
        let custom = Config::new()
            .with_bind_address("10.0.0.1".to_string())
            .with_port(9999);
        assert_eq!(custom.server_address(), "10.0.0.1:9999");
    }

    #[test]
    fn test_serialization() {
        let config = Config::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: Config = serde_json::from_str(&json).unwrap();
        
        assert_eq!(config.bind_address, deserialized.bind_address);
        assert_eq!(config.port, deserialized.port);
        assert_eq!(config.default_partitions, deserialized.default_partitions);
    }
}
