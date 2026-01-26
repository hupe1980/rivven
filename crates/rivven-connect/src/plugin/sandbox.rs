//! Sandbox configuration for WASM plugins

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Sandbox configuration for plugin execution
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SandboxConfig {
    /// Resource limits
    #[serde(default)]
    pub limits: ResourceLimits,
    /// Allowed host capabilities
    #[serde(default)]
    pub capabilities: Capabilities,
}

/// Resource limits for WASM execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Maximum memory in bytes (default: 64MB)
    #[serde(default = "default_max_memory")]
    pub max_memory_bytes: u64,

    /// Maximum execution time per call (default: 30s)
    #[serde(default = "default_max_execution")]
    pub max_execution: Duration,

    /// Maximum fuel (instruction count) per call
    /// None = unlimited (not recommended for production)
    #[serde(default = "default_max_fuel")]
    pub max_fuel: Option<u64>,

    /// Maximum table elements
    #[serde(default = "default_max_table_elements")]
    pub max_table_elements: u32,

    /// Maximum number of WASM instances
    #[serde(default = "default_max_instances")]
    pub max_instances: u32,

    /// Maximum number of WASM tables
    #[serde(default = "default_max_tables")]
    pub max_tables: u32,

    /// Maximum number of WASM memories
    #[serde(default = "default_max_memories")]
    pub max_memories: u32,

    /// Maximum recursion depth
    #[serde(default = "default_max_recursion")]
    pub max_recursion_depth: u32,
}

fn default_max_memory() -> u64 {
    64 * 1024 * 1024 // 64 MB
}

fn default_max_execution() -> Duration {
    Duration::from_secs(30)
}

fn default_max_fuel() -> Option<u64> {
    Some(10_000_000_000) // 10 billion instructions
}

fn default_max_table_elements() -> u32 {
    10_000
}

fn default_max_instances() -> u32 {
    10
}

fn default_max_tables() -> u32 {
    10
}

fn default_max_memories() -> u32 {
    1
}

fn default_max_recursion() -> u32 {
    1000
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: default_max_memory(),
            max_execution: default_max_execution(),
            max_fuel: default_max_fuel(),
            max_table_elements: default_max_table_elements(),
            max_instances: default_max_instances(),
            max_tables: default_max_tables(),
            max_memories: default_max_memories(),
            max_recursion_depth: default_max_recursion(),
        }
    }
}

impl ResourceLimits {
    /// Create minimal limits for testing
    pub fn minimal() -> Self {
        Self {
            max_memory_bytes: 1024 * 1024, // 1 MB
            max_execution: Duration::from_secs(1),
            max_fuel: Some(1_000_000),
            max_table_elements: 100,
            max_instances: 1,
            max_tables: 1,
            max_memories: 1,
            max_recursion_depth: 100,
        }
    }

    /// Create generous limits for development
    pub fn development() -> Self {
        Self {
            max_memory_bytes: 256 * 1024 * 1024, // 256 MB
            max_execution: Duration::from_secs(120),
            max_fuel: None, // Unlimited
            max_table_elements: 100_000,
            max_instances: 100,
            max_tables: 100,
            max_memories: 10,
            max_recursion_depth: 10_000,
        }
    }

    /// Create production-safe limits
    pub fn production() -> Self {
        Self::default()
    }
}

/// Allowed capabilities for plugins
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Capabilities {
    /// Allow network access
    #[serde(default)]
    pub network: bool,

    /// Allow filesystem read access
    #[serde(default)]
    pub fs_read: bool,

    /// Allow filesystem write access
    #[serde(default)]
    pub fs_write: bool,

    /// Allow environment variable access
    #[serde(default)]
    pub env: bool,

    /// Allow clock/time access
    #[serde(default = "default_true")]
    pub clock: bool,

    /// Allow random number generation
    #[serde(default = "default_true")]
    pub random: bool,

    /// Specific allowed network hosts
    #[serde(default)]
    pub allowed_hosts: Vec<String>,

    /// Specific allowed filesystem paths
    #[serde(default)]
    pub allowed_paths: Vec<std::path::PathBuf>,

    /// Specific allowed environment variables
    #[serde(default)]
    pub allowed_env_vars: Vec<String>,
}

fn default_true() -> bool {
    true
}

impl Capabilities {
    /// No capabilities (maximum isolation)
    pub fn none() -> Self {
        Self {
            network: false,
            fs_read: false,
            fs_write: false,
            env: false,
            clock: false,
            random: false,
            allowed_hosts: Vec::new(),
            allowed_paths: Vec::new(),
            allowed_env_vars: Vec::new(),
        }
    }

    /// Safe defaults (clock + random only)
    pub fn safe() -> Self {
        Self {
            network: false,
            fs_read: false,
            fs_write: false,
            env: false,
            clock: true,
            random: true,
            allowed_hosts: Vec::new(),
            allowed_paths: Vec::new(),
            allowed_env_vars: Vec::new(),
        }
    }

    /// Full capabilities (development only!)
    pub fn full() -> Self {
        Self {
            network: true,
            fs_read: true,
            fs_write: true,
            env: true,
            clock: true,
            random: true,
            allowed_hosts: Vec::new(),  // Empty = all allowed
            allowed_paths: Vec::new(),  // Empty = all allowed
            allowed_env_vars: Vec::new(),
        }
    }

    /// Check if a host is allowed
    pub fn is_host_allowed(&self, host: &str) -> bool {
        if !self.network {
            return false;
        }
        if self.allowed_hosts.is_empty() {
            return true; // Empty = all allowed
        }
        self.allowed_hosts.iter().any(|allowed| {
            allowed == host || host.ends_with(&format!(".{}", allowed))
        })
    }

    /// Check if a path is allowed for reading
    pub fn is_path_read_allowed(&self, path: &std::path::Path) -> bool {
        if !self.fs_read {
            return false;
        }
        if self.allowed_paths.is_empty() {
            return true; // Empty = all allowed
        }
        self.allowed_paths.iter().any(|allowed| {
            path.starts_with(allowed)
        })
    }

    /// Check if a path is allowed for writing
    pub fn is_path_write_allowed(&self, path: &std::path::Path) -> bool {
        if !self.fs_write {
            return false;
        }
        if self.allowed_paths.is_empty() {
            return true;
        }
        self.allowed_paths.iter().any(|allowed| {
            path.starts_with(allowed)
        })
    }

    /// Check if an environment variable is allowed
    pub fn is_env_var_allowed(&self, name: &str) -> bool {
        if !self.env {
            return false;
        }
        if self.allowed_env_vars.is_empty() {
            return true;
        }
        self.allowed_env_vars.iter().any(|allowed| {
            allowed == name || (allowed.ends_with('*') && name.starts_with(&allowed[..allowed.len()-1]))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_host_filtering() {
        let mut caps = Capabilities::safe();
        
        // Network disabled
        assert!(!caps.is_host_allowed("example.com"));
        
        // Enable network with specific hosts
        caps.network = true;
        caps.allowed_hosts = vec!["api.example.com".to_string(), "example.org".to_string()];
        
        assert!(caps.is_host_allowed("api.example.com"));
        assert!(caps.is_host_allowed("example.org"));
        assert!(!caps.is_host_allowed("other.com"));
        
        // Subdomain matching
        assert!(caps.is_host_allowed("sub.example.org"));
    }

    #[test]
    fn test_path_filtering() {
        let mut caps = Capabilities::safe();
        caps.fs_read = true;
        caps.allowed_paths = vec![std::path::PathBuf::from("/data")];
        
        assert!(caps.is_path_read_allowed(std::path::Path::new("/data/file.txt")));
        assert!(caps.is_path_read_allowed(std::path::Path::new("/data/subdir/file.txt")));
        assert!(!caps.is_path_read_allowed(std::path::Path::new("/etc/passwd")));
    }

    #[test]
    fn test_env_filtering() {
        let mut caps = Capabilities::safe();
        caps.env = true;
        caps.allowed_env_vars = vec!["API_KEY".to_string(), "MY_*".to_string()];
        
        assert!(caps.is_env_var_allowed("API_KEY"));
        assert!(caps.is_env_var_allowed("MY_VAR"));
        assert!(caps.is_env_var_allowed("MY_OTHER_VAR"));
        assert!(!caps.is_env_var_allowed("SECRET"));
    }
}
