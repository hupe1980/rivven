//! WASM Plugin types and error handling

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use thiserror::Error;

/// Plugin error types
#[derive(Debug, Error)]
pub enum PluginError {
    #[error("Failed to load WASM module: {0}")]
    LoadError(String),

    #[error("Failed to instantiate plugin: {0}")]
    InstantiationError(String),

    #[error("Plugin execution error: {0}")]
    ExecutionError(String),

    #[error("Plugin ABI error: {0}")]
    AbiError(String),

    #[error("Plugin not found: {0}")]
    NotFound(String),

    #[error("Invalid plugin manifest: {0}")]
    InvalidManifest(String),

    #[error("Resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),

    #[error("Plugin timeout")]
    Timeout,

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Plugin type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },
}

impl From<serde_json::Error> for PluginError {
    fn from(err: serde_json::Error) -> Self {
        PluginError::SerializationError(err.to_string())
    }
}

pub type PluginResult<T> = Result<T, PluginError>;

/// Plugin type (source, sink, or transform)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PluginType {
    Source,
    Sink,
    Transform,
}

impl std::fmt::Display for PluginType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PluginType::Source => write!(f, "source"),
            PluginType::Sink => write!(f, "sink"),
            PluginType::Transform => write!(f, "transform"),
        }
    }
}

/// Plugin manifest describing a WASM connector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginManifest {
    /// Plugin name (unique identifier)
    pub name: String,
    /// Plugin version
    pub version: String,
    /// Plugin type
    pub plugin_type: PluginType,
    /// Human-readable description
    pub description: Option<String>,
    /// Author information
    pub author: Option<String>,
    /// License
    pub license: Option<String>,
    /// Homepage URL
    pub homepage: Option<String>,
    /// Required ABI version
    #[serde(default = "default_abi_version")]
    pub abi_version: String,
    /// Configuration schema (JSON Schema)
    pub config_schema: Option<serde_json::Value>,
    /// Required host capabilities
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Resource requirements
    #[serde(default)]
    pub resources: ResourceRequirements,
}

fn default_abi_version() -> String {
    "1.0".to_string()
}

/// Resource requirements for a plugin
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceRequirements {
    /// Maximum memory in bytes
    #[serde(default)]
    pub max_memory_bytes: Option<u64>,
    /// Maximum execution time in milliseconds
    #[serde(default)]
    pub max_execution_ms: Option<u64>,
    /// Maximum fuel (instruction count)
    #[serde(default)]
    pub max_fuel: Option<u64>,
}

/// Plugin configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginConfig {
    /// Path to WASM file or URL
    pub path: PathBuf,
    /// Plugin-specific configuration
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
    /// Override resource limits
    #[serde(default)]
    pub resource_limits: Option<ResourceLimitsConfig>,
    /// Environment variables to expose
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Allow network access
    #[serde(default)]
    pub allow_network: bool,
    /// Allow filesystem access (with paths)
    #[serde(default)]
    pub allow_fs: Vec<PathBuf>,
}

impl Default for PluginConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::new(),
            config: HashMap::new(),
            resource_limits: None,
            env: HashMap::new(),
            allow_network: false,
            allow_fs: Vec::new(),
        }
    }
}

/// Resource limits configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimitsConfig {
    /// Maximum memory in MB
    pub max_memory_mb: Option<u32>,
    /// Maximum execution time in seconds
    pub max_execution_secs: Option<u32>,
    /// Maximum table elements
    pub max_table_elements: Option<u32>,
    /// Maximum instances
    pub max_instances: Option<u32>,
}

/// WASM Source connector wrapper
pub struct WasmSource {
    /// Plugin name
    pub name: String,
    /// Plugin instance handle
    pub instance_id: u64,
    /// Configuration
    pub config: serde_json::Value,
}

impl WasmSource {
    pub fn new(name: String, instance_id: u64, config: serde_json::Value) -> Self {
        Self {
            name,
            instance_id,
            config,
        }
    }
}

/// WASM Sink connector wrapper
pub struct WasmSink {
    /// Plugin name
    pub name: String,
    /// Plugin instance handle
    pub instance_id: u64,
    /// Configuration
    pub config: serde_json::Value,
}

impl WasmSink {
    pub fn new(name: String, instance_id: u64, config: serde_json::Value) -> Self {
        Self {
            name,
            instance_id,
            config,
        }
    }
}

/// WASM Transform connector wrapper
pub struct WasmTransform {
    /// Plugin name
    pub name: String,
    /// Plugin instance handle
    pub instance_id: u64,
    /// Configuration
    pub config: serde_json::Value,
}

impl WasmTransform {
    pub fn new(name: String, instance_id: u64, config: serde_json::Value) -> Self {
        Self {
            name,
            instance_id,
            config,
        }
    }
}

/// Plugin metadata returned from discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)] // Used by plugin discovery system
pub struct PluginInfo {
    /// Plugin name
    pub name: String,
    /// Plugin version
    pub version: String,
    /// Plugin type
    pub plugin_type: PluginType,
    /// Description
    pub description: Option<String>,
    /// Path to WASM file
    pub path: PathBuf,
    /// Full manifest
    pub manifest: PluginManifest,
}
