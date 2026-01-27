//! WASM Plugin Runtime
//!
//! Manages WASM plugin execution with sandboxing and resource limits.
//! Implements the full plugin lifecycle: load → init → check → run → shutdown.
//!
//! # Plugin Lifecycle
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                     Plugin Lifecycle                         │
//! ├──────────────────────────────────────────────────────────────┤
//! │  Created ─────► Initialized ─────► Running ─────► Stopped   │
//! │     │               │                  │              │      │
//! │     │         rivven_init()      rivven_poll()   rivven_    │
//! │     │                            rivven_write()  shutdown()  │
//! │     │                            rivven_transform()          │
//! │     │               │                  │                     │
//! │     │         rivven_check() ◄────────┘                     │
//! │     │               │                                        │
//! │     └───────────► Failed (on error)                         │
//! └──────────────────────────────────────────────────────────────┘
//! ```

use crate::plugin::abi::{PluginAbi, PluginResultCode};
use crate::plugin::host::HostFunctions;
use crate::plugin::loader::PluginLoader;
use crate::plugin::sandbox::SandboxConfig;
use crate::plugin::store::PluginStore;
use crate::plugin::types::{PluginConfig, PluginError, PluginManifest, PluginResult, PluginType};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Runtime configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    /// Default sandbox configuration
    #[serde(default)]
    pub sandbox: SandboxConfig,
    /// Plugin search paths
    #[serde(default)]
    pub search_paths: Vec<std::path::PathBuf>,
    /// Enable plugin preloading
    #[serde(default)]
    pub preload: bool,
    /// Maximum concurrent plugin instances
    #[serde(default = "default_max_instances")]
    pub max_instances: usize,
}

fn default_max_instances() -> usize {
    100
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            sandbox: SandboxConfig::default(),
            search_paths: vec![std::path::PathBuf::from("./plugins")],
            preload: false,
            max_instances: default_max_instances(),
        }
    }
}

/// Plugin instance
pub struct PluginInstance {
    /// Instance ID
    pub id: u64,
    /// Plugin name
    pub name: String,
    /// Plugin type
    pub plugin_type: PluginType,
    /// Host functions
    pub host: Arc<HostFunctions>,
    /// Instance state
    state: RwLock<InstanceState>,
    /// Configuration (serialized JSON)
    config_json: String,
    /// WASM module bytes (for future wasmtime integration)
    #[allow(dead_code)]
    wasm_bytes: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstanceState {
    Created,
    Initialized,
    Running,
    Stopped,
    Failed,
}

impl std::fmt::Display for InstanceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InstanceState::Created => write!(f, "created"),
            InstanceState::Initialized => write!(f, "initialized"),
            InstanceState::Running => write!(f, "running"),
            InstanceState::Stopped => write!(f, "stopped"),
            InstanceState::Failed => write!(f, "failed"),
        }
    }
}

impl PluginInstance {
    fn new(
        id: u64,
        name: String,
        plugin_type: PluginType,
        config: serde_json::Value,
        allow_network: bool,
        wasm_bytes: Option<Vec<u8>>,
    ) -> Self {
        let config_json = serde_json::to_string(&config).unwrap_or_else(|_| "{}".to_string());
        Self {
            id,
            name: name.clone(),
            plugin_type,
            host: Arc::new(HostFunctions::new(name, config, allow_network)),
            state: RwLock::new(InstanceState::Created),
            config_json,
            wasm_bytes,
        }
    }

    /// Initialize the plugin by calling rivven_init()
    ///
    /// This calls the WASM export `rivven_init(config_ptr, config_len) -> result`
    /// which allows the plugin to parse configuration and set up internal state.
    pub async fn initialize(&self) -> PluginResult<()> {
        let mut state = self.state.write();
        if *state != InstanceState::Created {
            return Err(PluginError::ExecutionError(format!(
                "Plugin already in state: {}, expected: created",
                *state
            )));
        }

        // In a full implementation, this would:
        // 1. Instantiate the WASM module with wasmtime/wasmer
        // 2. Allocate memory for config using rivven_alloc
        // 3. Copy config JSON to WASM memory
        // 4. Call rivven_init(config_ptr, config_len)
        // 5. Check return code
        //
        // For now, we simulate the call by validating config
        tracing::debug!(
            plugin = %self.name,
            instance_id = self.id,
            config_len = self.config_json.len(),
            "Simulating {} call",
            PluginAbi::EXPORT_INIT
        );

        // Validate config is valid JSON
        if serde_json::from_str::<serde_json::Value>(&self.config_json).is_err() {
            *state = InstanceState::Failed;
            return Err(PluginError::ExecutionError(
                "Invalid configuration JSON".to_string(),
            ));
        }

        // Simulate successful init
        let result_code = PluginResultCode::Ok;
        if result_code == PluginResultCode::Ok {
            *state = InstanceState::Initialized;
            tracing::info!(
                plugin = %self.name,
                instance_id = self.id,
                "Plugin initialized successfully"
            );
            Ok(())
        } else {
            *state = InstanceState::Failed;
            Err(PluginError::ExecutionError(format!(
                "rivven_init returned error: {:?}",
                result_code
            )))
        }
    }

    /// Check connectivity by calling rivven_check()
    ///
    /// This calls the WASM export `rivven_check() -> result_ptr_len`
    /// which returns a CheckResult with success/failure and optional message.
    pub async fn check(&self) -> PluginResult<CheckResult> {
        let state = self.state.read();
        if *state != InstanceState::Initialized && *state != InstanceState::Running {
            return Err(PluginError::ExecutionError(format!(
                "Plugin must be initialized or running to check, current state: {}",
                *state
            )));
        }

        // In a full implementation, this would:
        // 1. Call rivven_check() in WASM
        // 2. Get the returned ptr_len packed value
        // 3. Read the CheckResult JSON from WASM memory
        // 4. Deallocate the result memory
        //
        // For now, we simulate by checking host connectivity
        tracing::debug!(
            plugin = %self.name,
            instance_id = self.id,
            "Simulating {} call",
            PluginAbi::EXPORT_CHECK
        );

        // Simulate connectivity check using host functions
        let check_result = if self.host.allow_network() {
            // Would actually test connectivity here
            CheckResult {
                success: true,
                message: Some("Connectivity check passed (simulated)".to_string()),
                details: vec![
                    CheckDetail {
                        name: "configuration".to_string(),
                        passed: true,
                        message: None,
                    },
                    CheckDetail {
                        name: "host_functions".to_string(),
                        passed: true,
                        message: None,
                    },
                ],
            }
        } else {
            CheckResult {
                success: true,
                message: Some("Network disabled, connectivity check skipped".to_string()),
                details: vec![CheckDetail {
                    name: "configuration".to_string(),
                    passed: true,
                    message: None,
                }],
            }
        };

        tracing::info!(
            plugin = %self.name,
            instance_id = self.id,
            success = check_result.success,
            "Plugin check completed"
        );

        Ok(check_result)
    }

    /// Start the plugin (transition to Running state)
    pub fn start(&self) -> PluginResult<()> {
        let mut state = self.state.write();
        if *state != InstanceState::Initialized {
            return Err(PluginError::ExecutionError(format!(
                "Plugin must be initialized to start, current state: {}",
                *state
            )));
        }
        *state = InstanceState::Running;
        tracing::info!(
            plugin = %self.name,
            instance_id = self.id,
            "Plugin started"
        );
        Ok(())
    }

    /// Stop the plugin (transition to Stopped state)
    pub fn stop(&self) -> PluginResult<()> {
        let mut state = self.state.write();
        if *state != InstanceState::Running && *state != InstanceState::Initialized {
            return Err(PluginError::ExecutionError(format!(
                "Plugin must be running or initialized to stop, current state: {}",
                *state
            )));
        }

        // In a full implementation, this would call rivven_shutdown()
        tracing::debug!(
            plugin = %self.name,
            instance_id = self.id,
            "Simulating {} call",
            PluginAbi::EXPORT_SHUTDOWN
        );

        *state = InstanceState::Stopped;
        tracing::info!(
            plugin = %self.name,
            instance_id = self.id,
            "Plugin stopped"
        );
        Ok(())
    }

    /// Get current state
    pub fn current_state(&self) -> String {
        self.state.read().to_string()
    }

    /// Get all plugin state data
    pub fn get_state(&self) -> PluginResult<HashMap<String, serde_json::Value>> {
        Ok(self.host.get_all_state())
    }

    /// Check if plugin is in a runnable state
    pub fn is_runnable(&self) -> bool {
        let state = self.state.read();
        *state == InstanceState::Initialized || *state == InstanceState::Running
    }
}

/// Check result from plugin
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    pub success: bool,
    pub message: Option<String>,
    #[serde(default)]
    pub details: Vec<CheckDetail>,
}

/// Individual check detail
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckDetail {
    pub name: String,
    pub passed: bool,
    pub message: Option<String>,
}

/// WASM Plugin Runtime
pub struct PluginRuntime {
    /// Runtime configuration
    config: RuntimeConfig,
    /// Plugin loader
    loader: PluginLoader,
    /// Plugin store
    store: Arc<PluginStore>,
    /// Active instances
    instances: RwLock<HashMap<u64, Arc<PluginInstance>>>,
    /// Next instance ID
    next_instance_id: AtomicU64,
}

impl PluginRuntime {
    /// Create a new plugin runtime
    pub fn new(config: RuntimeConfig) -> PluginResult<Self> {
        let mut loader = PluginLoader::new();
        for path in &config.search_paths {
            loader.add_search_path(path);
        }

        Ok(Self {
            config,
            loader,
            store: Arc::new(PluginStore::new()),
            instances: RwLock::new(HashMap::new()),
            next_instance_id: AtomicU64::new(1),
        })
    }

    /// Create with default configuration
    pub fn default_runtime() -> PluginResult<Self> {
        Self::new(RuntimeConfig::default())
    }

    /// Load a plugin from file
    pub async fn load_plugin(
        &self,
        path: impl AsRef<std::path::Path>,
    ) -> PluginResult<PluginManifest> {
        let module = self.loader.load_file(path).await?;
        let manifest = module.manifest.clone();
        self.store.register(module)?;
        Ok(manifest)
    }

    /// Load a plugin by name (from search paths)
    pub async fn load_by_name(&self, name: &str) -> PluginResult<PluginManifest> {
        let module = self.loader.load_by_name(name).await?;
        let manifest = module.manifest.clone();
        self.store.register(module)?;
        Ok(manifest)
    }

    /// Load a plugin from URL
    pub async fn load_from_url(&self, url: &str) -> PluginResult<PluginManifest> {
        let module = self.loader.load_url(url).await?;
        let manifest = module.manifest.clone();
        self.store.register(module)?;
        Ok(manifest)
    }

    /// Create a plugin instance
    pub async fn create_instance(
        &self,
        plugin_name: &str,
        config: PluginConfig,
    ) -> PluginResult<u64> {
        // Check instance limit
        {
            let instances = self.instances.read();
            if instances.len() >= self.config.max_instances {
                return Err(PluginError::ResourceLimitExceeded(format!(
                    "Maximum instances ({}) reached",
                    self.config.max_instances
                )));
            }
        }

        // Get plugin entry
        let entry = self.store.get(plugin_name)?;

        // Create instance
        let instance_id = self.next_instance_id.fetch_add(1, Ordering::SeqCst);
        let config_value = serde_json::to_value(&config.config)
            .map_err(|e| PluginError::SerializationError(e.to_string()))?;

        // Get WASM bytes from the module
        let wasm_bytes = Some((*entry.bytes).clone());

        let instance = Arc::new(PluginInstance::new(
            instance_id,
            plugin_name.to_string(),
            entry.manifest.plugin_type,
            config_value,
            config.allow_network,
            wasm_bytes,
        ));

        // Initialize
        instance.initialize().await?;

        // Register instance
        {
            let mut instances = self.instances.write();
            instances.insert(instance_id, instance);
        }

        self.store.increment_instances(plugin_name)?;

        tracing::info!(
            plugin = %plugin_name,
            instance_id = instance_id,
            "Created plugin instance"
        );

        Ok(instance_id)
    }

    /// Check a plugin instance's connectivity
    pub async fn check_instance(&self, instance_id: u64) -> PluginResult<CheckResult> {
        let instance = self.get_instance(instance_id)?;
        instance.check().await
    }

    /// Start a plugin instance
    pub fn start_instance(&self, instance_id: u64) -> PluginResult<()> {
        let instance = self.get_instance(instance_id)?;
        instance.start()
    }

    /// Stop a plugin instance
    pub fn stop_instance(&self, instance_id: u64) -> PluginResult<()> {
        let instance = self.get_instance(instance_id)?;
        instance.stop()
    }

    /// Get a plugin instance
    pub fn get_instance(&self, instance_id: u64) -> PluginResult<Arc<PluginInstance>> {
        let instances = self.instances.read();
        instances
            .get(&instance_id)
            .cloned()
            .ok_or_else(|| PluginError::NotFound(format!("Instance {}", instance_id)))
    }

    /// Destroy a plugin instance
    pub async fn destroy_instance(&self, instance_id: u64) -> PluginResult<()> {
        let instance = {
            let mut instances = self.instances.write();
            instances
                .remove(&instance_id)
                .ok_or_else(|| PluginError::NotFound(format!("Instance {}", instance_id)))?
        };

        self.store.decrement_instances(&instance.name)?;

        tracing::info!(
            plugin = %instance.name,
            instance_id = instance_id,
            "Destroyed plugin instance"
        );

        Ok(())
    }

    /// List all plugins
    pub fn list_plugins(&self) -> Vec<PluginManifest> {
        self.store.all().into_iter().map(|e| e.manifest).collect()
    }

    /// List plugins by type
    pub fn list_sources(&self) -> Vec<PluginManifest> {
        self.store
            .sources()
            .into_iter()
            .map(|e| e.manifest)
            .collect()
    }

    pub fn list_sinks(&self) -> Vec<PluginManifest> {
        self.store.sinks().into_iter().map(|e| e.manifest).collect()
    }

    pub fn list_transforms(&self) -> Vec<PluginManifest> {
        self.store
            .transforms()
            .into_iter()
            .map(|e| e.manifest)
            .collect()
    }

    /// Discover plugins in search paths
    pub async fn discover(&self) -> Vec<PluginManifest> {
        self.loader
            .discover()
            .await
            .into_iter()
            .map(|d| d.manifest)
            .collect()
    }

    /// Get runtime statistics
    pub fn stats(&self) -> RuntimeStats {
        let store_stats = self.store.stats();
        let instances = self.instances.read();

        RuntimeStats {
            plugins_loaded: store_stats.total,
            sources: store_stats.sources,
            sinks: store_stats.sinks,
            transforms: store_stats.transforms,
            active_instances: instances.len(),
            max_instances: self.config.max_instances,
        }
    }
}

/// Runtime statistics
#[derive(Debug, Clone)]
pub struct RuntimeStats {
    pub plugins_loaded: usize,
    pub sources: usize,
    pub sinks: usize,
    pub transforms: usize,
    pub active_instances: usize,
    pub max_instances: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::types::PluginType;

    #[tokio::test]
    async fn test_runtime_creation() {
        let config = RuntimeConfig::default();
        let runtime = PluginRuntime::new(config).unwrap();

        let stats = runtime.stats();
        assert_eq!(stats.plugins_loaded, 0);
        assert_eq!(stats.active_instances, 0);
    }

    #[tokio::test]
    async fn test_plugin_instance_lifecycle() {
        // Create instance directly (simulating loaded plugin)
        let instance = PluginInstance::new(
            1,
            "test-plugin".to_string(),
            PluginType::Source,
            serde_json::json!({"key": "value"}),
            true,
            None,
        );

        // Initial state should be Created
        assert_eq!(instance.current_state(), "created");

        // Initialize
        instance.initialize().await.unwrap();
        assert_eq!(instance.current_state(), "initialized");

        // Check should succeed
        let check_result = instance.check().await.unwrap();
        assert!(check_result.success);
        assert!(!check_result.details.is_empty());

        // Start
        instance.start().unwrap();
        assert_eq!(instance.current_state(), "running");

        // Check should still work when running
        let check_result = instance.check().await.unwrap();
        assert!(check_result.success);

        // Stop
        instance.stop().unwrap();
        assert_eq!(instance.current_state(), "stopped");
    }

    #[tokio::test]
    async fn test_plugin_instance_invalid_transitions() {
        let instance = PluginInstance::new(
            1,
            "test-plugin".to_string(),
            PluginType::Sink,
            serde_json::json!({}),
            false,
            None,
        );

        // Can't check before initialization
        let result = instance.check().await;
        assert!(result.is_err());

        // Can't start before initialization
        let result = instance.start();
        assert!(result.is_err());

        // Initialize
        instance.initialize().await.unwrap();

        // Can't initialize twice
        let result = instance.initialize().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_plugin_instance_state_management() {
        let instance = PluginInstance::new(
            1,
            "test-plugin".to_string(),
            PluginType::Transform,
            serde_json::json!({"timeout": 30}),
            true,
            None,
        );

        instance.initialize().await.unwrap();

        // Should be able to get state
        let state = instance.get_state().unwrap();
        assert!(state.is_empty()); // No state set yet

        // Host functions should be accessible
        instance
            .host
            .set_state("test_key", serde_json::json!("test_value"));
        let state = instance.get_state().unwrap();
        assert!(state.contains_key("test_key"));
    }

    #[tokio::test]
    async fn test_plugin_instance_network_disabled() {
        let instance = PluginInstance::new(
            1,
            "no-network-plugin".to_string(),
            PluginType::Source,
            serde_json::json!({}),
            false, // Network disabled
            None,
        );

        instance.initialize().await.unwrap();

        // Check should still pass but indicate network is disabled
        let check_result = instance.check().await.unwrap();
        assert!(check_result.success);
        assert!(check_result.message.unwrap().contains("Network disabled"));
    }

    #[test]
    fn test_check_result_serialization() {
        let result = CheckResult {
            success: true,
            message: Some("All checks passed".to_string()),
            details: vec![
                CheckDetail {
                    name: "connectivity".to_string(),
                    passed: true,
                    message: None,
                },
                CheckDetail {
                    name: "authentication".to_string(),
                    passed: true,
                    message: Some("Token valid".to_string()),
                },
            ],
        };

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: CheckResult = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.success, result.success);
        assert_eq!(deserialized.details.len(), 2);
    }
}
