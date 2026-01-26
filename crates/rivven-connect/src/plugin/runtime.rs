//! WASM Plugin Runtime
//!
//! Manages WASM plugin execution with sandboxing and resource limits.
//! Note: This is a reference implementation. Full WASM execution requires
//! a runtime like wasmtime or wasmer, added via feature flag.

use crate::plugin::host::HostFunctions;
use crate::plugin::loader::PluginLoader;
use crate::plugin::sandbox::SandboxConfig;
use crate::plugin::store::PluginStore;
use crate::plugin::types::{PluginConfig, PluginError, PluginManifest, PluginResult, PluginType};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Part of plugin lifecycle, used in future WASM runtime
enum InstanceState {
    Created,
    Initialized,
    Running,
    Stopped,
    Failed,
}

impl PluginInstance {
    fn new(
        id: u64,
        name: String,
        plugin_type: PluginType,
        config: serde_json::Value,
        allow_network: bool,
    ) -> Self {
        Self {
            id,
            name: name.clone(),
            plugin_type,
            host: Arc::new(HostFunctions::new(name, config, allow_network)),
            state: RwLock::new(InstanceState::Created),
        }
    }

    /// Initialize the plugin
    pub async fn initialize(&self) -> PluginResult<()> {
        let mut state = self.state.write();
        if *state != InstanceState::Created {
            return Err(PluginError::ExecutionError(
                "Plugin already initialized".to_string(),
            ));
        }

        // TODO: Call WASM init function
        // For now, just transition state
        *state = InstanceState::Initialized;
        Ok(())
    }

    /// Check the plugin
    pub async fn check(&self) -> PluginResult<CheckResult> {
        let state = self.state.read();
        if *state != InstanceState::Initialized && *state != InstanceState::Running {
            return Err(PluginError::ExecutionError(
                "Plugin not initialized".to_string(),
            ));
        }

        // TODO: Call WASM check function
        Ok(CheckResult {
            success: true,
            message: None,
        })
    }

    /// Get state
    pub fn get_state(&self) -> PluginResult<HashMap<String, serde_json::Value>> {
        Ok(self.host.get_all_state())
    }
}

/// Check result from plugin
#[derive(Debug, Clone)]
pub struct CheckResult {
    pub success: bool,
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
    pub async fn load_plugin(&self, path: impl AsRef<std::path::Path>) -> PluginResult<PluginManifest> {
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

        let instance = Arc::new(PluginInstance::new(
            instance_id,
            plugin_name.to_string(),
            entry.manifest.plugin_type,
            config_value,
            config.allow_network,
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
        self.store.sources().into_iter().map(|e| e.manifest).collect()
    }

    pub fn list_sinks(&self) -> Vec<PluginManifest> {
        self.store.sinks().into_iter().map(|e| e.manifest).collect()
    }

    pub fn list_transforms(&self) -> Vec<PluginManifest> {
        self.store.transforms().into_iter().map(|e| e.manifest).collect()
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

    #[tokio::test]
    async fn test_runtime_creation() {
        let config = RuntimeConfig::default();
        let runtime = PluginRuntime::new(config).unwrap();

        let stats = runtime.stats();
        assert_eq!(stats.plugins_loaded, 0);
        assert_eq!(stats.active_instances, 0);
    }
}
