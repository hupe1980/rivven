//! Plugin store (registry of loaded plugins)

use crate::plugin::loader::WasmModule;
use crate::plugin::types::{PluginError, PluginManifest, PluginResult, PluginType};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Entry in the plugin store
#[derive(Clone)]
pub struct PluginEntry {
    /// Plugin name
    pub name: String,
    /// Plugin manifest
    pub manifest: PluginManifest,
    /// WASM bytes
    pub bytes: Arc<Vec<u8>>,
    /// Load timestamp
    pub loaded_at: chrono::DateTime<chrono::Utc>,
    /// Number of active instances
    pub instance_count: usize,
}

impl PluginEntry {
    pub fn new(module: WasmModule) -> Self {
        Self {
            name: module.name.clone(),
            manifest: module.manifest,
            bytes: Arc::new(module.bytes),
            loaded_at: chrono::Utc::now(),
            instance_count: 0,
        }
    }
}

/// Plugin store for managing loaded plugins
pub struct PluginStore {
    /// Loaded plugins by name
    plugins: RwLock<HashMap<String, PluginEntry>>,
    /// Plugin type index
    by_type: RwLock<HashMap<PluginType, Vec<String>>>,
}

impl PluginStore {
    /// Create a new plugin store
    pub fn new() -> Self {
        Self {
            plugins: RwLock::new(HashMap::new()),
            by_type: RwLock::new(HashMap::new()),
        }
    }

    /// Register a plugin
    pub fn register(&self, module: WasmModule) -> PluginResult<()> {
        let name = module.name.clone();
        let plugin_type = module.manifest.plugin_type;
        let entry = PluginEntry::new(module);

        {
            let mut plugins = self.plugins.write();
            if plugins.contains_key(&name) {
                return Err(PluginError::LoadError(format!(
                    "Plugin '{}' is already registered",
                    name
                )));
            }
            plugins.insert(name.clone(), entry);
        }

        {
            let mut by_type = self.by_type.write();
            by_type.entry(plugin_type).or_default().push(name.clone());
        }

        tracing::info!(name = %name, plugin_type = %plugin_type, "Registered plugin");
        Ok(())
    }

    /// Unregister a plugin
    pub fn unregister(&self, name: &str) -> PluginResult<PluginEntry> {
        let entry = {
            let mut plugins = self.plugins.write();
            plugins.remove(name).ok_or_else(|| {
                PluginError::NotFound(name.to_string())
            })?
        };

        {
            let mut by_type = self.by_type.write();
            if let Some(names) = by_type.get_mut(&entry.manifest.plugin_type) {
                names.retain(|n| n != name);
            }
        }

        tracing::info!(name = %name, "Unregistered plugin");
        Ok(entry)
    }

    /// Get a plugin by name
    pub fn get(&self, name: &str) -> PluginResult<PluginEntry> {
        let plugins = self.plugins.read();
        plugins
            .get(name)
            .cloned()
            .ok_or_else(|| PluginError::NotFound(name.to_string()))
    }

    /// Check if a plugin exists
    pub fn contains(&self, name: &str) -> bool {
        let plugins = self.plugins.read();
        plugins.contains_key(name)
    }

    /// List all plugin names
    pub fn list(&self) -> Vec<String> {
        let plugins = self.plugins.read();
        plugins.keys().cloned().collect()
    }

    /// List plugins by type
    pub fn list_by_type(&self, plugin_type: PluginType) -> Vec<String> {
        let by_type = self.by_type.read();
        by_type.get(&plugin_type).cloned().unwrap_or_default()
    }

    /// Get all plugins
    pub fn all(&self) -> Vec<PluginEntry> {
        let plugins = self.plugins.read();
        plugins.values().cloned().collect()
    }

    /// Get sources
    pub fn sources(&self) -> Vec<PluginEntry> {
        self.get_by_type(PluginType::Source)
    }

    /// Get sinks
    pub fn sinks(&self) -> Vec<PluginEntry> {
        self.get_by_type(PluginType::Sink)
    }

    /// Get transforms
    pub fn transforms(&self) -> Vec<PluginEntry> {
        self.get_by_type(PluginType::Transform)
    }

    fn get_by_type(&self, plugin_type: PluginType) -> Vec<PluginEntry> {
        let names = {
            let by_type = self.by_type.read();
            by_type.get(&plugin_type).cloned().unwrap_or_default()
        };

        let plugins = self.plugins.read();
        names
            .iter()
            .filter_map(|name| plugins.get(name).cloned())
            .collect()
    }

    /// Increment instance count for a plugin
    pub fn increment_instances(&self, name: &str) -> PluginResult<usize> {
        let mut plugins = self.plugins.write();
        let entry = plugins
            .get_mut(name)
            .ok_or_else(|| PluginError::NotFound(name.to_string()))?;
        entry.instance_count += 1;
        Ok(entry.instance_count)
    }

    /// Decrement instance count for a plugin
    pub fn decrement_instances(&self, name: &str) -> PluginResult<usize> {
        let mut plugins = self.plugins.write();
        let entry = plugins
            .get_mut(name)
            .ok_or_else(|| PluginError::NotFound(name.to_string()))?;
        entry.instance_count = entry.instance_count.saturating_sub(1);
        Ok(entry.instance_count)
    }

    /// Get plugin statistics
    pub fn stats(&self) -> PluginStoreStats {
        let plugins = self.plugins.read();
        let by_type = self.by_type.read();

        PluginStoreStats {
            total: plugins.len(),
            sources: by_type.get(&PluginType::Source).map(|v| v.len()).unwrap_or(0),
            sinks: by_type.get(&PluginType::Sink).map(|v| v.len()).unwrap_or(0),
            transforms: by_type.get(&PluginType::Transform).map(|v| v.len()).unwrap_or(0),
            total_instances: plugins.values().map(|e| e.instance_count).sum(),
        }
    }
}

impl Default for PluginStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Plugin store statistics
#[derive(Debug, Clone)]
pub struct PluginStoreStats {
    /// Total number of registered plugins
    pub total: usize,
    /// Number of source plugins
    pub sources: usize,
    /// Number of sink plugins
    pub sinks: usize,
    /// Number of transform plugins
    pub transforms: usize,
    /// Total number of active instances
    pub total_instances: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::types::ResourceRequirements;

    fn test_module(name: &str, plugin_type: PluginType) -> WasmModule {
        // Minimal WASM bytes
        let bytes = vec![
            0x00, 0x61, 0x73, 0x6D, // magic
            0x01, 0x00, 0x00, 0x00, // version 1
        ];

        WasmModule {
            name: name.to_string(),
            bytes,
            manifest: PluginManifest {
                name: name.to_string(),
                version: "1.0.0".to_string(),
                plugin_type,
                description: None,
                author: None,
                license: None,
                homepage: None,
                abi_version: "1.0".to_string(),
                config_schema: None,
                capabilities: Vec::new(),
                resources: ResourceRequirements::default(),
            },
        }
    }

    #[test]
    fn test_register_and_get() {
        let store = PluginStore::new();

        let module = test_module("test-source", PluginType::Source);
        store.register(module).unwrap();

        assert!(store.contains("test-source"));
        
        let entry = store.get("test-source").unwrap();
        assert_eq!(entry.name, "test-source");
        assert_eq!(entry.manifest.plugin_type, PluginType::Source);
    }

    #[test]
    fn test_list_by_type() {
        let store = PluginStore::new();

        store.register(test_module("source1", PluginType::Source)).unwrap();
        store.register(test_module("source2", PluginType::Source)).unwrap();
        store.register(test_module("sink1", PluginType::Sink)).unwrap();

        let sources = store.list_by_type(PluginType::Source);
        assert_eq!(sources.len(), 2);
        assert!(sources.contains(&"source1".to_string()));
        assert!(sources.contains(&"source2".to_string()));

        let sinks = store.list_by_type(PluginType::Sink);
        assert_eq!(sinks.len(), 1);
    }

    #[test]
    fn test_unregister() {
        let store = PluginStore::new();

        store.register(test_module("test", PluginType::Source)).unwrap();
        assert!(store.contains("test"));

        store.unregister("test").unwrap();
        assert!(!store.contains("test"));
    }

    #[test]
    fn test_instance_counting() {
        let store = PluginStore::new();
        store.register(test_module("test", PluginType::Source)).unwrap();

        assert_eq!(store.increment_instances("test").unwrap(), 1);
        assert_eq!(store.increment_instances("test").unwrap(), 2);
        assert_eq!(store.decrement_instances("test").unwrap(), 1);
    }

    #[test]
    fn test_stats() {
        let store = PluginStore::new();

        store.register(test_module("s1", PluginType::Source)).unwrap();
        store.register(test_module("s2", PluginType::Source)).unwrap();
        store.register(test_module("sink1", PluginType::Sink)).unwrap();
        store.register(test_module("t1", PluginType::Transform)).unwrap();

        store.increment_instances("s1").unwrap();
        store.increment_instances("s1").unwrap();
        store.increment_instances("sink1").unwrap();

        let stats = store.stats();
        assert_eq!(stats.total, 4);
        assert_eq!(stats.sources, 2);
        assert_eq!(stats.sinks, 1);
        assert_eq!(stats.transforms, 1);
        assert_eq!(stats.total_instances, 3);
    }
}
