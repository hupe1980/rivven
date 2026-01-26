//! WASM Plugin loader

use crate::plugin::types::{PluginError, PluginManifest, PluginResult};
use std::path::Path;

/// Error type for plugin loading
pub type LoadError = PluginError;

/// Plugin loader for WASM modules
pub struct PluginLoader {
    /// Search paths for plugins
    search_paths: Vec<std::path::PathBuf>,
    /// Cache of loaded manifests
    manifest_cache: parking_lot::RwLock<std::collections::HashMap<String, PluginManifest>>,
}

impl PluginLoader {
    /// Create a new plugin loader
    pub fn new() -> Self {
        Self {
            search_paths: vec![
                std::path::PathBuf::from("./plugins"),
                std::path::PathBuf::from("/etc/rivven/plugins"),
            ],
            manifest_cache: parking_lot::RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Add a search path for plugins
    pub fn add_search_path(&mut self, path: impl AsRef<Path>) {
        self.search_paths.push(path.as_ref().to_path_buf());
    }

    /// Load a WASM module from bytes
    pub fn load_bytes(&self, name: &str, bytes: &[u8]) -> PluginResult<WasmModule> {
        // Validate WASM magic number
        if bytes.len() < 8 || &bytes[0..4] != b"\0asm" {
            return Err(PluginError::LoadError(
                "Invalid WASM module: missing magic number".to_string(),
            ));
        }

        // Parse the module to extract manifest
        let manifest = self.extract_manifest(bytes)?;

        Ok(WasmModule {
            name: name.to_string(),
            bytes: bytes.to_vec(),
            manifest,
        })
    }

    /// Load a WASM module from file
    pub async fn load_file(&self, path: impl AsRef<Path>) -> PluginResult<WasmModule> {
        let path = path.as_ref();
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        let bytes = tokio::fs::read(path)
            .await
            .map_err(|e| PluginError::LoadError(format!("Failed to read file: {}", e)))?;

        self.load_bytes(&name, &bytes)
    }

    /// Load a plugin by name (search in paths)
    pub async fn load_by_name(&self, name: &str) -> PluginResult<WasmModule> {
        // Check cache first
        let cached_path = {
            let cache = self.manifest_cache.read();
            if cache.contains_key(name) {
                // Find the corresponding file
                let mut found_path = None;
                for path in &self.search_paths {
                    let wasm_path = path.join(format!("{}.wasm", name));
                    if wasm_path.exists() {
                        found_path = Some(wasm_path);
                        break;
                    }
                }
                found_path
            } else {
                None
            }
        };

        // If we found a cached path, load it (guard is now dropped)
        if let Some(wasm_path) = cached_path {
            return self.load_file(&wasm_path).await;
        }

        // Search for the plugin
        for search_path in &self.search_paths {
            let wasm_path = search_path.join(format!("{}.wasm", name));
            if wasm_path.exists() {
                return self.load_file(&wasm_path).await;
            }

            // Also check subdirectories
            if search_path.exists() {
                if let Ok(entries) = std::fs::read_dir(search_path) {
                    for entry in entries.flatten() {
                        if entry.path().is_dir() {
                            let wasm_path = entry.path().join(format!("{}.wasm", name));
                            if wasm_path.exists() {
                                return self.load_file(&wasm_path).await;
                            }
                        }
                    }
                }
            }
        }

        Err(PluginError::NotFound(name.to_string()))
    }

    /// Load a plugin from URL
    pub async fn load_url(&self, url: &str) -> PluginResult<WasmModule> {
        let client = reqwest::Client::new();
        
        let response = client
            .get(url)
            .send()
            .await
            .map_err(|e| PluginError::LoadError(format!("Failed to fetch plugin: {}", e)))?;

        if !response.status().is_success() {
            return Err(PluginError::LoadError(format!(
                "Failed to fetch plugin: HTTP {}",
                response.status()
            )));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| PluginError::LoadError(format!("Failed to read response: {}", e)))?;

        // Extract name from URL
        let name = url
            .rsplit('/')
            .next()
            .and_then(|s| s.strip_suffix(".wasm"))
            .unwrap_or("plugin")
            .to_string();

        self.load_bytes(&name, &bytes)
    }

    /// Discover all plugins in search paths
    pub async fn discover(&self) -> Vec<PluginDiscovery> {
        let mut discovered = Vec::new();

        for search_path in &self.search_paths {
            if !search_path.exists() {
                continue;
            }

            if let Ok(entries) = std::fs::read_dir(search_path) {
                for entry in entries.flatten() {
                    let path = entry.path();

                    // Check for .wasm files
                    if path.extension().map(|e| e == "wasm").unwrap_or(false) {
                        if let Ok(module) = self.load_file(&path).await {
                            discovered.push(PluginDiscovery {
                                path: path.clone(),
                                manifest: module.manifest,
                            });
                        }
                    }

                    // Check subdirectories for plugin bundles
                    if path.is_dir() {
                        let manifest_path = path.join("manifest.json");
                        let wasm_path = path.join("plugin.wasm");

                        if manifest_path.exists() && wasm_path.exists() {
                            if let Ok(manifest_bytes) = std::fs::read(&manifest_path) {
                                if let Ok(manifest) =
                                    serde_json::from_slice::<PluginManifest>(&manifest_bytes)
                                {
                                    discovered.push(PluginDiscovery {
                                        path: wasm_path,
                                        manifest,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        discovered
    }

    /// Extract manifest from WASM custom section or embedded JSON
    fn extract_manifest(&self, bytes: &[u8]) -> PluginResult<PluginManifest> {
        // Try to find a custom section named "rivven_manifest"
        if let Some(manifest) = self.find_custom_section(bytes, "rivven_manifest") {
            return serde_json::from_slice(&manifest)
                .map_err(|e| PluginError::InvalidManifest(e.to_string()));
        }

        // Fallback: create a minimal manifest
        Ok(PluginManifest {
            name: "unknown".to_string(),
            version: "0.0.0".to_string(),
            plugin_type: crate::plugin::types::PluginType::Source,
            description: None,
            author: None,
            license: None,
            homepage: None,
            abi_version: "1.0".to_string(),
            config_schema: None,
            capabilities: Vec::new(),
            resources: Default::default(),
        })
    }

    /// Find a custom section in WASM bytes
    fn find_custom_section(&self, bytes: &[u8], name: &str) -> Option<Vec<u8>> {
        // WASM format:
        // - magic: 4 bytes (0x00 0x61 0x73 0x6D)
        // - version: 4 bytes
        // - sections...

        if bytes.len() < 8 {
            return None;
        }

        let mut pos = 8;

        while pos < bytes.len() {
            let section_id = bytes.get(pos)?;
            pos += 1;

            // Read section size (LEB128)
            let (section_size, bytes_read) = self.read_leb128(&bytes[pos..])?;
            pos += bytes_read;

            if *section_id == 0 {
                // Custom section
                let section_start = pos;
                let section_end = pos + section_size as usize;

                if section_end > bytes.len() {
                    return None;
                }

                // Read name length (LEB128)
                let (name_len, name_bytes_read) = self.read_leb128(&bytes[pos..])?;
                pos += name_bytes_read;

                // Read name
                let name_end = pos + name_len as usize;
                if name_end > bytes.len() {
                    return None;
                }

                let section_name = std::str::from_utf8(&bytes[pos..name_end]).ok()?;
                pos = name_end;

                if section_name == name {
                    // Found the section, return the content
                    let content_end = section_start + section_size as usize;
                    return Some(bytes[pos..content_end].to_vec());
                }

                // Skip to next section
                pos = section_start + section_size as usize;
            } else {
                // Skip non-custom sections
                pos += section_size as usize;
            }
        }

        None
    }

    /// Read a LEB128 encoded unsigned integer
    fn read_leb128(&self, bytes: &[u8]) -> Option<(u64, usize)> {
        let mut result: u64 = 0;
        let mut shift = 0;
        let mut pos = 0;

        loop {
            let byte = *bytes.get(pos)?;
            pos += 1;

            result |= ((byte & 0x7F) as u64) << shift;

            if byte & 0x80 == 0 {
                return Some((result, pos));
            }

            shift += 7;
            if shift >= 64 {
                return None;
            }
        }
    }
}

impl Default for PluginLoader {
    fn default() -> Self {
        Self::new()
    }
}

/// A loaded WASM module
pub struct WasmModule {
    /// Plugin name
    pub name: String,
    /// Raw WASM bytes
    pub bytes: Vec<u8>,
    /// Parsed manifest
    pub manifest: PluginManifest,
}

/// Plugin discovery result
#[derive(Debug, Clone)]
pub struct PluginDiscovery {
    /// Path to WASM file
    pub path: std::path::PathBuf,
    /// Plugin manifest
    pub manifest: PluginManifest,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_wasm() {
        let loader = PluginLoader::new();
        
        // Not WASM
        let result = loader.load_bytes("test", b"not wasm");
        assert!(result.is_err());
        
        // Too short
        let result = loader.load_bytes("test", &[0, 0x61, 0x73, 0x6D]);
        assert!(result.is_err());
    }

    #[test]
    fn test_minimal_wasm() {
        let loader = PluginLoader::new();
        
        // Minimal valid WASM (magic + version only)
        let minimal_wasm = [
            0x00, 0x61, 0x73, 0x6D, // magic
            0x01, 0x00, 0x00, 0x00, // version 1
        ];
        
        let result = loader.load_bytes("test", &minimal_wasm);
        assert!(result.is_ok());
        
        let module = result.unwrap();
        assert_eq!(module.name, "test");
    }

    #[test]
    fn test_leb128_decode() {
        let loader = PluginLoader::new();
        
        // Single byte values
        assert_eq!(loader.read_leb128(&[0x00]), Some((0, 1)));
        assert_eq!(loader.read_leb128(&[0x7F]), Some((127, 1)));
        
        // Multi-byte value
        assert_eq!(loader.read_leb128(&[0x80, 0x01]), Some((128, 2)));
        assert_eq!(loader.read_leb128(&[0xE5, 0x8E, 0x26]), Some((624485, 3)));
    }
}
