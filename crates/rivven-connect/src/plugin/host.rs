//! Host functions provided to WASM plugins

use crate::plugin::abi::{PluginHttpRequest, PluginHttpResponse, PluginLogLevel};
use crate::plugin::types::{PluginError, PluginResult};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Host functions that plugins can call
pub struct HostFunctions {
    /// Plugin name (for logging)
    plugin_name: String,
    /// Plugin configuration
    config: Arc<serde_json::Value>,
    /// Plugin state (persisted across calls)
    state: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    /// HTTP client for network requests
    http_client: reqwest::Client,
    /// Network access allowed
    allow_network: bool,
}

impl HostFunctions {
    /// Create new host functions for a plugin
    pub fn new(plugin_name: String, config: serde_json::Value, allow_network: bool) -> Self {
        Self {
            plugin_name,
            config: Arc::new(config),
            state: Arc::new(RwLock::new(HashMap::new())),
            http_client: reqwest::Client::new(),
            allow_network,
        }
    }

    /// Log a message from the plugin
    pub fn log(&self, level: PluginLogLevel, message: &str) {
        match level {
            PluginLogLevel::Trace => tracing::trace!(plugin = %self.plugin_name, "{}", message),
            PluginLogLevel::Debug => tracing::debug!(plugin = %self.plugin_name, "{}", message),
            PluginLogLevel::Info => tracing::info!(plugin = %self.plugin_name, "{}", message),
            PluginLogLevel::Warn => tracing::warn!(plugin = %self.plugin_name, "{}", message),
            PluginLogLevel::Error => tracing::error!(plugin = %self.plugin_name, "{}", message),
        }
    }

    /// Get a configuration value
    pub fn get_config(&self, key: &str) -> Option<serde_json::Value> {
        // Support dot notation for nested keys
        let parts: Vec<&str> = key.split('.').collect();
        let mut current = &*self.config;

        for part in parts {
            match current {
                serde_json::Value::Object(map) => {
                    current = map.get(part)?;
                }
                _ => return None,
            }
        }

        Some(current.clone())
    }

    /// Get a state value
    pub fn get_state(&self, key: &str) -> Option<serde_json::Value> {
        let state = self.state.read();
        state.get(key).cloned()
    }

    /// Set a state value
    pub fn set_state(&self, key: &str, value: serde_json::Value) {
        let mut state = self.state.write();
        state.insert(key.to_string(), value);
    }

    /// Delete a state value
    pub fn delete_state(&self, key: &str) -> Option<serde_json::Value> {
        let mut state = self.state.write();
        state.remove(key)
    }

    /// Get all state
    pub fn get_all_state(&self) -> HashMap<String, serde_json::Value> {
        let state = self.state.read();
        state.clone()
    }

    /// Check if network access is allowed
    pub fn allow_network(&self) -> bool {
        self.allow_network
    }

    /// Make an HTTP request
    pub async fn http_request(
        &self,
        request: PluginHttpRequest,
    ) -> PluginResult<PluginHttpResponse> {
        if !self.allow_network {
            return Err(PluginError::ExecutionError(
                "Network access not allowed for this plugin".to_string(),
            ));
        }

        let method = request.method.parse().map_err(|_| {
            PluginError::ExecutionError(format!("Invalid HTTP method: {}", request.method))
        })?;

        let mut req_builder = self
            .http_client
            .request(method, &request.url)
            .timeout(std::time::Duration::from_millis(request.timeout_ms));

        for (name, value) in &request.headers {
            req_builder = req_builder.header(name, value);
        }

        if let Some(body) = request.body {
            req_builder = req_builder.body(body);
        }

        let response = req_builder
            .send()
            .await
            .map_err(|e| PluginError::ExecutionError(format!("HTTP request failed: {}", e)))?;

        let status = response.status().as_u16();
        let headers: Vec<(String, String)> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or_default().to_string()))
            .collect();

        let body = response.bytes().await.map_err(|e| {
            PluginError::ExecutionError(format!("Failed to read response body: {}", e))
        })?;

        Ok(PluginHttpResponse {
            status,
            headers,
            body: body.to_vec(),
        })
    }

    /// Get current timestamp (milliseconds since epoch)
    pub fn now(&self) -> i64 {
        chrono::Utc::now().timestamp_millis()
    }

    /// Generate a UUID v4
    pub fn uuid(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }
}

/// Trait for implementing custom host function providers
#[allow(dead_code)] // Extensibility point for custom WASM runtimes
pub trait HostFunctionProvider: Send + Sync {
    /// Get configuration value
    fn get_config(&self, key: &str) -> Option<serde_json::Value>;

    /// Get state value
    fn get_state(&self, key: &str) -> Option<serde_json::Value>;

    /// Set state value
    fn set_state(&self, key: &str, value: serde_json::Value);

    /// Log a message
    fn log(&self, level: PluginLogLevel, message: &str);

    /// Check if network access is allowed
    fn allow_network(&self) -> bool;
}

impl HostFunctionProvider for HostFunctions {
    fn get_config(&self, key: &str) -> Option<serde_json::Value> {
        HostFunctions::get_config(self, key)
    }

    fn get_state(&self, key: &str) -> Option<serde_json::Value> {
        HostFunctions::get_state(self, key)
    }

    fn set_state(&self, key: &str, value: serde_json::Value) {
        HostFunctions::set_state(self, key, value)
    }

    fn log(&self, level: PluginLogLevel, message: &str) {
        HostFunctions::log(self, level, message)
    }

    fn allow_network(&self) -> bool {
        self.allow_network
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_get_config_simple() {
        let config = json!({
            "endpoint": "http://localhost:8080",
            "timeout": 30
        });

        let host = HostFunctions::new("test".to_string(), config, false);

        assert_eq!(
            host.get_config("endpoint"),
            Some(json!("http://localhost:8080"))
        );
        assert_eq!(host.get_config("timeout"), Some(json!(30)));
        assert_eq!(host.get_config("missing"), None);
    }

    #[test]
    fn test_get_config_nested() {
        let config = json!({
            "database": {
                "host": "localhost",
                "port": 5432,
                "credentials": {
                    "username": "admin"
                }
            }
        });

        let host = HostFunctions::new("test".to_string(), config, false);

        assert_eq!(host.get_config("database.host"), Some(json!("localhost")));
        assert_eq!(
            host.get_config("database.credentials.username"),
            Some(json!("admin"))
        );
        assert_eq!(host.get_config("database.missing"), None);
    }

    #[test]
    fn test_state_operations() {
        let host = HostFunctions::new("test".to_string(), json!({}), false);

        // Initially empty
        assert_eq!(host.get_state("cursor"), None);

        // Set state
        host.set_state("cursor", json!(100));
        assert_eq!(host.get_state("cursor"), Some(json!(100)));

        // Update state
        host.set_state("cursor", json!(200));
        assert_eq!(host.get_state("cursor"), Some(json!(200)));

        // Delete state
        let old = host.delete_state("cursor");
        assert_eq!(old, Some(json!(200)));
        assert_eq!(host.get_state("cursor"), None);
    }

    #[test]
    fn test_uuid_generation() {
        let host = HostFunctions::new("test".to_string(), json!({}), false);

        let uuid1 = host.uuid();
        let uuid2 = host.uuid();

        // UUIDs should be different
        assert_ne!(uuid1, uuid2);

        // Should be valid UUID format
        assert!(uuid::Uuid::parse_str(&uuid1).is_ok());
        assert!(uuid::Uuid::parse_str(&uuid2).is_ok());
    }

    #[test]
    fn test_timestamp() {
        let host = HostFunctions::new("test".to_string(), json!({}), false);

        let before = chrono::Utc::now().timestamp_millis();
        let ts = host.now();
        let after = chrono::Utc::now().timestamp_millis();

        assert!(ts >= before);
        assert!(ts <= after);
    }
}
