//! Dashboard configuration
//!
//! Supports configuration injection for air-gapped deployments.
//! The server can inject config via `<meta>` tags in the HTML.

use wasm_bindgen::JsCast;

/// Dashboard configuration
#[derive(Debug, Clone)]
pub struct DashboardConfig {
    /// API base URL (e.g., "http://localhost:8080" or "/api")
    pub api_url: String,
    /// Server version (injected by server)
    pub version: Option<String>,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            api_url: String::new(), // Empty means use current origin
            version: None,
        }
    }
}

impl DashboardConfig {
    /// Load configuration from various sources (priority order):
    /// 1. `<meta name="rivven:api-url">` tag (server-injected)
    /// 2. `window.__RIVVEN_CONFIG__` object (JavaScript injection)
    /// 3. Current window origin (default)
    pub fn load() -> Self {
        let mut config = Self::default();

        // Try to load from <meta> tags first
        if let Some(document) = web_sys::window().and_then(|w| w.document()) {
            // API URL from meta tag
            if let Some(api_url) = get_meta_content(&document, "rivven:api-url") {
                if !api_url.is_empty() {
                    config.api_url = api_url;
                }
            }

            // Version from meta tag
            if let Some(version) = get_meta_content(&document, "rivven:version") {
                if !version.is_empty() {
                    config.version = Some(version);
                }
            }
        }

        // Try window.__RIVVEN_CONFIG__ as fallback
        if config.api_url.is_empty() {
            if let Some(url) = get_js_config("api_url") {
                config.api_url = url;
            }
        }

        // Final fallback: use current origin
        if config.api_url.is_empty() {
            config.api_url = web_sys::window()
                .and_then(|w| w.location().origin().ok())
                .unwrap_or_else(|| "http://localhost:8080".to_string());
        }

        config
    }

    /// Get the API base URL
    pub fn api_url(&self) -> &str {
        &self.api_url
    }
}

/// Get content from a <meta name="..."> tag
fn get_meta_content(document: &web_sys::Document, name: &str) -> Option<String> {
    let selector = format!("meta[name=\"{}\"]", name);
    document
        .query_selector(&selector)
        .ok()
        .flatten()
        .and_then(|el| el.dyn_into::<web_sys::HtmlMetaElement>().ok())
        .map(|meta| meta.content())
}

/// Get a value from window.__RIVVEN_CONFIG__
fn get_js_config(key: &str) -> Option<String> {
    let window = web_sys::window()?;
    let config = js_sys::Reflect::get(&window, &"__RIVVEN_CONFIG__".into()).ok()?;
    
    if config.is_undefined() || config.is_null() {
        return None;
    }
    
    let value = js_sys::Reflect::get(&config, &key.into()).ok()?;
    value.as_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DashboardConfig::default();
        assert!(config.api_url.is_empty());
        assert!(config.version.is_none());
    }
}
