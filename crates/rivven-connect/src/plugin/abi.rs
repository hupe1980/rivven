//! Plugin ABI (Application Binary Interface) definitions
//!
//! Defines the interface between the host (Rivven Connect) and WASM plugins.

use serde::{Deserialize, Serialize};

/// ABI version for compatibility checking
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbiVersion {
    pub major: u32,
    pub minor: u32,
}

impl AbiVersion {
    pub const CURRENT: AbiVersion = AbiVersion { major: 1, minor: 0 };

    pub fn new(major: u32, minor: u32) -> Self {
        Self { major, minor }
    }

    /// Check if this version is compatible with the required version
    pub fn is_compatible(&self, required: &AbiVersion) -> bool {
        // Major version must match, minor can be >= required
        self.major == required.major && self.minor >= required.minor
    }

    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() >= 2 {
            let major = parts[0].parse().ok()?;
            let minor = parts[1].parse().ok()?;
            Some(Self { major, minor })
        } else {
            None
        }
    }
}

impl std::fmt::Display for AbiVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// Plugin ABI function signatures
///
/// These are the functions that plugins must export and the host functions
/// they can import.
pub struct PluginAbi;

impl PluginAbi {
    // ========== Plugin Exports (called by host) ==========

    /// Get the plugin manifest as JSON
    /// Signature: () -> ptr_len (pointer and length packed)
    pub const EXPORT_GET_MANIFEST: &'static str = "rivven_get_manifest";

    /// Initialize the plugin with configuration
    /// Signature: (config_ptr, config_len) -> result
    pub const EXPORT_INIT: &'static str = "rivven_init";

    /// Check connectivity (for sources and sinks)
    /// Signature: () -> result_ptr_len
    pub const EXPORT_CHECK: &'static str = "rivven_check";

    /// Discover available streams (for sources)
    /// Signature: () -> catalog_ptr_len
    pub const EXPORT_DISCOVER: &'static str = "rivven_discover";

    /// Poll for next event(s) from source
    /// Signature: (max_events) -> events_ptr_len
    pub const EXPORT_POLL: &'static str = "rivven_poll";

    /// Write events to sink
    /// Signature: (events_ptr, events_len) -> result_ptr_len
    pub const EXPORT_WRITE: &'static str = "rivven_write";

    /// Transform an event
    /// Signature: (event_ptr, event_len) -> events_ptr_len
    pub const EXPORT_TRANSFORM: &'static str = "rivven_transform";

    /// Shutdown the plugin
    /// Signature: () -> result
    pub const EXPORT_SHUTDOWN: &'static str = "rivven_shutdown";

    /// Memory allocation function (for passing data to plugin)
    /// Signature: (size) -> ptr
    pub const EXPORT_ALLOC: &'static str = "rivven_alloc";

    /// Memory deallocation function
    /// Signature: (ptr, size) -> ()
    pub const EXPORT_DEALLOC: &'static str = "rivven_dealloc";

    // ========== Host Imports (called by plugin) ==========

    /// Log a message
    /// Signature: (level, msg_ptr, msg_len) -> ()
    pub const IMPORT_LOG: &'static str = "rivven_host_log";

    /// Get configuration value
    /// Signature: (key_ptr, key_len) -> value_ptr_len
    pub const IMPORT_GET_CONFIG: &'static str = "rivven_host_get_config";

    /// Get state value
    /// Signature: (key_ptr, key_len) -> value_ptr_len
    pub const IMPORT_GET_STATE: &'static str = "rivven_host_get_state";

    /// Set state value
    /// Signature: (key_ptr, key_len, value_ptr, value_len) -> result
    pub const IMPORT_SET_STATE: &'static str = "rivven_host_set_state";

    /// Make HTTP request
    /// Signature: (request_ptr, request_len) -> response_ptr_len
    pub const IMPORT_HTTP_REQUEST: &'static str = "rivven_host_http_request";

    /// Get current timestamp (milliseconds since epoch)
    /// Signature: () -> i64
    pub const IMPORT_NOW: &'static str = "rivven_host_now";

    /// Generate UUID v4
    /// Signature: () -> uuid_ptr_len
    pub const IMPORT_UUID: &'static str = "rivven_host_uuid";

    /// Sleep for milliseconds (cooperative yield)
    /// Signature: (ms) -> ()
    pub const IMPORT_SLEEP: &'static str = "rivven_host_sleep";
}

/// Log levels for plugin logging
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginLogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

impl From<u32> for PluginLogLevel {
    fn from(v: u32) -> Self {
        match v {
            0 => PluginLogLevel::Trace,
            1 => PluginLogLevel::Debug,
            2 => PluginLogLevel::Info,
            3 => PluginLogLevel::Warn,
            _ => PluginLogLevel::Error,
        }
    }
}

/// Result codes from plugin functions
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // ABI codes for WASM plugin interop
pub enum PluginResultCode {
    Ok = 0,
    Error = -1,
    InvalidInput = -2,
    NotInitialized = -3,
    ResourceExhausted = -4,
    Timeout = -5,
    NotSupported = -6,
}

impl From<i32> for PluginResultCode {
    fn from(v: i32) -> Self {
        match v {
            0 => PluginResultCode::Ok,
            -1 => PluginResultCode::Error,
            -2 => PluginResultCode::InvalidInput,
            -3 => PluginResultCode::NotInitialized,
            -4 => PluginResultCode::ResourceExhausted,
            -5 => PluginResultCode::Timeout,
            _ => PluginResultCode::NotSupported,
        }
    }
}

/// Packed pointer and length (for returning variable-size data)
/// Layout: upper 32 bits = pointer, lower 32 bits = length
#[allow(dead_code)] // ABI helper for WASM memory interop
pub fn pack_ptr_len(ptr: u32, len: u32) -> u64 {
    ((ptr as u64) << 32) | (len as u64)
}

#[allow(dead_code)] // ABI helper for WASM memory interop
pub fn unpack_ptr_len(packed: u64) -> (u32, u32) {
    let ptr = (packed >> 32) as u32;
    let len = packed as u32;
    (ptr, len)
}

/// HTTP request structure for host HTTP function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginHttpRequest {
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_timeout_ms() -> u64 {
    30000
}

/// HTTP response structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginHttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_abi_version_compatibility() {
        let current = AbiVersion::new(1, 0);
        let required_1_0 = AbiVersion::new(1, 0);
        let required_1_1 = AbiVersion::new(1, 1);
        let required_2_0 = AbiVersion::new(2, 0);

        assert!(current.is_compatible(&required_1_0));
        assert!(!current.is_compatible(&required_1_1)); // minor too high
        assert!(!current.is_compatible(&required_2_0)); // major mismatch
    }

    #[test]
    fn test_pack_unpack_ptr_len() {
        let ptr = 0x12345678u32;
        let len = 0xABCDu32;

        let packed = pack_ptr_len(ptr, len);
        let (unpacked_ptr, unpacked_len) = unpack_ptr_len(packed);

        assert_eq!(unpacked_ptr, ptr);
        assert_eq!(unpacked_len, len);
    }

    #[test]
    fn test_abi_version_parse() {
        assert_eq!(AbiVersion::parse("1.0"), Some(AbiVersion::new(1, 0)));
        assert_eq!(AbiVersion::parse("2.5"), Some(AbiVersion::new(2, 5)));
        assert_eq!(AbiVersion::parse("invalid"), None);
    }
}
