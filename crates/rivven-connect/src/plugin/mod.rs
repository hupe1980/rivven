//! WASM Plugin Runtime for Rivven Connect
//!
//! Load and execute custom connectors as WebAssembly modules.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      WASM Plugin Runtime                        │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
//! │  │ Plugin Loader│  │ Plugin Host  │  │ Plugin Store │          │
//! │  │ (.wasm files)│  │ (wasmtime)   │  │ (registry)   │          │
//! │  └──────────────┘  └──────────────┘  └──────────────┘          │
//! │         │                 │                 │                   │
//! │         ▼                 ▼                 ▼                   │
//! │  ┌─────────────────────────────────────────────────────────┐   │
//! │  │                     Host Functions                       │   │
//! │  │  • log(level, msg)           • get_config(key) -> val   │   │
//! │  │  • emit_event(event)         • get_state(key) -> val    │   │
//! │  │  • http_request(...)         • set_state(key, val)      │   │
//! │  └─────────────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::plugin::{PluginRuntime, PluginConfig};
//!
//! // Load WASM plugin
//! let config = PluginConfig {
//!     path: "/path/to/plugin.wasm".into(),
//!     ..Default::default()
//! };
//!
//! let mut runtime = PluginRuntime::new()?;
//! let plugin = runtime.load_plugin("my-source", config).await?;
//!
//! // Use as a source
//! let events = plugin.read(&catalog, state).await?;
//! ```

mod abi;
mod host;
mod loader;
mod runtime;
mod sandbox;
mod store;
mod types;

pub use abi::{AbiVersion, PluginAbi};
pub use host::HostFunctions;
pub use loader::{LoadError, PluginLoader};
pub use runtime::{PluginRuntime, RuntimeConfig};
pub use sandbox::{ResourceLimits, SandboxConfig};
pub use store::{PluginEntry, PluginStore};
pub use types::{
    PluginConfig, PluginError, PluginManifest, PluginResult, PluginType, WasmSink, WasmSource,
    WasmTransform,
};
