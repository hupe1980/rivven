//! Connector registry for dynamic connector loading
//!
//! This module provides the registry pattern for runtime connector lookup.
//! Connectors are built into rivven-connect with feature flags for optional connectors.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    rivven-connect (SDK + Runtime)               │
//! │  SourceFactory, SinkFactory, AnySource, AnySink, Registry       │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  Built-in: postgres-cdc, mysql-cdc, kafka, s3, snowflake, ...   │
//! │  External: rivven-cdc (optional CDC primitives)                 │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example: Creating a registry with specific connectors
//!
//! ```rust,ignore
//! use rivven_connect::{SourceRegistry, SinkRegistry};
//! use rivven_connect::connectors::{cdc, stdout};
//! use std::sync::Arc;
//!
//! fn main() {
//!     let mut sources = SourceRegistry::new();
//!     sources.register("postgres-cdc", Arc::new(cdc::PostgresCdcSourceFactory));
//!
//!     let mut sinks = SinkRegistry::new();
//!     sinks.register("stdout", Arc::new(stdout::StdoutSinkFactory));
//!
//!     // Run your connect instance...
//! }
//! ```

use super::catalog::{Catalog, ConfiguredCatalog};
use super::event::SourceEvent;
use super::sink::WriteResult;
use super::source::CheckResult;
use super::spec::ConnectorSpec;
use super::state::State;
use crate::error::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

/// Tracked state of a connector within a registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorRunState {
    /// Registered but not yet started.
    Registered,
    /// Currently running.
    Running,
    /// Stopped (graceful shutdown).
    Stopped,
    /// Failed with an error.
    Failed,
}

/// Factory trait for creating source instances
///
/// Implement this trait in your adapter crate to register sources.
pub trait SourceFactory: Send + Sync {
    /// Get the connector specification
    fn spec(&self) -> ConnectorSpec;

    /// Create a boxed source instance for runtime dispatch
    fn create(&self) -> Result<Box<dyn AnySource>>;
}

/// Type-erased source for runtime dispatch
///
/// This allows the CLI to work with sources without knowing their concrete types.
#[async_trait]
pub trait AnySource: Send + Sync {
    /// Check connectivity with raw YAML config
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult>;

    /// Discover streams with raw YAML config
    async fn discover_raw(&self, config: &serde_yaml::Value) -> Result<Catalog>;

    /// Read events with raw YAML config
    async fn read_raw(
        &self,
        config: &serde_yaml::Value,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<futures::stream::BoxStream<'static, Result<SourceEvent>>>;
}

/// Factory trait for creating sink instances
///
/// Implement this trait in your adapter crate to register sinks.
pub trait SinkFactory: Send + Sync {
    /// Get the connector specification
    fn spec(&self) -> ConnectorSpec;

    /// Create a boxed sink instance for runtime dispatch
    fn create(&self) -> Result<Box<dyn AnySink>>;
}

/// Type-erased sink for runtime dispatch
///
/// This allows the CLI to work with sinks without knowing their concrete types.
#[async_trait]
pub trait AnySink: Send + Sync {
    /// Check connectivity with raw YAML config
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult>;

    /// Write events with raw YAML config
    async fn write_raw(
        &self,
        config: &serde_yaml::Value,
        events: futures::stream::BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult>;
}

/// Registry of available source connectors
///
/// Users create their own registry and register the sources they need.
/// This enables custom binaries with only the required adapters.
///
/// Each entry tracks the connector's runtime state so callers can
/// query which sources are running, stopped, or failed.
pub struct SourceRegistry {
    sources: HashMap<String, SourceRegistryEntry>,
}

/// A source registry entry with factory and tracked state.
pub struct SourceRegistryEntry {
    pub factory: Arc<dyn SourceFactory>,
    pub state: ConnectorRunState,
}

impl SourceRegistry {
    /// Create an empty source registry
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
        }
    }

    /// Register a source factory
    pub fn register(&mut self, name: &str, factory: Arc<dyn SourceFactory>) {
        self.sources.insert(
            name.to_string(),
            SourceRegistryEntry {
                factory,
                state: ConnectorRunState::Registered,
            },
        );
    }

    /// Get a source factory by name
    pub fn get(&self, name: &str) -> Option<&Arc<dyn SourceFactory>> {
        self.sources.get(name).map(|e| &e.factory)
    }

    /// Get the run-state of a registered source connector.
    pub fn get_state(&self, name: &str) -> Option<ConnectorRunState> {
        self.sources.get(name).map(|e| e.state)
    }

    /// Update the run-state of a registered source connector.
    pub fn set_state(&mut self, name: &str, state: ConnectorRunState) {
        if let Some(entry) = self.sources.get_mut(name) {
            entry.state = state;
        }
    }

    /// List available source types with their specs
    pub fn list(&self) -> Vec<(&str, ConnectorSpec)> {
        self.sources
            .iter()
            .map(|(name, entry)| (name.as_str(), entry.factory.spec()))
            .collect()
    }

    /// List available sources with their specs and current state.
    pub fn list_with_state(&self) -> Vec<(&str, ConnectorSpec, ConnectorRunState)> {
        self.sources
            .iter()
            .map(|(name, entry)| (name.as_str(), entry.factory.spec(), entry.state))
            .collect()
    }

    /// Check if a source is registered
    pub fn contains(&self, name: &str) -> bool {
        self.sources.contains_key(name)
    }

    /// Number of registered sources
    pub fn len(&self) -> usize {
        self.sources.len()
    }

    /// Check if registry is empty
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }
}

impl Default for SourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Registry of available sink connectors
///
/// Users create their own registry and register the sinks they need.
/// This enables custom binaries with only the required adapters.
///
/// Sink instances created by factories are cached so that repeated
/// lookups via [`get_or_create`](SinkRegistry::get_or_create) return
/// the *same* `Arc<dyn AnySink>` instead of allocating a new object
/// on each call.
pub struct SinkRegistry {
    sinks: HashMap<String, Arc<dyn SinkFactory>>,
    /// Cached sink instances – populated lazily on first
    /// [`get_or_create`](SinkRegistry::get_or_create) call per name.
    cache: HashMap<String, Arc<dyn AnySink>>,
}

impl SinkRegistry {
    /// Create an empty sink registry
    pub fn new() -> Self {
        Self {
            sinks: HashMap::new(),
            cache: HashMap::new(),
        }
    }

    /// Register a sink factory
    pub fn register(&mut self, name: &str, factory: Arc<dyn SinkFactory>) {
        self.sinks.insert(name.to_string(), factory);
        // Invalidate any cached instance for this name so the new
        // factory takes effect on next lookup.
        self.cache.remove(name);
    }

    /// Get a sink factory by name
    pub fn get(&self, name: &str) -> Option<&Arc<dyn SinkFactory>> {
        self.sinks.get(name)
    }

    /// Get (or create and cache) a sink instance by name.
    ///
    /// The first call creates the instance via the factory and caches
    /// it.  Subsequent calls return the cached `Arc` without
    /// allocating.
    pub fn get_or_create(&mut self, name: &str) -> Option<Result<Arc<dyn AnySink>>> {
        if let Some(cached) = self.cache.get(name) {
            return Some(Ok(cached.clone()));
        }

        let factory = self.sinks.get(name)?;
        match factory.create() {
            Ok(sink) => {
                let arc: Arc<dyn AnySink> = Arc::from(sink);
                self.cache.insert(name.to_string(), arc.clone());
                Some(Ok(arc))
            }
            Err(e) => Some(Err(e)),
        }
    }

    /// List available sink types with their specs
    pub fn list(&self) -> Vec<(&str, ConnectorSpec)> {
        self.sinks
            .iter()
            .map(|(name, factory)| (name.as_str(), factory.spec()))
            .collect()
    }

    /// Check if a sink is registered
    pub fn contains(&self, name: &str) -> bool {
        self.sinks.contains_key(name)
    }

    /// Number of registered sinks
    pub fn len(&self) -> usize {
        self.sinks.len()
    }

    /// Check if registry is empty
    pub fn is_empty(&self) -> bool {
        self.sinks.is_empty()
    }
}

impl Default for SinkRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Transform Registry
// ============================================================================

/// Factory trait for creating transform instances
///
/// Implement this trait to register transforms.
pub trait TransformFactory: Send + Sync {
    /// Get the connector specification
    fn spec(&self) -> ConnectorSpec;

    /// Create a boxed transform instance for runtime dispatch
    fn create(&self) -> Box<dyn AnyTransform>;
}

/// Type-erased transform for runtime dispatch
///
/// This allows the runtime to work with transforms without knowing their concrete types.
#[async_trait]
pub trait AnyTransform: Send + Sync {
    /// Transform an event with raw YAML config
    async fn transform_raw(
        &self,
        config: &serde_yaml::Value,
        event: super::event::SourceEvent,
    ) -> Result<super::transform::TransformOutput>;

    /// Initialize the transform with raw YAML config
    async fn init_raw(&self, config: &serde_yaml::Value) -> Result<()>;

    /// Shutdown the transform
    async fn shutdown(&self) -> Result<()>;
}

/// Registry of available transform connectors
pub struct TransformRegistry {
    transforms: HashMap<String, Arc<dyn TransformFactory>>,
}

impl TransformRegistry {
    /// Create an empty transform registry
    pub fn new() -> Self {
        Self {
            transforms: HashMap::new(),
        }
    }

    /// Register a transform factory
    pub fn register(&mut self, name: &str, factory: Arc<dyn TransformFactory>) {
        self.transforms.insert(name.to_string(), factory);
    }

    /// Get a transform factory by name
    pub fn get(&self, name: &str) -> Option<&Arc<dyn TransformFactory>> {
        self.transforms.get(name)
    }

    /// List available transform types with their specs
    pub fn list(&self) -> Vec<(&str, ConnectorSpec)> {
        self.transforms
            .iter()
            .map(|(name, factory)| (name.as_str(), factory.spec()))
            .collect()
    }

    /// Check if a transform is registered
    pub fn contains(&self, name: &str) -> bool {
        self.transforms.contains_key(name)
    }

    /// Number of registered transforms
    pub fn len(&self) -> usize {
        self.transforms.len()
    }

    /// Check if registry is empty
    pub fn is_empty(&self) -> bool {
        self.transforms.is_empty()
    }
}

impl Default for TransformRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper macro to implement AnySource for a typed Source
///
/// This reduces boilerplate when implementing adapters.
#[macro_export]
macro_rules! impl_any_source {
    ($source_type:ty, $config_type:ty) => {
        #[async_trait::async_trait]
        impl $crate::registry::AnySource for $source_type {
            async fn check_raw(
                &self,
                config: &serde_yaml::Value,
            ) -> $crate::error::Result<$crate::source::CheckResult> {
                let typed_config: $config_type = serde_yaml::from_value(config.clone())
                    .map_err(|e| $crate::error::ConnectorError::Config(e.to_string()))?;
                typed_config
                    .validate()
                    .map_err(|e| $crate::error::ConnectorError::Config(e.to_string()))?;
                <Self as $crate::source::Source>::check(self, &typed_config).await
            }

            async fn discover_raw(
                &self,
                config: &serde_yaml::Value,
            ) -> $crate::error::Result<$crate::catalog::Catalog> {
                let typed_config: $config_type = serde_yaml::from_value(config.clone())
                    .map_err(|e| $crate::error::ConnectorError::Config(e.to_string()))?;
                typed_config
                    .validate()
                    .map_err(|e| $crate::error::ConnectorError::Config(e.to_string()))?;
                <Self as $crate::source::Source>::discover(self, &typed_config).await
            }

            async fn read_raw(
                &self,
                config: &serde_yaml::Value,
                catalog: &$crate::catalog::ConfiguredCatalog,
                state: Option<$crate::state::State>,
            ) -> $crate::error::Result<
                futures::stream::BoxStream<
                    'static,
                    $crate::error::Result<$crate::event::SourceEvent>,
                >,
            > {
                let typed_config: $config_type = serde_yaml::from_value(config.clone())
                    .map_err(|e| $crate::error::ConnectorError::Config(e.to_string()))?;
                typed_config
                    .validate()
                    .map_err(|e| $crate::error::ConnectorError::Config(e.to_string()))?;
                <Self as $crate::source::Source>::read(self, &typed_config, catalog, state).await
            }
        }
    };
}

/// Helper macro to implement AnySink for a typed Sink
///
/// This reduces boilerplate when implementing adapters.
#[macro_export]
macro_rules! impl_any_sink {
    ($sink_type:ty, $config_type:ty) => {
        #[async_trait::async_trait]
        impl $crate::AnySink for $sink_type {
            async fn check_raw(
                &self,
                config: &serde_yaml::Value,
            ) -> $crate::error::Result<$crate::CheckResult> {
                let typed_config: $config_type = serde_yaml::from_value(config.clone())
                    .map_err(|e| $crate::error::ConnectorError::Config(e.to_string()))?;
                typed_config
                    .validate()
                    .map_err(|e| $crate::error::ConnectorError::Config(e.to_string()))?;
                <Self as $crate::Sink>::check(self, &typed_config).await
            }

            async fn write_raw(
                &self,
                config: &serde_yaml::Value,
                events: futures::stream::BoxStream<'static, $crate::SourceEvent>,
            ) -> $crate::error::Result<$crate::WriteResult> {
                let typed_config: $config_type = serde_yaml::from_value(config.clone())
                    .map_err(|e| $crate::error::ConnectorError::Config(e.to_string()))?;
                typed_config
                    .validate()
                    .map_err(|e| $crate::error::ConnectorError::Config(e.to_string()))?;
                <Self as $crate::Sink>::write(self, &typed_config, events).await
            }
        }
    };
}

/// Helper macro to implement AnyTransform for a typed Transform
///
/// This reduces boilerplate when implementing transform adapters.
#[macro_export]
macro_rules! impl_any_transform {
    ($transform_type:ty, $config_type:ty) => {
        #[async_trait::async_trait]
        impl $crate::AnyTransform for $transform_type {
            async fn transform_raw(
                &self,
                config: &serde_yaml::Value,
                event: $crate::SourceEvent,
            ) -> $crate::error::Result<$crate::TransformOutput> {
                let typed_config: $config_type = serde_yaml::from_value(config.clone())
                    .map_err(|e| $crate::error::ConnectorError::Config(e.to_string()))?;
                typed_config
                    .validate()
                    .map_err(|e| $crate::error::ConnectorError::Config(e.to_string()))?;
                <Self as $crate::Transform>::transform(self, &typed_config, event).await
            }

            async fn init_raw(&self, config: &serde_yaml::Value) -> $crate::error::Result<()> {
                let typed_config: $config_type = serde_yaml::from_value(config.clone())
                    .map_err(|e| $crate::error::ConnectorError::Config(e.to_string()))?;
                typed_config
                    .validate()
                    .map_err(|e| $crate::error::ConnectorError::Config(e.to_string()))?;
                <Self as $crate::Transform>::init(self, &typed_config).await
            }

            async fn shutdown(&self) -> $crate::error::Result<()> {
                <Self as $crate::Transform>::shutdown(self).await
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_registry() {
        let registry = SourceRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_sink_registry() {
        let registry = SinkRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }
}
