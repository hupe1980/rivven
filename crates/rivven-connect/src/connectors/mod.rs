//! Connector implementations using rivven-connect SDK traits
//!
//! This module provides the registry pattern for runtime connector lookup.
//! Adapter implementations live in their own crates and are composed here.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    rivven-connect (SDK)                         │
//! │  SourceFactory, SinkFactory, AnySource, AnySink, Registry       │
//! └─────────────────────────────────────────────────────────────────┘
//!         ↑ implement
//! ┌─────────────────┬─────────────────┬─────────────────────────────┐
//! │ rivven-cdc      │ rivven-storage  │ rivven-warehouse            │
//! │ (sources)       │ (sink: s3/gcs)  │ (sink: snowflake/bq)        │
//! └─────────────────┴─────────────────┴─────────────────────────────┘
//!         ↑ compose
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     rivven-connect (CLI)                         │
//! │  Composes adapters, runs pipelines, no adapter code              │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

// Built-in sources (bundled, minimal dependencies)
pub mod datagen;
#[cfg(feature = "mysql")]
pub mod mysql_cdc;
pub mod postgres_cdc;

// Built-in sinks (bundled, minimal dependencies)
#[cfg(feature = "http")]
pub mod http;
pub mod stdout;

// Re-export registry types from SDK (now in traits module)
pub use super::traits::registry::{
    AnySink, AnySource, SinkFactory, SinkRegistry, SourceFactory, SourceRegistry,
};

// Re-export SensitiveString for backward compatibility
pub use postgres_cdc::SensitiveString;

use std::sync::Arc;

/// Create a source registry with all enabled sources
pub fn create_source_registry() -> SourceRegistry {
    let mut registry = SourceRegistry::new();

    // Always register datagen (bundled, no dependencies)
    registry.register("datagen", Arc::new(datagen::DatagenSourceFactory));

    // Always register postgres-cdc (bundled)
    #[cfg(feature = "postgres")]
    registry.register(
        "postgres-cdc",
        Arc::new(postgres_cdc::PostgresCdcSourceFactory),
    );

    // MySQL CDC (bundled, feature-gated)
    #[cfg(feature = "mysql")]
    registry.register("mysql-cdc", Arc::new(mysql_cdc::MySqlCdcSourceFactory));

    registry
}

/// Create a sink registry with all enabled sinks
pub fn create_sink_registry() -> SinkRegistry {
    let mut registry = SinkRegistry::new();

    // Always register stdout (bundled, no dependencies)
    registry.register("stdout", Arc::new(stdout::StdoutSinkFactory));

    // HTTP webhook (built-in, feature-gated)
    #[cfg(feature = "http")]
    registry.register("http-webhook", Arc::new(http::HttpWebhookSinkFactory));

    // Note: External sinks (snowflake, s3) are registered via:
    // - rivven_warehouse::register_all(&mut registry)
    // - rivven_storage::register_all(&mut registry)
    // These should be called in the final binary that composes all connectors.

    registry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_registry() {
        let registry = create_source_registry();
        let sources = registry.list();

        #[cfg(feature = "postgres")]
        assert!(sources.iter().any(|(name, _)| *name == "postgres-cdc"));
    }

    #[test]
    fn test_sink_registry() {
        let registry = create_sink_registry();
        let sinks = registry.list();

        // stdout is always available
        assert!(sinks.iter().any(|(name, _)| *name == "stdout"));
    }

    #[test]
    fn test_registry_empty_initially() {
        // Using the SDK registries directly starts empty
        let sources = SourceRegistry::new();
        assert!(sources.is_empty());

        let sinks = SinkRegistry::new();
        assert!(sinks.is_empty());
    }
}
