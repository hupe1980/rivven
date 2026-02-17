//! Connector Inventory System for scalable connector management
//!
//! This module provides automatic connector discovery and registration
//! designed to scale to 300+ connectors (like Redpanda Connect).
//!
//! # Key Features
//!
//! - **Auto-registration**: Connectors register themselves via inventory
//! - **Lazy loading**: Connectors are only instantiated when needed
//! - **Search & filter**: Rich metadata enables powerful discovery
//! - **Documentation**: Auto-generates connector docs
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                   Connector Inventory                            │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
//! │  │ Source       │  │ Sink         │  │ Processor    │          │
//! │  │ Inventory    │  │ Inventory    │  │ Inventory    │          │
//! │  │ (HashMap)    │  │ (HashMap)    │  │ (HashMap)    │          │
//! │  └──────────────┘  └──────────────┘  └──────────────┘          │
//! │         │                 │                 │                   │
//! │         ▼                 ▼                 ▼                   │
//! │  ┌─────────────────────────────────────────────────────────┐   │
//! │  │              Metadata Index (for search)                 │   │
//! │  │  • By category  • By tags                               │   │
//! │  └─────────────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use super::metadata::{
    ConnectorCategory, ConnectorMetadata, ConnectorMetadataBuilder, ConnectorType,
};
use super::registry::{SinkFactory, SourceFactory, TransformFactory};
use super::spec::ConnectorSpec;
use std::collections::HashMap;
use std::sync::Arc;

/// Entry in the connector inventory
pub struct SourceEntry {
    /// Connector metadata for discovery
    pub metadata: ConnectorMetadata,
    /// Factory for creating instances
    pub factory: Arc<dyn SourceFactory>,
}

/// Entry in the connector inventory
pub struct SinkEntry {
    /// Connector metadata for discovery
    pub metadata: ConnectorMetadata,
    /// Factory for creating instances
    pub factory: Arc<dyn SinkFactory>,
}

/// Entry in the connector inventory for transforms
pub struct TransformEntry {
    /// Connector metadata for discovery
    pub metadata: ConnectorMetadata,
    /// Factory for creating instances
    pub factory: Arc<dyn TransformFactory>,
}

/// Unified connector inventory for sources, sinks, and transforms
///
/// Designed to scale to 300+ connectors with efficient lookup and search.
#[derive(Default)]
pub struct ConnectorInventory {
    /// Source connectors
    sources: HashMap<String, SourceEntry>,
    /// Sink connectors
    sinks: HashMap<String, SinkEntry>,
    /// Transform connectors
    transforms: HashMap<String, TransformEntry>,
    /// Metadata index for fast category lookup
    by_category: HashMap<ConnectorCategory, Vec<String>>,
    /// Metadata index for fast tag lookup
    by_tag: HashMap<String, Vec<String>>,
}

impl ConnectorInventory {
    /// Create a new empty inventory
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
            sinks: HashMap::new(),
            transforms: HashMap::new(),
            by_category: HashMap::new(),
            by_tag: HashMap::new(),
        }
    }

    /// Register a source connector
    pub fn register_source(
        &mut self,
        metadata: ConnectorMetadata,
        factory: Arc<dyn SourceFactory>,
    ) {
        let name = metadata.name.clone();

        // Index by category
        self.by_category
            .entry(metadata.category)
            .or_default()
            .push(name.clone());

        // Index by tags
        for tag in &metadata.tags {
            self.by_tag
                .entry(tag.clone())
                .or_default()
                .push(name.clone());
        }

        self.sources.insert(name, SourceEntry { metadata, factory });
    }

    /// Register a sink connector
    pub fn register_sink(&mut self, metadata: ConnectorMetadata, factory: Arc<dyn SinkFactory>) {
        let name = metadata.name.clone();

        // Index by category
        self.by_category
            .entry(metadata.category)
            .or_default()
            .push(name.clone());

        // Index by tags
        for tag in &metadata.tags {
            self.by_tag
                .entry(tag.clone())
                .or_default()
                .push(name.clone());
        }

        self.sinks.insert(name, SinkEntry { metadata, factory });
    }

    /// Register a transform connector
    pub fn register_transform(
        &mut self,
        metadata: ConnectorMetadata,
        factory: Arc<dyn TransformFactory>,
    ) {
        let name = metadata.name.clone();

        // Index by category
        self.by_category
            .entry(metadata.category)
            .or_default()
            .push(name.clone());

        // Index by tags
        for tag in &metadata.tags {
            self.by_tag
                .entry(tag.clone())
                .or_default()
                .push(name.clone());
        }

        self.transforms
            .insert(name, TransformEntry { metadata, factory });
    }

    /// Get a source factory by name
    pub fn get_source(&self, name: &str) -> Option<&Arc<dyn SourceFactory>> {
        self.sources.get(name).map(|e| &e.factory)
    }

    /// Get a sink factory by name
    pub fn get_sink(&self, name: &str) -> Option<&Arc<dyn SinkFactory>> {
        self.sinks.get(name).map(|e| &e.factory)
    }

    /// Get a transform factory by name
    pub fn get_transform(&self, name: &str) -> Option<&Arc<dyn TransformFactory>> {
        self.transforms.get(name).map(|e| &e.factory)
    }

    /// Get source metadata by name
    pub fn get_source_metadata(&self, name: &str) -> Option<&ConnectorMetadata> {
        self.sources.get(name).map(|e| &e.metadata)
    }

    /// Get sink metadata by name
    pub fn get_sink_metadata(&self, name: &str) -> Option<&ConnectorMetadata> {
        self.sinks.get(name).map(|e| &e.metadata)
    }

    /// Get transform metadata by name
    pub fn get_transform_metadata(&self, name: &str) -> Option<&ConnectorMetadata> {
        self.transforms.get(name).map(|e| &e.metadata)
    }

    /// List all sources with their specs
    pub fn list_sources(&self) -> Vec<(&str, ConnectorSpec)> {
        self.sources
            .iter()
            .map(|(name, entry)| (name.as_str(), entry.factory.spec()))
            .collect()
    }

    /// List all sinks with their specs
    pub fn list_sinks(&self) -> Vec<(&str, ConnectorSpec)> {
        self.sinks
            .iter()
            .map(|(name, entry)| (name.as_str(), entry.factory.spec()))
            .collect()
    }

    /// List all transforms with their specs
    pub fn list_transforms(&self) -> Vec<(&str, ConnectorSpec)> {
        self.transforms
            .iter()
            .map(|(name, entry)| (name.as_str(), entry.factory.spec()))
            .collect()
    }

    /// List all source metadata
    pub fn list_source_metadata(&self) -> Vec<&ConnectorMetadata> {
        self.sources.values().map(|e| &e.metadata).collect()
    }

    /// List all sink metadata
    pub fn list_sink_metadata(&self) -> Vec<&ConnectorMetadata> {
        self.sinks.values().map(|e| &e.metadata).collect()
    }

    /// List all transform metadata
    pub fn list_transform_metadata(&self) -> Vec<&ConnectorMetadata> {
        self.transforms.values().map(|e| &e.metadata).collect()
    }

    /// Search sources by query string
    pub fn search_sources(&self, query: &str) -> Vec<&ConnectorMetadata> {
        self.sources
            .values()
            .filter(|e| e.metadata.matches_search(query))
            .map(|e| &e.metadata)
            .collect()
    }

    /// Search sinks by query string
    pub fn search_sinks(&self, query: &str) -> Vec<&ConnectorMetadata> {
        self.sinks
            .values()
            .filter(|e| e.metadata.matches_search(query))
            .map(|e| &e.metadata)
            .collect()
    }

    /// Search all connectors by query string
    pub fn search(&self, query: &str) -> Vec<&ConnectorMetadata> {
        let mut results: Vec<_> = self
            .sources
            .values()
            .filter(|e| e.metadata.matches_search(query))
            .map(|e| &e.metadata)
            .chain(
                self.sinks
                    .values()
                    .filter(|e| e.metadata.matches_search(query))
                    .map(|e| &e.metadata),
            )
            .chain(
                self.transforms
                    .values()
                    .filter(|e| e.metadata.matches_search(query))
                    .map(|e| &e.metadata),
            )
            .collect();

        // Deduplicate by name (some connectors are both source and sink)
        results.sort_by_key(|m| &m.name);
        results.dedup_by_key(|m| &m.name);
        results
    }

    /// Filter connectors by criteria
    pub fn filter(
        &self,
        category: Option<ConnectorCategory>,
        connector_type: Option<ConnectorType>,
    ) -> Vec<&ConnectorMetadata> {
        let mut results: Vec<_> = self
            .sources
            .values()
            .filter(|e| e.metadata.matches_filters(category, connector_type))
            .map(|e| &e.metadata)
            .chain(
                self.sinks
                    .values()
                    .filter(|e| e.metadata.matches_filters(category, connector_type))
                    .map(|e| &e.metadata),
            )
            .chain(
                self.transforms
                    .values()
                    .filter(|e| e.metadata.matches_filters(category, connector_type))
                    .map(|e| &e.metadata),
            )
            .collect();

        // Deduplicate by name
        results.sort_by_key(|m| &m.name);
        results.dedup_by_key(|m| &m.name);
        results
    }

    /// Get connectors by category (fast lookup via index)
    pub fn by_category(&self, category: ConnectorCategory) -> Vec<&ConnectorMetadata> {
        self.by_category
            .get(&category)
            .map(|names| {
                names
                    .iter()
                    .filter_map(|name| {
                        self.sources
                            .get(name)
                            .map(|e| &e.metadata)
                            .or_else(|| self.sinks.get(name).map(|e| &e.metadata))
                            .or_else(|| self.transforms.get(name).map(|e| &e.metadata))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get connectors by tag (fast lookup via index)
    pub fn by_tag(&self, tag: &str) -> Vec<&ConnectorMetadata> {
        self.by_tag
            .get(tag)
            .map(|names| {
                names
                    .iter()
                    .filter_map(|name| {
                        self.sources
                            .get(name)
                            .map(|e| &e.metadata)
                            .or_else(|| self.sinks.get(name).map(|e| &e.metadata))
                            .or_else(|| self.transforms.get(name).map(|e| &e.metadata))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get all categories with connector counts
    pub fn category_stats(&self) -> HashMap<ConnectorCategory, usize> {
        self.by_category
            .iter()
            .map(|(cat, names)| (*cat, names.len()))
            .collect()
    }

    /// Get all tags with connector counts
    pub fn tag_stats(&self) -> HashMap<String, usize> {
        self.by_tag
            .iter()
            .map(|(tag, names)| (tag.clone(), names.len()))
            .collect()
    }

    /// Total number of unique connectors
    pub fn total_count(&self) -> usize {
        // Collect unique names
        let mut names: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for name in self.sources.keys() {
            names.insert(name);
        }
        for name in self.sinks.keys() {
            names.insert(name);
        }
        for name in self.transforms.keys() {
            names.insert(name);
        }
        names.len()
    }

    /// Number of sources
    pub fn source_count(&self) -> usize {
        self.sources.len()
    }

    /// Number of sinks
    pub fn sink_count(&self) -> usize {
        self.sinks.len()
    }

    /// Number of transforms
    pub fn transform_count(&self) -> usize {
        self.transforms.len()
    }

    /// Generate markdown documentation for all connectors
    pub fn generate_docs(&self) -> String {
        let mut doc = String::new();
        doc.push_str("# Connector Catalog\n\n");

        // Summary
        doc.push_str(&format!(
            "**Total Connectors**: {} ({} sources, {} sinks, {} transforms)\n\n",
            self.total_count(),
            self.source_count(),
            self.sink_count(),
            self.transform_count()
        ));

        // Group by parent category
        let mut by_parent: HashMap<&str, Vec<&ConnectorMetadata>> = HashMap::new();
        for meta in self.list_source_metadata() {
            let parent = meta.category.parent().unwrap_or("other");
            by_parent.entry(parent).or_default().push(meta);
        }
        for meta in self.list_sink_metadata() {
            let parent = meta.category.parent().unwrap_or("other");
            by_parent.entry(parent).or_default().push(meta);
        }
        for meta in self.list_transform_metadata() {
            let parent = meta.category.parent().unwrap_or("other");
            by_parent.entry(parent).or_default().push(meta);
        }

        // Sort categories
        let mut parents: Vec<_> = by_parent.keys().copied().collect();
        parents.sort();

        for parent in parents {
            let connectors = by_parent.get(parent).unwrap();
            doc.push_str(&format!("## {}\n\n", parent.to_uppercase()));
            doc.push_str("| Name | Type | Description |\n");
            doc.push_str("|------|------|-------------|\n");

            let mut sorted: Vec<_> = connectors.iter().collect();
            sorted.sort_by_key(|m| &m.name);
            sorted.dedup_by_key(|m| &m.name);

            for meta in sorted {
                let types: Vec<_> = meta
                    .types
                    .iter()
                    .map(|t| match t {
                        ConnectorType::Source => "Source",
                        ConnectorType::Sink => "Sink",
                        ConnectorType::Processor => "Processor",
                        ConnectorType::Cache => "Cache",
                        ConnectorType::Scanner => "Scanner",
                    })
                    .collect();
                let types_str = types.join(", ");

                doc.push_str(&format!(
                    "| `{}` | {} | {} |\n",
                    meta.name, types_str, meta.description
                ));
            }

            doc.push('\n');
        }

        doc
    }
}

/// Trait for connectors to provide their metadata
///
/// Implement this trait to enable auto-registration in the inventory.
pub trait HasMetadata {
    /// Get connector metadata for the inventory
    fn metadata() -> ConnectorMetadata;

    /// Convenience builder for metadata
    fn metadata_builder(name: &str) -> ConnectorMetadataBuilder {
        ConnectorMetadata::builder(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::registry::SourceFactory;

    // Mock factory for testing
    struct MockSourceFactory;

    impl SourceFactory for MockSourceFactory {
        fn spec(&self) -> ConnectorSpec {
            ConnectorSpec::builder("mock", "1.0.0")
                .description("Mock connector")
                .build()
        }

        fn create(&self) -> crate::error::Result<Box<dyn crate::traits::registry::AnySource>> {
            unimplemented!("Mock only")
        }
    }

    #[test]
    fn test_inventory_registration() {
        let mut inventory = ConnectorInventory::new();

        let metadata = ConnectorMetadata::builder("mock_source")
            .title("Mock Source")
            .description("A mock source for testing")
            .source()
            .category(ConnectorCategory::UtilityDebug)
            .tags(["mock", "test", "debug"])
            .build();

        inventory.register_source(metadata, Arc::new(MockSourceFactory));

        assert_eq!(inventory.source_count(), 1);
        assert!(inventory.get_source("mock_source").is_some());
    }

    #[test]
    fn test_inventory_search() {
        let mut inventory = ConnectorInventory::new();

        let meta1 = ConnectorMetadata::builder("postgres_cdc")
            .description("PostgreSQL CDC")
            .source()
            .category(ConnectorCategory::DatabaseCdc)
            .tags(["postgres", "cdc", "database"])
            .build();

        let meta2 = ConnectorMetadata::builder("mysql_cdc")
            .description("MySQL CDC")
            .source()
            .category(ConnectorCategory::DatabaseCdc)
            .tags(["mysql", "cdc", "database"])
            .build();

        inventory.register_source(meta1, Arc::new(MockSourceFactory));
        inventory.register_source(meta2, Arc::new(MockSourceFactory));

        // Search by name
        let results = inventory.search("postgres");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "postgres_cdc");

        // Search by tag
        let results = inventory.search("cdc");
        assert_eq!(results.len(), 2);

        // Search by description
        let results = inventory.search("mysql");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_inventory_filter() {
        let mut inventory = ConnectorInventory::new();

        let meta1 = ConnectorMetadata::builder("postgres_cdc")
            .source()
            .category(ConnectorCategory::DatabaseCdc)
            .build();

        let meta2 = ConnectorMetadata::builder("mysql_cdc")
            .source()
            .category(ConnectorCategory::DatabaseCdc)
            .build();

        inventory.register_source(meta1, Arc::new(MockSourceFactory));
        inventory.register_source(meta2, Arc::new(MockSourceFactory));

        // Filter by category
        let results = inventory.filter(Some(ConnectorCategory::DatabaseCdc), None);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_category_index() {
        let mut inventory = ConnectorInventory::new();

        let meta = ConnectorMetadata::builder("kafka")
            .source()
            .sink()
            .category(ConnectorCategory::MessagingKafka)
            .build();

        inventory.register_source(meta.clone(), Arc::new(MockSourceFactory));

        let results = inventory.by_category(ConnectorCategory::MessagingKafka);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "kafka");
    }

    #[test]
    fn test_docs_generation() {
        let mut inventory = ConnectorInventory::new();

        let meta = ConnectorMetadata::builder("datagen")
            .title("Data Generator")
            .description("Generate synthetic test data")
            .source()
            .category(ConnectorCategory::UtilityGenerate)
            .build();

        inventory.register_source(meta, Arc::new(MockSourceFactory));

        let docs = inventory.generate_docs();
        assert!(docs.contains("Connector Catalog"));
        assert!(docs.contains("datagen"));
    }
}
