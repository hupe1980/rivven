//! Connector specification types

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Connector specification describing its capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorSpec {
    /// Unique connector type identifier (e.g., "postgres-cdc", "s3-sink")
    pub connector_type: String,

    /// Semantic version
    pub version: String,

    /// Human-readable description
    pub description: Option<String>,

    /// Author or maintainer
    pub author: Option<String>,

    /// Documentation URL
    pub documentation_url: Option<String>,

    /// License identifier (e.g., "MIT", "Apache-2.0")
    pub license: Option<String>,

    /// JSON Schema for the connector's configuration
    pub config_schema: Option<serde_json::Value>,

    /// Supported sync modes
    pub supported_sync_modes: Vec<SyncModeSpec>,

    /// Whether this connector supports incremental sync with state
    pub supports_incremental: bool,

    /// Whether this connector supports schema discovery
    pub supports_discover: bool,

    /// Custom metadata
    pub metadata: HashMap<String, String>,
}

impl ConnectorSpec {
    /// Create a new connector spec
    pub fn new(connector_type: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            connector_type: connector_type.into(),
            version: version.into(),
            description: None,
            author: None,
            documentation_url: None,
            license: None,
            config_schema: None,
            supported_sync_modes: vec![SyncModeSpec::FullRefresh],
            supports_incremental: false,
            supports_discover: true,
            metadata: HashMap::new(),
        }
    }

    /// Create a builder for fluent construction
    pub fn builder(
        connector_type: impl Into<String>,
        version: impl Into<String>,
    ) -> ConnectorSpecBuilder {
        ConnectorSpecBuilder::new(connector_type, version)
    }

    /// Set description
    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Set author
    pub fn author(mut self, author: impl Into<String>) -> Self {
        self.author = Some(author.into());
        self
    }

    /// Set documentation URL
    pub fn documentation_url(mut self, url: impl Into<String>) -> Self {
        self.documentation_url = Some(url.into());
        self
    }

    /// Set license
    pub fn license(mut self, license: impl Into<String>) -> Self {
        self.license = Some(license.into());
        self
    }

    /// Set config schema from a type implementing JsonSchema
    pub fn config_schema_from<T: JsonSchema>(mut self) -> Self {
        let schema = schemars::schema_for!(T);
        self.config_schema = Some(serde_json::to_value(schema).unwrap_or_default());
        self
    }

    /// Set supported sync modes
    pub fn sync_modes(mut self, modes: Vec<SyncModeSpec>) -> Self {
        self.supported_sync_modes = modes;
        self
    }

    /// Enable incremental sync support
    pub fn incremental(mut self, supported: bool) -> Self {
        self.supports_incremental = supported;
        if supported
            && !self
                .supported_sync_modes
                .contains(&SyncModeSpec::Incremental)
        {
            self.supported_sync_modes.push(SyncModeSpec::Incremental);
        }
        self
    }

    /// Add metadata
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Finalize the spec (no-op, for builder pattern compatibility)
    pub fn build(self) -> Self {
        self
    }
}

/// Builder for ConnectorSpec
#[derive(Debug)]
pub struct ConnectorSpecBuilder {
    spec: ConnectorSpec,
}

impl ConnectorSpecBuilder {
    /// Create a new builder
    pub fn new(connector_type: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            spec: ConnectorSpec::new(connector_type, version),
        }
    }

    /// Set description
    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.spec.description = Some(desc.into());
        self
    }

    /// Set author
    pub fn author(mut self, author: impl Into<String>) -> Self {
        self.spec.author = Some(author.into());
        self
    }

    /// Set documentation URL
    pub fn documentation_url(mut self, url: impl Into<String>) -> Self {
        self.spec.documentation_url = Some(url.into());
        self
    }

    /// Set license
    pub fn license(mut self, license: impl Into<String>) -> Self {
        self.spec.license = Some(license.into());
        self
    }

    /// Set config schema from a type implementing JsonSchema
    pub fn config_schema<T: JsonSchema>(mut self) -> Self {
        let schema = schemars::schema_for!(T);
        self.spec.config_schema = Some(serde_json::to_value(schema).unwrap_or_default());
        self
    }

    /// Set supported sync modes
    pub fn sync_modes(mut self, modes: Vec<SyncModeSpec>) -> Self {
        self.spec.supported_sync_modes = modes;
        self
    }

    /// Enable incremental sync support
    pub fn incremental(mut self, supported: bool) -> Self {
        self.spec.supports_incremental = supported;
        if supported
            && !self
                .spec
                .supported_sync_modes
                .contains(&SyncModeSpec::Incremental)
        {
            self.spec
                .supported_sync_modes
                .push(SyncModeSpec::Incremental);
        }
        self
    }

    /// Add metadata
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.spec.metadata.insert(key.into(), value.into());
        self
    }

    /// Build the spec
    pub fn build(self) -> ConnectorSpec {
        self.spec
    }
}

/// Sync mode specification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncModeSpec {
    /// Full refresh: re-read all data each sync
    FullRefresh,
    /// Incremental: read only new/changed data since last sync
    Incremental,
    /// CDC: real-time change data capture
    Cdc,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_spec_builder() {
        let spec = ConnectorSpec::builder("test-source", "1.0.0")
            .description("A test source")
            .author("Test Author")
            .license("MIT")
            .incremental(true)
            .build();

        assert_eq!(spec.connector_type, "test-source");
        assert_eq!(spec.version, "1.0.0");
        assert_eq!(spec.description, Some("A test source".to_string()));
        assert!(spec.supports_incremental);
    }
}
