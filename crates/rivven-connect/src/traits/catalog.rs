//! Catalog types for describing available streams

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Catalog of available streams from a source
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Catalog {
    /// Available streams
    pub streams: Vec<Stream>,
}

impl Catalog {
    /// Create an empty catalog
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a catalog with a single stream
    pub fn single_stream(name: impl Into<String>, schema: serde_json::Value) -> Self {
        Self {
            streams: vec![Stream::new(name, schema)],
        }
    }

    /// Add a stream to the catalog
    pub fn add_stream(mut self, stream: Stream) -> Self {
        self.streams.push(stream);
        self
    }

    /// Find a stream by name
    pub fn find_stream(&self, name: &str) -> Option<&Stream> {
        self.streams.iter().find(|s| s.name == name)
    }
}

/// A stream represents a collection of records (e.g., a table, API endpoint)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stream {
    /// Unique name of the stream (e.g., "public.users")
    pub name: String,

    /// Namespace (e.g., schema name, database name)
    pub namespace: Option<String>,

    /// JSON Schema of the stream's records
    pub json_schema: serde_json::Value,

    /// Supported sync modes for this stream
    pub supported_sync_modes: Vec<SyncMode>,

    /// Default cursor field for incremental sync
    pub default_cursor_field: Option<Vec<String>>,

    /// Primary key fields
    pub source_defined_primary_key: Option<Vec<Vec<String>>>,

    /// Additional metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

impl Stream {
    /// Create a new stream
    pub fn new(name: impl Into<String>, json_schema: serde_json::Value) -> Self {
        Self {
            name: name.into(),
            namespace: None,
            json_schema,
            supported_sync_modes: vec![SyncMode::FullRefresh],
            default_cursor_field: None,
            source_defined_primary_key: None,
            metadata: HashMap::new(),
        }
    }

    /// Set namespace
    pub fn namespace(mut self, ns: impl Into<String>) -> Self {
        self.namespace = Some(ns.into());
        self
    }

    /// Set supported sync modes
    pub fn sync_modes(mut self, modes: Vec<SyncMode>) -> Self {
        self.supported_sync_modes = modes;
        self
    }

    /// Set supported sync modes (alias)
    pub fn with_sync_modes(self, modes: Vec<SyncMode>) -> Self {
        self.sync_modes(modes)
    }

    /// Set default cursor field
    pub fn cursor_field(mut self, field: Vec<String>) -> Self {
        self.default_cursor_field = Some(field);
        self
    }

    /// Set primary key
    pub fn primary_key(mut self, key: Vec<Vec<String>>) -> Self {
        self.source_defined_primary_key = Some(key);
        self
    }

    /// Set primary key (alias)
    pub fn with_primary_key(self, key: Vec<Vec<String>>) -> Self {
        self.primary_key(key)
    }

    /// Add metadata
    pub fn metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Get fully qualified name
    pub fn full_name(&self) -> String {
        match &self.namespace {
            Some(ns) => format!("{}.{}", ns, self.name),
            None => self.name.clone(),
        }
    }
}

/// Sync mode for a stream
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    /// Full refresh: re-read all data each sync
    #[default]
    FullRefresh,
    /// Incremental: read only new/changed data
    Incremental,
}

/// Configured catalog (user's selection of streams and sync modes)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConfiguredCatalog {
    /// Configured streams
    pub streams: Vec<ConfiguredStream>,
}

impl ConfiguredCatalog {
    /// Create an empty configured catalog
    pub fn new() -> Self {
        Self::default()
    }

    /// Create from a catalog, selecting all streams with default settings
    pub fn from_catalog(catalog: &Catalog) -> Self {
        Self {
            streams: catalog
                .streams
                .iter()
                .map(ConfiguredStream::from_stream)
                .collect(),
        }
    }

    /// Add a configured stream
    pub fn add_stream(mut self, stream: ConfiguredStream) -> Self {
        self.streams.push(stream);
        self
    }

    /// Find a configured stream by name
    pub fn find_stream(&self, name: &str) -> Option<&ConfiguredStream> {
        self.streams.iter().find(|s| s.stream.name == name)
    }
}

/// A stream configured by the user
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfiguredStream {
    /// The stream definition
    pub stream: Stream,

    /// Selected sync mode
    pub sync_mode: SyncMode,

    /// Cursor field for incremental sync
    pub cursor_field: Option<Vec<String>>,

    /// Destination sync mode
    pub destination_sync_mode: DestinationSyncMode,

    /// Primary key override
    pub primary_key: Option<Vec<Vec<String>>>,
}

impl ConfiguredStream {
    /// Create from a stream with default settings
    pub fn from_stream(stream: &Stream) -> Self {
        let sync_mode = stream
            .supported_sync_modes
            .first()
            .copied()
            .unwrap_or(SyncMode::FullRefresh);

        Self {
            stream: stream.clone(),
            sync_mode,
            cursor_field: stream.default_cursor_field.clone(),
            destination_sync_mode: DestinationSyncMode::Append,
            primary_key: stream.source_defined_primary_key.clone(),
        }
    }

    /// Set sync mode
    pub fn sync_mode(mut self, mode: SyncMode) -> Self {
        self.sync_mode = mode;
        self
    }

    /// Set destination sync mode
    pub fn destination_sync_mode(mut self, mode: DestinationSyncMode) -> Self {
        self.destination_sync_mode = mode;
        self
    }
}

/// Destination sync mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DestinationSyncMode {
    /// Append new records
    Append,
    /// Overwrite existing data
    Overwrite,
    /// Append with deduplication
    AppendDedup,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_stream_full_name() {
        let stream = Stream::new("users", json!({})).namespace("public");
        assert_eq!(stream.full_name(), "public.users");

        let stream_no_ns = Stream::new("events", json!({}));
        assert_eq!(stream_no_ns.full_name(), "events");
    }

    #[test]
    fn test_catalog_operations() {
        let catalog = Catalog::new()
            .add_stream(Stream::new("users", json!({})).namespace("public"))
            .add_stream(Stream::new("orders", json!({})).namespace("public"));

        assert_eq!(catalog.streams.len(), 2);
        assert!(catalog.find_stream("users").is_some());
        assert!(catalog.find_stream("nonexistent").is_none());
    }
}
