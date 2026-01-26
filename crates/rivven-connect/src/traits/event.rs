//! Source event types

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// An event produced by a source connector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceEvent {
    /// Event type
    pub event_type: SourceEventType,

    /// Stream name this event belongs to
    pub stream: String,

    /// Namespace (e.g., schema, database)
    pub namespace: Option<String>,

    /// Event timestamp (source system time)
    pub timestamp: DateTime<Utc>,

    /// Event data (depends on event type)
    pub data: serde_json::Value,

    /// Additional metadata
    #[serde(default)]
    pub metadata: EventMetadata,
}

impl SourceEvent {
    /// Create a builder for constructing events
    pub fn builder() -> SourceEventBuilder {
        SourceEventBuilder::default()
    }

    /// Create a record event
    pub fn record(stream: impl Into<String>, data: serde_json::Value) -> Self {
        Self::builder()
            .stream(stream)
            .event_type(SourceEventType::Record)
            .data(data)
            .build()
    }

    /// Create an insert event (CDC)
    pub fn insert(stream: impl Into<String>, data: serde_json::Value) -> Self {
        Self::builder()
            .stream(stream)
            .event_type(SourceEventType::Insert)
            .data(data)
            .build()
    }

    /// Create an update event (CDC)
    pub fn update(
        stream: impl Into<String>,
        before: Option<serde_json::Value>,
        after: serde_json::Value,
    ) -> Self {
        let data = serde_json::json!({
            "before": before,
            "after": after,
        });
        Self::builder()
            .stream(stream)
            .event_type(SourceEventType::Update)
            .data(data)
            .build()
    }

    /// Create a delete event (CDC)
    pub fn delete(stream: impl Into<String>, before: serde_json::Value) -> Self {
        Self::builder()
            .stream(stream)
            .event_type(SourceEventType::Delete)
            .data(before)
            .build()
    }

    /// Create a state event
    pub fn state(data: serde_json::Value) -> Self {
        Self::builder()
            .stream("_state")
            .event_type(SourceEventType::State)
            .data(data)
            .build()
    }

    /// Create a log event
    pub fn log(level: LogLevel, message: impl Into<String>) -> Self {
        Self::builder()
            .stream("_log")
            .event_type(SourceEventType::Log)
            .data(serde_json::json!({
                "level": level,
                "message": message.into(),
            }))
            .build()
    }

    /// Set namespace (builder-style)
    pub fn with_namespace(mut self, ns: impl Into<String>) -> Self {
        self.namespace = Some(ns.into());
        self
    }

    /// Set timestamp (builder-style)
    pub fn with_timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = ts;
        self
    }

    /// Add metadata (builder-style)
    pub fn with_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.extra.insert(key.into(), value);
        self
    }

    /// Set source position for checkpointing (builder-style)
    pub fn with_position(mut self, position: impl Into<String>) -> Self {
        self.metadata.position = Some(position.into());
        self
    }

    /// Set transaction ID (builder-style)
    pub fn with_transaction(mut self, tx_id: impl Into<String>) -> Self {
        self.metadata.transaction_id = Some(tx_id.into());
        self
    }

    // Legacy aliases for backward compatibility
    #[doc(hidden)]
    pub fn namespace(self, ns: impl Into<String>) -> Self {
        self.with_namespace(ns)
    }

    #[doc(hidden)]
    pub fn timestamp(self, ts: DateTime<Utc>) -> Self {
        self.with_timestamp(ts)
    }

    #[doc(hidden)]
    pub fn position(self, position: impl Into<String>) -> Self {
        self.with_position(position)
    }

    /// Check if this is a data event (record, insert, update, delete)
    pub fn is_data(&self) -> bool {
        matches!(
            self.event_type,
            SourceEventType::Record
                | SourceEventType::Insert
                | SourceEventType::Update
                | SourceEventType::Delete
        )
    }

    /// Check if this is a CDC event
    pub fn is_cdc(&self) -> bool {
        matches!(
            self.event_type,
            SourceEventType::Insert | SourceEventType::Update | SourceEventType::Delete
        )
    }

    /// Check if this is a control event (state, log, schema)
    pub fn is_control(&self) -> bool {
        matches!(
            self.event_type,
            SourceEventType::State | SourceEventType::Log | SourceEventType::Schema
        )
    }

    /// Get the "after" value for CDC events (insert/update data, None for delete)
    pub fn after(&self) -> Option<&serde_json::Value> {
        match self.event_type {
            SourceEventType::Insert | SourceEventType::Record => Some(&self.data),
            SourceEventType::Update => self.data.get("after"),
            SourceEventType::Delete => None,
            _ => None,
        }
    }

    /// Get the "before" value for CDC events (update/delete old data)
    pub fn before(&self) -> Option<&serde_json::Value> {
        match self.event_type {
            SourceEventType::Update => {
                self.data
                    .get("before")
                    .and_then(|v| if v.is_null() { None } else { Some(v) })
            }
            SourceEventType::Delete => Some(&self.data),
            _ => None,
        }
    }

    /// Convert to CloudEvent format
    pub fn to_cloud_event(&self, source: &str) -> serde_json::Value {
        serde_json::json!({
            "specversion": "1.0",
            "type": format!("rivven.{}.{}", self.stream, self.event_type.as_str()),
            "source": source,
            "id": uuid::Uuid::new_v4().to_string(),
            "time": self.timestamp.to_rfc3339(),
            "datacontenttype": "application/json",
            "data": self.data,
            "rivvenstream": self.stream,
            "rivvennamespace": self.namespace,
            "rivvenop": self.event_type.as_str(),
        })
    }
}

/// Builder for constructing SourceEvent instances
#[derive(Debug, Default)]
pub struct SourceEventBuilder {
    event_type: Option<SourceEventType>,
    stream: Option<String>,
    namespace: Option<String>,
    timestamp: Option<DateTime<Utc>>,
    data: Option<serde_json::Value>,
    metadata: EventMetadata,
}

impl SourceEventBuilder {
    /// Set the event type
    pub fn event_type(mut self, event_type: SourceEventType) -> Self {
        self.event_type = Some(event_type);
        self
    }

    /// Set the stream name
    pub fn stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }

    /// Set the namespace
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Set the timestamp
    pub fn timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Set the event data
    pub fn data(mut self, data: serde_json::Value) -> Self {
        self.data = Some(data);
        self
    }

    /// Set the position for checkpointing
    pub fn position(mut self, position: impl Into<String>) -> Self {
        self.metadata.position = Some(position.into());
        self
    }

    /// Set the transaction ID
    pub fn transaction_id(mut self, tx_id: impl Into<String>) -> Self {
        self.metadata.transaction_id = Some(tx_id.into());
        self
    }

    /// Set the sequence number
    pub fn sequence(mut self, seq: u64) -> Self {
        self.metadata.sequence = Some(seq);
        self
    }

    /// Add extra metadata
    pub fn meta(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.extra.insert(key.into(), value);
        self
    }

    /// Build the SourceEvent
    pub fn build(self) -> SourceEvent {
        SourceEvent {
            event_type: self.event_type.unwrap_or(SourceEventType::Record),
            stream: self.stream.unwrap_or_else(|| "default".to_string()),
            namespace: self.namespace,
            timestamp: self.timestamp.unwrap_or_else(Utc::now),
            data: self.data.unwrap_or(serde_json::Value::Null),
            metadata: self.metadata,
        }
    }
}

/// Type of source event
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceEventType {
    /// A data record (non-CDC sources)
    Record,
    /// An insert operation (CDC)
    Insert,
    /// An update operation (CDC)
    Update,
    /// A delete operation (CDC)
    Delete,
    /// State/checkpoint information
    State,
    /// Log message
    Log,
    /// Schema change
    Schema,
}

impl SourceEventType {
    /// Get string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Record => "record",
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::State => "state",
            Self::Log => "log",
            Self::Schema => "schema",
        }
    }

    /// Check if this is a data operation
    pub fn is_data(&self) -> bool {
        matches!(
            self,
            Self::Record | Self::Insert | Self::Update | Self::Delete
        )
    }

    /// Check if this is a CDC operation
    pub fn is_cdc(&self) -> bool {
        matches!(self, Self::Insert | Self::Update | Self::Delete)
    }
}

impl std::fmt::Display for SourceEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Log level for log events
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    /// Trace level
    Trace,
    /// Debug level
    Debug,
    /// Info level
    Info,
    /// Warning level
    Warn,
    /// Error level
    Error,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Trace => write!(f, "trace"),
            Self::Debug => write!(f, "debug"),
            Self::Info => write!(f, "info"),
            Self::Warn => write!(f, "warn"),
            Self::Error => write!(f, "error"),
        }
    }
}

/// Event metadata
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventMetadata {
    /// Source position for checkpointing (LSN, offset, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<String>,

    /// Transaction ID (for CDC)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,

    /// Sequence number within transaction
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,

    /// Additional metadata
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl EventMetadata {
    /// Create empty metadata
    pub fn new() -> Self {
        Self::default()
    }

    /// Create metadata with position
    pub fn with_position(position: impl Into<String>) -> Self {
        Self {
            position: Some(position.into()),
            ..Default::default()
        }
    }

    /// Check if metadata has any values
    pub fn is_empty(&self) -> bool {
        self.position.is_none()
            && self.transaction_id.is_none()
            && self.sequence.is_none()
            && self.extra.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_record_event() {
        let event = SourceEvent::record("users", json!({"id": 1, "name": "Alice"}));
        assert_eq!(event.event_type, SourceEventType::Record);
        assert_eq!(event.stream, "users");
        assert!(event.is_data());
        assert!(!event.is_cdc());
    }

    #[test]
    fn test_cdc_events() {
        let insert = SourceEvent::insert("users", json!({"id": 1}));
        assert_eq!(insert.event_type, SourceEventType::Insert);
        assert!(insert.is_cdc());
        assert_eq!(insert.after(), Some(&json!({"id": 1})));
        assert_eq!(insert.before(), None);

        let update = SourceEvent::update(
            "users",
            Some(json!({"id": 1, "name": "Alice"})),
            json!({"id": 1, "name": "Bob"}),
        );
        assert_eq!(update.event_type, SourceEventType::Update);
        assert!(update.is_cdc());
        assert_eq!(update.after(), Some(&json!({"id": 1, "name": "Bob"})));
        assert_eq!(update.before(), Some(&json!({"id": 1, "name": "Alice"})));

        let delete = SourceEvent::delete("users", json!({"id": 1}));
        assert_eq!(delete.event_type, SourceEventType::Delete);
        assert!(delete.is_cdc());
        assert_eq!(delete.after(), None);
        assert_eq!(delete.before(), Some(&json!({"id": 1})));
    }

    #[test]
    fn test_builder_pattern() {
        let event = SourceEvent::builder()
            .stream("orders")
            .namespace("public")
            .event_type(SourceEventType::Insert)
            .data(json!({"order_id": 123}))
            .position("lsn:12345")
            .transaction_id("tx-001")
            .sequence(1)
            .meta("source", json!("postgres"))
            .build();

        assert_eq!(event.stream, "orders");
        assert_eq!(event.namespace, Some("public".to_string()));
        assert_eq!(event.event_type, SourceEventType::Insert);
        assert_eq!(event.metadata.position, Some("lsn:12345".to_string()));
        assert_eq!(event.metadata.transaction_id, Some("tx-001".to_string()));
        assert_eq!(event.metadata.sequence, Some(1));
    }

    #[test]
    fn test_cloud_event_conversion() {
        let event = SourceEvent::insert("users", json!({"id": 1}));
        let ce = event.to_cloud_event("postgres://localhost/mydb");

        assert_eq!(ce["specversion"], "1.0");
        assert!(ce["type"].as_str().unwrap().contains("users"));
    }

    #[test]
    fn test_fluent_api() {
        let event = SourceEvent::insert("users", json!({"id": 1}))
            .with_namespace("public")
            .with_position("lsn:999")
            .with_transaction("tx-123")
            .with_metadata("custom", json!("value"));

        assert_eq!(event.namespace, Some("public".to_string()));
        assert_eq!(event.metadata.position, Some("lsn:999".to_string()));
        assert_eq!(event.metadata.transaction_id, Some("tx-123".to_string()));
        assert_eq!(event.metadata.extra.get("custom"), Some(&json!("value")));
    }
}
