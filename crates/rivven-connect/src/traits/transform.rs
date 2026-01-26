//! Transform connector trait
//!
//! Transforms are composable event processors that can filter, modify, route,
//! or fan-out events in a pipeline.

use super::event::SourceEvent;
use super::spec::ConnectorSpec;
use crate::error::Result;
use async_trait::async_trait;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use validator::Validate;

/// Trait for transform configuration
pub trait TransformConfig: DeserializeOwned + Validate + JsonSchema + Send + Sync {}

// Blanket implementation
impl<T> TransformConfig for T where T: DeserializeOwned + Validate + JsonSchema + Send + Sync {}

/// Result of a transform operation
#[derive(Debug)]
pub enum TransformOutput {
    /// Single event output (boxed to reduce enum size)
    Single(Box<SourceEvent>),
    /// Multiple events output (fan-out)
    Multiple(Vec<SourceEvent>),
    /// Event was filtered out
    Filtered,
}

impl TransformOutput {
    /// Create single event output
    pub fn single(event: SourceEvent) -> Self {
        Self::Single(Box::new(event))
    }

    /// Create multiple events output
    pub fn multiple(events: Vec<SourceEvent>) -> Self {
        Self::Multiple(events)
    }

    /// Create filtered output (drop event)
    pub fn filtered() -> Self {
        Self::Filtered
    }

    /// Convert to vec of events
    pub fn into_events(self) -> Vec<SourceEvent> {
        match self {
            Self::Single(e) => vec![*e],
            Self::Multiple(events) => events,
            Self::Filtered => vec![],
        }
    }

    /// Check if output is filtered
    pub fn is_filtered(&self) -> bool {
        matches!(self, Self::Filtered)
    }

    /// Check if output is a single event
    pub fn is_single(&self) -> bool {
        matches!(self, Self::Single(_))
    }

    /// Check if output is multiple events
    pub fn is_multiple(&self) -> bool {
        matches!(self, Self::Multiple(_))
    }

    /// Get the number of output events
    pub fn len(&self) -> usize {
        match self {
            Self::Single(_) => 1,
            Self::Multiple(events) => events.len(),
            Self::Filtered => 0,
        }
    }

    /// Check if output has no events
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Map over the output events
    pub fn map<F>(self, mut f: F) -> Self
    where
        F: FnMut(SourceEvent) -> SourceEvent,
    {
        match self {
            Self::Single(event) => Self::Single(Box::new(f(*event))),
            Self::Multiple(events) => Self::Multiple(events.into_iter().map(f).collect()),
            Self::Filtered => Self::Filtered,
        }
    }

    /// Filter the output events
    pub fn filter<F>(self, mut predicate: F) -> Self
    where
        F: FnMut(&SourceEvent) -> bool,
    {
        match self {
            Self::Single(event) if predicate(&event) => Self::Single(event),
            Self::Single(_) => Self::Filtered,
            Self::Multiple(events) => {
                let filtered: Vec<_> = events.into_iter().filter(|e| predicate(e)).collect();
                if filtered.is_empty() {
                    Self::Filtered
                } else if filtered.len() == 1 {
                    Self::Single(Box::new(filtered.into_iter().next().unwrap()))
                } else {
                    Self::Multiple(filtered)
                }
            }
            Self::Filtered => Self::Filtered,
        }
    }
}

/// Trait for transform connectors
///
/// Transforms process events, modifying, filtering, or routing them.
///
/// # Example
///
/// ```rust,ignore
/// use rivven_connect::prelude::*;
///
/// #[derive(Debug, Deserialize, Validate, JsonSchema)]
/// pub struct FilterConfig {
///     pub field: String,
///     pub pattern: String,
/// }
///
/// pub struct FilterTransform;
///
/// #[async_trait]
/// impl Transform for FilterTransform {
///     type Config = FilterConfig;
///
///     fn spec() -> ConnectorSpec {
///         ConnectorSpec::new("filter", "1.0.0").build()
///     }
///
///     async fn transform(
///         &self,
///         config: &Self::Config,
///         event: SourceEvent,
///     ) -> Result<TransformOutput> {
///         // Check if event matches filter
///         if event_matches(&event, &config.field, &config.pattern) {
///             Ok(TransformOutput::single(event))
///         } else {
///             Ok(TransformOutput::filtered())
///         }
///     }
/// }
/// ```
#[async_trait]
pub trait Transform: Send + Sync {
    /// Configuration type for this transform
    type Config: TransformConfig;

    /// Return the connector specification
    fn spec() -> ConnectorSpec;

    /// Transform a single event
    async fn transform(&self, config: &Self::Config, event: SourceEvent)
        -> Result<TransformOutput>;

    /// Called when the transform is initialized
    async fn init(&self, _config: &Self::Config) -> Result<()> {
        Ok(())
    }

    /// Called when the transform is shutting down
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

/// Builder for creating common transform patterns
pub mod transforms {
    use super::*;
    use serde_json::Value;

    /// Apply a field mapping to event data
    pub fn rename_field(event: &mut SourceEvent, from: &str, to: &str) {
        if let Some(data) = event.data.as_object_mut() {
            if let Some(value) = data.remove(from) {
                data.insert(to.to_string(), value);
            }
        }
    }

    /// Remove a field from event data
    pub fn remove_field(event: &mut SourceEvent, field: &str) {
        if let Some(data) = event.data.as_object_mut() {
            data.remove(field);
        }
    }

    /// Add a field to event data
    pub fn add_field(event: &mut SourceEvent, field: &str, value: Value) {
        if let Some(data) = event.data.as_object_mut() {
            data.insert(field.to_string(), value);
        }
    }

    /// Check if a field exists in event data
    pub fn has_field(event: &SourceEvent, field: &str) -> bool {
        event
            .data
            .as_object()
            .is_some_and(|data| data.contains_key(field))
    }

    /// Get a field value from event data
    pub fn get_field<'a>(event: &'a SourceEvent, field: &str) -> Option<&'a Value> {
        event.data.as_object().and_then(|data| data.get(field))
    }

    /// Get a nested field value using dot notation (e.g., "address.city")
    pub fn get_nested_field<'a>(event: &'a SourceEvent, path: &str) -> Option<&'a Value> {
        let mut current: &Value = &event.data;
        for part in path.split('.') {
            current = current.as_object()?.get(part)?;
        }
        Some(current)
    }

    /// Set a field value in event data
    pub fn set_field(event: &mut SourceEvent, field: &str, value: Value) {
        if let Some(data) = event.data.as_object_mut() {
            data.insert(field.to_string(), value);
        }
    }

    /// Remove multiple fields at once
    pub fn remove_fields(event: &mut SourceEvent, fields: &[&str]) {
        if let Some(data) = event.data.as_object_mut() {
            for field in fields {
                data.remove(*field);
            }
        }
    }

    /// Keep only specified fields, removing all others
    pub fn keep_only_fields(event: &mut SourceEvent, fields: &[&str]) {
        if let Some(data) = event.data.as_object_mut() {
            let fields_set: std::collections::HashSet<_> = fields.iter().collect();
            let keys_to_remove: Vec<_> = data
                .keys()
                .filter(|k| !fields_set.contains(&k.as_str()))
                .cloned()
                .collect();
            for key in keys_to_remove {
                data.remove(&key);
            }
        }
    }

    /// Apply a function to a specific field if it exists
    pub fn map_field<F>(event: &mut SourceEvent, field: &str, f: F)
    where
        F: FnOnce(&Value) -> Value,
    {
        if let Some(data) = event.data.as_object_mut() {
            if let Some(value) = data.get(field) {
                let new_value = f(value);
                data.insert(field.to_string(), new_value);
            }
        }
    }

    /// Filter predicate: check if a field equals a specific value
    pub fn field_equals(event: &SourceEvent, field: &str, expected: &Value) -> bool {
        get_field(event, field).is_some_and(|v| v == expected)
    }

    /// Filter predicate: check if a string field contains a substring
    pub fn field_contains(event: &SourceEvent, field: &str, substring: &str) -> bool {
        get_field(event, field)
            .and_then(|v| v.as_str())
            .is_some_and(|s| s.contains(substring))
    }

    /// Filter predicate: check if a string field starts with a prefix
    pub fn field_starts_with(event: &SourceEvent, field: &str, prefix: &str) -> bool {
        get_field(event, field)
            .and_then(|v| v.as_str())
            .is_some_and(|s| s.starts_with(prefix))
    }

    /// Filter predicate: check if a string field ends with a suffix
    pub fn field_ends_with(event: &SourceEvent, field: &str, suffix: &str) -> bool {
        get_field(event, field)
            .and_then(|v| v.as_str())
            .is_some_and(|s| s.ends_with(suffix))
    }

    /// Copy a field value to another field
    pub fn copy_field(event: &mut SourceEvent, from: &str, to: &str) {
        if let Some(value) = get_field(event, from).cloned() {
            set_field(event, to, value);
        }
    }

    /// Merge another JSON object into event data
    pub fn merge_data(event: &mut SourceEvent, additional: serde_json::Map<String, Value>) {
        if let Some(data) = event.data.as_object_mut() {
            for (key, value) in additional {
                data.insert(key, value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::event::SourceEvent;
    use super::*;
    use serde_json::json;

    #[test]
    fn test_transform_output_single() {
        let event = SourceEvent::record("test_stream", json!({"id": 1}));
        let output = TransformOutput::single(event);

        assert!(output.is_single());
        assert!(!output.is_filtered());
        assert_eq!(output.len(), 1);
        let events = output.into_events();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_transform_output_multiple() {
        let events = vec![
            SourceEvent::record("test_stream", json!({"id": 1})),
            SourceEvent::record("test_stream", json!({"id": 2})),
        ];
        let output = TransformOutput::multiple(events);

        assert!(output.is_multiple());
        assert_eq!(output.len(), 2);
        let events = output.into_events();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_transform_output_filtered() {
        let output = TransformOutput::filtered();

        assert!(output.is_filtered());
        assert!(output.is_empty());
        assert_eq!(output.len(), 0);
        let events = output.into_events();
        assert!(events.is_empty());
    }

    #[test]
    fn test_transform_output_map() {
        let event = SourceEvent::record("test", json!({"count": 1}));
        let output = TransformOutput::single(event);

        let mapped = output.map(|mut e| {
            transforms::set_field(&mut e, "count", json!(2));
            e
        });

        let events = mapped.into_events();
        assert_eq!(events.len(), 1);
        assert_eq!(transforms::get_field(&events[0], "count"), Some(&json!(2)));
    }

    #[test]
    fn test_transform_output_filter() {
        let events = vec![
            SourceEvent::record("test", json!({"id": 1})),
            SourceEvent::record("test", json!({"id": 2})),
            SourceEvent::record("test", json!({"id": 3})),
        ];
        let output = TransformOutput::multiple(events);

        // Keep only events with id > 1
        let filtered = output.filter(|e| {
            transforms::get_field(e, "id")
                .and_then(|v| v.as_i64())
                .is_some_and(|id| id > 1)
        });

        assert!(filtered.is_multiple());
        let events = filtered.into_events();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_field_operations() {
        let mut event = SourceEvent::record("test", json!({"name": "Alice", "age": 30}));

        assert!(transforms::has_field(&event, "name"));
        assert!(!transforms::has_field(&event, "email"));

        transforms::rename_field(&mut event, "name", "full_name");
        assert!(!transforms::has_field(&event, "name"));
        assert!(transforms::has_field(&event, "full_name"));

        transforms::add_field(&mut event, "email", json!("alice@example.com"));
        assert!(transforms::has_field(&event, "email"));

        transforms::remove_field(&mut event, "age");
        assert!(!transforms::has_field(&event, "age"));
    }

    #[test]
    fn test_nested_field() {
        let event = SourceEvent::record(
            "test",
            json!({
                "user": {
                    "address": {
                        "city": "NYC"
                    }
                }
            }),
        );

        let city = transforms::get_nested_field(&event, "user.address.city");
        assert_eq!(city, Some(&json!("NYC")));

        let missing = transforms::get_nested_field(&event, "user.address.country");
        assert!(missing.is_none());
    }

    #[test]
    fn test_remove_fields() {
        let mut event = SourceEvent::record(
            "test",
            json!({
                "id": 1,
                "name": "test",
                "secret": "password",
                "internal": true
            }),
        );

        transforms::remove_fields(&mut event, &["secret", "internal"]);

        assert!(transforms::has_field(&event, "id"));
        assert!(transforms::has_field(&event, "name"));
        assert!(!transforms::has_field(&event, "secret"));
        assert!(!transforms::has_field(&event, "internal"));
    }

    #[test]
    fn test_keep_only_fields() {
        let mut event = SourceEvent::record(
            "test",
            json!({
                "id": 1,
                "name": "test",
                "secret": "password",
                "internal": true
            }),
        );

        transforms::keep_only_fields(&mut event, &["id", "name"]);

        assert!(transforms::has_field(&event, "id"));
        assert!(transforms::has_field(&event, "name"));
        assert!(!transforms::has_field(&event, "secret"));
        assert!(!transforms::has_field(&event, "internal"));
    }

    #[test]
    fn test_map_field() {
        let mut event = SourceEvent::record("test", json!({"count": 5}));

        transforms::map_field(&mut event, "count", |v| json!(v.as_i64().unwrap_or(0) * 2));

        assert_eq!(transforms::get_field(&event, "count"), Some(&json!(10)));
    }

    #[test]
    fn test_field_equals() {
        let event = SourceEvent::record("test", json!({"status": "active"}));

        assert!(transforms::field_equals(&event, "status", &json!("active")));
        assert!(!transforms::field_equals(
            &event,
            "status",
            &json!("inactive")
        ));
    }

    #[test]
    fn test_copy_field() {
        let mut event = SourceEvent::record("test", json!({"id": 123}));

        transforms::copy_field(&mut event, "id", "original_id");

        assert_eq!(transforms::get_field(&event, "id"), Some(&json!(123)));
        assert_eq!(
            transforms::get_field(&event, "original_id"),
            Some(&json!(123))
        );
    }

    #[test]
    fn test_merge_data() {
        let mut event = SourceEvent::record("test", json!({"id": 1}));

        let additional: serde_json::Map<String, serde_json::Value> =
            serde_json::from_value(json!({"name": "test", "value": 42})).unwrap();

        transforms::merge_data(&mut event, additional);

        assert!(transforms::has_field(&event, "id"));
        assert!(transforms::has_field(&event, "name"));
        assert!(transforms::has_field(&event, "value"));
    }

    #[test]
    fn test_string_field_predicates() {
        let event = SourceEvent::record(
            "test",
            json!({
                "email": "alice@example.com",
                "name": "Alice Smith"
            }),
        );

        // field_contains
        assert!(transforms::field_contains(&event, "email", "@example"));
        assert!(!transforms::field_contains(&event, "email", "@test"));

        // field_starts_with
        assert!(transforms::field_starts_with(&event, "name", "Alice"));
        assert!(!transforms::field_starts_with(&event, "name", "Bob"));

        // field_ends_with
        assert!(transforms::field_ends_with(&event, "email", ".com"));
        assert!(!transforms::field_ends_with(&event, "email", ".org"));
    }
}
