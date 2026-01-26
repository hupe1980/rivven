//! State management for incremental sync
//!
//! State is used to track sync progress and enable resumable, incremental data syncs.

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;

/// State for tracking sync progress
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct State {
    /// Per-stream state
    pub streams: HashMap<String, StreamState>,

    /// Global state (shared across streams)
    pub global: HashMap<String, serde_json::Value>,
}

impl State {
    /// Create empty state
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Create a builder for constructing state
    pub fn builder() -> StateBuilder {
        StateBuilder::new()
    }

    /// Get state for a specific stream
    pub fn get_stream(&self, stream_name: &str) -> Option<&StreamState> {
        self.streams.get(stream_name)
    }
    
    /// Get mutable state for a specific stream
    pub fn get_stream_mut(&mut self, stream_name: &str) -> Option<&mut StreamState> {
        self.streams.get_mut(stream_name)
    }
    
    /// Get or create state for a specific stream
    pub fn get_or_create_stream(&mut self, stream_name: impl Into<String>) -> &mut StreamState {
        let name = stream_name.into();
        self.streams.entry(name.clone()).or_insert_with(|| StreamState::new(name))
    }

    /// Set state for a specific stream
    pub fn set_stream(&mut self, stream_name: impl Into<String>, state: StreamState) {
        self.streams.insert(stream_name.into(), state);
    }

    /// Update stream cursor
    pub fn update_cursor(
        &mut self,
        stream_name: impl Into<String>,
        cursor_field: impl Into<String>,
        cursor_value: serde_json::Value,
    ) {
        let stream_name = stream_name.into();
        let state = self
            .streams
            .entry(stream_name.clone())
            .or_insert_with(|| StreamState::new(stream_name));
        state.cursor_field = Some(cursor_field.into());
        state.cursor_value = Some(cursor_value);
    }

    /// Get global value
    pub fn get_global(&self, key: &str) -> Option<&serde_json::Value> {
        self.global.get(key)
    }
    
    /// Get global value as typed
    pub fn get_global_as<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.global.get(key).and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    /// Set global value
    pub fn set_global(&mut self, key: impl Into<String>, value: serde_json::Value) {
        self.global.insert(key.into(), value);
    }
    
    /// Set global value from serializable type
    pub fn set_global_from<T: Serialize>(&mut self, key: impl Into<String>, value: &T) -> Result<(), serde_json::Error> {
        let json_value = serde_json::to_value(value)?;
        self.global.insert(key.into(), json_value);
        Ok(())
    }

    /// Merge with another state (other takes precedence)
    pub fn merge(&mut self, other: State) {
        for (k, v) in other.streams {
            self.streams.insert(k, v);
        }
        for (k, v) in other.global {
            self.global.insert(k, v);
        }
    }
    
    /// Check if state is empty
    pub fn is_empty(&self) -> bool {
        self.streams.is_empty() && self.global.is_empty()
    }
    
    /// Get all stream names
    pub fn stream_names(&self) -> impl Iterator<Item = &str> {
        self.streams.keys().map(|s| s.as_str())
    }
    
    /// Remove a stream's state
    pub fn remove_stream(&mut self, stream_name: &str) -> Option<StreamState> {
        self.streams.remove(stream_name)
    }
    
    /// Clear all state
    pub fn clear(&mut self) {
        self.streams.clear();
        self.global.clear();
    }
}

/// Builder for constructing State
pub struct StateBuilder {
    state: State,
}

impl StateBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self { state: State::new() }
    }
    
    /// Add a stream with cursor
    pub fn stream_cursor(
        mut self,
        stream_name: impl Into<String>,
        cursor_field: impl Into<String>,
        cursor_value: serde_json::Value,
    ) -> Self {
        self.state.update_cursor(stream_name, cursor_field, cursor_value);
        self
    }
    
    /// Add a stream state
    pub fn stream(mut self, stream_name: impl Into<String>, stream_state: StreamState) -> Self {
        self.state.set_stream(stream_name, stream_state);
        self
    }
    
    /// Add global state
    pub fn global(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.state.set_global(key, value);
        self
    }
    
    /// Build the state
    pub fn build(self) -> State {
        self.state
    }
}

impl Default for StateBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// State for a single stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamState {
    /// Stream name
    pub stream_name: String,

    /// Cursor field name
    pub cursor_field: Option<String>,

    /// Last cursor value
    pub cursor_value: Option<serde_json::Value>,

    /// Additional state data
    pub data: HashMap<String, serde_json::Value>,
}

impl StreamState {
    /// Create new stream state
    pub fn new(stream_name: impl Into<String>) -> Self {
        Self {
            stream_name: stream_name.into(),
            cursor_field: None,
            cursor_value: None,
            data: HashMap::new(),
        }
    }

    /// Set cursor (builder style)
    pub fn cursor(
        mut self,
        field: impl Into<String>,
        value: serde_json::Value,
    ) -> Self {
        self.cursor_field = Some(field.into());
        self.cursor_value = Some(value);
        self
    }
    
    /// Update cursor (mutable reference)
    pub fn set_cursor(&mut self, field: impl Into<String>, value: serde_json::Value) {
        self.cursor_field = Some(field.into());
        self.cursor_value = Some(value);
    }
    
    /// Get cursor value as typed
    pub fn cursor_as<T: DeserializeOwned>(&self) -> Option<T> {
        self.cursor_value.as_ref().and_then(|v| serde_json::from_value(v.clone()).ok())
    }
    
    /// Get cursor value as i64
    pub fn cursor_as_i64(&self) -> Option<i64> {
        self.cursor_value.as_ref().and_then(|v| v.as_i64())
    }
    
    /// Get cursor value as string
    pub fn cursor_as_str(&self) -> Option<&str> {
        self.cursor_value.as_ref().and_then(|v| v.as_str())
    }

    /// Set additional data (builder style)
    pub fn data(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.data.insert(key.into(), value);
        self
    }
    
    /// Set additional data (mutable reference)
    pub fn set_data(&mut self, key: impl Into<String>, value: serde_json::Value) {
        self.data.insert(key.into(), value);
    }

    /// Get additional data
    pub fn get_data(&self, key: &str) -> Option<&serde_json::Value> {
        self.data.get(key)
    }
    
    /// Get additional data as typed
    pub fn get_data_as<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.data.get(key).and_then(|v| serde_json::from_value(v.clone()).ok())
    }
    
    /// Check if state has cursor set
    pub fn has_cursor(&self) -> bool {
        self.cursor_field.is_some() && self.cursor_value.is_some()
    }
    
    /// Check if state has additional data
    pub fn has_data(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }
    
    /// Remove data key
    pub fn remove_data(&mut self, key: &str) -> Option<serde_json::Value> {
        self.data.remove(key)
    }
    
    /// Clear cursor
    pub fn clear_cursor(&mut self) {
        self.cursor_field = None;
        self.cursor_value = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_state_operations() {
        let mut state = State::new();

        state.update_cursor("users", "updated_at", json!("2026-01-23T10:00:00Z"));

        let stream_state = state.get_stream("users").unwrap();
        assert_eq!(stream_state.cursor_field, Some("updated_at".to_string()));
        assert_eq!(
            stream_state.cursor_value,
            Some(json!("2026-01-23T10:00:00Z"))
        );
    }

    #[test]
    fn test_state_merge() {
        let mut state1 = State::new();
        state1.update_cursor("users", "id", json!(100));

        let mut state2 = State::new();
        state2.update_cursor("users", "id", json!(200));
        state2.update_cursor("orders", "id", json!(50));

        state1.merge(state2);

        // state2 values take precedence
        assert_eq!(
            state1.get_stream("users").unwrap().cursor_value,
            Some(json!(200))
        );
        assert!(state1.get_stream("orders").is_some());
    }
    
    #[test]
    fn test_state_builder() {
        let state = State::builder()
            .stream_cursor("users", "id", json!(100))
            .stream_cursor("orders", "created_at", json!("2026-01-25"))
            .global("last_sync", json!("2026-01-25T10:00:00Z"))
            .build();
        
        assert!(!state.is_empty());
        assert_eq!(state.streams.len(), 2);
        assert!(state.get_stream("users").is_some());
        assert!(state.get_global("last_sync").is_some());
    }
    
    #[test]
    fn test_state_typed_access() {
        let mut state = State::new();
        state.set_global("count", json!(42));
        state.set_global("name", json!("test"));
        
        assert_eq!(state.get_global_as::<i64>("count"), Some(42));
        assert_eq!(state.get_global_as::<String>("name"), Some("test".to_string()));
        assert_eq!(state.get_global_as::<i64>("nonexistent"), None);
    }
    
    #[test]
    fn test_state_get_or_create() {
        let mut state = State::new();
        
        // Creates new stream state
        let stream = state.get_or_create_stream("new_stream");
        stream.set_cursor("id", json!(1));
        
        // Returns existing stream state
        let stream = state.get_or_create_stream("new_stream");
        assert_eq!(stream.cursor_as_i64(), Some(1));
    }
    
    #[test]
    fn test_stream_state_cursor_typed() {
        let stream = StreamState::new("test")
            .cursor("id", json!(12345));
        
        assert!(stream.has_cursor());
        assert_eq!(stream.cursor_as_i64(), Some(12345));
        
        let stream2 = StreamState::new("test2")
            .cursor("timestamp", json!("2026-01-25"));
        
        assert_eq!(stream2.cursor_as_str(), Some("2026-01-25"));
    }
    
    #[test]
    fn test_stream_state_data_typed() {
        let stream = StreamState::new("test")
            .data("offset", json!(100))
            .data("partition", json!(3));
        
        assert!(stream.has_data("offset"));
        assert!(!stream.has_data("nonexistent"));
        assert_eq!(stream.get_data_as::<i64>("offset"), Some(100));
        assert_eq!(stream.get_data_as::<i64>("partition"), Some(3));
    }
    
    #[test]
    fn test_state_stream_names() {
        let state = State::builder()
            .stream_cursor("users", "id", json!(1))
            .stream_cursor("orders", "id", json!(1))
            .stream_cursor("products", "id", json!(1))
            .build();
        
        let names: Vec<_> = state.stream_names().collect();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"users"));
        assert!(names.contains(&"orders"));
        assert!(names.contains(&"products"));
    }
}
