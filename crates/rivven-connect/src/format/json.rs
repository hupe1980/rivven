//! JSON and JSONL Format Writers
//!
//! Provides JSON (array) and JSONL (newline-delimited) format writers for events.
//!
//! # Formats
//!
//! ## JSON Array
//! ```json
//! [
//!   {"id": 1, "name": "Event 1"},
//!   {"id": 2, "name": "Event 2"}
//! ]
//! ```
//!
//! ## JSONL (Newline-Delimited)
//! ```text
//! {"id": 1, "name": "Event 1"}
//! {"id": 2, "name": "Event 2"}
//! ```
//!
//! # Example
//!
//! ```rust
//! use rivven_connect::format::json::{JsonWriter, JsonFormat, JsonWriterConfig};
//! use serde_json::json;
//!
//! let writer = JsonWriter::new(JsonWriterConfig {
//!     format: JsonFormat::Jsonl,
//!     pretty: false,
//! });
//!
//! let events = vec![
//!     json!({"id": 1, "name": "Event 1"}),
//!     json!({"id": 2, "name": "Event 2"}),
//! ];
//!
//! let bytes = writer.write_batch(&events).unwrap();
//! let output = String::from_utf8(bytes).unwrap();
//! assert!(output.contains(r#"{"id":1"#));
//! ```

use super::{FormatError, FormatWriter};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// JSON output format variant
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum JsonFormat {
    /// JSON array of events: `[{...}, {...}]`
    Json,
    /// Newline-delimited JSON (one event per line)
    #[default]
    Jsonl,
}

/// Configuration for JSON writer
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct JsonWriterConfig {
    /// Output format (json array or jsonl)
    #[serde(default)]
    pub format: JsonFormat,

    /// Pretty-print output (adds indentation)
    #[serde(default)]
    pub pretty: bool,
}

impl Default for JsonWriterConfig {
    fn default() -> Self {
        Self {
            format: JsonFormat::Jsonl,
            pretty: false,
        }
    }
}

/// JSON format writer for events
pub struct JsonWriter {
    config: JsonWriterConfig,
}

impl JsonWriter {
    /// Create a new JSON writer with the given configuration
    pub fn new(config: JsonWriterConfig) -> Self {
        Self { config }
    }

    /// Create a JSON array writer
    pub fn json() -> Self {
        Self::new(JsonWriterConfig {
            format: JsonFormat::Json,
            pretty: false,
        })
    }

    /// Create a JSONL (newline-delimited) writer
    pub fn jsonl() -> Self {
        Self::new(JsonWriterConfig {
            format: JsonFormat::Jsonl,
            pretty: false,
        })
    }

    /// Create a pretty-printed JSON array writer
    pub fn json_pretty() -> Self {
        Self::new(JsonWriterConfig {
            format: JsonFormat::Json,
            pretty: true,
        })
    }

    /// Write batch as JSON array
    fn write_json(&self, events: &[Value]) -> Result<Vec<u8>, FormatError> {
        if self.config.pretty {
            Ok(serde_json::to_vec_pretty(events)?)
        } else {
            Ok(serde_json::to_vec(events)?)
        }
    }

    /// Write batch as JSONL (newline-delimited)
    fn write_jsonl(&self, events: &[Value]) -> Result<Vec<u8>, FormatError> {
        let mut output = Vec::new();
        for event in events {
            let line = if self.config.pretty {
                serde_json::to_string_pretty(event)?
            } else {
                serde_json::to_string(event)?
            };
            output.extend_from_slice(line.as_bytes());
            output.push(b'\n');
        }
        Ok(output)
    }

    /// Write a batch of events to bytes
    pub fn write_batch(&self, events: &[Value]) -> Result<Vec<u8>, FormatError> {
        if events.is_empty() {
            return Err(FormatError::EmptyBatch);
        }

        match self.config.format {
            JsonFormat::Json => self.write_json(events),
            JsonFormat::Jsonl => self.write_jsonl(events),
        }
    }
}

impl FormatWriter for JsonWriter {
    fn name(&self) -> &'static str {
        match self.config.format {
            JsonFormat::Json => "json",
            JsonFormat::Jsonl => "jsonl",
        }
    }

    fn extension(&self) -> &'static str {
        match self.config.format {
            JsonFormat::Json => ".json",
            JsonFormat::Jsonl => ".jsonl",
        }
    }

    fn content_type(&self) -> &'static str {
        match self.config.format {
            JsonFormat::Json => "application/json",
            JsonFormat::Jsonl => "application/x-ndjson",
        }
    }

    fn write_batch(&self, events: &[Value]) -> Result<Vec<u8>, FormatError> {
        JsonWriter::write_batch(self, events)
    }

    fn supports_append(&self) -> bool {
        // JSONL supports append, JSON array does not
        matches!(self.config.format, JsonFormat::Jsonl)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_jsonl_writer() {
        let writer = JsonWriter::jsonl();
        let events = vec![
            json!({"id": 1, "name": "Event 1"}),
            json!({"id": 2, "name": "Event 2"}),
        ];

        let bytes = writer.write_batch(&events).unwrap();
        let output = String::from_utf8(bytes).unwrap();

        assert!(output.contains(r#"{"id":1,"name":"Event 1"}"#));
        assert!(output.contains(r#"{"id":2,"name":"Event 2"}"#));
        assert!(output.ends_with('\n'));
        assert_eq!(output.lines().count(), 2);
    }

    #[test]
    fn test_json_writer() {
        let writer = JsonWriter::json();
        let events = vec![
            json!({"id": 1, "name": "Event 1"}),
            json!({"id": 2, "name": "Event 2"}),
        ];

        let bytes = writer.write_batch(&events).unwrap();
        let output = String::from_utf8(bytes).unwrap();

        // Should be a valid JSON array
        let parsed: Vec<Value> = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn test_json_pretty() {
        let writer = JsonWriter::json_pretty();
        let events = vec![json!({"id": 1})];

        let bytes = writer.write_batch(&events).unwrap();
        let output = String::from_utf8(bytes).unwrap();

        // Pretty output has newlines and indentation
        assert!(output.contains('\n'));
        assert!(output.contains("  "));
    }

    #[test]
    fn test_empty_batch() {
        let writer = JsonWriter::jsonl();
        let result = writer.write_batch(&[]);
        assert!(matches!(result, Err(FormatError::EmptyBatch)));
    }

    #[test]
    fn test_format_writer_trait() {
        let writer: Box<dyn FormatWriter> = Box::new(JsonWriter::jsonl());
        assert_eq!(writer.name(), "jsonl");
        assert_eq!(writer.extension(), ".jsonl");
        assert_eq!(writer.content_type(), "application/x-ndjson");
        assert!(writer.supports_append());

        let writer: Box<dyn FormatWriter> = Box::new(JsonWriter::json());
        assert_eq!(writer.name(), "json");
        assert!(!writer.supports_append());
    }
}
