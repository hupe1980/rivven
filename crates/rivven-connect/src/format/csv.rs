//! CSV Format Writer for Rivven
//!
//! This module provides CSV (Comma-Separated Values) format support for writing events
//! to object storage. It's useful for data interchange with spreadsheet applications
//! and legacy systems.
//!
//! # Features
//!
//! - Automatic header inference from JSON records
//! - Configurable delimiter (comma, tab, semicolon, pipe)
//! - RFC 4180 compliant quoting and escaping
//! - Optional header row
//! - Handles nested objects by JSON-encoding them
//! - Implements `FormatWriter` trait for use with any storage sink
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::format::{CsvWriter, CsvWriterConfig, CsvDelimiter, FormatWriter};
//!
//! let config = CsvWriterConfig {
//!     delimiter: CsvDelimiter::Comma,
//!     include_header: true,
//!     ..Default::default()
//! };
//!
//! let writer = CsvWriter::new(config);
//!
//! // Use via FormatWriter trait
//! let csv_bytes = writer.write_batch(&events)?;
//! println!("Format: {}, Extension: {}", writer.name(), writer.extension());
//! ```

use super::{FormatError, FormatWriter};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::io::Write;
use thiserror::Error;

/// Errors that can occur during CSV operations
#[derive(Error, Debug)]
pub enum CsvWriterError {
    #[error("Empty batch: cannot write zero records")]
    EmptyBatch,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),
}

pub type CsvWriterResult<T> = Result<T, CsvWriterError>;

/// CSV delimiter options
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CsvDelimiter {
    /// Comma (,) - standard CSV
    #[default]
    Comma,
    /// Tab (\t) - TSV format
    Tab,
    /// Semicolon (;) - common in European locales
    Semicolon,
    /// Pipe (|) - useful when data contains commas
    Pipe,
}

impl CsvDelimiter {
    fn as_char(&self) -> char {
        match self {
            CsvDelimiter::Comma => ',',
            CsvDelimiter::Tab => '\t',
            CsvDelimiter::Semicolon => ';',
            CsvDelimiter::Pipe => '|',
        }
    }

    fn extension(&self) -> &'static str {
        match self {
            CsvDelimiter::Comma => ".csv",
            CsvDelimiter::Tab => ".tsv",
            CsvDelimiter::Semicolon => ".csv",
            CsvDelimiter::Pipe => ".csv",
        }
    }

    fn content_type(&self) -> &'static str {
        match self {
            CsvDelimiter::Comma => "text/csv",
            CsvDelimiter::Tab => "text/tab-separated-values",
            CsvDelimiter::Semicolon => "text/csv",
            CsvDelimiter::Pipe => "text/csv",
        }
    }
}

/// Line ending style
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CsvLineEnding {
    /// Unix-style line endings (\n)
    #[default]
    Lf,
    /// Windows-style line endings (\r\n) - RFC 4180 standard
    Crlf,
}

impl CsvLineEnding {
    fn as_str(&self) -> &'static str {
        match self {
            CsvLineEnding::Lf => "\n",
            CsvLineEnding::Crlf => "\r\n",
        }
    }
}

/// Configuration for CSV writer
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct CsvWriterConfig {
    /// Field delimiter
    #[serde(default)]
    pub delimiter: CsvDelimiter,

    /// Include header row with column names
    #[serde(default = "default_include_header")]
    pub include_header: bool,

    /// Quote character (default: double quote)
    #[serde(default = "default_quote_char")]
    pub quote_char: char,

    /// Always quote fields, even if not necessary
    #[serde(default)]
    pub always_quote: bool,

    /// Line ending style
    #[serde(default)]
    pub line_ending: CsvLineEnding,

    /// Null value representation
    #[serde(default = "default_null_value")]
    pub null_value: String,

    /// Explicit column order (if not provided, columns are sorted alphabetically)
    #[serde(default)]
    pub columns: Option<Vec<String>>,
}

fn default_include_header() -> bool {
    true
}

fn default_quote_char() -> char {
    '"'
}

fn default_null_value() -> String {
    String::new()
}

impl Default for CsvWriterConfig {
    fn default() -> Self {
        Self {
            delimiter: CsvDelimiter::default(),
            include_header: default_include_header(),
            quote_char: default_quote_char(),
            always_quote: false,
            line_ending: CsvLineEnding::default(),
            null_value: default_null_value(),
            columns: None,
        }
    }
}

/// CSV writer that converts JSON events to CSV format
pub struct CsvWriter {
    config: CsvWriterConfig,
}

impl CsvWriter {
    /// Create a new CSV writer with the given configuration
    pub fn new(config: CsvWriterConfig) -> Self {
        Self { config }
    }

    /// Write a batch of JSON events to CSV format
    pub fn write_batch_internal(&self, events: &[Value]) -> CsvWriterResult<Vec<u8>> {
        if events.is_empty() {
            return Err(CsvWriterError::EmptyBatch);
        }

        let mut output = Vec::new();
        let delimiter = self.config.delimiter.as_char();
        let line_ending = self.config.line_ending.as_str();

        // Determine columns
        let columns = if let Some(cols) = &self.config.columns {
            cols.clone()
        } else {
            // Collect all unique column names across all records
            let mut col_set: BTreeSet<String> = BTreeSet::new();
            for event in events {
                if let Value::Object(map) = event {
                    for key in map.keys() {
                        col_set.insert(key.clone());
                    }
                }
            }
            col_set.into_iter().collect()
        };

        // Write header row
        if self.config.include_header {
            let header_row: Vec<String> =
                columns.iter().map(|col| self.escape_field(col)).collect();
            write!(
                output,
                "{}{}",
                header_row.join(&delimiter.to_string()),
                line_ending
            )?;
        }

        // Write data rows
        for event in events {
            let row: Vec<String> = columns
                .iter()
                .map(|col| {
                    let value = if let Value::Object(map) = event {
                        map.get(col).cloned().unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    };
                    self.value_to_csv(&value)
                })
                .collect();
            write!(
                output,
                "{}{}",
                row.join(&delimiter.to_string()),
                line_ending
            )?;
        }

        Ok(output)
    }

    /// Convert a JSON value to a CSV field
    fn value_to_csv(&self, value: &Value) -> String {
        let field = match value {
            Value::Null => self.config.null_value.clone(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => n.to_string(),
            Value::String(s) => s.clone(),
            Value::Array(_) | Value::Object(_) => {
                // JSON-encode complex types
                serde_json::to_string(value).unwrap_or_default()
            }
        };
        self.escape_field(&field)
    }

    /// Escape a field value according to RFC 4180
    fn escape_field(&self, field: &str) -> String {
        let delimiter = self.config.delimiter.as_char();
        let quote = self.config.quote_char;

        let needs_quoting = self.config.always_quote
            || field.contains(delimiter)
            || field.contains(quote)
            || field.contains('\n')
            || field.contains('\r');

        if needs_quoting {
            // Escape quotes by doubling them
            let escaped = field.replace(quote, &format!("{}{}", quote, quote));
            format!("{}{}{}", quote, escaped, quote)
        } else {
            field.to_string()
        }
    }
}

/// Implement FormatWriter trait for CsvWriter
impl FormatWriter for CsvWriter {
    fn name(&self) -> &'static str {
        match self.config.delimiter {
            CsvDelimiter::Tab => "tsv",
            _ => "csv",
        }
    }

    fn extension(&self) -> &'static str {
        self.config.delimiter.extension()
    }

    fn content_type(&self) -> &'static str {
        self.config.delimiter.content_type()
    }

    fn write_batch(&self, events: &[Value]) -> Result<Vec<u8>, FormatError> {
        self.write_batch_internal(events)
            .map_err(|e| FormatError::Write(e.to_string()))
    }

    fn supports_append(&self) -> bool {
        // CSV supports appending (just add more rows)
        true
    }
}

/// Helper to write events directly to CSV bytes
pub fn write_events_to_csv(
    events: &[Value],
    config: Option<CsvWriterConfig>,
) -> CsvWriterResult<Vec<u8>> {
    let writer = CsvWriter::new(config.unwrap_or_default());
    writer.write_batch_internal(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_simple_csv() {
        let events = vec![
            json!({"name": "Alice", "age": 30}),
            json!({"name": "Bob", "age": 25}),
        ];

        let config = CsvWriterConfig::default();
        let writer = CsvWriter::new(config);

        let csv_bytes = writer.write_batch_internal(&events).unwrap();
        let csv_str = String::from_utf8(csv_bytes).unwrap();

        assert!(csv_str.contains("age,name")); // Alphabetically sorted
        assert!(csv_str.contains("30,Alice"));
        assert!(csv_str.contains("25,Bob"));
    }

    #[test]
    fn test_csv_without_header() {
        let events = vec![json!({"name": "Alice", "age": 30})];

        let config = CsvWriterConfig {
            include_header: false,
            ..Default::default()
        };
        let writer = CsvWriter::new(config);

        let csv_bytes = writer.write_batch_internal(&events).unwrap();
        let csv_str = String::from_utf8(csv_bytes).unwrap();

        assert!(!csv_str.contains("name"));
        assert!(csv_str.contains("30,Alice"));
    }

    #[test]
    fn test_csv_escaping() {
        let events = vec![json!({"name": "Alice, Bob", "quote": "She said \"Hello\""})];

        let config = CsvWriterConfig::default();
        let writer = CsvWriter::new(config);

        let csv_bytes = writer.write_batch_internal(&events).unwrap();
        let csv_str = String::from_utf8(csv_bytes).unwrap();

        // Fields with commas/quotes should be quoted and escaped
        assert!(csv_str.contains("\"Alice, Bob\""));
        assert!(csv_str.contains("\"\"Hello\"\"")); // Doubled quotes
    }

    #[test]
    fn test_tsv_format() {
        let events = vec![json!({"a": 1, "b": 2})];

        let config = CsvWriterConfig {
            delimiter: CsvDelimiter::Tab,
            ..Default::default()
        };
        let writer = CsvWriter::new(config);

        assert_eq!(writer.name(), "tsv");
        assert_eq!(writer.extension(), ".tsv");
        assert_eq!(writer.content_type(), "text/tab-separated-values");

        let csv_bytes = writer.write_batch_internal(&events).unwrap();
        let csv_str = String::from_utf8(csv_bytes).unwrap();

        assert!(csv_str.contains("a\tb")); // Tab-separated
    }

    #[test]
    fn test_explicit_columns() {
        let events = vec![json!({"z": 1, "a": 2, "m": 3})];

        let config = CsvWriterConfig {
            columns: Some(vec!["m".to_string(), "z".to_string(), "a".to_string()]),
            ..Default::default()
        };
        let writer = CsvWriter::new(config);

        let csv_bytes = writer.write_batch_internal(&events).unwrap();
        let csv_str = String::from_utf8(csv_bytes).unwrap();

        // Should maintain explicit column order
        assert!(csv_str.starts_with("m,z,a"));
    }

    #[test]
    fn test_null_handling() {
        let events = vec![json!({"name": "Alice", "email": null})];

        let config = CsvWriterConfig {
            null_value: "NULL".to_string(),
            ..Default::default()
        };
        let writer = CsvWriter::new(config);

        let csv_bytes = writer.write_batch_internal(&events).unwrap();
        let csv_str = String::from_utf8(csv_bytes).unwrap();

        assert!(csv_str.contains("NULL"));
    }

    #[test]
    fn test_nested_objects() {
        let events = vec![json!({"id": 1, "data": {"nested": "value"}})];

        let config = CsvWriterConfig::default();
        let writer = CsvWriter::new(config);

        let csv_bytes = writer.write_batch_internal(&events).unwrap();
        let csv_str = String::from_utf8(csv_bytes).unwrap();

        // Nested objects should be JSON-encoded and quoted
        // The JSON will be: {"nested":"value"} which needs CSV quoting
        // CSV escapes quotes by doubling them, so we expect: "{""nested"":""value""}"
        assert!(
            csv_str.contains(r#""{""nested"":""value""}""#),
            "CSV output should contain properly escaped JSON. Got: {}",
            csv_str
        );
    }

    #[test]
    fn test_empty_batch() {
        let events: Vec<Value> = vec![];
        let writer = CsvWriter::new(CsvWriterConfig::default());

        let result = writer.write_batch_internal(&events);
        assert!(matches!(result, Err(CsvWriterError::EmptyBatch)));
    }

    #[test]
    fn test_format_writer_trait() {
        let writer = CsvWriter::new(CsvWriterConfig::default());

        assert_eq!(writer.name(), "csv");
        assert_eq!(writer.extension(), ".csv");
        assert_eq!(writer.content_type(), "text/csv");
        assert!(writer.supports_append()); // CSV supports appending

        let events = vec![json!({"a": 1, "b": 2})];

        let format_writer: &dyn FormatWriter = &writer;
        let bytes = format_writer.write_batch(&events).unwrap();

        let csv_str = String::from_utf8(bytes).unwrap();
        assert!(csv_str.contains("a,b"));
    }

    #[test]
    fn test_crlf_line_endings() {
        let events = vec![json!({"a": 1}), json!({"a": 2})];

        let config = CsvWriterConfig {
            line_ending: CsvLineEnding::Crlf,
            ..Default::default()
        };
        let writer = CsvWriter::new(config);

        let csv_bytes = writer.write_batch_internal(&events).unwrap();
        let csv_str = String::from_utf8(csv_bytes).unwrap();

        assert!(csv_str.contains("\r\n"));
    }

    #[test]
    fn test_always_quote() {
        let events = vec![json!({"name": "Alice"})];

        let config = CsvWriterConfig {
            always_quote: true,
            ..Default::default()
        };
        let writer = CsvWriter::new(config);

        let csv_bytes = writer.write_batch_internal(&events).unwrap();
        let csv_str = String::from_utf8(csv_bytes).unwrap();

        assert!(csv_str.contains("\"name\""));
        assert!(csv_str.contains("\"Alice\""));
    }

    #[test]
    fn test_helper_function() {
        let events = vec![json!({"key": "value"})];

        let bytes = write_events_to_csv(&events, None).unwrap();
        let csv_str = String::from_utf8(bytes).unwrap();
        assert!(csv_str.contains("key"));
    }
}
