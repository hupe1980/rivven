//! Output Formats for Rivven Connect
//!
//! This module provides serialization formats for writing events to storage backends.
//! Formats are **how** data is serialized; storage connectors are **where** data is written.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Storage Connectors                           │
//! │  S3Sink, GcsSink, AzureBlobSink, LocalFileSink                  │
//! └───────────────────────────┬─────────────────────────────────────┘
//!                             │ uses
//!                             ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Format Writers                               │
//! │  JsonWriter, ParquetWriter, AvroWriter, CsvWriter               │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Available Formats
//!
//! | Format | Extension | Use Case | Compression | Append |
//! |--------|-----------|----------|-------------|--------|
//! | JSON | `.json` | Human-readable, debugging | gzip | ❌ |
//! | JSONL | `.jsonl` | Streaming, log files | gzip | ✅ |
//! | Parquet | `.parquet` | Analytics, data lakes | snappy, zstd | ❌ |
//! | Avro | `.avro` | Schema evolution, compact | deflate, snappy | ❌ |
//! | CSV | `.csv` | Spreadsheets, legacy systems | gzip | ✅ |
//!
//! # Format Selection Guide
//!
//! ```text
//!                       ┌─────────────────────┐
//!                       │   What's your use   │
//!                       │       case?         │
//!                       └─────────┬───────────┘
//!                                 │
//!         ┌───────────────────────┼───────────────────────┐
//!         │           │           │           │           │
//!         ▼           ▼           ▼           ▼           ▼
//!   ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐
//!   │ Analytics │ │ Streaming │ │ Schema    │ │ Legacy    │ │ Debugging │
//!   │ Data Lake │ │ Real-time │ │ Evolution │ │ Systems   │ │ Logs      │
//!   └─────┬─────┘ └─────┬─────┘ └─────┬─────┘ └─────┬─────┘ └─────┬─────┘
//!         │             │             │             │             │
//!         ▼             ▼             ▼             ▼             ▼
//!   ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐
//!   │  Parquet  │ │   JSONL   │ │   Avro    │ │   CSV     │ │   JSON    │
//!   │  +zstd    │ │   +gzip   │ │ +deflate  │ │           │ │  (pretty) │
//!   └───────────┘ └───────────┘ └───────────┘ └───────────┘ └───────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::format::{FormatWriter, JsonWriter, JsonWriterConfig, JsonFormat};
//!
//! // Create a JSONL format writer
//! let config = JsonWriterConfig {
//!     format: JsonFormat::Jsonl,
//!     ..Default::default()
//! };
//! let writer = JsonWriter::new(config);
//!
//! // Serialize events
//! let bytes = writer.write_batch(&events)?;
//! println!("Format: {}, Extension: {}", writer.name(), writer.extension());
//! ```

// Format implementations
pub mod avro;
pub mod csv;
pub mod json;
pub mod output_compression;

#[cfg(feature = "parquet")]
pub mod parquet;

// Re-exports for ergonomic use
pub use avro::{
    AvroCompression, AvroSchemaInference, AvroWriter, AvroWriterConfig, AvroWriterError,
    AvroWriterResult,
};

pub use csv::{
    CsvDelimiter, CsvLineEnding, CsvWriter, CsvWriterConfig, CsvWriterError, CsvWriterResult,
};

pub use json::{JsonFormat, JsonWriter, JsonWriterConfig};

pub use output_compression::{
    create_compressed_writer, full_extension, CompressedFormatWriter, OutputCompression,
};

#[cfg(feature = "parquet")]
pub use parquet::{
    ParquetCompression, ParquetError, ParquetResult, ParquetWriter, ParquetWriterConfig,
    SchemaInference,
};

use serde_json::Value;

/// Trait for format writers that serialize events to bytes
///
/// This trait provides a uniform interface for all format writers, allowing
/// storage connectors to be format-agnostic.
///
/// # Implementors
///
/// - [`JsonWriter`] - JSON and JSONL formats
/// - `ParquetWriter` - Apache Parquet columnar format (requires `parquet` feature)
/// - [`AvroWriter`] - Apache Avro with schema support
/// - [`CsvWriter`] - CSV and TSV tabular formats
pub trait FormatWriter: Send + Sync {
    /// Format name (e.g., "json", "jsonl", "parquet", "avro", "csv")
    fn name(&self) -> &'static str;

    /// File extension (e.g., ".json", ".jsonl", ".parquet", ".avro", ".csv")
    fn extension(&self) -> &'static str;

    /// MIME type for the format (e.g., "application/json", "application/avro")
    fn content_type(&self) -> &'static str;

    /// Serialize a batch of events to bytes
    ///
    /// # Arguments
    /// * `events` - Slice of JSON values to serialize
    ///
    /// # Returns
    /// * `Ok(Vec<u8>)` - Serialized bytes
    /// * `Err(FormatError)` - If serialization fails
    fn write_batch(&self, events: &[Value]) -> Result<Vec<u8>, FormatError>;

    /// Whether this format supports streaming/append operations
    ///
    /// Returns `true` for line-oriented formats (JSONL, CSV) that can be
    /// appended to without rewriting the entire file.
    fn supports_append(&self) -> bool {
        false
    }
}

/// Errors that can occur during format operations
#[derive(Debug, thiserror::Error)]
pub enum FormatError {
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Empty batch: cannot serialize zero records")]
    EmptyBatch,

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Write error: {0}")]
    Write(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[cfg(feature = "parquet")]
    #[error("Parquet error: {0}")]
    Parquet(#[from] ParquetError),

    #[error("Avro error: {0}")]
    Avro(#[from] AvroWriterError),

    #[error("CSV error: {0}")]
    Csv(#[from] CsvWriterError),
}

/// Output format enumeration for configuration
///
/// Used by storage connectors to specify the desired output format.
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    /// JSON array format
    Json,
    /// Newline-delimited JSON (one event per line)
    #[default]
    Jsonl,
    /// Apache Parquet columnar format
    #[cfg(feature = "parquet")]
    Parquet,
    /// Apache Avro format with schema
    Avro,
    /// CSV (Comma-Separated Values)
    Csv,
    /// TSV (Tab-Separated Values)
    Tsv,
}

impl OutputFormat {
    /// Create a format writer for this output format with default configuration
    ///
    /// # Errors
    /// Returns an error if the format writer cannot be initialized (e.g., Avro schema setup fails).
    pub fn create_writer(&self) -> crate::error::Result<Box<dyn FormatWriter>> {
        Ok(match self {
            OutputFormat::Json => Box::new(JsonWriter::new(JsonWriterConfig {
                format: JsonFormat::Json,
                ..Default::default()
            })),
            OutputFormat::Jsonl => Box::new(JsonWriter::new(JsonWriterConfig {
                format: JsonFormat::Jsonl,
                ..Default::default()
            })),
            #[cfg(feature = "parquet")]
            OutputFormat::Parquet => Box::new(ParquetWriter::new(ParquetWriterConfig::default())),
            OutputFormat::Avro => {
                Box::new(AvroWriter::new(AvroWriterConfig::default()).map_err(|e| {
                    crate::error::ConnectorError::config(format!("Avro writer init failed: {}", e))
                })?)
            }
            OutputFormat::Csv => Box::new(CsvWriter::new(CsvWriterConfig::default())),
            OutputFormat::Tsv => Box::new(CsvWriter::new(CsvWriterConfig {
                delimiter: CsvDelimiter::Tab,
                ..Default::default()
            })),
        })
    }

    /// Get the file extension for this format
    pub fn extension(&self) -> &'static str {
        match self {
            OutputFormat::Json => ".json",
            OutputFormat::Jsonl => ".jsonl",
            #[cfg(feature = "parquet")]
            OutputFormat::Parquet => ".parquet",
            OutputFormat::Avro => ".avro",
            OutputFormat::Csv => ".csv",
            OutputFormat::Tsv => ".tsv",
        }
    }

    /// Get the content type for this format
    pub fn content_type(&self) -> &'static str {
        match self {
            OutputFormat::Json => "application/json",
            OutputFormat::Jsonl => "application/x-ndjson",
            #[cfg(feature = "parquet")]
            OutputFormat::Parquet => "application/vnd.apache.parquet",
            OutputFormat::Avro => "application/avro",
            OutputFormat::Csv => "text/csv",
            OutputFormat::Tsv => "text/tab-separated-values",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_output_format_create_writer() {
        let events = vec![json!({"a": 1}), json!({"a": 2})];

        // Test JSON
        let writer = OutputFormat::Json.create_writer().unwrap();
        assert_eq!(writer.name(), "json");
        let bytes = writer.write_batch(&events).unwrap();
        assert!(!bytes.is_empty());

        // Test JSONL
        let writer = OutputFormat::Jsonl.create_writer().unwrap();
        assert_eq!(writer.name(), "jsonl");

        // Test Avro
        let writer = OutputFormat::Avro.create_writer().unwrap();
        assert_eq!(writer.name(), "avro");

        // Test CSV
        let writer = OutputFormat::Csv.create_writer().unwrap();
        assert_eq!(writer.name(), "csv");

        // Test TSV
        let writer = OutputFormat::Tsv.create_writer().unwrap();
        assert_eq!(writer.name(), "tsv");
    }

    #[test]
    fn test_output_format_extensions() {
        assert_eq!(OutputFormat::Json.extension(), ".json");
        assert_eq!(OutputFormat::Jsonl.extension(), ".jsonl");
        assert_eq!(OutputFormat::Avro.extension(), ".avro");
        assert_eq!(OutputFormat::Csv.extension(), ".csv");
        assert_eq!(OutputFormat::Tsv.extension(), ".tsv");
    }

    #[test]
    fn test_output_format_content_types() {
        assert_eq!(OutputFormat::Json.content_type(), "application/json");
        assert_eq!(OutputFormat::Jsonl.content_type(), "application/x-ndjson");
        assert_eq!(OutputFormat::Avro.content_type(), "application/avro");
        assert_eq!(OutputFormat::Csv.content_type(), "text/csv");
        assert_eq!(
            OutputFormat::Tsv.content_type(),
            "text/tab-separated-values"
        );
    }

    #[test]
    fn test_format_writer_polymorphism() {
        let events = vec![json!({"name": "Alice", "age": 30})];

        let writers: Vec<Box<dyn FormatWriter>> = vec![
            Box::new(JsonWriter::new(JsonWriterConfig::default())),
            Box::new(CsvWriter::new(CsvWriterConfig::default())),
            Box::new(AvroWriter::new(AvroWriterConfig::default()).unwrap()),
        ];

        for writer in writers {
            let result = writer.write_batch(&events);
            assert!(result.is_ok(), "Failed for format: {}", writer.name());
        }
    }

    #[test]
    fn test_append_support() {
        // JSONL and CSV support append
        let jsonl = JsonWriter::new(JsonWriterConfig {
            format: JsonFormat::Jsonl,
            ..Default::default()
        });
        assert!(jsonl.supports_append());

        let csv = CsvWriter::new(CsvWriterConfig::default());
        assert!(csv.supports_append());

        // JSON and Avro don't support append
        let json = JsonWriter::new(JsonWriterConfig {
            format: JsonFormat::Json,
            ..Default::default()
        });
        assert!(!json.supports_append());

        let avro = AvroWriter::new(AvroWriterConfig::default()).unwrap();
        assert!(!avro.supports_append());
    }
}
