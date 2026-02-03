//! Common types and utilities for object storage connectors
//!
//! This module provides shared functionality across S3, GCS, and Azure Blob connectors:
//! - Output format enums (JSON, JSONL, Avro, CSV, Parquet)
//! - Partitioning strategies (None, Day, Hour)
//! - Compression options (None, Gzip)
//! - Object path generation
//! - Batch serialization
//! - Content-type resolution

use crate::error::{ConnectorError, Result};
use crate::format::avro::{write_events_to_avro, AvroWriterConfig};
use crate::traits::event::SourceEvent;
use chrono::{DateTime, Utc};
use flate2::write::GzEncoder;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::io::Write;

// ============================================================================
// CSV Helpers
// ============================================================================

/// Escape a field for CSV output (RFC 4180 compliant)
fn escape_csv_field(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') || field.contains('\r') {
        // Quote the field and escape any quotes by doubling them
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

// ============================================================================
// Output Format
// ============================================================================

/// Output format for storage connectors
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StorageFormat {
    /// JSON - single JSON object per file
    #[default]
    Json,
    /// JSONL - newline-delimited JSON (one object per line)
    Jsonl,
    /// Apache Avro binary format
    Avro,
    /// CSV with headers
    Csv,
    /// Apache Parquet columnar format
    #[cfg(feature = "parquet")]
    Parquet,
}

impl StorageFormat {
    /// Get the file extension for this format (without compression)
    pub fn extension(&self) -> &'static str {
        match self {
            StorageFormat::Json => "json",
            StorageFormat::Jsonl => "jsonl",
            StorageFormat::Avro => "avro",
            StorageFormat::Csv => "csv",
            #[cfg(feature = "parquet")]
            StorageFormat::Parquet => "parquet",
        }
    }

    /// Get the MIME content type for this format (without compression)
    pub fn content_type(&self) -> &'static str {
        match self {
            StorageFormat::Json => "application/json",
            StorageFormat::Jsonl => "application/x-ndjson",
            StorageFormat::Avro => "application/avro",
            StorageFormat::Csv => "text/csv",
            #[cfg(feature = "parquet")]
            StorageFormat::Parquet => "application/vnd.apache.parquet",
        }
    }
}

// ============================================================================
// Partitioning Strategy
// ============================================================================

/// Time-based partitioning strategy for object keys
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StoragePartitioning {
    /// No partitioning - flat file structure
    #[default]
    None,
    /// Daily partitioning: year=YYYY/month=MM/day=DD/
    Day,
    /// Hourly partitioning: year=YYYY/month=MM/day=DD/hour=HH/
    Hour,
}

impl StoragePartitioning {
    /// Generate the partition path for a given timestamp
    pub fn partition_path(&self, timestamp: DateTime<Utc>) -> String {
        match self {
            StoragePartitioning::None => String::new(),
            StoragePartitioning::Day => {
                format!(
                    "year={}/month={:02}/day={:02}/",
                    timestamp.format("%Y"),
                    timestamp.format("%m"),
                    timestamp.format("%d")
                )
            }
            StoragePartitioning::Hour => {
                format!(
                    "year={}/month={:02}/day={:02}/hour={:02}/",
                    timestamp.format("%Y"),
                    timestamp.format("%m"),
                    timestamp.format("%d"),
                    timestamp.format("%H")
                )
            }
        }
    }
}

// ============================================================================
// Compression
// ============================================================================

/// Compression mode for storage output
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StorageCompression {
    /// No compression
    #[default]
    None,
    /// Gzip compression
    Gzip,
}

impl StorageCompression {
    /// Get the file extension suffix for compression (empty for None)
    pub fn extension_suffix(&self) -> &'static str {
        match self {
            StorageCompression::None => "",
            StorageCompression::Gzip => ".gz",
        }
    }

    /// Compress data if needed
    pub fn maybe_compress(&self, data: Vec<u8>) -> Result<Vec<u8>> {
        match self {
            StorageCompression::None => Ok(data),
            StorageCompression::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(&data).map_err(ConnectorError::Io)?;
                Ok(encoder.finish().map_err(ConnectorError::Io)?)
            }
        }
    }
}

// ============================================================================
// Object Key Generation
// ============================================================================

/// Generate an object key/path for storage
pub fn generate_object_key(
    prefix: &str,
    partitioning: &StoragePartitioning,
    format: &StorageFormat,
    compression: &StorageCompression,
    timestamp: DateTime<Utc>,
    batch_id: u64,
) -> String {
    // Normalize prefix (remove trailing slash if present)
    let prefix = prefix.trim_end_matches('/');

    // Build partition path
    let partition = partitioning.partition_path(timestamp);

    // Build extension
    let ext = format!("{}{}", format.extension(), compression.extension_suffix());

    // Format: prefix/partition/timestamp_batchid.ext
    if prefix.is_empty() {
        format!(
            "{}{}_{:06}.{}",
            partition,
            timestamp.format("%Y%m%d_%H%M%S"),
            batch_id,
            ext
        )
    } else {
        format!(
            "{}/{}{}_{:06}.{}",
            prefix,
            partition,
            timestamp.format("%Y%m%d_%H%M%S"),
            batch_id,
            ext
        )
    }
}

/// Get the content type for a format + compression combination
pub fn get_content_type(format: &StorageFormat, compression: &StorageCompression) -> &'static str {
    match compression {
        StorageCompression::Gzip => "application/gzip",
        StorageCompression::None => format.content_type(),
    }
}

// ============================================================================
// Batch Serialization
// ============================================================================

/// Convert a SourceEvent to a JSON Value for storage
pub fn event_to_json(event: &SourceEvent) -> serde_json::Value {
    serde_json::json!({
        "event_type": event.event_type,
        "stream": event.stream,
        "namespace": event.namespace,
        "timestamp": event.timestamp.to_rfc3339(),
        "data": event.data,
        "metadata": event.metadata,
    })
}

/// Serialize a batch of events to the specified format
pub fn serialize_batch(
    batch: &[serde_json::Value],
    format: &StorageFormat,
    compression: &StorageCompression,
) -> Result<Vec<u8>> {
    let data = match format {
        StorageFormat::Json => serde_json::to_vec_pretty(batch)
            .map_err(|e| ConnectorError::Serialization(e.to_string()))?,
        StorageFormat::Jsonl => {
            let mut output = Vec::new();
            for event in batch {
                serde_json::to_writer(&mut output, event)
                    .map_err(|e| ConnectorError::Serialization(e.to_string()))?;
                output.push(b'\n');
            }
            output
        }
        StorageFormat::Avro => {
            // Use the proper AvroWriter with schema inference
            let config = AvroWriterConfig::default();
            write_events_to_avro(batch, Some(config))
                .map_err(|e| ConnectorError::Serialization(e.to_string()))?
        }
        StorageFormat::Csv => {
            // Simple CSV serialization with headers
            let mut output = Vec::new();
            // Write header
            output.extend_from_slice(b"event_type,stream,namespace,timestamp,data,metadata\n");
            for event in batch {
                // Escape and quote fields for CSV
                let row = format!(
                    "{},{},{},{},{},{}\n",
                    escape_csv_field(event["event_type"].as_str().unwrap_or("")),
                    escape_csv_field(event["stream"].as_str().unwrap_or("")),
                    escape_csv_field(event["namespace"].as_str().unwrap_or("")),
                    escape_csv_field(event["timestamp"].as_str().unwrap_or("")),
                    escape_csv_field(&event["data"].to_string()),
                    escape_csv_field(&event["metadata"].to_string()),
                );
                output.extend_from_slice(row.as_bytes());
            }
            output
        }
        #[cfg(feature = "parquet")]
        StorageFormat::Parquet => {
            // Parquet serialization using arrow-json for conversion
            use arrow::json::ReaderBuilder;
            use parquet::arrow::ArrowWriter;
            use std::io::Cursor;
            use std::sync::Arc;

            if batch.is_empty() {
                return Ok(Vec::new());
            }

            // Convert batch to NDJSON for arrow-json reader
            let mut ndjson = Vec::new();
            for event in batch {
                serde_json::to_writer(&mut ndjson, event)
                    .map_err(|e| ConnectorError::Serialization(e.to_string()))?;
                ndjson.push(b'\n');
            }

            // Infer schema from the first record (returns (Schema, usize) tuple)
            let (schema, _) = arrow::json::reader::infer_json_schema_from_seekable(
                &mut Cursor::new(&ndjson),
                None,
            )
            .map_err(|e| {
                ConnectorError::Serialization(format!("Arrow schema inference failed: {}", e))
            })?;

            let schema = Arc::new(schema);

            // Create record batch reader
            let reader = ReaderBuilder::new(schema.clone())
                .build(Cursor::new(&ndjson))
                .map_err(|e| {
                    ConnectorError::Serialization(format!("Arrow reader failed: {}", e))
                })?;

            // Write to Parquet
            let mut parquet_buffer = Vec::new();
            let props = parquet::file::properties::WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(&mut parquet_buffer, schema, Some(props))
                .map_err(|e| {
                    ConnectorError::Serialization(format!("Parquet writer failed: {}", e))
                })?;

            for batch_result in reader {
                let record_batch = batch_result.map_err(|e| {
                    ConnectorError::Serialization(format!("Arrow batch failed: {}", e))
                })?;
                writer.write(&record_batch).map_err(|e| {
                    ConnectorError::Serialization(format!("Parquet write failed: {}", e))
                })?;
            }

            writer.close().map_err(|e| {
                ConnectorError::Serialization(format!("Parquet close failed: {}", e))
            })?;

            parquet_buffer
        }
    };

    compression.maybe_compress(data)
}

// ============================================================================
// Common Defaults
// ============================================================================

/// Default batch size for storage connectors
pub fn default_batch_size() -> usize {
    1000
}

/// Default flush interval in seconds for storage connectors
pub fn default_flush_interval_secs() -> u64 {
    60
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_storage_format_extension() {
        assert_eq!(StorageFormat::Json.extension(), "json");
        assert_eq!(StorageFormat::Jsonl.extension(), "jsonl");
        assert_eq!(StorageFormat::Avro.extension(), "avro");
        assert_eq!(StorageFormat::Csv.extension(), "csv");
    }

    #[test]
    fn test_storage_format_content_type() {
        assert_eq!(StorageFormat::Json.content_type(), "application/json");
        assert_eq!(StorageFormat::Jsonl.content_type(), "application/x-ndjson");
        assert_eq!(StorageFormat::Csv.content_type(), "text/csv");
    }

    #[test]
    fn test_partitioning_none() {
        let ts = Utc.with_ymd_and_hms(2024, 3, 15, 10, 30, 0).unwrap();
        assert_eq!(StoragePartitioning::None.partition_path(ts), "");
    }

    #[test]
    fn test_partitioning_day() {
        let ts = Utc.with_ymd_and_hms(2024, 3, 15, 10, 30, 0).unwrap();
        assert_eq!(
            StoragePartitioning::Day.partition_path(ts),
            "year=2024/month=03/day=15/"
        );
    }

    #[test]
    fn test_partitioning_hour() {
        let ts = Utc.with_ymd_and_hms(2024, 3, 15, 10, 30, 0).unwrap();
        assert_eq!(
            StoragePartitioning::Hour.partition_path(ts),
            "year=2024/month=03/day=15/hour=10/"
        );
    }

    #[test]
    fn test_compression_suffix() {
        assert_eq!(StorageCompression::None.extension_suffix(), "");
        assert_eq!(StorageCompression::Gzip.extension_suffix(), ".gz");
    }

    #[test]
    fn test_generate_object_key_no_partition() {
        let ts = Utc.with_ymd_and_hms(2024, 3, 15, 10, 30, 0).unwrap();
        let key = generate_object_key(
            "events",
            &StoragePartitioning::None,
            &StorageFormat::Json,
            &StorageCompression::None,
            ts,
            42,
        );
        assert_eq!(key, "events/20240315_103000_000042.json");
    }

    #[test]
    fn test_generate_object_key_day_partition() {
        let ts = Utc.with_ymd_and_hms(2024, 3, 15, 10, 30, 0).unwrap();
        let key = generate_object_key(
            "events",
            &StoragePartitioning::Day,
            &StorageFormat::Jsonl,
            &StorageCompression::Gzip,
            ts,
            1,
        );
        assert_eq!(
            key,
            "events/year=2024/month=03/day=15/20240315_103000_000001.jsonl.gz"
        );
    }

    #[test]
    fn test_content_type_with_compression() {
        assert_eq!(
            get_content_type(&StorageFormat::Json, &StorageCompression::None),
            "application/json"
        );
        assert_eq!(
            get_content_type(&StorageFormat::Json, &StorageCompression::Gzip),
            "application/gzip"
        );
    }

    #[test]
    fn test_serialize_batch_json() {
        let batch = vec![serde_json::json!({"id": 1}), serde_json::json!({"id": 2})];
        let data =
            serialize_batch(&batch, &StorageFormat::Json, &StorageCompression::None).unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_slice(&data).unwrap();
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn test_serialize_batch_jsonl() {
        let batch = vec![serde_json::json!({"id": 1}), serde_json::json!({"id": 2})];
        let data =
            serialize_batch(&batch, &StorageFormat::Jsonl, &StorageCompression::None).unwrap();
        let lines: Vec<&str> = std::str::from_utf8(&data)
            .unwrap()
            .lines()
            .filter(|l| !l.is_empty())
            .collect();
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn test_compression_gzip() {
        let data = b"hello world".to_vec();
        let compressed = StorageCompression::Gzip
            .maybe_compress(data.clone())
            .unwrap();
        assert!(!compressed.is_empty());
        // Gzip magic number
        assert_eq!(compressed[0], 0x1f);
        assert_eq!(compressed[1], 0x8b);
    }

    #[test]
    fn test_serialize_batch_avro() {
        let batch = vec![
            serde_json::json!({"id": 1, "name": "Alice", "active": true}),
            serde_json::json!({"id": 2, "name": "Bob", "active": false}),
        ];
        let data =
            serialize_batch(&batch, &StorageFormat::Avro, &StorageCompression::None).unwrap();

        // Verify it's a valid Avro OCF (starts with "Obj\x01")
        assert!(data.len() > 4);
        assert_eq!(&data[0..4], b"Obj\x01");
    }

    #[test]
    fn test_serialize_batch_avro_empty() {
        let batch: Vec<serde_json::Value> = vec![];
        let result = serialize_batch(&batch, &StorageFormat::Avro, &StorageCompression::None);
        // Empty batch should fail because we can't infer schema
        assert!(result.is_err());
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn test_serialize_batch_parquet() {
        let batch = vec![
            serde_json::json!({"id": 1, "name": "Alice"}),
            serde_json::json!({"id": 2, "name": "Bob"}),
        ];
        let data =
            serialize_batch(&batch, &StorageFormat::Parquet, &StorageCompression::None).unwrap();

        // Verify it's a valid Parquet file (starts with "PAR1")
        assert!(data.len() > 4);
        assert_eq!(&data[0..4], b"PAR1");
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn test_serialize_batch_parquet_empty() {
        let batch: Vec<serde_json::Value> = vec![];
        let data =
            serialize_batch(&batch, &StorageFormat::Parquet, &StorageCompression::None).unwrap();
        // Empty batch should return empty data
        assert!(data.is_empty());
    }
}
