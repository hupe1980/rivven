//! Output Compression for Format Writers
//!
//! This module provides compression wrappers for text-based formats like JSON, JSONL, and CSV.
//! Columnar formats (Parquet, Avro) have built-in compression and don't use this module.
//!
//! # Supported Compression Algorithms
//!
//! | Algorithm | Extension | Use Case | Speed | Ratio |
//! |-----------|-----------|----------|-------|-------|
//! | Gzip | `.gz` | Universal compatibility | Medium | Good |
//! | Zstd | `.zst` | Modern systems, best balance | Fast | Excellent |
//! | Snappy | `.snappy` | Kafka compatibility | Very Fast | Moderate |
//! | LZ4 | `.lz4` | Real-time streaming | Fastest | Moderate |
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::format::{
//!     OutputCompression, CompressedFormatWriter, JsonWriter, JsonWriterConfig, JsonFormat
//! };
//!
//! // Create a JSONL writer with Gzip compression
//! let inner = JsonWriter::new(JsonWriterConfig {
//!     format: JsonFormat::Jsonl,
//!     ..Default::default()
//! });
//!
//! let compressed_writer = CompressedFormatWriter::new(
//!     Box::new(inner),
//!     OutputCompression::Gzip,
//! );
//!
//! // Output will be written as .jsonl.gz
//! let bytes = compressed_writer.write_batch(&events)?;
//! ```

use super::{FormatError, FormatWriter};
use flate2::write::GzEncoder;
use flate2::Compression as GzipLevel;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::Write;

/// Output compression algorithm for text-based formats
///
/// Note: Parquet and Avro have their own built-in compression settings.
/// This is for compressing JSON, JSONL, and CSV output files.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum OutputCompression {
    /// No compression
    #[default]
    None,
    /// Gzip compression (.gz) - Universal compatibility
    Gzip,
    /// Zstandard compression (.zst) - Best balance of speed and ratio
    Zstd,
    /// Snappy compression (.snappy) - Kafka compatibility, very fast
    Snappy,
    /// LZ4 compression (.lz4) - Fastest, real-time streaming
    Lz4,
}

impl OutputCompression {
    /// Get file extension suffix for this compression
    pub fn extension_suffix(&self) -> &'static str {
        match self {
            Self::None => "",
            Self::Gzip => ".gz",
            Self::Zstd => ".zst",
            Self::Snappy => ".snappy",
            Self::Lz4 => ".lz4",
        }
    }

    /// Get content encoding header value
    pub fn content_encoding(&self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Gzip => Some("gzip"),
            Self::Zstd => Some("zstd"),
            Self::Snappy => Some("snappy"),
            Self::Lz4 => Some("lz4"),
        }
    }

    /// Check if compression is enabled
    pub fn is_enabled(&self) -> bool {
        !matches!(self, Self::None)
    }

    /// Compress data using this algorithm
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, FormatError> {
        match self {
            Self::None => Ok(data.to_vec()),
            Self::Gzip => compress_gzip(data),
            Self::Zstd => compress_zstd(data),
            Self::Snappy => compress_snappy(data),
            Self::Lz4 => compress_lz4(data),
        }
    }

    /// Parse from string (case-insensitive)
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "none" | "" => Some(Self::None),
            "gzip" | "gz" => Some(Self::Gzip),
            "zstd" | "zstandard" => Some(Self::Zstd),
            "snappy" => Some(Self::Snappy),
            "lz4" => Some(Self::Lz4),
            _ => None,
        }
    }
}

impl std::fmt::Display for OutputCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Gzip => write!(f, "gzip"),
            Self::Zstd => write!(f, "zstd"),
            Self::Snappy => write!(f, "snappy"),
            Self::Lz4 => write!(f, "lz4"),
        }
    }
}

// ============================================================================
// Compression Functions
// ============================================================================

fn compress_gzip(data: &[u8]) -> Result<Vec<u8>, FormatError> {
    let mut encoder = GzEncoder::new(Vec::new(), GzipLevel::default());
    encoder.write_all(data)?;
    encoder.finish().map_err(FormatError::Io)
}

fn compress_zstd(data: &[u8]) -> Result<Vec<u8>, FormatError> {
    zstd::bulk::compress(data, 3)
        .map_err(|e| FormatError::Write(format!("Zstd compression failed: {}", e)))
}

fn compress_snappy(data: &[u8]) -> Result<Vec<u8>, FormatError> {
    let mut encoder = snap::raw::Encoder::new();
    encoder
        .compress_vec(data)
        .map_err(|e| FormatError::Write(format!("Snappy compression failed: {}", e)))
}

fn compress_lz4(data: &[u8]) -> Result<Vec<u8>, FormatError> {
    Ok(lz4_flex::block::compress_prepend_size(data))
}

// ============================================================================
// Compressed Format Writer
// ============================================================================

/// A wrapper that adds compression to any FormatWriter
///
/// This allows compressing the output of any text-based format writer.
/// For columnar formats (Parquet, Avro), use their built-in compression instead.
pub struct CompressedFormatWriter {
    inner: Box<dyn FormatWriter>,
    compression: OutputCompression,
}

impl CompressedFormatWriter {
    /// Create a new compressed format writer
    pub fn new(inner: Box<dyn FormatWriter>, compression: OutputCompression) -> Self {
        Self { inner, compression }
    }

    /// Get the inner writer
    pub fn inner(&self) -> &dyn FormatWriter {
        self.inner.as_ref()
    }

    /// Get the compression algorithm
    pub fn compression(&self) -> &OutputCompression {
        &self.compression
    }
}

impl FormatWriter for CompressedFormatWriter {
    fn name(&self) -> &'static str {
        // Return the inner format name since compression is separate
        self.inner.name()
    }

    fn extension(&self) -> &'static str {
        // The actual extension with compression suffix is handled by storage connectors
        self.inner.extension()
    }

    fn content_type(&self) -> &'static str {
        self.inner.content_type()
    }

    fn write_batch(&self, events: &[Value]) -> Result<Vec<u8>, FormatError> {
        // First, write using the inner format
        let data = self.inner.write_batch(events)?;

        // Then compress the output
        self.compression.compress(&data)
    }

    fn supports_append(&self) -> bool {
        // Compressed output doesn't support append (need to decompress, append, recompress)
        false
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Create a compressed writer from an output format and compression setting
pub fn create_compressed_writer(
    format_writer: Box<dyn FormatWriter>,
    compression: OutputCompression,
) -> Box<dyn FormatWriter> {
    if compression.is_enabled() {
        Box::new(CompressedFormatWriter::new(format_writer, compression))
    } else {
        format_writer
    }
}

/// Get the full extension including compression suffix
pub fn full_extension(base_extension: &str, compression: &OutputCompression) -> String {
    format!("{}{}", base_extension, compression.extension_suffix())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{JsonFormat, JsonWriter, JsonWriterConfig};
    use serde_json::json;

    #[test]
    fn test_output_compression_extensions() {
        assert_eq!(OutputCompression::None.extension_suffix(), "");
        assert_eq!(OutputCompression::Gzip.extension_suffix(), ".gz");
        assert_eq!(OutputCompression::Zstd.extension_suffix(), ".zst");
        assert_eq!(OutputCompression::Snappy.extension_suffix(), ".snappy");
        assert_eq!(OutputCompression::Lz4.extension_suffix(), ".lz4");
    }

    #[test]
    fn test_output_compression_content_encoding() {
        assert_eq!(OutputCompression::None.content_encoding(), None);
        assert_eq!(OutputCompression::Gzip.content_encoding(), Some("gzip"));
        assert_eq!(OutputCompression::Zstd.content_encoding(), Some("zstd"));
        assert_eq!(OutputCompression::Snappy.content_encoding(), Some("snappy"));
        assert_eq!(OutputCompression::Lz4.content_encoding(), Some("lz4"));
    }

    #[test]
    fn test_gzip_compression() {
        let data = b"Hello, World! ".repeat(100);
        let compressed = compress_gzip(&data).unwrap();
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_zstd_compression() {
        let data = b"Hello, World! ".repeat(100);
        let compressed = compress_zstd(&data).unwrap();
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_snappy_compression() {
        let data = b"Hello, World! ".repeat(100);
        let compressed = compress_snappy(&data).unwrap();
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_lz4_compression() {
        let data = b"Hello, World! ".repeat(100);
        let compressed = compress_lz4(&data).unwrap();
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_compressed_format_writer() {
        let events = vec![
            json!({"name": "Alice", "value": 100}),
            json!({"name": "Bob", "value": 200}),
        ];

        let inner = JsonWriter::new(JsonWriterConfig {
            format: JsonFormat::Jsonl,
            ..Default::default()
        });

        let compressed_writer =
            CompressedFormatWriter::new(Box::new(inner), OutputCompression::Gzip);

        let result = compressed_writer.write_batch(&events).unwrap();

        // Compressed output should be smaller than uncompressed
        let inner2 = JsonWriter::new(JsonWriterConfig {
            format: JsonFormat::Jsonl,
            ..Default::default()
        });
        let uncompressed = inner2.write_batch(&events).unwrap();

        // For small data, compression might not help, but it should still work
        assert!(!result.is_empty());
        println!(
            "Uncompressed: {} bytes, Compressed: {} bytes",
            uncompressed.len(),
            result.len()
        );
    }

    #[test]
    fn test_full_extension() {
        assert_eq!(full_extension(".jsonl", &OutputCompression::None), ".jsonl");
        assert_eq!(
            full_extension(".jsonl", &OutputCompression::Gzip),
            ".jsonl.gz"
        );
        assert_eq!(full_extension(".csv", &OutputCompression::Zstd), ".csv.zst");
        assert_eq!(
            full_extension(".json", &OutputCompression::Snappy),
            ".json.snappy"
        );
    }

    #[test]
    fn test_compression_parse() {
        assert_eq!(
            OutputCompression::parse("gzip"),
            Some(OutputCompression::Gzip)
        );
        assert_eq!(
            OutputCompression::parse("GZIP"),
            Some(OutputCompression::Gzip)
        );
        assert_eq!(
            OutputCompression::parse("gz"),
            Some(OutputCompression::Gzip)
        );
        assert_eq!(
            OutputCompression::parse("zstd"),
            Some(OutputCompression::Zstd)
        );
        assert_eq!(
            OutputCompression::parse("snappy"),
            Some(OutputCompression::Snappy)
        );
        assert_eq!(
            OutputCompression::parse("lz4"),
            Some(OutputCompression::Lz4)
        );
        assert_eq!(
            OutputCompression::parse("none"),
            Some(OutputCompression::None)
        );
        assert_eq!(OutputCompression::parse(""), Some(OutputCompression::None));
        assert_eq!(OutputCompression::parse("invalid"), None);
    }

    #[test]
    fn test_create_compressed_writer_none() {
        let events = vec![json!({"a": 1})];

        let inner = JsonWriter::new(JsonWriterConfig::default());
        let writer = create_compressed_writer(Box::new(inner), OutputCompression::None);

        // Should return the inner writer (no compression wrapper)
        let result = writer.write_batch(&events).unwrap();
        assert!(!result.is_empty());
    }
}
