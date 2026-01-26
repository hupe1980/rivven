//! High-performance compression layer for Rivven
//!
//! This module provides a zero-copy, adaptive compression layer supporting:
//! - **LZ4**: Ultra-fast compression for latency-sensitive paths (~3GB/s)
//! - **Zstd**: High-ratio compression for storage and network (~500MB/s)
//! - **None**: Passthrough for already-compressed or tiny payloads
//!
//! # Design Principles
//!
//! 1. **Adaptive Selection**: Automatically choose algorithm based on payload characteristics
//! 2. **Zero-Copy**: Use `Bytes` throughout to avoid unnecessary copies
//! 3. **Streaming Support**: Compress/decompress incrementally for large payloads
//! 4. **Header Format**: Minimal overhead (1-byte header for algorithm + optional size)
//!
//! # Wire Format
//!
//! ```text
//! +-------+----------------+------------------+
//! | Flags | Original Size  | Compressed Data  |
//! | 1 byte| 4 bytes (opt)  | N bytes          |
//! +-------+----------------+------------------+
//!
//! Flags byte:
//!   bits 0-1: Algorithm (00=None, 01=LZ4, 10=Zstd)
//!   bits 2-3: Reserved
//!   bit 4:    Has original size (for decompression buffer allocation)
//!   bits 5-7: Reserved
//! ```

use bytes::{BufMut, Bytes, BytesMut};
use std::io::{Read, Write};
use thiserror::Error;

// ============================================================================
// Error Types
// ============================================================================

#[derive(Debug, Error)]
pub enum CompressionError {
    #[error("LZ4 compression failed: {0}")]
    Lz4Error(String),

    #[error("Zstd compression failed: {0}")]
    ZstdError(String),

    #[error("Invalid compression header")]
    InvalidHeader,

    #[error("Decompression buffer too small: need {needed}, have {available}")]
    BufferTooSmall { needed: usize, available: usize },

    #[error("Unknown compression algorithm: {0}")]
    UnknownAlgorithm(u8),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, CompressionError>;

// ============================================================================
// Compression Algorithm
// ============================================================================

/// Compression algorithm selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum CompressionAlgorithm {
    /// No compression (passthrough)
    #[default]
    None = 0,
    /// LZ4 - Ultra-fast, moderate compression ratio
    /// Best for: Real-time streaming, low-latency paths
    Lz4 = 1,
    /// Zstd - Balanced speed and compression ratio
    /// Best for: Storage, network transfers, cold data
    Zstd = 2,
}

impl CompressionAlgorithm {
    /// Parse from flags byte
    pub fn from_flags(flags: u8) -> Result<Self> {
        match flags & 0x03 {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            2 => Ok(Self::Zstd),
            n => Err(CompressionError::UnknownAlgorithm(n)),
        }
    }

    /// Convert to flags byte
    pub fn to_flags(self, has_size: bool) -> u8 {
        let mut flags = self as u8;
        if has_size {
            flags |= 0x10; // Set bit 4
        }
        flags
    }

    /// Get human-readable name
    pub fn name(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }
}

// ============================================================================
// Compression Level
// ============================================================================

/// Compression level presets
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionLevel {
    /// Fastest compression, lowest ratio
    Fast,
    /// Balanced speed and ratio (default)
    #[default]
    Default,
    /// Best compression ratio, slower
    Best,
    /// Custom level (algorithm-specific)
    Custom(i32),
}

impl CompressionLevel {
    /// Get LZ4 acceleration parameter (higher = faster, lower ratio)
    fn lz4_acceleration(&self) -> i32 {
        match self {
            Self::Fast => 65537, // Max acceleration
            Self::Default => 1,  // Default
            Self::Best => 1,     // LZ4 doesn't have "best", use default
            Self::Custom(n) => *n,
        }
    }

    /// Get Zstd compression level (1-22, higher = better ratio)
    fn zstd_level(&self) -> i32 {
        match self {
            Self::Fast => 1,
            Self::Default => 3, // Zstd default
            Self::Best => 19,   // High compression
            Self::Custom(n) => *n,
        }
    }
}

// ============================================================================
// Compressor Configuration
// ============================================================================

/// Configuration for the compression layer
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Default algorithm for new data
    pub algorithm: CompressionAlgorithm,
    /// Compression level
    pub level: CompressionLevel,
    /// Minimum payload size to compress (bytes)
    /// Payloads smaller than this are stored uncompressed
    pub min_size: usize,
    /// Compression ratio threshold (0.0-1.0)
    /// If compressed size > original * threshold, store uncompressed
    pub ratio_threshold: f32,
    /// Enable adaptive algorithm selection
    pub adaptive: bool,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            algorithm: CompressionAlgorithm::Lz4,
            level: CompressionLevel::Default,
            min_size: 64,          // Don't compress < 64 bytes
            ratio_threshold: 0.95, // Must achieve at least 5% reduction
            adaptive: true,
        }
    }
}

impl CompressionConfig {
    /// Create config optimized for low latency
    pub fn low_latency() -> Self {
        Self {
            algorithm: CompressionAlgorithm::Lz4,
            level: CompressionLevel::Fast,
            min_size: 128,
            ratio_threshold: 0.90,
            adaptive: false,
        }
    }

    /// Create config optimized for storage efficiency
    pub fn storage() -> Self {
        Self {
            algorithm: CompressionAlgorithm::Zstd,
            level: CompressionLevel::Default,
            min_size: 32,
            ratio_threshold: 0.98,
            adaptive: true,
        }
    }

    /// Create config optimized for network transfer
    pub fn network() -> Self {
        Self {
            algorithm: CompressionAlgorithm::Zstd,
            level: CompressionLevel::Fast,
            min_size: 64,
            ratio_threshold: 0.95,
            adaptive: true,
        }
    }
}

// ============================================================================
// Core Compression Functions
// ============================================================================

/// Compress data using LZ4
fn compress_lz4(data: &[u8], level: CompressionLevel) -> Result<Vec<u8>> {
    // LZ4 block compression
    // For LZ4, we use FAST mode with acceleration (higher = faster but less compression)
    let mode = match level {
        CompressionLevel::Fast => lz4::block::CompressionMode::FAST(65537),
        CompressionLevel::Default => lz4::block::CompressionMode::DEFAULT,
        CompressionLevel::Best => lz4::block::CompressionMode::HIGHCOMPRESSION(9),
        CompressionLevel::Custom(n) if n > 0 => lz4::block::CompressionMode::FAST(n),
        CompressionLevel::Custom(n) => lz4::block::CompressionMode::HIGHCOMPRESSION(-n),
    };

    lz4::block::compress(data, Some(mode), false)
        .map_err(|e| CompressionError::Lz4Error(e.to_string()))
}

/// Decompress LZ4 data
fn decompress_lz4(data: &[u8], original_size: Option<usize>) -> Result<Vec<u8>> {
    let uncompressed_size = original_size.unwrap_or(data.len() * 4); // Estimate 4x expansion

    lz4::block::decompress(data, Some(uncompressed_size as i32))
        .map_err(|e| CompressionError::Lz4Error(e.to_string()))
}

/// Compress data using Zstd
fn compress_zstd(data: &[u8], level: CompressionLevel) -> Result<Vec<u8>> {
    let level = level.zstd_level();

    zstd::bulk::compress(data, level).map_err(|e| CompressionError::ZstdError(e.to_string()))
}

/// Decompress Zstd data
fn decompress_zstd(data: &[u8]) -> Result<Vec<u8>> {
    zstd::bulk::decompress(data, 16 * 1024 * 1024) // 16 MB max
        .map_err(|e| CompressionError::ZstdError(e.to_string()))
}

// ============================================================================
// Compressor
// ============================================================================

/// High-performance compressor with configurable algorithms
#[derive(Debug, Clone)]
pub struct Compressor {
    config: CompressionConfig,
}

impl Compressor {
    /// Create compressor with default config
    pub fn new() -> Self {
        Self {
            config: CompressionConfig::default(),
        }
    }

    /// Create compressor with custom config
    pub fn with_config(config: CompressionConfig) -> Self {
        Self { config }
    }

    /// Compress data, returning compressed bytes with header
    pub fn compress(&self, data: &[u8]) -> Result<Bytes> {
        // Skip compression for small payloads
        if data.len() < self.config.min_size {
            return Ok(self.encode_uncompressed(data));
        }

        // Select algorithm (adaptive or configured)
        let algorithm = if self.config.adaptive {
            self.select_algorithm(data)
        } else {
            self.config.algorithm
        };

        // Compress based on algorithm
        let compressed = match algorithm {
            CompressionAlgorithm::None => {
                return Ok(self.encode_uncompressed(data));
            }
            CompressionAlgorithm::Lz4 => compress_lz4(data, self.config.level)?,
            CompressionAlgorithm::Zstd => compress_zstd(data, self.config.level)?,
        };

        // Check if compression was worthwhile
        let ratio = compressed.len() as f32 / data.len() as f32;
        if ratio > self.config.ratio_threshold {
            // Compression didn't help enough, store uncompressed
            return Ok(self.encode_uncompressed(data));
        }

        // Encode with header
        self.encode_compressed(algorithm, data.len(), &compressed)
    }

    /// Compress data with explicit algorithm choice
    pub fn compress_with(&self, data: &[u8], algorithm: CompressionAlgorithm) -> Result<Bytes> {
        if algorithm == CompressionAlgorithm::None || data.len() < self.config.min_size {
            return Ok(self.encode_uncompressed(data));
        }

        let compressed = match algorithm {
            CompressionAlgorithm::None => unreachable!(),
            CompressionAlgorithm::Lz4 => compress_lz4(data, self.config.level)?,
            CompressionAlgorithm::Zstd => compress_zstd(data, self.config.level)?,
        };

        self.encode_compressed(algorithm, data.len(), &compressed)
    }

    /// Decompress data (auto-detects algorithm from header)
    pub fn decompress(&self, data: &[u8]) -> Result<Bytes> {
        if data.is_empty() {
            return Err(CompressionError::InvalidHeader);
        }

        let flags = data[0];
        let algorithm = CompressionAlgorithm::from_flags(flags)?;
        let has_size = (flags & 0x10) != 0;

        let (original_size, payload_start) = if has_size {
            if data.len() < 5 {
                return Err(CompressionError::InvalidHeader);
            }
            let size_bytes: [u8; 4] = data[1..5].try_into().unwrap();
            (Some(u32::from_le_bytes(size_bytes) as usize), 5)
        } else {
            (None, 1)
        };

        let payload = &data[payload_start..];

        let decompressed = match algorithm {
            CompressionAlgorithm::None => payload.to_vec(),
            CompressionAlgorithm::Lz4 => decompress_lz4(payload, original_size)?,
            CompressionAlgorithm::Zstd => decompress_zstd(payload)?,
        };

        Ok(Bytes::from(decompressed))
    }

    /// Get compression statistics for data
    pub fn stats(&self, data: &[u8]) -> CompressionStats {
        let lz4_result = compress_lz4(data, self.config.level);
        let zstd_result = compress_zstd(data, self.config.level);

        CompressionStats {
            original_size: data.len(),
            lz4_size: lz4_result.as_ref().map(|v| v.len()).ok(),
            zstd_size: zstd_result.as_ref().map(|v| v.len()).ok(),
            recommended: self.select_algorithm(data),
        }
    }

    /// Select best algorithm based on payload characteristics
    fn select_algorithm(&self, data: &[u8]) -> CompressionAlgorithm {
        // Heuristics for algorithm selection:
        // 1. Very small data: No compression
        // 2. Detect high entropy (random/encrypted): No compression
        // 3. Text-like data: Zstd (better ratio)
        // 4. Binary data: LZ4 (faster)

        if data.len() < self.config.min_size {
            return CompressionAlgorithm::None;
        }

        // Quick entropy estimation using byte frequency
        let entropy = estimate_entropy(data);

        if entropy > 7.5 {
            // High entropy - likely already compressed or encrypted
            return CompressionAlgorithm::None;
        }

        if entropy < 5.0 || data.len() > 64 * 1024 {
            // Low entropy or large payload - Zstd shines
            return CompressionAlgorithm::Zstd;
        }

        // Default to LZ4 for speed
        CompressionAlgorithm::Lz4
    }

    /// Encode uncompressed data with header
    fn encode_uncompressed(&self, data: &[u8]) -> Bytes {
        let mut buf = BytesMut::with_capacity(1 + data.len());
        buf.put_u8(CompressionAlgorithm::None.to_flags(false));
        buf.put_slice(data);
        buf.freeze()
    }

    /// Encode compressed data with header
    fn encode_compressed(
        &self,
        algorithm: CompressionAlgorithm,
        original_size: usize,
        compressed: &[u8],
    ) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(5 + compressed.len());
        buf.put_u8(algorithm.to_flags(true));
        buf.put_u32_le(original_size as u32);
        buf.put_slice(compressed);
        Ok(buf.freeze())
    }
}

impl Default for Compressor {
    fn default() -> Self {
        Self::new()
    }
}

/// Compression statistics for analysis
#[derive(Debug, Clone)]
pub struct CompressionStats {
    pub original_size: usize,
    pub lz4_size: Option<usize>,
    pub zstd_size: Option<usize>,
    pub recommended: CompressionAlgorithm,
}

impl CompressionStats {
    pub fn lz4_ratio(&self) -> Option<f32> {
        self.lz4_size.map(|s| s as f32 / self.original_size as f32)
    }

    pub fn zstd_ratio(&self) -> Option<f32> {
        self.zstd_size.map(|s| s as f32 / self.original_size as f32)
    }
}

/// Estimate Shannon entropy of data (bits per byte, 0-8)
fn estimate_entropy(data: &[u8]) -> f32 {
    if data.is_empty() {
        return 0.0;
    }

    // Sample for large data to keep this fast
    let sample_size = data.len().min(4096);
    let sample = &data[..sample_size];

    // Count byte frequencies
    let mut freq = [0u32; 256];
    for &byte in sample {
        freq[byte as usize] += 1;
    }

    // Calculate entropy
    let len = sample.len() as f32;
    let mut entropy = 0.0f32;

    for count in freq.iter() {
        if *count > 0 {
            let p = *count as f32 / len;
            entropy -= p * p.log2();
        }
    }

    entropy
}

// ============================================================================
// Streaming Compression (for large payloads)
// ============================================================================

/// Streaming compressor for large data
pub struct StreamingCompressor<W: Write> {
    encoder: StreamingEncoder<W>,
}

enum StreamingEncoder<W: Write> {
    Lz4(lz4::Encoder<W>),
    Zstd(zstd::Encoder<'static, W>),
    None(W),
}

impl<W: Write> StreamingCompressor<W> {
    /// Create streaming compressor
    pub fn new(
        writer: W,
        algorithm: CompressionAlgorithm,
        level: CompressionLevel,
    ) -> Result<Self> {
        let encoder = match algorithm {
            CompressionAlgorithm::None => StreamingEncoder::None(writer),
            CompressionAlgorithm::Lz4 => {
                let encoder = lz4::EncoderBuilder::new()
                    .level(level.lz4_acceleration().try_into().unwrap_or(4))
                    .build(writer)
                    .map_err(|e| CompressionError::Lz4Error(e.to_string()))?;
                StreamingEncoder::Lz4(encoder)
            }
            CompressionAlgorithm::Zstd => {
                let encoder = zstd::Encoder::new(writer, level.zstd_level())
                    .map_err(|e| CompressionError::ZstdError(e.to_string()))?;
                StreamingEncoder::Zstd(encoder)
            }
        };

        Ok(Self { encoder })
    }

    /// Write data to compressor
    pub fn write(&mut self, data: &[u8]) -> Result<usize> {
        match &mut self.encoder {
            StreamingEncoder::None(w) => Ok(w.write(data)?),
            StreamingEncoder::Lz4(e) => Ok(e.write(data)?),
            StreamingEncoder::Zstd(e) => Ok(e.write(data)?),
        }
    }

    /// Finish compression and return the underlying writer
    pub fn finish(self) -> Result<W> {
        match self.encoder {
            StreamingEncoder::None(w) => Ok(w),
            StreamingEncoder::Lz4(e) => {
                let (w, result) = e.finish();
                result.map_err(|e| CompressionError::Lz4Error(e.to_string()))?;
                Ok(w)
            }
            StreamingEncoder::Zstd(e) => e
                .finish()
                .map_err(|e| CompressionError::ZstdError(e.to_string())),
        }
    }
}

/// Streaming decompressor for large data
pub struct StreamingDecompressor<R: Read> {
    decoder: StreamingDecoder<R>,
}

enum StreamingDecoder<R: Read> {
    Lz4(lz4::Decoder<R>),
    Zstd(zstd::Decoder<'static, std::io::BufReader<R>>),
    None(R),
}

impl<R: Read> StreamingDecompressor<R> {
    /// Create streaming decompressor
    pub fn new(reader: R, algorithm: CompressionAlgorithm) -> Result<Self> {
        let decoder = match algorithm {
            CompressionAlgorithm::None => StreamingDecoder::None(reader),
            CompressionAlgorithm::Lz4 => {
                let decoder = lz4::Decoder::new(reader)
                    .map_err(|e| CompressionError::Lz4Error(e.to_string()))?;
                StreamingDecoder::Lz4(decoder)
            }
            CompressionAlgorithm::Zstd => {
                let decoder = zstd::Decoder::new(reader)
                    .map_err(|e| CompressionError::ZstdError(e.to_string()))?;
                StreamingDecoder::Zstd(decoder)
            }
        };

        Ok(Self { decoder })
    }

    /// Read decompressed data
    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match &mut self.decoder {
            StreamingDecoder::None(r) => Ok(r.read(buf)?),
            StreamingDecoder::Lz4(d) => Ok(d.read(buf)?),
            StreamingDecoder::Zstd(d) => Ok(d.read(buf)?),
        }
    }
}

// ============================================================================
// Batch Compression (for message batches)
// ============================================================================

/// Compress multiple messages as a batch for better compression ratio
pub struct BatchCompressor {
    compressor: Compressor,
    buffer: BytesMut,
    message_offsets: Vec<u32>,
}

impl BatchCompressor {
    /// Create new batch compressor
    pub fn new(config: CompressionConfig) -> Self {
        Self {
            compressor: Compressor::with_config(config),
            buffer: BytesMut::with_capacity(64 * 1024),
            message_offsets: Vec::with_capacity(100),
        }
    }

    /// Add message to batch
    pub fn add(&mut self, data: &[u8]) {
        self.message_offsets.push(self.buffer.len() as u32);
        // Write length-prefixed message
        self.buffer.put_u32_le(data.len() as u32);
        self.buffer.put_slice(data);
    }

    /// Compress the batch and return compressed data with metadata
    pub fn finish(self) -> Result<CompressedBatch> {
        let message_count = self.message_offsets.len();
        let uncompressed_size = self.buffer.len();

        // Compress the batch
        let compressed = self.compressor.compress(&self.buffer)?;

        Ok(CompressedBatch {
            data: compressed,
            message_count,
            uncompressed_size,
        })
    }

    /// Reset for reuse
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.message_offsets.clear();
    }
}

/// A compressed batch of messages
#[derive(Debug, Clone)]
pub struct CompressedBatch {
    pub data: Bytes,
    pub message_count: usize,
    pub uncompressed_size: usize,
}

impl CompressedBatch {
    /// Decompress and iterate over messages
    pub fn decompress(&self) -> Result<BatchIterator> {
        let compressor = Compressor::new();
        let decompressed = compressor.decompress(&self.data)?;

        Ok(BatchIterator {
            data: decompressed,
            position: 0,
        })
    }

    /// Get compression ratio
    pub fn ratio(&self) -> f32 {
        self.data.len() as f32 / self.uncompressed_size as f32
    }
}

/// Iterator over messages in a decompressed batch
pub struct BatchIterator {
    data: Bytes,
    position: usize,
}

impl Iterator for BatchIterator {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position + 4 > self.data.len() {
            return None;
        }

        let len_bytes: [u8; 4] = self.data[self.position..self.position + 4]
            .try_into()
            .ok()?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        self.position += 4;

        if self.position + len > self.data.len() {
            return None;
        }

        let message = self.data.slice(self.position..self.position + len);
        self.position += len;

        Some(message)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_lz4() {
        let data = b"Hello, World! This is a test of LZ4 compression. ".repeat(100);
        let compressor = Compressor::with_config(CompressionConfig {
            algorithm: CompressionAlgorithm::Lz4,
            adaptive: false,
            ..Default::default()
        });

        let compressed = compressor.compress(&data).unwrap();
        assert!(compressed.len() < data.len());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(&decompressed[..], &data[..]);
    }

    #[test]
    fn test_compress_decompress_zstd() {
        let data = b"Hello, World! This is a test of Zstd compression. ".repeat(100);
        let compressor = Compressor::with_config(CompressionConfig {
            algorithm: CompressionAlgorithm::Zstd,
            adaptive: false,
            ..Default::default()
        });

        let compressed = compressor.compress(&data).unwrap();
        assert!(compressed.len() < data.len());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(&decompressed[..], &data[..]);
    }

    #[test]
    fn test_small_payload_not_compressed() {
        let data = b"tiny";
        let compressor = Compressor::new();

        let compressed = compressor.compress(data).unwrap();
        // Should be flags (1) + data (4) = 5 bytes
        assert_eq!(compressed.len(), 5);

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(&decompressed[..], &data[..]);
    }

    #[test]
    fn test_adaptive_algorithm_selection() {
        let compressor = Compressor::with_config(CompressionConfig {
            adaptive: true,
            ..Default::default()
        });

        // Low entropy text should use Zstd
        let text = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let algo = compressor.select_algorithm(text.as_bytes());
        assert_eq!(algo, CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_batch_compression() {
        let config = CompressionConfig::default();
        let mut batch = BatchCompressor::new(config);

        for i in 0..100 {
            let msg = format!("Message {} with some content to compress", i);
            batch.add(msg.as_bytes());
        }

        let compressed = batch.finish().unwrap();
        assert!(compressed.ratio() < 0.5); // Should achieve at least 50% compression

        let messages: Vec<_> = compressed.decompress().unwrap().collect();
        assert_eq!(messages.len(), 100);
        assert_eq!(&messages[0][..], b"Message 0 with some content to compress");
    }

    #[test]
    fn test_entropy_estimation() {
        // Low entropy
        let low = b"aaaaaaaaaaaaaaaa";
        assert!(estimate_entropy(low) < 1.0);

        // High entropy (random-ish)
        let high: Vec<u8> = (0..=255).collect();
        assert!(estimate_entropy(&high) > 7.0);
    }

    #[test]
    fn test_compression_stats() {
        let data = b"Test data for compression statistics analysis ".repeat(50);
        let compressor = Compressor::new();

        let stats = compressor.stats(&data);
        assert!(stats.lz4_size.is_some());
        assert!(stats.zstd_size.is_some());
        assert!(stats.zstd_ratio().unwrap() <= stats.lz4_ratio().unwrap());
    }
}
