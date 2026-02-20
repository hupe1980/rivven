---
layout: default
title: Compression
nav_order: 19
---

# Compression
{: .no_toc }

LZ4, Zstd, and Snappy compression for storage and network efficiency.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Rivven supports **four compression algorithms** optimized for different use cases:

| Algorithm | Speed | Ratio | Best For |
|:----------|:------|:------|:---------|
| **LZ4** | ~4 GB/s | ~2-3x | Real-time streaming, lowest latency |
| **Snappy** | ~1.5 GB/s | ~2-3x | Interoperability, balanced workloads |
| **Zstd** | ~1 GB/s | ~3-5x | Storage, network transfers, cold data |
| **None** | N/A | 1x | Pre-compressed data, tiny payloads |

---

## Configuration

### Feature Gate

Producer-side compression in `rivven-client` is controlled by the `compression` Cargo feature (enabled by default):

```toml
[dependencies]
rivven-client = { version = "0.0.18" }                    # compression included
rivven-client = { version = "0.0.18", features = [] }     # no compression
```

When disabled, `CompressionType` config is accepted but ignored — all batches are sent uncompressed.

### Producer Compression

```yaml
# Producer config
producer:
  compression: lz4  # none, lz4, snappy, zstd

# Or per-message
producer.send(Record::new()
    .topic("events")
    .value(&data)
    .compression(Compression::Zstd)
).await?;
```

### Topic-Level Compression

Force compression at the broker:

```bash
rivven topic create events \
  --config compression.type=zstd
```

| `compression.type` | Behavior |
|:-------------------|:---------|
| `producer` | Use producer's compression (default) |
| `none` | Decompress and store uncompressed |
| `lz4` | Re-compress with LZ4 |
| `snappy` | Re-compress with Snappy |
| `zstd` | Re-compress with Zstd |

### Server Defaults

```yaml
# rivvend.yaml
defaults:
  compression:
    # Compression for internal replication
    replication_compression: lz4
    
    # Decompression buffer pool
    decompress_buffer_size: 1048576  # 1MB
    
    # Zstd compression level (1-22, higher = smaller but slower)
    zstd_level: 3
```

---

## Algorithm Comparison

> **Note**: Gzip is not natively supported. If configured, it is automatically mapped to Zstd (a superior algorithm) with a warning logged. Use LZ4, Snappy, or Zstd directly.

### LZ4

**Characteristics**:
- Extremely fast decompression (~4 GB/s)
- Fast compression (~800 MB/s at level 1)
- Moderate compression ratio (2-3x)
- Block-based format
- Pure Rust implementation (lz4_flex) — no C dependencies

**Best for**:
- Real-time streaming
- Low-latency consumers
- High-throughput workloads
- When CPU is the bottleneck

### Snappy

**Characteristics**:
- Very fast decompression (~1.5 GB/s)
- Fast compression (~500 MB/s)
- Moderate compression ratio (2-3x)
- Widely supported protocol format

**Best for**:
- Balanced speed/ratio workloads
- Interoperability with existing systems
- Google Cloud integrations (native Snappy support)

### Zstd

**Characteristics**:
- Fast decompression (~1 GB/s)
- Configurable compression (levels 1-22)
- Excellent ratio (3-5x, up to 10x at high levels)
- Dictionary support for small payloads

**Best for**:
- Network transfer over WAN
- Cold storage (tiered storage)
- Bandwidth-constrained environments
- When storage cost is important

---

## Protocol Compatibility

Rivven supports standard compression formats with type IDs:

| Type ID | Algorithm | Rivven Support |
|:--------|:----------|:---------------|
| 0 | None | ✅ |
| 2 | Snappy | ✅ |
| 3 | LZ4 | ✅ |
| 4 | Zstd | ✅ |

```rust
// Convert between protocol and Rivven compression types
let type_id = CompressionAlgorithm::Snappy.type_id(); // Returns 2
let algo = CompressionAlgorithm::from_type_id(2); // Returns Some(Snappy)
```

---

## Compression Ratios

Typical compression ratios by data type:

| Data Type | LZ4 | Snappy | Zstd |
|:----------|:----|:-------|:-----|
| JSON logs | 4-6x | 4-5x | 6-10x |
| Protobuf | 2-3x | 2-3x | 3-5x |
| Avro | 2-3x | 2-3x | 3-4x |
| Plain text | 3-4x | 3-4x | 5-8x |
| Already compressed | 1x | 1x | 1x |
| Random bytes | 1x | 1x | 1x |

---

## Wire Format

```
+-------+----------------+----------+------------------+
| Flags | Original Size  | Checksum | Compressed Data  |
| 1 byte| 4 bytes (opt)  | 4B (opt) | N bytes          |
+-------+----------------+----------+------------------+

Flags byte:
  bits 0-2: Algorithm (000=None, 001=LZ4, 010=Zstd, 011=Snappy)
  bit 3:    Reserved
  bit 4:    Has original size prefix
  bit 5:    Has CRC32 checksum
  bits 6-7: Reserved
```

---

## Batch Compression

Rivven compresses at the **batch level**, not per-message:

```
┌─────────────────────────────────────────┐
│            Compressed Batch             │
├─────────────────────────────────────────┤
│  Header (algorithm, original size)      │
│  ┌───────────────────────────────────┐  │
│  │ Message 1 + Message 2 + Message 3 │  │
│  │      (compressed together)        │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

**Benefits**:
- Better compression ratio (more context)
- Amortized compression overhead
- Protocol-compatible batching

---

## Performance Tuning

### High-Throughput (Prioritize Speed)

```yaml
producer:
  compression: lz4
  batch_size: 65536        # 64KB batches
  linger_ms: 5             # Accumulate for 5ms
```

### Low-Bandwidth (Prioritize Size)

```yaml
producer:
  compression: zstd
  zstd_level: 6            # Higher compression
  batch_size: 131072       # 128KB batches
  linger_ms: 50            # More time to batch
```

### Mixed Workload

```yaml
# Different compression per topic
topics:
  - name: realtime-events
    compression.type: lz4   # Speed priority
    
  - name: audit-logs
    compression.type: zstd  # Size priority
```

---

## End-to-End Compression

For sensitive data, consider end-to-end encryption + compression:

```rust
// Compress then encrypt (better ratio)
let compressed = lz4_flex::block::compress_prepend_size(&plaintext);
let encrypted = aes_gcm::encrypt(&compressed)?;

producer.send(Record::new()
    .value(&encrypted)
    .compression(Compression::None)  // Already compressed
).await?;
```

---

## Monitoring

### Metrics

| Metric | Description |
|:-------|:------------|
| `rivven_compression_ratio` | Achieved compression ratio |
| `rivven_compression_time_seconds` | Compression latency |
| `rivven_decompression_time_seconds` | Decompression latency |
| `rivven_compressed_bytes_total` | Total compressed bytes |
| `rivven_uncompressed_bytes_total` | Total uncompressed bytes |

### Check Compression Effectiveness

```bash
# Topic statistics
rivven topic stats events

# Output:
# Topic: events
# Messages: 1,234,567
# Compressed Size: 1.2 GB
# Uncompressed Size: 4.8 GB
# Compression Ratio: 4.0x
# Algorithm: zstd
```

---

## Troubleshooting

### Low Compression Ratio

**Symptoms**: Ratio close to 1x

**Causes**:
- Data already compressed (images, videos)
- Random/encrypted data
- Very small messages (overhead dominates)

**Solutions**:
1. Use `compression: none` for pre-compressed data
2. Increase batch size for small messages
3. Consider pre-processing to improve compressibility

---

### High CPU Usage

**Symptoms**: Producer CPU-bound

**Causes**:
- High Zstd compression level
- Small batches (frequent compression)

**Solutions**:
```yaml
producer:
  compression: lz4          # Switch to faster algorithm
  zstd_level: 1             # Or use lower Zstd level
  batch_size: 131072        # Larger batches
```

---

### Decompression Bottleneck

**Symptoms**: Consumer CPU-bound on decompression

**Causes**:
- Very high compression (Zstd level 15+)
- Single-threaded consumer

**Solutions**:
1. Use LZ4 for latency-sensitive consumers
2. Enable parallel decompression
3. Scale out consumers

---

## Security

All decompression algorithms enforce a **256 MiB output size limit** (`MAX_DECOMPRESSION_SIZE`) to prevent decompression-bomb DoS attacks:

- **LZ4**: The 4-byte prepended uncompressed size header is validated before allocation
- **Snappy**: `decompress_len()` is called to validate the header before decompression
- **Zstd**: The `original_size` parameter is capped at 256 MiB; unknown-size payloads fall back to a 16 MiB limit

Payloads exceeding the limit are rejected with a `DecompressionBomb` error. This protects against crafted payloads that claim gigabytes of output from a few bytes of input.
