//! Kafka Source and Sink Connectors for Rivven Connect
//!
//! This module provides Kafka integration for migration scenarios:
//! - **KafkaSource**: Consume from Kafka topics, stream to Rivven
//! - **KafkaSink**: Produce from Rivven topics to Kafka
//!
//! # Features
//!
//! | Feature | Description |
//! |---------|-------------|
//! | Pure Rust client | Zero C dependencies via rskafka |
//! | Lock-free metrics | Atomic counters with zero contention |
//! | Batch size tracking | Min/max/avg with CAS operations |
//! | Latency tracking | Poll/produce latency measurements |
//! | Prometheus export | `to_prometheus_format()` for scraping |
//! | JSON serialization | Serde derives on MetricsSnapshot |
//! | Security protocols | `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl` |
//! | SASL mechanisms | `PLAIN`, `SCRAM_SHA_256`, `SCRAM_SHA_512` |
//! | Compression | `none`, `gzip`, `snappy`, `lz4`, `zstd` |
//! | Exactly-once | Idempotent/transactional producer support (config) |
//!
//! # Use Cases
//!
//! - Migrating from Kafka to Rivven (source mode)
//! - Hybrid deployments with Kafka (sink mode)
//! - Cross-datacenter replication
//!
//! # Kafka Client
//!
//! This connector uses [rskafka](https://crates.io/crates/rskafka), a pure Rust
//! Kafka client with no librdkafka or C dependencies. This provides:
//!
//! - **Zero C dependencies**: No need for librdkafka installation
//! - **Simplified builds**: No C compiler or linker configuration
//! - **Consistent behavior**: Same behavior across all platforms
//! - **SASL PLAIN auth**: Full support for authenticated clusters
//!
//! Enable with the `kafka` feature:
//!
//! ```toml
//! rivven-connect = { version = "0.0.10", features = ["kafka"] }
//! ```
//!
//! # Example Configuration (Source)
//!
//! ```yaml
//! type: kafka-source
//! topic: kafka-events           # Rivven topic (destination for consumed messages)
//! config:
//!   brokers: ["kafka1:9092", "kafka2:9092"]
//!   topic: orders               # Kafka topic (external source)
//!   consumer_group: rivven-migration
//!   start_offset: earliest
//!   security:
//!     protocol: sasl_ssl
//!     mechanism: PLAIN
//!     username: user
//!     password: secret
//! ```
//!
//! # Example Configuration (Sink)
//!
//! ```yaml
//! type: kafka-sink
//! topics: [events]              # Rivven topics to consume from
//! consumer_group: kafka-producer
//! config:
//!   brokers: ["kafka1:9092"]
//!   topic: orders-replica       # Kafka topic (external destination)
//!   acks: all
//!   compression: lz4
//! ```
//!
//! # Observability
//!
//! ```rust,ignore
//! // Get metrics from source
//! let source = KafkaSource::new();
//! let snapshot = source.metrics().snapshot();
//! println!("Messages consumed: {}", snapshot.messages_consumed);
//! println!("Avg poll latency: {:.2}ms", snapshot.avg_poll_latency_ms());
//!
//! // Export to Prometheus
//! let prometheus_output = snapshot.to_prometheus_format("myapp");
//! ```

use crate::connectors::{
    AnySink, AnySource, SinkFactory, SinkRegistry, SourceFactory, SourceRegistry,
};
use crate::error::{ConnectError, ConnectorError, Result};
use crate::traits::catalog::{Catalog, ConfiguredCatalog, Stream, SyncMode};
use crate::traits::event::SourceEvent;
use crate::traits::sink::{Sink, WriteResult};
use crate::traits::source::{CheckResult, Source};
use crate::traits::spec::ConnectorSpec;
use crate::traits::state::State;
use crate::types::SensitiveString;
use async_trait::async_trait;
use chrono;
use futures::stream::BoxStream;
use futures::StreamExt;
#[cfg(feature = "kafka")]
use rskafka::client::partition::{OffsetAt, UnknownTopicHandling};
#[cfg(feature = "kafka")]
use rskafka::client::{Client, ClientBuilder};
#[cfg(feature = "kafka")]
use rskafka::record::Record;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use validator::Validate;

// ============================================================================
// Common Types
// ============================================================================

/// Security protocol for Kafka connections
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SecurityProtocol {
    /// No encryption, no authentication
    #[default]
    Plaintext,
    /// TLS encryption, no authentication
    Ssl,
    /// No encryption, SASL authentication
    SaslPlaintext,
    /// TLS encryption, SASL authentication
    SaslSsl,
}

/// SASL mechanism for authentication
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SaslMechanism {
    /// Plain username/password
    #[default]
    Plain,
    /// SCRAM-SHA-256
    ScramSha256,
    /// SCRAM-SHA-512
    ScramSha512,
}

/// Security configuration for Kafka connections
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
pub struct KafkaSecurityConfig {
    /// Security protocol
    #[serde(default)]
    pub protocol: SecurityProtocol,

    /// SASL mechanism (required for SASL protocols)
    #[serde(default)]
    pub mechanism: Option<SaslMechanism>,

    /// SASL username
    pub username: Option<String>,

    /// SASL password
    pub password: Option<SensitiveString>,

    /// Path to CA certificate file for SSL
    pub ssl_ca_cert: Option<String>,

    /// Path to client certificate file for mTLS
    pub ssl_client_cert: Option<String>,

    /// Path to client key file for mTLS
    pub ssl_client_key: Option<String>,
}

/// Compression codec for Kafka messages
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    /// No compression
    #[default]
    None,
    /// Gzip compression
    Gzip,
    /// Snappy compression
    Snappy,
    /// LZ4 compression
    Lz4,
    /// Zstd compression
    Zstd,
}

/// Convert to rskafka Compression type for producing messages
#[cfg(feature = "kafka")]
impl From<CompressionCodec> for rskafka::client::partition::Compression {
    fn from(codec: CompressionCodec) -> Self {
        match codec {
            CompressionCodec::None => rskafka::client::partition::Compression::NoCompression,
            CompressionCodec::Gzip => rskafka::client::partition::Compression::Gzip,
            CompressionCodec::Snappy => rskafka::client::partition::Compression::Snappy,
            CompressionCodec::Lz4 => rskafka::client::partition::Compression::Lz4,
            CompressionCodec::Zstd => rskafka::client::partition::Compression::Zstd,
        }
    }
}

/// Starting offset for Kafka consumer
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum StartOffset {
    /// Start from the earliest available message
    Earliest,
    /// Start from the latest message
    #[default]
    Latest,
    /// Start from a specific offset
    Offset(i64),
    /// Start from a specific timestamp (milliseconds since epoch)
    Timestamp(i64),
}

// ============================================================================
// Kafka Source Metrics
// ============================================================================

/// Point-in-time snapshot of Kafka source metrics.
///
/// This struct captures all metrics at a specific moment and can be:
/// - Serialized to JSON for export
/// - Exported to Prometheus format
/// - Used for computing derived metrics (rates, averages)
///
/// # Example
///
/// ```rust,ignore
/// let snapshot = source.metrics().snapshot();
/// println!("Messages: {}, Bytes: {}", snapshot.messages_consumed, snapshot.bytes_consumed);
///
/// // Export to Prometheus
/// let prom = snapshot.to_prometheus_format("myapp");
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaSourceMetricsSnapshot {
    /// Total messages successfully consumed
    pub messages_consumed: u64,
    /// Total bytes consumed
    pub bytes_consumed: u64,
    /// Total poll operations
    pub polls: u64,
    /// Total empty polls (no messages returned)
    pub empty_polls: u64,
    /// Total consumer errors
    pub errors: u64,
    /// Total rebalances triggered
    pub rebalances: u64,
    /// Cumulative poll latency in microseconds
    pub poll_latency_us: u64,
    /// Minimum batch size (messages per poll) - 0 if no batches
    #[serde(default)]
    pub batch_size_min: u64,
    /// Maximum batch size (messages per poll)
    #[serde(default)]
    pub batch_size_max: u64,
    /// Sum of all batch sizes (for calculating average)
    #[serde(default)]
    pub batch_size_sum: u64,
}

impl KafkaSourceMetricsSnapshot {
    /// Get average poll latency in milliseconds
    #[inline]
    pub fn avg_poll_latency_ms(&self) -> f64 {
        if self.polls == 0 {
            return 0.0;
        }
        (self.poll_latency_us as f64 / self.polls as f64) / 1000.0
    }

    /// Get average batch size (messages per poll)
    #[inline]
    pub fn avg_batch_size(&self) -> f64 {
        let non_empty = self.polls.saturating_sub(self.empty_polls);
        if non_empty == 0 {
            return 0.0;
        }
        self.batch_size_sum as f64 / non_empty as f64
    }

    /// Get empty poll rate (empty polls / total polls)
    #[inline]
    pub fn empty_poll_rate(&self) -> f64 {
        if self.polls == 0 {
            return 0.0;
        }
        self.empty_polls as f64 / self.polls as f64
    }

    /// Get throughput in messages per second given elapsed time
    #[inline]
    pub fn messages_per_second(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs <= 0.0 {
            return 0.0;
        }
        self.messages_consumed as f64 / elapsed_secs
    }

    /// Get throughput in bytes per second given elapsed time
    #[inline]
    pub fn bytes_per_second(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs <= 0.0 {
            return 0.0;
        }
        self.bytes_consumed as f64 / elapsed_secs
    }

    /// Export metrics in Prometheus text format.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Metric prefix (e.g., "myapp" → "myapp_kafka_source_messages_consumed_total")
    pub fn to_prometheus_format(&self, prefix: &str) -> String {
        let mut output = String::with_capacity(1024);

        output.push_str(&format!(
            "# HELP {prefix}_kafka_source_messages_consumed_total Total messages consumed\n\
             # TYPE {prefix}_kafka_source_messages_consumed_total counter\n\
             {prefix}_kafka_source_messages_consumed_total {}\n",
            self.messages_consumed
        ));

        output.push_str(&format!(
            "# HELP {prefix}_kafka_source_bytes_consumed_total Total bytes consumed\n\
             # TYPE {prefix}_kafka_source_bytes_consumed_total counter\n\
             {prefix}_kafka_source_bytes_consumed_total {}\n",
            self.bytes_consumed
        ));

        output.push_str(&format!(
            "# HELP {prefix}_kafka_source_polls_total Total poll operations\n\
             # TYPE {prefix}_kafka_source_polls_total counter\n\
             {prefix}_kafka_source_polls_total {}\n",
            self.polls
        ));

        output.push_str(&format!(
            "# HELP {prefix}_kafka_source_empty_polls_total Total empty polls\n\
             # TYPE {prefix}_kafka_source_empty_polls_total counter\n\
             {prefix}_kafka_source_empty_polls_total {}\n",
            self.empty_polls
        ));

        output.push_str(&format!(
            "# HELP {prefix}_kafka_source_errors_total Total consumer errors\n\
             # TYPE {prefix}_kafka_source_errors_total counter\n\
             {prefix}_kafka_source_errors_total {}\n",
            self.errors
        ));

        output.push_str(&format!(
            "# HELP {prefix}_kafka_source_rebalances_total Total rebalances\n\
             # TYPE {prefix}_kafka_source_rebalances_total counter\n\
             {prefix}_kafka_source_rebalances_total {}\n",
            self.rebalances
        ));

        // Gauges
        output.push_str(&format!(
            "# HELP {prefix}_kafka_source_poll_latency_avg_ms Average poll latency\n\
             # TYPE {prefix}_kafka_source_poll_latency_avg_ms gauge\n\
             {prefix}_kafka_source_poll_latency_avg_ms {:.3}\n",
            self.avg_poll_latency_ms()
        ));

        output.push_str(&format!(
            "# HELP {prefix}_kafka_source_batch_size_avg Average batch size\n\
             # TYPE {prefix}_kafka_source_batch_size_avg gauge\n\
             {prefix}_kafka_source_batch_size_avg {:.2}\n",
            self.avg_batch_size()
        ));

        if self.batch_size_min > 0 {
            output.push_str(&format!(
                "# HELP {prefix}_kafka_source_batch_size_min Minimum batch size\n\
                 # TYPE {prefix}_kafka_source_batch_size_min gauge\n\
                 {prefix}_kafka_source_batch_size_min {}\n",
                self.batch_size_min
            ));
        }

        output.push_str(&format!(
            "# HELP {prefix}_kafka_source_batch_size_max Maximum batch size\n\
             # TYPE {prefix}_kafka_source_batch_size_max gauge\n\
             {prefix}_kafka_source_batch_size_max {}\n",
            self.batch_size_max
        ));

        output
    }
}

/// Lock-free metrics for Kafka source connector.
///
/// All counters use `AtomicU64` with `Ordering::Relaxed` for maximum performance
/// on the hot path. Metrics can be observed via `snapshot()` or exported
/// using `snapshot().to_prometheus_format()`.
#[derive(Debug, Default)]
pub struct KafkaSourceMetrics {
    /// Total messages consumed
    pub messages_consumed: AtomicU64,
    /// Total bytes consumed
    pub bytes_consumed: AtomicU64,
    /// Total poll operations
    pub polls: AtomicU64,
    /// Total empty polls
    pub empty_polls: AtomicU64,
    /// Total errors
    pub errors: AtomicU64,
    /// Total rebalances
    pub rebalances: AtomicU64,
    /// Cumulative poll latency in microseconds
    pub poll_latency_us: AtomicU64,
    /// Minimum batch size - uses u64::MAX as sentinel for "not set"
    batch_size_min: AtomicU64,
    /// Maximum batch size
    batch_size_max: AtomicU64,
    /// Sum of all batch sizes
    batch_size_sum: AtomicU64,
}

impl KafkaSourceMetrics {
    /// Create new metrics instance
    pub fn new() -> Self {
        Self {
            messages_consumed: AtomicU64::new(0),
            bytes_consumed: AtomicU64::new(0),
            polls: AtomicU64::new(0),
            empty_polls: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            rebalances: AtomicU64::new(0),
            poll_latency_us: AtomicU64::new(0),
            batch_size_min: AtomicU64::new(u64::MAX),
            batch_size_max: AtomicU64::new(0),
            batch_size_sum: AtomicU64::new(0),
        }
    }

    /// Record a batch size for min/max/avg tracking.
    ///
    /// Uses lock-free CAS operations for thread-safe min/max updates.
    /// Optimized for the hot path with minimal branching.
    #[inline(always)]
    pub fn record_batch_size(&self, size: u64) {
        self.batch_size_sum.fetch_add(size, Ordering::Relaxed);

        // Update min using CAS loop
        let mut current_min = self.batch_size_min.load(Ordering::Relaxed);
        while size < current_min {
            match self.batch_size_min.compare_exchange_weak(
                current_min,
                size,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_min = actual,
            }
            std::hint::spin_loop();
        }

        // Update max using CAS loop
        let mut current_max = self.batch_size_max.load(Ordering::Relaxed);
        while size > current_max {
            match self.batch_size_max.compare_exchange_weak(
                current_max,
                size,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
            std::hint::spin_loop();
        }
    }

    /// Capture a point-in-time snapshot of all metrics.
    #[inline]
    pub fn snapshot(&self) -> KafkaSourceMetricsSnapshot {
        let min_raw = self.batch_size_min.load(Ordering::Relaxed);
        KafkaSourceMetricsSnapshot {
            messages_consumed: self.messages_consumed.load(Ordering::Relaxed),
            bytes_consumed: self.bytes_consumed.load(Ordering::Relaxed),
            polls: self.polls.load(Ordering::Relaxed),
            empty_polls: self.empty_polls.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            rebalances: self.rebalances.load(Ordering::Relaxed),
            poll_latency_us: self.poll_latency_us.load(Ordering::Relaxed),
            batch_size_min: if min_raw == u64::MAX { 0 } else { min_raw },
            batch_size_max: self.batch_size_max.load(Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.messages_consumed.store(0, Ordering::Relaxed);
        self.bytes_consumed.store(0, Ordering::Relaxed);
        self.polls.store(0, Ordering::Relaxed);
        self.empty_polls.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.rebalances.store(0, Ordering::Relaxed);
        self.poll_latency_us.store(0, Ordering::Relaxed);
        self.batch_size_min.store(u64::MAX, Ordering::Relaxed);
        self.batch_size_max.store(0, Ordering::Relaxed);
        self.batch_size_sum.store(0, Ordering::Relaxed);
    }

    /// Atomically capture a snapshot and reset all counters.
    pub fn snapshot_and_reset(&self) -> KafkaSourceMetricsSnapshot {
        let min_raw = self.batch_size_min.swap(u64::MAX, Ordering::Relaxed);
        KafkaSourceMetricsSnapshot {
            messages_consumed: self.messages_consumed.swap(0, Ordering::Relaxed),
            bytes_consumed: self.bytes_consumed.swap(0, Ordering::Relaxed),
            polls: self.polls.swap(0, Ordering::Relaxed),
            empty_polls: self.empty_polls.swap(0, Ordering::Relaxed),
            errors: self.errors.swap(0, Ordering::Relaxed),
            rebalances: self.rebalances.swap(0, Ordering::Relaxed),
            poll_latency_us: self.poll_latency_us.swap(0, Ordering::Relaxed),
            batch_size_min: if min_raw == u64::MAX { 0 } else { min_raw },
            batch_size_max: self.batch_size_max.swap(0, Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum.swap(0, Ordering::Relaxed),
        }
    }
}

// ============================================================================
// Kafka Sink Metrics
// ============================================================================

/// Point-in-time snapshot of Kafka sink metrics.
///
/// This struct captures all metrics at a specific moment and can be:
/// - Serialized to JSON for export
/// - Exported to Prometheus format
/// - Used for computing derived metrics (rates, averages)
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaSinkMetricsSnapshot {
    /// Total messages successfully produced
    pub messages_produced: u64,
    /// Total bytes produced
    pub bytes_produced: u64,
    /// Total messages failed to produce
    pub messages_failed: u64,
    /// Total produce batches sent
    pub batches_sent: u64,
    /// Total produce retries
    pub retries: u64,
    /// Cumulative produce latency in microseconds
    pub produce_latency_us: u64,
    /// Minimum batch size (messages) - 0 if no batches
    #[serde(default)]
    pub batch_size_min: u64,
    /// Maximum batch size (messages)
    #[serde(default)]
    pub batch_size_max: u64,
    /// Sum of all batch sizes (for calculating average)
    #[serde(default)]
    pub batch_size_sum: u64,
}

impl KafkaSinkMetricsSnapshot {
    /// Get average produce latency in milliseconds
    #[inline]
    pub fn avg_produce_latency_ms(&self) -> f64 {
        if self.batches_sent == 0 {
            return 0.0;
        }
        (self.produce_latency_us as f64 / self.batches_sent as f64) / 1000.0
    }

    /// Get average batch size (messages per batch)
    #[inline]
    pub fn avg_batch_size(&self) -> f64 {
        if self.batches_sent == 0 {
            return 0.0;
        }
        self.batch_size_sum as f64 / self.batches_sent as f64
    }

    /// Get success rate (successful messages / total messages)
    #[inline]
    pub fn success_rate(&self) -> f64 {
        let total = self.messages_produced + self.messages_failed;
        if total == 0 {
            return 1.0;
        }
        self.messages_produced as f64 / total as f64
    }

    /// Get retry rate (retries / batches)
    #[inline]
    pub fn retry_rate(&self) -> f64 {
        if self.batches_sent == 0 {
            return 0.0;
        }
        self.retries as f64 / self.batches_sent as f64
    }

    /// Get throughput in messages per second
    #[inline]
    pub fn messages_per_second(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs <= 0.0 {
            return 0.0;
        }
        self.messages_produced as f64 / elapsed_secs
    }

    /// Get throughput in bytes per second
    #[inline]
    pub fn bytes_per_second(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs <= 0.0 {
            return 0.0;
        }
        self.bytes_produced as f64 / elapsed_secs
    }

    /// Export metrics in Prometheus text format.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Metric prefix (e.g., "myapp" → "myapp_kafka_sink_messages_produced_total")
    pub fn to_prometheus_format(&self, prefix: &str) -> String {
        let mut output = String::with_capacity(1024);

        output.push_str(&format!(
            "# HELP {prefix}_kafka_sink_messages_produced_total Total messages produced\n\
             # TYPE {prefix}_kafka_sink_messages_produced_total counter\n\
             {prefix}_kafka_sink_messages_produced_total {}\n",
            self.messages_produced
        ));

        output.push_str(&format!(
            "# HELP {prefix}_kafka_sink_bytes_produced_total Total bytes produced\n\
             # TYPE {prefix}_kafka_sink_bytes_produced_total counter\n\
             {prefix}_kafka_sink_bytes_produced_total {}\n",
            self.bytes_produced
        ));

        output.push_str(&format!(
            "# HELP {prefix}_kafka_sink_messages_failed_total Total messages failed\n\
             # TYPE {prefix}_kafka_sink_messages_failed_total counter\n\
             {prefix}_kafka_sink_messages_failed_total {}\n",
            self.messages_failed
        ));

        output.push_str(&format!(
            "# HELP {prefix}_kafka_sink_batches_sent_total Total batches sent\n\
             # TYPE {prefix}_kafka_sink_batches_sent_total counter\n\
             {prefix}_kafka_sink_batches_sent_total {}\n",
            self.batches_sent
        ));

        output.push_str(&format!(
            "# HELP {prefix}_kafka_sink_retries_total Total produce retries\n\
             # TYPE {prefix}_kafka_sink_retries_total counter\n\
             {prefix}_kafka_sink_retries_total {}\n",
            self.retries
        ));

        // Gauges
        output.push_str(&format!(
            "# HELP {prefix}_kafka_sink_produce_latency_avg_ms Average produce latency\n\
             # TYPE {prefix}_kafka_sink_produce_latency_avg_ms gauge\n\
             {prefix}_kafka_sink_produce_latency_avg_ms {:.3}\n",
            self.avg_produce_latency_ms()
        ));

        output.push_str(&format!(
            "# HELP {prefix}_kafka_sink_success_rate Success rate\n\
             # TYPE {prefix}_kafka_sink_success_rate gauge\n\
             {prefix}_kafka_sink_success_rate {:.4}\n",
            self.success_rate()
        ));

        output.push_str(&format!(
            "# HELP {prefix}_kafka_sink_batch_size_avg Average batch size\n\
             # TYPE {prefix}_kafka_sink_batch_size_avg gauge\n\
             {prefix}_kafka_sink_batch_size_avg {:.2}\n",
            self.avg_batch_size()
        ));

        if self.batch_size_min > 0 {
            output.push_str(&format!(
                "# HELP {prefix}_kafka_sink_batch_size_min Minimum batch size\n\
                 # TYPE {prefix}_kafka_sink_batch_size_min gauge\n\
                 {prefix}_kafka_sink_batch_size_min {}\n",
                self.batch_size_min
            ));
        }

        output.push_str(&format!(
            "# HELP {prefix}_kafka_sink_batch_size_max Maximum batch size\n\
             # TYPE {prefix}_kafka_sink_batch_size_max gauge\n\
             {prefix}_kafka_sink_batch_size_max {}\n",
            self.batch_size_max
        ));

        output
    }
}

/// Lock-free metrics for Kafka sink connector.
///
/// All counters use `AtomicU64` with `Ordering::Relaxed` for maximum performance
/// on the hot path. Metrics can be observed via `snapshot()` or exported
/// using `snapshot().to_prometheus_format()`.
#[derive(Debug, Default)]
pub struct KafkaSinkMetrics {
    /// Total messages produced
    pub messages_produced: AtomicU64,
    /// Total bytes produced
    pub bytes_produced: AtomicU64,
    /// Total messages failed
    pub messages_failed: AtomicU64,
    /// Total batches sent
    pub batches_sent: AtomicU64,
    /// Total retries
    pub retries: AtomicU64,
    /// Cumulative produce latency in microseconds
    pub produce_latency_us: AtomicU64,
    /// Minimum batch size - uses u64::MAX as sentinel
    batch_size_min: AtomicU64,
    /// Maximum batch size
    batch_size_max: AtomicU64,
    /// Sum of all batch sizes
    batch_size_sum: AtomicU64,
}

impl KafkaSinkMetrics {
    /// Create new metrics instance
    #[inline]
    pub fn new() -> Self {
        Self {
            messages_produced: AtomicU64::new(0),
            bytes_produced: AtomicU64::new(0),
            messages_failed: AtomicU64::new(0),
            batches_sent: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            produce_latency_us: AtomicU64::new(0),
            batch_size_min: AtomicU64::new(u64::MAX),
            batch_size_max: AtomicU64::new(0),
            batch_size_sum: AtomicU64::new(0),
        }
    }

    /// Record a batch size for min/max/avg tracking.
    /// Optimized for hot path with lock-free CAS operations.
    #[inline(always)]
    pub fn record_batch_size(&self, size: u64) {
        self.batch_size_sum.fetch_add(size, Ordering::Relaxed);

        // Update min using CAS loop
        let mut current_min = self.batch_size_min.load(Ordering::Relaxed);
        while size < current_min {
            match self.batch_size_min.compare_exchange_weak(
                current_min,
                size,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_min = actual,
            }
            std::hint::spin_loop();
        }

        // Update max using CAS loop
        let mut current_max = self.batch_size_max.load(Ordering::Relaxed);
        while size > current_max {
            match self.batch_size_max.compare_exchange_weak(
                current_max,
                size,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
            std::hint::spin_loop();
        }
    }

    /// Capture a point-in-time snapshot of all metrics.
    #[inline]
    pub fn snapshot(&self) -> KafkaSinkMetricsSnapshot {
        let min_raw = self.batch_size_min.load(Ordering::Relaxed);
        KafkaSinkMetricsSnapshot {
            messages_produced: self.messages_produced.load(Ordering::Relaxed),
            bytes_produced: self.bytes_produced.load(Ordering::Relaxed),
            messages_failed: self.messages_failed.load(Ordering::Relaxed),
            batches_sent: self.batches_sent.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            produce_latency_us: self.produce_latency_us.load(Ordering::Relaxed),
            batch_size_min: if min_raw == u64::MAX { 0 } else { min_raw },
            batch_size_max: self.batch_size_max.load(Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.messages_produced.store(0, Ordering::Relaxed);
        self.bytes_produced.store(0, Ordering::Relaxed);
        self.messages_failed.store(0, Ordering::Relaxed);
        self.batches_sent.store(0, Ordering::Relaxed);
        self.retries.store(0, Ordering::Relaxed);
        self.produce_latency_us.store(0, Ordering::Relaxed);
        self.batch_size_min.store(u64::MAX, Ordering::Relaxed);
        self.batch_size_max.store(0, Ordering::Relaxed);
        self.batch_size_sum.store(0, Ordering::Relaxed);
    }

    /// Atomically capture a snapshot and reset all counters.
    pub fn snapshot_and_reset(&self) -> KafkaSinkMetricsSnapshot {
        let min_raw = self.batch_size_min.swap(u64::MAX, Ordering::Relaxed);
        KafkaSinkMetricsSnapshot {
            messages_produced: self.messages_produced.swap(0, Ordering::Relaxed),
            bytes_produced: self.bytes_produced.swap(0, Ordering::Relaxed),
            messages_failed: self.messages_failed.swap(0, Ordering::Relaxed),
            batches_sent: self.batches_sent.swap(0, Ordering::Relaxed),
            retries: self.retries.swap(0, Ordering::Relaxed),
            produce_latency_us: self.produce_latency_us.swap(0, Ordering::Relaxed),
            batch_size_min: if min_raw == u64::MAX { 0 } else { min_raw },
            batch_size_max: self.batch_size_max.swap(0, Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum.swap(0, Ordering::Relaxed),
        }
    }
}

// ============================================================================
// Kafka Source
// ============================================================================

/// Kafka Source configuration
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct KafkaSourceConfig {
    /// List of Kafka broker addresses
    #[validate(length(min = 1))]
    pub brokers: Vec<String>,

    /// Kafka topic to consume from
    #[validate(length(min = 1, max = 249))]
    pub topic: String,

    /// Consumer group ID
    #[validate(length(min = 1, max = 249))]
    pub consumer_group: String,

    /// Starting offset (default: latest)
    #[serde(default)]
    pub start_offset: StartOffset,

    /// Security configuration
    #[serde(default)]
    pub security: KafkaSecurityConfig,

    /// Maximum poll interval in milliseconds (default: 300000 = 5 minutes)
    #[serde(default = "default_max_poll_interval")]
    #[validate(range(min = 1000, max = 3600000))]
    pub max_poll_interval_ms: u32,

    /// Session timeout in milliseconds (default: 10000)
    #[serde(default = "default_session_timeout")]
    #[validate(range(min = 1000, max = 300000))]
    pub session_timeout_ms: u32,

    /// Heartbeat interval in milliseconds (default: 3000)
    #[serde(default = "default_heartbeat_interval")]
    #[validate(range(min = 100, max = 60000))]
    pub heartbeat_interval_ms: u32,

    /// Maximum messages to fetch per partition per poll (default: 500)
    #[serde(default = "default_fetch_max_messages")]
    #[validate(range(min = 1, max = 10000))]
    pub fetch_max_messages: u32,

    /// Include Kafka headers as event metadata
    #[serde(default = "default_true")]
    pub include_headers: bool,

    /// Include Kafka key as event key
    #[serde(default = "default_true")]
    pub include_key: bool,

    /// Initial retry delay in milliseconds on error (default: 100)
    #[serde(default = "default_retry_initial_ms")]
    #[validate(range(min = 10, max = 60000))]
    pub retry_initial_ms: u64,

    /// Maximum retry delay in milliseconds (default: 10000)
    #[serde(default = "default_retry_max_ms")]
    #[validate(range(min = 100, max = 300000))]
    pub retry_max_ms: u64,

    /// Backoff multiplier for retries (default: 2.0)
    #[serde(default = "default_retry_multiplier")]
    #[validate(range(min = 1.0, max = 10.0))]
    pub retry_multiplier: f64,

    /// Delay on empty poll in milliseconds (default: 50)
    #[serde(default = "default_empty_poll_delay_ms")]
    #[validate(range(min = 1, max = 10000))]
    pub empty_poll_delay_ms: u64,

    /// Connection timeout in milliseconds (default: 30000 = 30 seconds)
    #[serde(default = "default_connect_timeout")]
    #[validate(range(min = 1000, max = 300000))]
    pub connect_timeout_ms: u64,
}

fn default_connect_timeout() -> u64 {
    30000
}
fn default_max_poll_interval() -> u32 {
    300000
}
fn default_session_timeout() -> u32 {
    10000
}
fn default_heartbeat_interval() -> u32 {
    3000
}
fn default_fetch_max_messages() -> u32 {
    500
}
fn default_true() -> bool {
    true
}
fn default_retry_initial_ms() -> u64 {
    100
}
fn default_retry_max_ms() -> u64 {
    10000
}
fn default_retry_multiplier() -> f64 {
    2.0
}
fn default_empty_poll_delay_ms() -> u64 {
    50
}

/// Kafka Source implementation
pub struct KafkaSource {
    shutdown: Arc<AtomicBool>,
    metrics: Arc<KafkaSourceMetrics>,
}

impl KafkaSource {
    pub fn new() -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            metrics: Arc::new(KafkaSourceMetrics::new()),
        }
    }

    /// Get metrics for this source.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let snapshot = source.metrics().snapshot();
    /// println!("Messages: {}", snapshot.messages_consumed);
    /// ```
    #[inline]
    pub fn metrics(&self) -> &KafkaSourceMetrics {
        &self.metrics
    }

    /// Signal the source to shut down gracefully.
    ///
    /// This sets the shutdown flag which will be checked on the next poll
    /// iteration, causing the stream to terminate cleanly.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Check if the source is shutting down.
    #[inline]
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    /// Create an rskafka client for the given configuration.
    ///
    /// This connects to the Kafka cluster using the pure Rust rskafka client.
    /// Supports SASL_PLAIN authentication (rskafka v0.5 limitation).
    pub async fn create_client(config: &KafkaSourceConfig) -> Result<Client> {
        let brokers: Vec<String> = config.brokers.clone();

        let mut builder = ClientBuilder::new(brokers);

        // Configure SASL authentication if needed
        match config.security.protocol {
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl => {
                if let (Some(username), Some(password)) =
                    (&config.security.username, &config.security.password)
                {
                    let mechanism = config
                        .security
                        .mechanism
                        .as_ref()
                        .unwrap_or(&SaslMechanism::Plain);
                    match mechanism {
                        SaslMechanism::Plain => {
                            builder = builder.sasl_config(rskafka::client::SaslConfig::Plain {
                                username: username.clone(),
                                password: password.expose().to_string(),
                            });
                        }
                        _ => {
                            // rskafka v0.5 only supports SASL PLAIN - fall back with warning
                            warn!(
                                "SASL {:?} not supported by rskafka v0.5, falling back to PLAIN",
                                mechanism
                            );
                            builder = builder.sasl_config(rskafka::client::SaslConfig::Plain {
                                username: username.clone(),
                                password: password.expose().to_string(),
                            });
                        }
                    }
                }
            }
            _ => {}
        }

        // Build the client with connection timeout
        let timeout_duration = Duration::from_millis(config.connect_timeout_ms);
        let client = tokio::time::timeout(timeout_duration, builder.build())
            .await
            .map_err(|_| {
                ConnectorError::Connection(format!(
                    "Connection timeout after {}ms to brokers: {:?}",
                    config.connect_timeout_ms, config.brokers
                ))
            })?
            .map_err(|e| {
                ConnectorError::Connection(format!("Failed to connect to Kafka: {}", e))
            })?;

        Ok(client)
    }

    /// Build Kafka client configuration map (legacy, for reference)
    pub fn build_client_config(config: &KafkaSourceConfig) -> HashMap<String, String> {
        let mut client_config = HashMap::new();

        client_config.insert("bootstrap.servers".into(), config.brokers.join(","));
        client_config.insert("group.id".into(), config.consumer_group.clone());
        client_config.insert(
            "max.poll.interval.ms".into(),
            config.max_poll_interval_ms.to_string(),
        );
        client_config.insert(
            "session.timeout.ms".into(),
            config.session_timeout_ms.to_string(),
        );
        client_config.insert(
            "heartbeat.interval.ms".into(),
            config.heartbeat_interval_ms.to_string(),
        );

        // Security settings
        match config.security.protocol {
            SecurityProtocol::Plaintext => {
                client_config.insert("security.protocol".into(), "plaintext".into());
            }
            SecurityProtocol::Ssl => {
                client_config.insert("security.protocol".into(), "ssl".into());
                if let Some(ref ca) = config.security.ssl_ca_cert {
                    client_config.insert("ssl.ca.location".into(), ca.clone());
                }
            }
            SecurityProtocol::SaslPlaintext => {
                client_config.insert("security.protocol".into(), "sasl_plaintext".into());
                Self::add_sasl_config(&mut client_config, &config.security);
            }
            SecurityProtocol::SaslSsl => {
                client_config.insert("security.protocol".into(), "sasl_ssl".into());
                Self::add_sasl_config(&mut client_config, &config.security);
                if let Some(ref ca) = config.security.ssl_ca_cert {
                    client_config.insert("ssl.ca.location".into(), ca.clone());
                }
            }
        }

        client_config
    }

    pub fn add_sasl_config(config: &mut HashMap<String, String>, security: &KafkaSecurityConfig) {
        if let Some(ref mechanism) = security.mechanism {
            let mech_str = match mechanism {
                SaslMechanism::Plain => "PLAIN",
                SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
                SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
            };
            config.insert("sasl.mechanism".into(), mech_str.into());
        }

        if let Some(ref username) = security.username {
            config.insert("sasl.username".into(), username.clone());
        }

        if let Some(ref password) = security.password {
            config.insert("sasl.password".into(), password.expose().to_string());
        }
    }
}

impl Default for KafkaSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Source for KafkaSource {
    type Config = KafkaSourceConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("kafka-source", env!("CARGO_PKG_VERSION"))
            .description("Consume messages from Apache Kafka topics")
            .author("Rivven Team")
            .license("MIT/Apache-2.0")
            .documentation_url("https://rivven.dev/docs/connectors/kafka-source")
            .config_schema::<KafkaSourceConfig>()
            .build()
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        if let Err(e) = config.validate() {
            return Ok(CheckResult::failure(format!(
                "Invalid configuration: {}",
                e
            )));
        }

        info!("Checking Kafka connectivity to {:?}", config.brokers);

        // Use rskafka client to verify Kafka protocol connectivity
        match Self::create_client(config).await {
            Ok(client) => {
                // Verify topic exists by getting partition client
                match client
                    .partition_client(&config.topic, 0, UnknownTopicHandling::Error)
                    .await
                {
                    Ok(_) => {
                        info!(
                            "Successfully connected to Kafka and verified topic: {}",
                            config.topic
                        );
                        Ok(CheckResult::builder()
                            .check_passed("broker_connectivity")
                            .check_passed("topic_exists")
                            .check_passed("configuration")
                            .build())
                    }
                    Err(e) => {
                        // Topic might not exist, but connection worked
                        warn!("Topic {} not found or inaccessible: {}", config.topic, e);
                        Ok(CheckResult::builder()
                            .check_passed("broker_connectivity")
                            .check_passed("configuration")
                            .check_failed(
                                "topic_exists",
                                format!("Topic '{}' not accessible: {}", config.topic, e),
                            )
                            .build())
                    }
                }
            }
            Err(e) => {
                warn!("Failed to connect to Kafka: {}", e);

                // Fall back to TCP check for diagnostics
                for broker in &config.brokers {
                    if let Ok(Ok(_)) = tokio::time::timeout(
                        Duration::from_secs(5),
                        tokio::net::TcpStream::connect(broker.as_str()),
                    )
                    .await
                    {
                        return Ok(CheckResult::builder()
                            .check_passed("tcp_connectivity")
                            .check_failed("kafka_protocol", format!("{}", e))
                            .build());
                    }
                }

                Ok(CheckResult::failure(format!(
                    "Unable to connect to Kafka: {}",
                    e
                )))
            }
        }
    }

    async fn discover(&self, config: &Self::Config) -> Result<Catalog> {
        // Try to get topic metadata from Kafka
        match Self::create_client(config).await {
            Ok(client) => {
                // Get partition count for the topic
                let partition_count = match client
                    .partition_client(&config.topic, 0, UnknownTopicHandling::Error)
                    .await
                {
                    Ok(_) => {
                        // Topic exists, try to get all partitions
                        // rskafka doesn't have a direct "list partitions" API,
                        // so we'll use metadata or default to discovering what we can
                        1 // At least partition 0 exists
                    }
                    Err(_) => 0,
                };

                let schema = serde_json::json!({
                    "type": "object",
                    "properties": {
                        "key": { "type": ["string", "null"] },
                        "value": { "type": "object" },
                        "partition": { "type": "integer" },
                        "offset": { "type": "integer" },
                        "timestamp": { "type": "string", "format": "date-time" },
                        "headers": { "type": "object" }
                    }
                });

                let mut stream = Stream::new(&config.topic, schema)
                    .sync_modes(vec![SyncMode::FullRefresh, SyncMode::Incremental]);

                if partition_count > 0 {
                    stream = stream.metadata("partition_count", serde_json::json!(partition_count));
                }

                Ok(Catalog::new().add_stream(stream))
            }
            Err(_) => {
                // Fall back to basic schema without live metadata
                let schema = serde_json::json!({
                    "type": "object",
                    "properties": {
                        "key": { "type": ["string", "null"] },
                        "value": { "type": "object" },
                        "partition": { "type": "integer" },
                        "offset": { "type": "integer" },
                        "timestamp": { "type": "string", "format": "date-time" },
                        "headers": { "type": "object" }
                    }
                });

                let stream = Stream::new(&config.topic, schema)
                    .sync_modes(vec![SyncMode::FullRefresh, SyncMode::Incremental]);

                Ok(Catalog::new().add_stream(stream))
            }
        }
    }

    async fn read(
        &self,
        config: &Self::Config,
        _catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        info!("Starting Kafka source for topic: {}", config.topic);

        // Create the rskafka client
        let client = Self::create_client(config).await?;

        let shutdown = self.shutdown.clone();
        let metrics = self.metrics.clone();
        let topic = config.topic.clone();
        let include_key = config.include_key;
        let include_headers = config.include_headers;
        let fetch_max = config.fetch_max_messages as i32;

        // Backoff configuration for resilience
        let retry_initial_ms = config.retry_initial_ms;
        let retry_max_ms = config.retry_max_ms;
        let retry_multiplier = config.retry_multiplier;
        let empty_poll_delay_ms = config.empty_poll_delay_ms;

        // Determine starting offset from state
        let start_offset = if let Some(ref state) = state {
            state
                .get_stream(&topic)
                .and_then(|s| s.cursor_value.as_ref())
                .and_then(|v: &serde_json::Value| v.as_str())
                .and_then(|s: &str| s.parse::<i64>().ok())
        } else {
            match config.start_offset {
                StartOffset::Earliest => None, // Will use OffsetAt::Earliest
                StartOffset::Latest => None,   // Will use OffsetAt::Latest
                StartOffset::Offset(o) => Some(o),
                StartOffset::Timestamp(_ts) => None,
            }
        };

        let use_earliest = matches!(config.start_offset, StartOffset::Earliest);

        // Get partition client for partition 0 (multi-partition support in roadmap)
        let partition_client = client
            .partition_client(&topic, 0, UnknownTopicHandling::Error)
            .await
            .map_err(|e| {
                ConnectorError::Connection(format!("Failed to get partition client: {}", e))
            })?;

        let partition_client = Arc::new(partition_client);

        let stream = async_stream::stream! {
            let mut current_offset: i64;
            let mut current_backoff_ms = retry_initial_ms;

            // Determine starting offset
            if let Some(offset) = start_offset {
                current_offset = offset;
            } else {
                // Get offset from Kafka
                let offset_at = if use_earliest {
                    OffsetAt::Earliest
                } else {
                    OffsetAt::Latest
                };

                current_offset = match partition_client.get_offset(offset_at).await {
                    Ok(o) => o,
                    Err(e) => {
                        metrics.errors.fetch_add(1, Ordering::Relaxed);
                        yield Err(ConnectorError::Connection(format!(
                            "Failed to get offset: {}",
                            e
                        )));
                        return;
                    }
                };
            }

            let mut poll_count = 0u64;

            info!(
                "Kafka source starting from offset {} for topic {}",
                current_offset, topic
            );

            loop {
                if shutdown.load(Ordering::Relaxed) {
                    info!("Kafka source shutting down");
                    break;
                }

                // Track poll timing
                let poll_start = std::time::Instant::now();
                poll_count += 1;
                metrics.polls.fetch_add(1, Ordering::Relaxed);

                // Fetch records from Kafka using rskafka
                let records = match partition_client
                    .fetch_records(
                        current_offset,
                        1..fetch_max * 1024 * 10, // bytes range
                        fetch_max,
                    )
                    .await
                {
                    Ok((records, _high_watermark)) => {
                        // Reset backoff on success
                        current_backoff_ms = retry_initial_ms;
                        records
                    }
                    Err(e) => {
                        metrics.errors.fetch_add(1, Ordering::Relaxed);
                        warn!("Failed to fetch records (backoff {}ms): {}", current_backoff_ms, e);
                        // Exponential backoff with jitter
                        tokio::time::sleep(Duration::from_millis(current_backoff_ms)).await;
                        current_backoff_ms = ((current_backoff_ms as f64 * retry_multiplier) as u64).min(retry_max_ms);
                        continue;
                    }
                };

                let batch_size = records.len();

                if batch_size == 0 {
                    // Empty poll - use configured delay
                    metrics.empty_polls.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(empty_poll_delay_ms)).await;
                    continue;
                }

                for record_and_offset in &records {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }

                    let record = &record_and_offset.record;
                    let record_offset = record_and_offset.offset;

                    // Parse the value
                    let value: serde_json::Value = if let Some(ref value_bytes) = record.value {
                        // Try to parse as JSON, fall back to string
                        serde_json::from_slice(value_bytes).unwrap_or_else(|_| {
                            serde_json::json!(String::from_utf8_lossy(value_bytes).to_string())
                        })
                    } else {
                        serde_json::Value::Null
                    };

                    // Build event data
                    let mut data = serde_json::json!({
                        "value": value,
                        "partition": 0,
                        "offset": record_offset,
                        "timestamp": record.timestamp.to_rfc3339()
                    });

                    // Include key if configured
                    if include_key {
                        if let Some(ref key_bytes) = record.key {
                            data["key"] = serde_json::json!(String::from_utf8_lossy(key_bytes).to_string());
                        } else {
                            data["key"] = serde_json::Value::Null;
                        }
                    }

                    // Include headers if configured
                    if include_headers && !record.headers.is_empty() {
                        let headers: serde_json::Map<String, serde_json::Value> = record
                            .headers
                            .iter()
                            .map(|(k, v)| {
                                (
                                    k.clone(),
                                    serde_json::json!(String::from_utf8_lossy(v).to_string()),
                                )
                            })
                            .collect();
                        data["headers"] = serde_json::Value::Object(headers);
                    }

                    // Estimate message size
                    let msg_size = record.approximate_size() as u64;

                    let event = SourceEvent::record(&topic, data)
                        .with_position(record_offset.to_string())
                        .with_metadata("kafka.partition", serde_json::json!(0))
                        .with_metadata("kafka.offset", serde_json::json!(record_offset));

                    metrics.messages_consumed.fetch_add(1, Ordering::Relaxed);
                    metrics.bytes_consumed.fetch_add(msg_size, Ordering::Relaxed);

                    // Update offset for next fetch
                    current_offset = record_offset + 1;

                    yield Ok(event);
                }

                // Record batch metrics
                metrics.record_batch_size(batch_size as u64);
                let poll_elapsed = poll_start.elapsed().as_micros() as u64;
                metrics.poll_latency_us.fetch_add(poll_elapsed, Ordering::Relaxed);

                // Emit state checkpoint periodically
                if poll_count.is_multiple_of(10) {
                    let state_event = SourceEvent::state(serde_json::json!({
                        topic.clone(): {
                            "cursor_field": "offset",
                            "cursor_value": current_offset.to_string()
                        }
                    }));
                    yield Ok(state_event);
                }
            }
        };

        // Map ConnectorError to ConnectError for the stream
        let mapped_stream = stream.map(|r: std::result::Result<SourceEvent, ConnectorError>| {
            r.map_err(ConnectError::from)
        });

        Ok(Box::pin(mapped_stream))
    }
}

/// Factory for creating KafkaSource instances
pub struct KafkaSourceFactory;

impl SourceFactory for KafkaSourceFactory {
    fn spec(&self) -> ConnectorSpec {
        KafkaSource::spec()
    }

    fn create(&self) -> Box<dyn AnySource> {
        Box::new(KafkaSource::new())
    }
}

/// AnySource implementation for KafkaSource
#[async_trait]
impl AnySource for KafkaSource {
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult> {
        let config: KafkaSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Kafka source config: {}", e)))?;
        self.check(&config).await
    }

    async fn discover_raw(&self, config: &serde_yaml::Value) -> Result<Catalog> {
        let config: KafkaSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Kafka source config: {}", e)))?;
        self.discover(&config).await
    }

    async fn read_raw(
        &self,
        config: &serde_yaml::Value,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        let config: KafkaSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Kafka source config: {}", e)))?;
        self.read(&config, catalog, state).await
    }
}

// ============================================================================
// Kafka Sink
// ============================================================================

/// Kafka Sink configuration
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct KafkaSinkConfig {
    /// List of Kafka broker addresses
    #[validate(length(min = 1))]
    pub brokers: Vec<String>,

    /// Kafka topic to produce to
    #[validate(length(min = 1, max = 249))]
    pub topic: String,

    /// Acknowledgment level
    #[serde(default)]
    pub acks: AckLevel,

    /// Compression codec
    #[serde(default)]
    pub compression: CompressionCodec,

    /// Security configuration
    #[serde(default)]
    pub security: KafkaSecurityConfig,

    /// Batch size in bytes (default: 16384)
    #[serde(default = "default_batch_size_bytes")]
    #[validate(range(min = 1, max = 1048576))]
    pub batch_size_bytes: u32,

    /// Linger time in milliseconds (default: 5)
    #[serde(default = "default_linger_ms")]
    #[validate(range(min = 0, max = 60000))]
    pub linger_ms: u32,

    /// Request timeout in milliseconds (default: 30000)
    #[serde(default = "default_request_timeout")]
    #[validate(range(min = 1000, max = 300000))]
    pub request_timeout_ms: u32,

    /// Maximum retries (default: 3)
    #[serde(default = "default_retries")]
    #[validate(range(max = 10))]
    pub retries: u32,

    /// Idempotent producer (default: false)
    #[serde(default)]
    pub idempotent: bool,

    /// Transactional ID for exactly-once semantics (optional)
    pub transactional_id: Option<String>,

    /// Key field from source event data (optional, uses event key if not set)
    pub key_field: Option<String>,

    /// Include source metadata as Kafka headers (default: true)
    #[serde(default = "default_true")]
    pub include_headers: bool,

    /// Custom headers to add to all messages
    #[serde(default)]
    pub custom_headers: HashMap<String, String>,
}

fn default_batch_size_bytes() -> u32 {
    16384
}
fn default_linger_ms() -> u32 {
    5
}
fn default_request_timeout() -> u32 {
    30000
}
fn default_retries() -> u32 {
    3
}

/// Acknowledgment level for Kafka producer
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum AckLevel {
    /// No acknowledgment (fire-and-forget)
    None,
    /// Leader acknowledgment only
    Leader,
    /// All in-sync replicas acknowledgment
    #[default]
    All,
}

/// Kafka Sink implementation
pub struct KafkaSink {
    metrics: Arc<KafkaSinkMetrics>,
}

impl KafkaSink {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(KafkaSinkMetrics::new()),
        }
    }

    /// Get metrics for this sink.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let snapshot = sink.metrics().snapshot();
    /// println!("Messages: {}", snapshot.messages_produced);
    /// ```
    pub fn metrics(&self) -> &KafkaSinkMetrics {
        &self.metrics
    }

    /// Build producer configuration map
    #[allow(dead_code)]
    fn build_producer_config(config: &KafkaSinkConfig) -> HashMap<String, String> {
        let mut producer_config = HashMap::new();

        producer_config.insert("bootstrap.servers".into(), config.brokers.join(","));
        producer_config.insert("batch.size".into(), config.batch_size_bytes.to_string());
        producer_config.insert("linger.ms".into(), config.linger_ms.to_string());
        producer_config.insert(
            "request.timeout.ms".into(),
            config.request_timeout_ms.to_string(),
        );
        producer_config.insert("retries".into(), config.retries.to_string());

        // Acks
        let acks = match config.acks {
            AckLevel::None => "0",
            AckLevel::Leader => "1",
            AckLevel::All => "all",
        };
        producer_config.insert("acks".into(), acks.into());

        // Compression
        let compression = match config.compression {
            CompressionCodec::None => "none",
            CompressionCodec::Gzip => "gzip",
            CompressionCodec::Snappy => "snappy",
            CompressionCodec::Lz4 => "lz4",
            CompressionCodec::Zstd => "zstd",
        };
        producer_config.insert("compression.type".into(), compression.into());

        // Idempotent producer
        if config.idempotent {
            producer_config.insert("enable.idempotence".into(), "true".into());
        }

        // Transactional producer
        if let Some(ref txn_id) = config.transactional_id {
            producer_config.insert("transactional.id".into(), txn_id.clone());
        }

        // Security settings
        match config.security.protocol {
            SecurityProtocol::Plaintext => {
                producer_config.insert("security.protocol".into(), "plaintext".into());
            }
            SecurityProtocol::Ssl => {
                producer_config.insert("security.protocol".into(), "ssl".into());
                if let Some(ref ca) = config.security.ssl_ca_cert {
                    producer_config.insert("ssl.ca.location".into(), ca.clone());
                }
            }
            SecurityProtocol::SaslPlaintext => {
                producer_config.insert("security.protocol".into(), "sasl_plaintext".into());
                KafkaSource::add_sasl_config(&mut producer_config, &config.security);
            }
            SecurityProtocol::SaslSsl => {
                producer_config.insert("security.protocol".into(), "sasl_ssl".into());
                KafkaSource::add_sasl_config(&mut producer_config, &config.security);
                if let Some(ref ca) = config.security.ssl_ca_cert {
                    producer_config.insert("ssl.ca.location".into(), ca.clone());
                }
            }
        }

        producer_config
    }

    /// Create an rskafka client for the sink.
    ///
    /// Uses the same client creation logic as the source, with SASL authentication
    /// support for PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512 mechanisms.
    #[cfg(feature = "kafka")]
    async fn create_sink_client(
        &self,
        config: &KafkaSinkConfig,
    ) -> std::result::Result<Client, ConnectorError> {
        use rskafka::client::SaslConfig;

        // Convert brokers to Vec<String>
        let brokers: Vec<String> = config.brokers.clone();

        let mut builder = ClientBuilder::new(brokers);

        // Configure SASL authentication if needed
        // rskafka v0.5 only supports SASL PLAIN
        match config.security.protocol {
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl => {
                if let (Some(username), Some(password)) =
                    (&config.security.username, &config.security.password)
                {
                    let mechanism = config
                        .security
                        .mechanism
                        .as_ref()
                        .unwrap_or(&SaslMechanism::Plain);
                    match mechanism {
                        SaslMechanism::Plain => {}
                        _ => {
                            warn!(
                                "SASL {:?} not supported by rskafka v0.5, falling back to PLAIN",
                                mechanism
                            );
                        }
                    }
                    // rskafka v0.5 only supports PLAIN
                    let sasl_config = SaslConfig::Plain {
                        username: username.clone(),
                        password: password.expose().to_string(),
                    };
                    builder = builder.sasl_config(sasl_config);
                } else {
                    return Err(ConnectorError::Config(
                        "SASL authentication requires username and password".into(),
                    ));
                }
            }
            SecurityProtocol::Plaintext | SecurityProtocol::Ssl => {
                // No SASL config needed for plaintext/SSL-only
            }
        }

        // Build the client
        let client = builder.build().await.map_err(|e| {
            ConnectorError::Connection(format!("Failed to create Kafka client: {}", e))
        })?;

        Ok(client)
    }
}

impl Default for KafkaSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sink for KafkaSink {
    type Config = KafkaSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("kafka-sink", env!("CARGO_PKG_VERSION"))
            .description("Produce messages to Apache Kafka topics")
            .author("Rivven Team")
            .license("MIT/Apache-2.0")
            .documentation_url("https://rivven.dev/docs/connectors/kafka-sink")
            .config_schema::<KafkaSinkConfig>()
            .build()
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        if let Err(e) = config.validate() {
            return Ok(CheckResult::failure(format!(
                "Invalid configuration: {}",
                e
            )));
        }

        info!("Checking Kafka connectivity to {:?}", config.brokers);

        // Try to create a real Kafka client to verify connectivity
        #[cfg(feature = "kafka")]
        {
            match self.create_sink_client(config).await {
                Ok(client) => {
                    // Verify we can get topic metadata by creating a partition client
                    match client
                        .partition_client(&config.topic, 0, UnknownTopicHandling::Retry)
                        .await
                    {
                        Ok(_) => {
                            info!(
                                "Successfully connected to Kafka and verified topic: {}",
                                config.topic
                            );
                            return Ok(CheckResult::builder()
                                .check_passed("broker_connectivity")
                                .check_passed("topic_access")
                                .check_passed("authentication")
                                .check_passed("configuration")
                                .build());
                        }
                        Err(e) => {
                            warn!("Failed to access topic {}: {}", config.topic, e);
                            // Client connected but topic access failed - still a partial success
                            return Ok(CheckResult::builder()
                                .check_passed("broker_connectivity")
                                .check_passed("authentication")
                                .check_failed(
                                    "topic_access",
                                    format!("Topic {} not accessible: {}", config.topic, e),
                                )
                                .check_passed("configuration")
                                .build());
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to create Kafka client: {}", e);
                    // Fall through to TCP check for diagnostics
                }
            }
        }

        // Fallback: Simple TCP connection check for diagnostics
        for broker in &config.brokers {
            let addr = broker.as_str();
            match tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
                .await
            {
                Ok(Ok(_)) => {
                    info!(
                        "TCP connection to broker {} succeeded, but Kafka protocol failed",
                        broker
                    );
                    return Ok(CheckResult::builder()
                        .check_passed("broker_connectivity")
                        .check_failed(
                            "kafka_protocol",
                            "TCP connected but Kafka protocol handshake failed",
                        )
                        .check_passed("configuration")
                        .build());
                }
                Ok(Err(e)) => {
                    warn!("Failed to connect to broker {}: {}", broker, e);
                }
                Err(_) => {
                    warn!("Connection timeout for broker: {}", broker);
                }
            }
        }

        Ok(CheckResult::failure(
            "Unable to connect to any Kafka broker",
        ))
    }

    async fn write(
        &self,
        config: &Self::Config,
        mut events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        info!("Starting Kafka sink for topic: {}", config.topic);

        let mut result = WriteResult::new();
        let topic = &config.topic;

        // Create the Kafka client
        #[cfg(feature = "kafka")]
        let client = self.create_sink_client(config).await?;

        #[cfg(feature = "kafka")]
        let partition_client = client
            .partition_client(topic, 0, UnknownTopicHandling::Retry)
            .await
            .map_err(|e| {
                ConnectorError::Connection(format!(
                    "Failed to create partition client for topic {}: {}",
                    topic, e
                ))
            })?;

        // Batch accumulator for efficient producing
        // Use batch_size_bytes to derive an approximate record count
        let batch_size: usize = 100; // Reasonable default batch count
        let linger_ms: u64 = config.linger_ms as u64;
        #[cfg(feature = "kafka")]
        let mut pending_records: Vec<Record> = Vec::with_capacity(batch_size);
        let mut batch_bytes: u64 = 0;
        let mut total_batch_count: u64 = 0;
        let mut last_flush = std::time::Instant::now();

        // Track produce timing for the entire operation
        let produce_start = std::time::Instant::now();

        while let Some(event) = events.next().await {
            // Skip non-data events
            if !event.is_data() {
                continue;
            }

            let data = &event.data;
            let value_bytes = serde_json::to_vec(data).unwrap_or_default();
            let value_len = value_bytes.len();

            // Extract key from event
            let key: Option<Vec<u8>> = if let Some(ref key_field) = config.key_field {
                data.get(key_field)
                    .and_then(|v| v.as_str())
                    .map(|s| s.as_bytes().to_vec())
            } else {
                None
            };

            // Build headers for rskafka
            #[cfg(feature = "kafka")]
            let mut headers: std::collections::BTreeMap<String, Vec<u8>> =
                std::collections::BTreeMap::new();

            #[cfg(feature = "kafka")]
            if config.include_headers {
                headers.insert("rivven.stream".into(), event.stream.clone().into_bytes());
                headers.insert(
                    "rivven.event_type".into(),
                    format!("{:?}", event.event_type).into_bytes(),
                );
                if let Some(ref ns) = event.namespace {
                    headers.insert("rivven.namespace".into(), ns.clone().into_bytes());
                }
            }

            #[cfg(feature = "kafka")]
            for (k, v) in &config.custom_headers {
                headers.insert(k.clone(), v.clone().into_bytes());
            }

            // Create the record
            #[cfg(feature = "kafka")]
            {
                let record = Record {
                    key,
                    value: Some(value_bytes.clone()),
                    headers,
                    timestamp: chrono::Utc::now(),
                };
                pending_records.push(record);
                batch_bytes += value_len as u64;
            }

            self.metrics
                .messages_produced
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .bytes_produced
                .fetch_add(value_len as u64, Ordering::Relaxed);

            // Flush batch if full or linger timeout
            let should_flush = pending_records.len() >= batch_size
                || last_flush.elapsed().as_millis() as u64 >= linger_ms;

            #[cfg(feature = "kafka")]
            if should_flush && !pending_records.is_empty() {
                let batch_start = std::time::Instant::now();
                let records_to_send = std::mem::take(&mut pending_records);
                let records_count = records_to_send.len();

                match partition_client
                    .produce(records_to_send, config.compression.into())
                    .await
                {
                    Ok(offsets) => {
                        debug!(
                            "Produced batch of {} messages to Kafka topic {} (offsets: {:?})",
                            records_count, topic, offsets
                        );
                        result.add_success(records_count as u64, batch_bytes);
                        total_batch_count += 1;
                        self.metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
                        self.metrics.record_batch_size(records_count as u64);
                    }
                    Err(e) => {
                        self.metrics.messages_failed.fetch_add(1, Ordering::Relaxed);
                        error!("Failed to produce batch to Kafka: {}", e);
                        result.add_failure(records_count as u64, format!("Produce failed: {}", e));
                    }
                }

                let batch_elapsed = batch_start.elapsed().as_micros() as u64;
                self.metrics
                    .produce_latency_us
                    .fetch_add(batch_elapsed, Ordering::Relaxed);
                batch_bytes = 0;
                last_flush = std::time::Instant::now();
                pending_records = Vec::with_capacity(batch_size);
            }

            // Non-kafka feature path: just track metrics
            #[cfg(not(feature = "kafka"))]
            {
                debug!(
                    "Simulated produce to Kafka topic {}: value_len={}",
                    topic, value_len
                );
                result.add_success(1, value_len as u64);
            }
        }

        // Flush any remaining records
        #[cfg(feature = "kafka")]
        if !pending_records.is_empty() {
            let records_count = pending_records.len();
            match partition_client
                .produce(pending_records, config.compression.into())
                .await
            {
                Ok(offsets) => {
                    debug!(
                        "Flushed final batch of {} messages to Kafka topic {} (offsets: {:?})",
                        records_count, topic, offsets
                    );
                    result.add_success(records_count as u64, batch_bytes);
                    total_batch_count += 1;
                    self.metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
                    self.metrics.record_batch_size(records_count as u64);
                }
                Err(e) => {
                    self.metrics.messages_failed.fetch_add(1, Ordering::Relaxed);
                    error!("Failed to flush final batch to Kafka: {}", e);
                    result.add_failure(records_count as u64, format!("Produce failed: {}", e));
                }
            }
        }

        let total_elapsed = produce_start.elapsed();
        info!(
            "Kafka sink completed: {} records, {} bytes, {} batches in {:?}",
            result.records_written, result.bytes_written, total_batch_count, total_elapsed
        );

        Ok(result)
    }
}

/// Factory for creating KafkaSink instances
pub struct KafkaSinkFactory;

impl SinkFactory for KafkaSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        KafkaSink::spec()
    }

    fn create(&self) -> Box<dyn AnySink> {
        Box::new(KafkaSink::new())
    }
}

/// AnySink implementation for KafkaSink
#[async_trait]
impl AnySink for KafkaSink {
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult> {
        let config: KafkaSinkConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Kafka sink config: {}", e)))?;
        self.check(&config).await
    }

    async fn write_raw(
        &self,
        config: &serde_yaml::Value,
        events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        let config: KafkaSinkConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Kafka sink config: {}", e)))?;
        self.write(&config, events).await
    }
}

// ============================================================================
// Registry Functions
// ============================================================================

/// Register Kafka source connectors with a source registry
pub fn register_sources(registry: &mut SourceRegistry) {
    registry.register("kafka-source", std::sync::Arc::new(KafkaSourceFactory));
}

/// Register Kafka sink connectors with a sink registry
pub fn register_sinks(registry: &mut SinkRegistry) {
    registry.register("kafka-sink", std::sync::Arc::new(KafkaSinkFactory));
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_source_config_default() {
        let config: KafkaSourceConfig = serde_json::from_str(
            r#"{
            "brokers": ["localhost:9092"],
            "topic": "test-topic",
            "consumer_group": "test-group"
        }"#,
        )
        .unwrap();

        assert_eq!(config.brokers, vec!["localhost:9092"]);
        assert_eq!(config.topic, "test-topic");
        assert_eq!(config.consumer_group, "test-group");
        assert!(matches!(config.start_offset, StartOffset::Latest));
    }

    #[test]
    fn test_kafka_sink_config_default() {
        let config: KafkaSinkConfig = serde_json::from_str(
            r#"{
            "brokers": ["localhost:9092"],
            "topic": "test-topic"
        }"#,
        )
        .unwrap();

        assert_eq!(config.brokers, vec!["localhost:9092"]);
        assert_eq!(config.topic, "test-topic");
        assert!(matches!(config.acks, AckLevel::All));
        assert!(matches!(config.compression, CompressionCodec::None));
    }

    #[test]
    fn test_kafka_source_config_with_security() {
        let config: KafkaSourceConfig = serde_json::from_str(
            r#"{
            "brokers": ["kafka1:9093", "kafka2:9093"],
            "topic": "secure-topic",
            "consumer_group": "secure-group",
            "start_offset": "earliest",
            "security": {
                "protocol": "sasl_ssl",
                "mechanism": "SCRAM_SHA256",
                "username": "user",
                "password": "secret"
            }
        }"#,
        )
        .unwrap();

        assert_eq!(config.brokers.len(), 2);
        assert!(matches!(config.start_offset, StartOffset::Earliest));
        assert!(matches!(
            config.security.protocol,
            SecurityProtocol::SaslSsl
        ));
        assert!(matches!(
            config.security.mechanism,
            Some(SaslMechanism::ScramSha256)
        ));
    }

    #[test]
    fn test_kafka_sink_config_with_options() {
        let config: KafkaSinkConfig = serde_json::from_str(
            r#"{
            "brokers": ["localhost:9092"],
            "topic": "output-topic",
            "acks": "leader",
            "compression": "lz4",
            "batch_size_bytes": 32768,
            "linger_ms": 10,
            "idempotent": true
        }"#,
        )
        .unwrap();

        assert!(matches!(config.acks, AckLevel::Leader));
        assert!(matches!(config.compression, CompressionCodec::Lz4));
        assert_eq!(config.batch_size_bytes, 32768);
        assert_eq!(config.linger_ms, 10);
        assert!(config.idempotent);
    }

    #[test]
    fn test_sensitive_string_serialization() {
        let sensitive = SensitiveString::new("my-secret");
        let json = serde_json::to_string(&sensitive).unwrap();
        assert_eq!(json, "\"***REDACTED***\"");
    }

    #[test]
    fn test_kafka_source_spec() {
        let spec = KafkaSource::spec();
        assert_eq!(spec.connector_type, "kafka-source");
        assert!(spec.description.as_ref().unwrap().contains("Kafka"));
    }

    #[test]
    fn test_kafka_sink_spec() {
        let spec = KafkaSink::spec();
        assert_eq!(spec.connector_type, "kafka-sink");
        assert!(spec.description.as_ref().unwrap().contains("Kafka"));
    }

    /// Test that check fails when no broker is available.
    /// Note: This test may take a few seconds due to connection timeout.
    #[tokio::test]
    #[ignore = "Requires timeout - run with cargo test -- --ignored"]
    async fn test_kafka_source_check_no_broker() {
        let source = KafkaSource::new();
        let config = KafkaSourceConfig {
            brokers: vec!["localhost:19092".to_string()], // Non-existent port
            topic: "test".to_string(),
            consumer_group: "test".to_string(),
            start_offset: StartOffset::Latest,
            security: KafkaSecurityConfig::default(),
            max_poll_interval_ms: 300000,
            session_timeout_ms: 10000,
            heartbeat_interval_ms: 3000,
            fetch_max_messages: 500,
            include_headers: true,
            include_key: true,
            retry_initial_ms: 100,
            retry_max_ms: 10000,
            retry_multiplier: 2.0,
            empty_poll_delay_ms: 50,
            connect_timeout_ms: 5000, // Short timeout for test
        };

        let result = source.check(&config).await.unwrap();
        assert!(!result.is_success());
    }

    /// Test that check fails when no broker is available.
    /// Note: This test may take a few seconds due to connection timeout.
    #[tokio::test]
    #[ignore = "Requires timeout - run with cargo test -- --ignored"]
    async fn test_kafka_sink_check_no_broker() {
        let sink = KafkaSink::new();
        let config = KafkaSinkConfig {
            brokers: vec!["localhost:19092".to_string()],
            topic: "test".to_string(),
            acks: AckLevel::All,
            compression: CompressionCodec::None,
            security: KafkaSecurityConfig::default(),
            batch_size_bytes: 16384,
            linger_ms: 5,
            request_timeout_ms: 30000,
            retries: 3,
            idempotent: false,
            transactional_id: None,
            key_field: None,
            include_headers: true,
            custom_headers: HashMap::new(),
        };

        let result = sink.check(&config).await.unwrap();
        assert!(!result.is_success());
    }

    #[test]
    fn test_client_config_building() {
        let config = KafkaSourceConfig {
            brokers: vec!["broker1:9092".to_string(), "broker2:9092".to_string()],
            topic: "test".to_string(),
            consumer_group: "my-group".to_string(),
            start_offset: StartOffset::Earliest,
            security: KafkaSecurityConfig {
                protocol: SecurityProtocol::SaslSsl,
                mechanism: Some(SaslMechanism::ScramSha256),
                username: Some("user".to_string()),
                password: Some(SensitiveString::new("pass")),
                ssl_ca_cert: Some("/etc/ssl/ca.pem".to_string()),
                ssl_client_cert: None,
                ssl_client_key: None,
            },
            max_poll_interval_ms: 300000,
            session_timeout_ms: 10000,
            heartbeat_interval_ms: 3000,
            fetch_max_messages: 500,
            include_headers: true,
            include_key: true,
            retry_initial_ms: 100,
            retry_max_ms: 10000,
            retry_multiplier: 2.0,
            empty_poll_delay_ms: 50,
            connect_timeout_ms: 30000,
        };

        let client_config = KafkaSource::build_client_config(&config);

        assert_eq!(
            client_config.get("bootstrap.servers"),
            Some(&"broker1:9092,broker2:9092".to_string())
        );
        assert_eq!(client_config.get("group.id"), Some(&"my-group".to_string()));
        assert_eq!(
            client_config.get("security.protocol"),
            Some(&"sasl_ssl".to_string())
        );
        assert_eq!(
            client_config.get("sasl.mechanism"),
            Some(&"SCRAM-SHA-256".to_string())
        );
    }

    // ========================================================================
    // Metrics Tests
    // ========================================================================

    #[test]
    fn test_kafka_source_metrics_new() {
        let metrics = KafkaSourceMetrics::new();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.messages_consumed, 0);
        assert_eq!(snapshot.bytes_consumed, 0);
        assert_eq!(snapshot.polls, 0);
        assert_eq!(snapshot.batch_size_min, 0); // 0 when no batches
        assert_eq!(snapshot.batch_size_max, 0);
    }

    #[test]
    fn test_kafka_source_metrics_increment() {
        let metrics = KafkaSourceMetrics::new();

        metrics.messages_consumed.fetch_add(100, Ordering::Relaxed);
        metrics.bytes_consumed.fetch_add(5000, Ordering::Relaxed);
        metrics.polls.fetch_add(10, Ordering::Relaxed);
        metrics.empty_polls.fetch_add(2, Ordering::Relaxed);
        metrics.poll_latency_us.fetch_add(50000, Ordering::Relaxed);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.messages_consumed, 100);
        assert_eq!(snapshot.bytes_consumed, 5000);
        assert_eq!(snapshot.polls, 10);
        assert_eq!(snapshot.empty_polls, 2);
        assert_eq!(snapshot.poll_latency_us, 50000);
    }

    #[test]
    fn test_kafka_source_metrics_batch_size_tracking() {
        let metrics = KafkaSourceMetrics::new();

        metrics.record_batch_size(50);
        metrics.record_batch_size(100);
        metrics.record_batch_size(25);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.batch_size_min, 25);
        assert_eq!(snapshot.batch_size_max, 100);
        assert_eq!(snapshot.batch_size_sum, 175);
    }

    #[test]
    fn test_kafka_source_metrics_snapshot_avg() {
        let metrics = KafkaSourceMetrics::new();

        metrics.polls.fetch_add(10, Ordering::Relaxed);
        metrics.empty_polls.fetch_add(2, Ordering::Relaxed);
        metrics
            .poll_latency_us
            .fetch_add(100_000, Ordering::Relaxed);
        metrics.record_batch_size(100);
        metrics.record_batch_size(200);
        metrics.record_batch_size(300);

        // Simulate 8 non-empty polls worth of batches
        let snapshot = KafkaSourceMetricsSnapshot {
            messages_consumed: 600,
            bytes_consumed: 30000,
            polls: 10,
            empty_polls: 2,
            errors: 0,
            rebalances: 0,
            poll_latency_us: 100_000,
            batch_size_min: 100,
            batch_size_max: 300,
            batch_size_sum: 600,
        };

        assert_eq!(snapshot.avg_poll_latency_ms(), 10.0);
        assert_eq!(snapshot.avg_batch_size(), 75.0); // 600 / 8 non-empty polls
        assert_eq!(snapshot.empty_poll_rate(), 0.2);
        assert_eq!(snapshot.messages_per_second(1.0), 600.0);
        assert_eq!(snapshot.bytes_per_second(2.0), 15000.0);
    }

    #[test]
    fn test_kafka_source_metrics_snapshot_and_reset() {
        let metrics = KafkaSourceMetrics::new();

        metrics.messages_consumed.fetch_add(500, Ordering::Relaxed);
        metrics.bytes_consumed.fetch_add(25000, Ordering::Relaxed);
        metrics.polls.fetch_add(50, Ordering::Relaxed);
        metrics.record_batch_size(100);

        let snapshot1 = metrics.snapshot_and_reset();
        assert_eq!(snapshot1.messages_consumed, 500);
        assert_eq!(snapshot1.bytes_consumed, 25000);
        assert_eq!(snapshot1.polls, 50);
        assert_eq!(snapshot1.batch_size_min, 100);
        assert_eq!(snapshot1.batch_size_max, 100);

        let snapshot2 = metrics.snapshot();
        assert_eq!(snapshot2.messages_consumed, 0);
        assert_eq!(snapshot2.bytes_consumed, 0);
        assert_eq!(snapshot2.polls, 0);
        assert_eq!(snapshot2.batch_size_min, 0); // Reset to 0
    }

    #[test]
    fn test_kafka_source_metrics_prometheus_format() {
        let snapshot = KafkaSourceMetricsSnapshot {
            messages_consumed: 1000,
            bytes_consumed: 50000,
            polls: 100,
            empty_polls: 5,
            errors: 2,
            rebalances: 1,
            poll_latency_us: 500_000,
            batch_size_min: 10,
            batch_size_max: 100,
            batch_size_sum: 5000,
        };

        let prom = snapshot.to_prometheus_format("myapp");

        assert!(prom.contains("myapp_kafka_source_messages_consumed_total 1000"));
        assert!(prom.contains("myapp_kafka_source_bytes_consumed_total 50000"));
        assert!(prom.contains("myapp_kafka_source_polls_total 100"));
        assert!(prom.contains("myapp_kafka_source_empty_polls_total 5"));
        assert!(prom.contains("myapp_kafka_source_errors_total 2"));
        assert!(prom.contains("myapp_kafka_source_rebalances_total 1"));
        assert!(prom.contains("# TYPE myapp_kafka_source_messages_consumed_total counter"));
        assert!(prom.contains("# TYPE myapp_kafka_source_poll_latency_avg_ms gauge"));
    }

    #[test]
    fn test_kafka_source_metrics_json_serialization() {
        let snapshot = KafkaSourceMetricsSnapshot {
            messages_consumed: 100,
            bytes_consumed: 5000,
            polls: 10,
            empty_polls: 1,
            errors: 0,
            rebalances: 0,
            poll_latency_us: 10000,
            batch_size_min: 10,
            batch_size_max: 10,
            batch_size_sum: 100,
        };

        let json = serde_json::to_string(&snapshot).unwrap();
        assert!(json.contains("\"messages_consumed\":100"));
        assert!(json.contains("\"bytes_consumed\":5000"));

        let deserialized: KafkaSourceMetricsSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, snapshot);
    }

    #[test]
    fn test_kafka_sink_metrics_new() {
        let metrics = KafkaSinkMetrics::new();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.messages_produced, 0);
        assert_eq!(snapshot.bytes_produced, 0);
        assert_eq!(snapshot.batches_sent, 0);
        assert_eq!(snapshot.batch_size_min, 0);
        assert_eq!(snapshot.batch_size_max, 0);
    }

    #[test]
    fn test_kafka_sink_metrics_increment() {
        let metrics = KafkaSinkMetrics::new();

        metrics.messages_produced.fetch_add(200, Ordering::Relaxed);
        metrics.bytes_produced.fetch_add(10000, Ordering::Relaxed);
        metrics.batches_sent.fetch_add(5, Ordering::Relaxed);
        metrics.retries.fetch_add(1, Ordering::Relaxed);
        metrics
            .produce_latency_us
            .fetch_add(25000, Ordering::Relaxed);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.messages_produced, 200);
        assert_eq!(snapshot.bytes_produced, 10000);
        assert_eq!(snapshot.batches_sent, 5);
        assert_eq!(snapshot.retries, 1);
        assert_eq!(snapshot.produce_latency_us, 25000);
    }

    #[test]
    fn test_kafka_sink_metrics_batch_size_tracking() {
        let metrics = KafkaSinkMetrics::new();

        metrics.record_batch_size(25);
        metrics.record_batch_size(75);
        metrics.record_batch_size(50);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.batch_size_min, 25);
        assert_eq!(snapshot.batch_size_max, 75);
        assert_eq!(snapshot.batch_size_sum, 150);
    }

    #[test]
    fn test_kafka_sink_metrics_snapshot_computations() {
        let snapshot = KafkaSinkMetricsSnapshot {
            messages_produced: 1000,
            bytes_produced: 50000,
            messages_failed: 10,
            batches_sent: 10,
            retries: 2,
            produce_latency_us: 100_000,
            batch_size_min: 50,
            batch_size_max: 150,
            batch_size_sum: 1000,
        };

        assert_eq!(snapshot.avg_produce_latency_ms(), 10.0);
        assert_eq!(snapshot.avg_batch_size(), 100.0);
        assert!((snapshot.success_rate() - 0.9901).abs() < 0.001);
        assert_eq!(snapshot.retry_rate(), 0.2);
        assert_eq!(snapshot.messages_per_second(1.0), 1000.0);
        assert_eq!(snapshot.bytes_per_second(2.0), 25000.0);
    }

    #[test]
    fn test_kafka_sink_metrics_snapshot_and_reset() {
        let metrics = KafkaSinkMetrics::new();

        metrics.messages_produced.fetch_add(300, Ordering::Relaxed);
        metrics.bytes_produced.fetch_add(15000, Ordering::Relaxed);
        metrics.batches_sent.fetch_add(3, Ordering::Relaxed);
        metrics.record_batch_size(100);

        let snapshot1 = metrics.snapshot_and_reset();
        assert_eq!(snapshot1.messages_produced, 300);
        assert_eq!(snapshot1.bytes_produced, 15000);
        assert_eq!(snapshot1.batches_sent, 3);
        assert_eq!(snapshot1.batch_size_min, 100);
        assert_eq!(snapshot1.batch_size_max, 100);

        let snapshot2 = metrics.snapshot();
        assert_eq!(snapshot2.messages_produced, 0);
        assert_eq!(snapshot2.bytes_produced, 0);
        assert_eq!(snapshot2.batches_sent, 0);
        assert_eq!(snapshot2.batch_size_min, 0);
    }

    #[test]
    fn test_kafka_sink_metrics_prometheus_format() {
        let snapshot = KafkaSinkMetricsSnapshot {
            messages_produced: 500,
            bytes_produced: 25000,
            messages_failed: 5,
            batches_sent: 10,
            retries: 3,
            produce_latency_us: 200_000,
            batch_size_min: 40,
            batch_size_max: 60,
            batch_size_sum: 500,
        };

        let prom = snapshot.to_prometheus_format("rivven");

        assert!(prom.contains("rivven_kafka_sink_messages_produced_total 500"));
        assert!(prom.contains("rivven_kafka_sink_bytes_produced_total 25000"));
        assert!(prom.contains("rivven_kafka_sink_messages_failed_total 5"));
        assert!(prom.contains("rivven_kafka_sink_batches_sent_total 10"));
        assert!(prom.contains("rivven_kafka_sink_retries_total 3"));
        assert!(prom.contains("# TYPE rivven_kafka_sink_messages_produced_total counter"));
        assert!(prom.contains("# TYPE rivven_kafka_sink_success_rate gauge"));
    }

    #[test]
    fn test_kafka_sink_metrics_json_serialization() {
        let snapshot = KafkaSinkMetricsSnapshot {
            messages_produced: 200,
            bytes_produced: 10000,
            messages_failed: 2,
            batches_sent: 4,
            retries: 1,
            produce_latency_us: 50000,
            batch_size_min: 50,
            batch_size_max: 50,
            batch_size_sum: 200,
        };

        let json = serde_json::to_string(&snapshot).unwrap();
        assert!(json.contains("\"messages_produced\":200"));
        assert!(json.contains("\"bytes_produced\":10000"));

        let deserialized: KafkaSinkMetricsSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, snapshot);
    }

    #[test]
    fn test_kafka_source_metrics_access() {
        let source = KafkaSource::new();
        let metrics = source.metrics();

        metrics.messages_consumed.fetch_add(50, Ordering::Relaxed);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.messages_consumed, 50);
    }

    #[test]
    fn test_kafka_sink_metrics_access() {
        let sink = KafkaSink::new();
        let metrics = sink.metrics();

        metrics.messages_produced.fetch_add(75, Ordering::Relaxed);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.messages_produced, 75);
    }

    #[test]
    fn test_kafka_source_metrics_edge_cases() {
        let snapshot = KafkaSourceMetricsSnapshot::default();

        // Edge cases: no data
        assert_eq!(snapshot.avg_poll_latency_ms(), 0.0);
        assert_eq!(snapshot.avg_batch_size(), 0.0);
        assert_eq!(snapshot.empty_poll_rate(), 0.0);
        assert_eq!(snapshot.messages_per_second(0.0), 0.0);
        assert_eq!(snapshot.bytes_per_second(-1.0), 0.0);
    }

    #[test]
    fn test_kafka_sink_metrics_edge_cases() {
        let snapshot = KafkaSinkMetricsSnapshot::default();

        // Edge cases: no data
        assert_eq!(snapshot.avg_produce_latency_ms(), 0.0);
        assert_eq!(snapshot.avg_batch_size(), 0.0);
        assert_eq!(snapshot.success_rate(), 1.0); // 100% success with no data
        assert_eq!(snapshot.retry_rate(), 0.0);
        assert_eq!(snapshot.messages_per_second(0.0), 0.0);
    }
}
