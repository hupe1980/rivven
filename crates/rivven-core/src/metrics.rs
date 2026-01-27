//! Unified metrics infrastructure for Rivven
//!
//! Uses the `metrics` crate facade for backend-agnostic instrumentation.
//! The Prometheus exporter is enabled by the `metrics` feature flag.
//!
//! # Usage
//!
//! ```rust,ignore
//! use rivven_core::metrics::{CoreMetrics, init_metrics};
//! use std::net::SocketAddr;
//!
//! // Initialize at startup (optional - works without initialization too)
//! let addr: SocketAddr = "0.0.0.0:9090".parse().unwrap();
//! init_metrics(addr).expect("Failed to init metrics");
//!
//! // Record metrics anywhere
//! CoreMetrics::increment_messages_appended();
//! CoreMetrics::record_append_latency_us(150);
//! CoreMetrics::set_partition_count(100);
//! ```
//!
//! # Metric Naming Convention
//!
//! All metrics follow the pattern: `rivven_{component}_{name}_{unit}`
//!
//! - `rivven_core_*` - Storage engine metrics
//! - `rivven_raft_*` - Consensus metrics (from rivven-cluster)
//! - `rivven_cdc_*` - CDC connector metrics (from rivven-cdc)
//! - `rivven_connect_*` - Connector framework metrics

#[cfg(feature = "metrics")]
use std::sync::OnceLock;
use std::time::{Duration, Instant};

// Re-export metrics macros for convenience
pub use metrics::{counter, gauge, histogram};

#[cfg(feature = "metrics")]
static METRICS_INITIALIZED: OnceLock<()> = OnceLock::new();

/// Initialize the Prometheus metrics exporter
///
/// This starts an HTTP server that serves metrics at `/metrics`.
/// Safe to call multiple times (only initializes once).
///
/// # Example
///
/// ```rust,ignore
/// use std::net::SocketAddr;
/// use rivven_core::metrics::init_metrics;
///
/// let addr: SocketAddr = "0.0.0.0:9090".parse().unwrap();
/// init_metrics(addr).expect("Failed to start metrics server");
/// ```
#[cfg(feature = "metrics")]
pub fn init_metrics(
    addr: std::net::SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    METRICS_INITIALIZED.get_or_init(
        || match metrics_exporter_prometheus::PrometheusBuilder::new()
            .with_http_listener(addr)
            .install()
        {
            Ok(()) => {
                tracing::info!(
                    "Prometheus metrics server listening on http://{}/metrics",
                    addr
                );
            }
            Err(e) => {
                tracing::error!("Failed to start Prometheus exporter: {}", e);
            }
        },
    );
    Ok(())
}

/// Initialize metrics without starting a server (for embedding in existing HTTP server)
#[cfg(feature = "metrics")]
pub fn init_metrics_recorder(
) -> Result<metrics_exporter_prometheus::PrometheusHandle, Box<dyn std::error::Error + Send + Sync>>
{
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let handle = builder.install_recorder()?;
    Ok(handle)
}

/// No-op initialization when metrics feature is disabled
#[cfg(not(feature = "metrics"))]
pub fn init_metrics(
    _addr: std::net::SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    Ok(())
}

// ============================================================================
// Core Storage Metrics
// ============================================================================

/// Core storage engine metrics
pub struct CoreMetrics;

impl CoreMetrics {
    // ---- Counters ----

    /// Total messages appended to partitions
    pub fn increment_messages_appended() {
        metrics::counter!("rivven_core_messages_appended_total").increment(1);
    }

    /// Add multiple messages to the counter
    pub fn add_messages_appended(count: u64) {
        metrics::counter!("rivven_core_messages_appended_total").increment(count);
    }

    /// Total messages read from partitions
    pub fn add_messages_read(count: u64) {
        metrics::counter!("rivven_core_messages_read_total").increment(count);
    }

    /// Total batch append operations
    pub fn increment_batch_appends() {
        metrics::counter!("rivven_core_batch_appends_total").increment(1);
    }

    /// Schema registered
    pub fn increment_schemas_registered() {
        metrics::counter!("rivven_core_schemas_registered_total").increment(1);
    }

    /// Schema cache hits
    pub fn increment_schema_cache_hits() {
        metrics::counter!("rivven_core_schema_cache_hits_total").increment(1);
    }

    /// Schema cache misses
    pub fn increment_schema_cache_misses() {
        metrics::counter!("rivven_core_schema_cache_misses_total").increment(1);
    }

    // ---- Security Counters ----

    /// Authentication failures by type
    pub fn increment_auth_failures(auth_type: &str) {
        metrics::counter!("rivven_core_auth_failures_total", "type" => auth_type.to_string())
            .increment(1);
    }

    /// Rate limit rejections
    pub fn increment_rate_limit_rejections() {
        metrics::counter!("rivven_core_rate_limit_rejections_total").increment(1);
    }

    /// Circuit breaker trips
    pub fn increment_circuit_breaker_trips() {
        metrics::counter!("rivven_core_circuit_breaker_trips_total").increment(1);
    }

    /// SQL injection attempts blocked
    pub fn increment_sql_injection_blocked() {
        metrics::counter!("rivven_core_sql_injection_blocked_total").increment(1);
    }

    /// Connection timeouts
    pub fn increment_connection_timeouts() {
        metrics::counter!("rivven_core_connection_timeouts_total").increment(1);
    }

    /// Message size limit exceeded
    pub fn increment_message_size_exceeded() {
        metrics::counter!("rivven_core_message_size_exceeded_total").increment(1);
    }

    // ---- Gauges ----

    /// Active connections
    pub fn set_active_connections(count: u64) {
        metrics::gauge!("rivven_core_active_connections").set(count as f64);
    }

    /// Total partitions
    pub fn set_partition_count(count: u64) {
        metrics::gauge!("rivven_core_partition_count").set(count as f64);
    }

    /// Total segments
    pub fn set_segment_count(count: u64) {
        metrics::gauge!("rivven_core_segment_count").set(count as f64);
    }

    /// Partition offset (per topic/partition)
    pub fn set_partition_offset(topic: &str, partition: u32, offset: u64) {
        metrics::gauge!(
            "rivven_core_partition_offset",
            "topic" => topic.to_string(),
            "partition" => partition.to_string()
        )
        .set(offset as f64);
    }

    /// Schema cache size
    pub fn set_schema_cache_size(size: u64) {
        metrics::gauge!("rivven_core_schema_cache_size").set(size as f64);
    }

    // ---- Histograms ----

    /// Record append latency in microseconds
    pub fn record_append_latency_us(us: u64) {
        metrics::histogram!("rivven_core_append_latency_seconds").record(us as f64 / 1_000_000.0);
    }

    /// Record batch append latency in microseconds
    pub fn record_batch_append_latency_us(us: u64) {
        metrics::histogram!("rivven_core_batch_append_latency_seconds")
            .record(us as f64 / 1_000_000.0);
    }

    /// Record read latency in microseconds
    pub fn record_read_latency_us(us: u64) {
        metrics::histogram!("rivven_core_read_latency_seconds").record(us as f64 / 1_000_000.0);
    }

    /// Record schema lookup latency in nanoseconds
    pub fn record_schema_lookup_latency_ns(ns: u64) {
        metrics::histogram!("rivven_core_schema_lookup_latency_seconds")
            .record(ns as f64 / 1_000_000_000.0);
    }
}

// ============================================================================
// Timer Utility
// ============================================================================

/// Timer for measuring operation durations
pub struct Timer {
    start: Instant,
}

impl Timer {
    /// Create a new timer starting now
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Get elapsed time in microseconds
    pub fn elapsed_us(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }

    /// Get elapsed time in nanoseconds
    pub fn elapsed_ns(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }

    /// Get elapsed Duration
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Default for Timer {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_core_metrics_compile() {
        // Verify all metric methods compile and don't panic
        CoreMetrics::increment_messages_appended();
        CoreMetrics::add_messages_appended(100);
        CoreMetrics::add_messages_read(50);
        CoreMetrics::increment_batch_appends();
        CoreMetrics::increment_schemas_registered();
        CoreMetrics::increment_schema_cache_hits();
        CoreMetrics::increment_schema_cache_misses();
        CoreMetrics::increment_auth_failures("md5");
        CoreMetrics::increment_rate_limit_rejections();
        CoreMetrics::increment_circuit_breaker_trips();
        CoreMetrics::increment_sql_injection_blocked();
        CoreMetrics::increment_connection_timeouts();
        CoreMetrics::increment_message_size_exceeded();
        CoreMetrics::set_active_connections(10);
        CoreMetrics::set_partition_count(100);
        CoreMetrics::set_segment_count(1000);
        CoreMetrics::set_partition_offset("orders", 0, 12345);
        CoreMetrics::set_schema_cache_size(50);
        CoreMetrics::record_append_latency_us(100);
        CoreMetrics::record_batch_append_latency_us(500);
        CoreMetrics::record_read_latency_us(200);
        CoreMetrics::record_schema_lookup_latency_ns(30);
    }

    #[test]
    fn test_timer() {
        let timer = Timer::new();
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(timer.elapsed_us() >= 1000);
        assert!(timer.elapsed_ns() >= 1_000_000);
    }
}
