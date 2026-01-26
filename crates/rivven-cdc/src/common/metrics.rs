//! # CDC Observability and Metrics
//!
//! Production-grade metrics collection for CDC operations.
//!
//! ## Metrics Categories
//!
//! - **Throughput**: Events/sec, bytes/sec, batches/sec
//! - **Latency**: End-to-end, decode, route latencies
//! - **Health**: Connection state, errors, circuit breaker
//! - **Resources**: Memory usage, queue depths, active connections
//!
//! ## Metrics Export
//!
//! Metrics are exported via the `metrics` crate facade, compatible with:
//! - Prometheus (via metrics-exporter-prometheus)
//! - Datadog, Statsd, and other backends
//!
//! ## Usage
//!
//! ```ignore
//! use rivven_cdc::common::CdcMetrics;
//!
//! let metrics = CdcMetrics::new("postgres", "mydb");
//!
//! // Record events (also emitted to metrics facade)
//! metrics.record_event(1024); // 1KB event
//! metrics.record_latency(Duration::from_micros(150));
//!
//! // Get snapshot
//! let snapshot = metrics.snapshot();
//! println!("Throughput: {} events/sec", snapshot.events_per_second);
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// CDC metrics collector with atomic counters for lock-free updates.
#[derive(Debug)]
pub struct CdcMetrics {
    /// Source type (postgres, mysql, mariadb)
    source_type: String,
    /// Database name
    database: String,
    /// Start time for rate calculations
    start_time: Instant,
    /// Last reset time for windowed metrics
    last_reset: RwLock<Instant>,

    // Event counters
    /// Total events processed
    events_total: AtomicU64,
    /// Events in current window
    events_window: AtomicU64,
    /// Total bytes processed
    bytes_total: AtomicU64,
    /// Bytes in current window
    bytes_window: AtomicU64,

    // Operation counters
    /// Insert operations
    inserts: AtomicU64,
    /// Update operations
    updates: AtomicU64,
    /// Delete operations
    deletes: AtomicU64,
    /// Transactions processed
    transactions: AtomicU64,

    // Error counters
    /// Total errors
    errors_total: AtomicU64,
    /// Decode errors
    decode_errors: AtomicU64,
    /// Connection errors
    connection_errors: AtomicU64,
    /// Route errors (failed to deliver to topic)
    route_errors: AtomicU64,

    // Latency tracking (using histogram buckets)
    /// Latency samples for percentile calculation
    latency_samples: RwLock<LatencyHistogram>,

    // Health state
    /// Whether CDC is connected
    connected: AtomicBool,
    /// Whether circuit breaker is open
    circuit_open: AtomicBool,
    /// Last successful event timestamp (epoch ms)
    last_event_timestamp: AtomicU64,
    /// Current lag in milliseconds
    lag_ms: AtomicU64,

    // Queue metrics
    /// Events pending in buffer
    buffer_depth: AtomicU64,
    /// Maximum buffer size
    buffer_capacity: AtomicU64,
}

impl CdcMetrics {
    /// Create a new metrics collector.
    pub fn new(source_type: &str, database: &str) -> Self {
        let now = Instant::now();
        Self {
            source_type: source_type.to_string(),
            database: database.to_string(),
            start_time: now,
            last_reset: RwLock::new(now),

            events_total: AtomicU64::new(0),
            events_window: AtomicU64::new(0),
            bytes_total: AtomicU64::new(0),
            bytes_window: AtomicU64::new(0),

            inserts: AtomicU64::new(0),
            updates: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            transactions: AtomicU64::new(0),

            errors_total: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
            connection_errors: AtomicU64::new(0),
            route_errors: AtomicU64::new(0),

            latency_samples: RwLock::new(LatencyHistogram::new()),

            connected: AtomicBool::new(false),
            circuit_open: AtomicBool::new(false),
            last_event_timestamp: AtomicU64::new(0),
            lag_ms: AtomicU64::new(0),

            buffer_depth: AtomicU64::new(0),
            buffer_capacity: AtomicU64::new(1000),
        }
    }

    /// Record a processed event.
    #[inline]
    pub fn record_event(&self, bytes: u64) {
        self.events_total.fetch_add(1, Ordering::Relaxed);
        self.events_window.fetch_add(1, Ordering::Relaxed);
        self.bytes_total.fetch_add(bytes, Ordering::Relaxed);
        self.bytes_window.fetch_add(bytes, Ordering::Relaxed);
        self.last_event_timestamp.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            Ordering::Relaxed,
        );

        // Emit to metrics facade
        metrics::counter!(
            "rivven_cdc_events_total",
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .increment(1);
        metrics::counter!(
            "rivven_cdc_bytes_total",
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .increment(bytes);
    }

    /// Record an insert operation.
    #[inline]
    pub fn record_insert(&self) {
        self.inserts.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(
            "rivven_cdc_inserts_total",
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .increment(1);
    }

    /// Record an update operation.
    #[inline]
    pub fn record_update(&self) {
        self.updates.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(
            "rivven_cdc_updates_total",
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .increment(1);
    }

    /// Record a delete operation.
    #[inline]
    pub fn record_delete(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(
            "rivven_cdc_deletes_total",
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .increment(1);
    }

    /// Record a committed transaction.
    #[inline]
    pub fn record_transaction(&self) {
        self.transactions.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(
            "rivven_cdc_transactions_total",
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .increment(1);
    }

    /// Record event processing latency.
    pub async fn record_latency(&self, latency: Duration) {
        let mut histogram = self.latency_samples.write().await;
        histogram.record(latency);

        // Emit to metrics facade (in seconds)
        metrics::histogram!(
            "rivven_cdc_latency_seconds",
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .record(latency.as_secs_f64());
    }

    /// Record an error.
    #[inline]
    pub fn record_error(&self, error_type: ErrorType) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
        let type_str = match error_type {
            ErrorType::Decode => {
                self.decode_errors.fetch_add(1, Ordering::Relaxed);
                "decode"
            }
            ErrorType::Connection => {
                self.connection_errors.fetch_add(1, Ordering::Relaxed);
                "connection"
            }
            ErrorType::Route => {
                self.route_errors.fetch_add(1, Ordering::Relaxed);
                "route"
            }
            ErrorType::Other => "other",
        };

        metrics::counter!(
            "rivven_cdc_errors_total",
            "source" => self.source_type.clone(),
            "database" => self.database.clone(),
            "type" => type_str
        )
        .increment(1);
    }

    /// Set connection state.
    #[inline]
    pub fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Relaxed);
        metrics::gauge!(
            "rivven_cdc_connected",
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .set(if connected { 1.0 } else { 0.0 });
    }

    /// Set circuit breaker state.
    #[inline]
    pub fn set_circuit_open(&self, open: bool) {
        self.circuit_open.store(open, Ordering::Relaxed);
        metrics::gauge!(
            "rivven_cdc_circuit_open",
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .set(if open { 1.0 } else { 0.0 });
    }

    /// Update replication lag.
    #[inline]
    pub fn set_lag_ms(&self, lag: u64) {
        self.lag_ms.store(lag, Ordering::Relaxed);
        metrics::gauge!(
            "rivven_cdc_lag_milliseconds",
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .set(lag as f64);
    }

    /// Update buffer depth.
    #[inline]
    pub fn set_buffer_depth(&self, depth: u64) {
        self.buffer_depth.store(depth, Ordering::Relaxed);
        metrics::gauge!(
            "rivven_cdc_buffer_depth",
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .set(depth as f64);
    }

    /// Set buffer capacity.
    #[inline]
    pub fn set_buffer_capacity(&self, capacity: u64) {
        self.buffer_capacity.store(capacity, Ordering::Relaxed);
    }

    /// Get a snapshot of current metrics.
    pub async fn snapshot(&self) -> MetricsSnapshot {
        let elapsed = self.start_time.elapsed();
        let elapsed_secs = elapsed.as_secs_f64().max(0.001);

        let events_total = self.events_total.load(Ordering::Relaxed);
        let bytes_total = self.bytes_total.load(Ordering::Relaxed);

        // Calculate window rates
        let last_reset = *self.last_reset.read().await;
        let window_elapsed = last_reset.elapsed().as_secs_f64().max(0.001);
        let events_window = self.events_window.load(Ordering::Relaxed);
        let bytes_window = self.bytes_window.load(Ordering::Relaxed);

        // Get latency percentiles
        let histogram = self.latency_samples.read().await;
        let latencies = histogram.percentiles();

        MetricsSnapshot {
            source_type: self.source_type.clone(),
            database: self.database.clone(),
            uptime_secs: elapsed.as_secs(),

            // Counters
            events_total,
            bytes_total,
            inserts: self.inserts.load(Ordering::Relaxed),
            updates: self.updates.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            transactions: self.transactions.load(Ordering::Relaxed),

            // Rates (lifetime)
            events_per_second: events_total as f64 / elapsed_secs,
            bytes_per_second: bytes_total as f64 / elapsed_secs,

            // Rates (window)
            events_per_second_window: events_window as f64 / window_elapsed,
            bytes_per_second_window: bytes_window as f64 / window_elapsed,

            // Errors
            errors_total: self.errors_total.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
            connection_errors: self.connection_errors.load(Ordering::Relaxed),
            route_errors: self.route_errors.load(Ordering::Relaxed),

            // Latencies
            latency_p50: latencies.p50,
            latency_p95: latencies.p95,
            latency_p99: latencies.p99,
            latency_max: latencies.max,

            // Health
            connected: self.connected.load(Ordering::Relaxed),
            circuit_open: self.circuit_open.load(Ordering::Relaxed),
            last_event_timestamp: self.last_event_timestamp.load(Ordering::Relaxed),
            lag_ms: self.lag_ms.load(Ordering::Relaxed),

            // Resources
            buffer_depth: self.buffer_depth.load(Ordering::Relaxed),
            buffer_capacity: self.buffer_capacity.load(Ordering::Relaxed),
            buffer_utilization: {
                let cap = self.buffer_capacity.load(Ordering::Relaxed);
                if cap > 0 {
                    self.buffer_depth.load(Ordering::Relaxed) as f64 / cap as f64
                } else {
                    0.0
                }
            },
        }
    }

    /// Reset windowed metrics (call periodically for accurate rate calculations).
    pub async fn reset_window(&self) {
        self.events_window.store(0, Ordering::Relaxed);
        self.bytes_window.store(0, Ordering::Relaxed);
        *self.last_reset.write().await = Instant::now();
    }

    /// Check if the CDC source is healthy.
    pub fn is_healthy(&self) -> bool {
        self.connected.load(Ordering::Relaxed) && !self.circuit_open.load(Ordering::Relaxed)
    }

    /// Get the error rate (errors per event).
    pub fn error_rate(&self) -> f64 {
        let events = self.events_total.load(Ordering::Relaxed);
        let errors = self.errors_total.load(Ordering::Relaxed);
        if events > 0 {
            errors as f64 / events as f64
        } else {
            0.0
        }
    }
}

/// Error types for metrics categorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorType {
    /// Decode/parse error
    Decode,
    /// Connection error
    Connection,
    /// Route/delivery error
    Route,
    /// Other error
    Other,
}

/// Snapshot of CDC metrics at a point in time.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub source_type: String,
    pub database: String,
    pub uptime_secs: u64,

    // Counters
    pub events_total: u64,
    pub bytes_total: u64,
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub transactions: u64,

    // Rates (lifetime average)
    pub events_per_second: f64,
    pub bytes_per_second: f64,

    // Rates (current window)
    pub events_per_second_window: f64,
    pub bytes_per_second_window: f64,

    // Errors
    pub errors_total: u64,
    pub decode_errors: u64,
    pub connection_errors: u64,
    pub route_errors: u64,

    // Latencies (microseconds)
    pub latency_p50: u64,
    pub latency_p95: u64,
    pub latency_p99: u64,
    pub latency_max: u64,

    // Health
    pub connected: bool,
    pub circuit_open: bool,
    pub last_event_timestamp: u64,
    pub lag_ms: u64,

    // Resources
    pub buffer_depth: u64,
    pub buffer_capacity: u64,
    pub buffer_utilization: f64,
}

impl MetricsSnapshot {
    /// Format as Prometheus exposition format.
    pub fn to_prometheus(&self) -> String {
        let labels = format!(
            "source=\"{}\",database=\"{}\"",
            self.source_type, self.database
        );

        format!(
            r#"# HELP rivven_cdc_events_total Total CDC events processed
# TYPE rivven_cdc_events_total counter
rivven_cdc_events_total{{{labels}}} {events_total}

# HELP rivven_cdc_bytes_total Total bytes processed
# TYPE rivven_cdc_bytes_total counter
rivven_cdc_bytes_total{{{labels}}} {bytes_total}

# HELP rivven_cdc_inserts_total Total insert operations
# TYPE rivven_cdc_inserts_total counter
rivven_cdc_inserts_total{{{labels}}} {inserts}

# HELP rivven_cdc_updates_total Total update operations
# TYPE rivven_cdc_updates_total counter
rivven_cdc_updates_total{{{labels}}} {updates}

# HELP rivven_cdc_deletes_total Total delete operations
# TYPE rivven_cdc_deletes_total counter
rivven_cdc_deletes_total{{{labels}}} {deletes}

# HELP rivven_cdc_transactions_total Total transactions processed
# TYPE rivven_cdc_transactions_total counter
rivven_cdc_transactions_total{{{labels}}} {transactions}

# HELP rivven_cdc_errors_total Total errors by type
# TYPE rivven_cdc_errors_total counter
rivven_cdc_errors_total{{{labels},type="decode"}} {decode_errors}
rivven_cdc_errors_total{{{labels},type="connection"}} {connection_errors}
rivven_cdc_errors_total{{{labels},type="route"}} {route_errors}

# HELP rivven_cdc_events_per_second Current event throughput
# TYPE rivven_cdc_events_per_second gauge
rivven_cdc_events_per_second{{{labels}}} {events_per_second:.2}

# HELP rivven_cdc_latency_microseconds Event processing latency
# TYPE rivven_cdc_latency_microseconds summary
rivven_cdc_latency_microseconds{{{labels},quantile="0.5"}} {latency_p50}
rivven_cdc_latency_microseconds{{{labels},quantile="0.95"}} {latency_p95}
rivven_cdc_latency_microseconds{{{labels},quantile="0.99"}} {latency_p99}

# HELP rivven_cdc_connected Connection state (1=connected, 0=disconnected)
# TYPE rivven_cdc_connected gauge
rivven_cdc_connected{{{labels}}} {connected}

# HELP rivven_cdc_lag_milliseconds Replication lag in milliseconds
# TYPE rivven_cdc_lag_milliseconds gauge
rivven_cdc_lag_milliseconds{{{labels}}} {lag_ms}

# HELP rivven_cdc_buffer_utilization Buffer utilization ratio
# TYPE rivven_cdc_buffer_utilization gauge
rivven_cdc_buffer_utilization{{{labels}}} {buffer_utilization:.4}
"#,
            labels = labels,
            events_total = self.events_total,
            bytes_total = self.bytes_total,
            inserts = self.inserts,
            updates = self.updates,
            deletes = self.deletes,
            transactions = self.transactions,
            decode_errors = self.decode_errors,
            connection_errors = self.connection_errors,
            route_errors = self.route_errors,
            events_per_second = self.events_per_second_window,
            latency_p50 = self.latency_p50,
            latency_p95 = self.latency_p95,
            latency_p99 = self.latency_p99,
            connected = if self.connected { 1 } else { 0 },
            lag_ms = self.lag_ms,
            buffer_utilization = self.buffer_utilization,
        )
    }

    /// Format as JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_default()
    }
}

impl serde::Serialize for MetricsSnapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("MetricsSnapshot", 24)?;
        state.serialize_field("source_type", &self.source_type)?;
        state.serialize_field("database", &self.database)?;
        state.serialize_field("uptime_secs", &self.uptime_secs)?;
        state.serialize_field("events_total", &self.events_total)?;
        state.serialize_field("bytes_total", &self.bytes_total)?;
        state.serialize_field("inserts", &self.inserts)?;
        state.serialize_field("updates", &self.updates)?;
        state.serialize_field("deletes", &self.deletes)?;
        state.serialize_field("transactions", &self.transactions)?;
        state.serialize_field("events_per_second", &self.events_per_second)?;
        state.serialize_field("bytes_per_second", &self.bytes_per_second)?;
        state.serialize_field("events_per_second_window", &self.events_per_second_window)?;
        state.serialize_field("errors_total", &self.errors_total)?;
        state.serialize_field("latency_p50_us", &self.latency_p50)?;
        state.serialize_field("latency_p95_us", &self.latency_p95)?;
        state.serialize_field("latency_p99_us", &self.latency_p99)?;
        state.serialize_field("connected", &self.connected)?;
        state.serialize_field("circuit_open", &self.circuit_open)?;
        state.serialize_field("lag_ms", &self.lag_ms)?;
        state.serialize_field("buffer_utilization", &self.buffer_utilization)?;
        state.end()
    }
}

/// Latency percentiles.
#[derive(Debug, Clone, Default)]
pub struct LatencyPercentiles {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
}

/// Histogram for latency tracking using HDR histogram technique.
/// Uses logarithmic buckets for efficient percentile calculation.
#[derive(Debug)]
struct LatencyHistogram {
    /// Bucket counts (logarithmic scale: 0-1µs, 1-10µs, 10-100µs, etc.)
    buckets: [u64; 16],
    /// Sample reservoir for accurate percentiles
    samples: Vec<u64>,
    /// Maximum samples to keep
    max_samples: usize,
    /// Total count
    count: u64,
    /// Maximum observed value
    max: u64,
}

impl LatencyHistogram {
    fn new() -> Self {
        Self {
            buckets: [0; 16],
            samples: Vec::with_capacity(1000),
            max_samples: 1000,
            count: 0,
            max: 0,
        }
    }

    fn record(&mut self, latency: Duration) {
        let micros = latency.as_micros() as u64;
        self.count += 1;
        self.max = self.max.max(micros);

        // Update bucket (log10 scale)
        let bucket = if micros == 0 {
            0
        } else {
            (micros as f64).log10().ceil() as usize
        }
        .min(15);
        self.buckets[bucket] += 1;

        // Reservoir sampling for percentiles
        if self.samples.len() < self.max_samples {
            self.samples.push(micros);
        } else {
            // Random replacement for reservoir sampling
            let idx = (self.count % self.max_samples as u64) as usize;
            self.samples[idx] = micros;
        }
    }

    fn percentiles(&self) -> LatencyPercentiles {
        if self.samples.is_empty() {
            return LatencyPercentiles::default();
        }

        let mut sorted = self.samples.clone();
        sorted.sort_unstable();

        let len = sorted.len();
        LatencyPercentiles {
            p50: sorted[len * 50 / 100],
            p95: sorted[len * 95 / 100],
            p99: sorted[(len * 99 / 100).min(len - 1)],
            max: self.max,
        }
    }
}

/// Shared metrics handle for use across async tasks.
pub type SharedMetrics = Arc<CdcMetrics>;

/// Create a new shared metrics instance.
pub fn new_shared_metrics(source_type: &str, database: &str) -> SharedMetrics {
    Arc::new(CdcMetrics::new(source_type, database))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_recording() {
        let metrics = CdcMetrics::new("postgres", "testdb");

        metrics.record_event(1024);
        metrics.record_event(2048);
        metrics.record_insert();
        metrics.record_update();
        metrics.record_delete();
        metrics.record_transaction();

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.events_total, 2);
        assert_eq!(snapshot.bytes_total, 3072);
        assert_eq!(snapshot.inserts, 1);
        assert_eq!(snapshot.updates, 1);
        assert_eq!(snapshot.deletes, 1);
        assert_eq!(snapshot.transactions, 1);
    }

    #[tokio::test]
    async fn test_error_recording() {
        let metrics = CdcMetrics::new("mysql", "testdb");

        metrics.record_error(ErrorType::Decode);
        metrics.record_error(ErrorType::Connection);
        metrics.record_error(ErrorType::Route);

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.errors_total, 3);
        assert_eq!(snapshot.decode_errors, 1);
        assert_eq!(snapshot.connection_errors, 1);
        assert_eq!(snapshot.route_errors, 1);
    }

    #[tokio::test]
    async fn test_latency_percentiles() {
        let metrics = CdcMetrics::new("postgres", "testdb");

        // Record various latencies
        for i in 1..=100 {
            metrics.record_latency(Duration::from_micros(i * 10)).await;
        }

        let snapshot = metrics.snapshot().await;
        // p50 should be around 500µs
        assert!(snapshot.latency_p50 >= 400 && snapshot.latency_p50 <= 600);
        // p99 should be around 990µs
        assert!(snapshot.latency_p99 >= 900);
    }

    #[tokio::test]
    async fn test_health_state() {
        let metrics = CdcMetrics::new("postgres", "testdb");

        assert!(!metrics.is_healthy());

        metrics.set_connected(true);
        assert!(metrics.is_healthy());

        metrics.set_circuit_open(true);
        assert!(!metrics.is_healthy());
    }

    #[tokio::test]
    async fn test_prometheus_format() {
        let metrics = CdcMetrics::new("postgres", "testdb");
        metrics.record_event(1024);
        metrics.set_connected(true);

        let snapshot = metrics.snapshot().await;
        let prom = snapshot.to_prometheus();

        assert!(prom.contains("rivven_cdc_events_total"));
        assert!(prom.contains("source=\"postgres\""));
        assert!(prom.contains("database=\"testdb\""));
    }

    #[test]
    fn test_error_rate() {
        let metrics = CdcMetrics::new("postgres", "testdb");

        // No events, error rate should be 0
        assert_eq!(metrics.error_rate(), 0.0);

        // Record events and errors
        for _ in 0..100 {
            metrics.record_event(100);
        }
        for _ in 0..5 {
            metrics.record_error(ErrorType::Decode);
        }

        let rate = metrics.error_rate();
        assert!((rate - 0.05).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_window_reset() {
        let metrics = CdcMetrics::new("postgres", "testdb");

        metrics.record_event(1024);
        metrics.record_event(1024);

        let snapshot1 = metrics.snapshot().await;
        assert_eq!(snapshot1.events_total, 2);

        metrics.reset_window().await;

        metrics.record_event(1024);

        let snapshot2 = metrics.snapshot().await;
        assert_eq!(snapshot2.events_total, 3); // Total unchanged
    }
}
