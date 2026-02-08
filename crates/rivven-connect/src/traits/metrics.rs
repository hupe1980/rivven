//! Metrics trait for connector observability
//!
//! This module provides a trait for collecting and reporting metrics from connectors.
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::prelude::*;
//!
//! struct MySource {
//!     metrics: MetricsCollector,
//! }
//!
//! impl MySource {
//!     async fn read_record(&mut self) {
//!         let timer = self.metrics.start_timer("read_latency");
//!         // ... do work ...
//!         timer.stop();
//!         self.metrics.increment("records_read", 1);
//!     }
//! }
//! ```

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Metric types
#[derive(Debug, Clone)]
pub enum MetricValue {
    /// A counter that only goes up
    Counter(u64),
    /// A gauge that can go up or down
    Gauge(i64),
    /// A histogram of values
    Histogram(HistogramSnapshot),
}

/// Snapshot of histogram data
#[derive(Debug, Clone, Default)]
pub struct HistogramSnapshot {
    /// Total count of observations
    pub count: u64,
    /// Sum of all observations
    pub sum: f64,
    /// Minimum observed value
    pub min: f64,
    /// Maximum observed value
    pub max: f64,
    /// Mean value
    pub mean: f64,
    /// Percentiles (p50, p90, p95, p99)
    pub percentiles: HashMap<String, f64>,
}

/// A metric label pair
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Label {
    /// Label name
    pub name: String,
    /// Label value
    pub value: String,
}

impl Label {
    /// Create a new label
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

/// Trait for types that can collect metrics
pub trait Metrics: Send + Sync {
    /// Increment a counter by the given value
    fn increment(&self, name: &str, value: u64);

    /// Increment a counter with labels
    fn increment_with_labels(&self, name: &str, value: u64, labels: &[Label]);

    /// Set a gauge value
    fn gauge(&self, name: &str, value: i64);

    /// Set a gauge with labels
    fn gauge_with_labels(&self, name: &str, value: i64, labels: &[Label]);

    /// Record a histogram observation
    fn histogram(&self, name: &str, value: f64);

    /// Record a histogram observation with labels
    fn histogram_with_labels(&self, name: &str, value: f64, labels: &[Label]);

    /// Start a timer for measuring duration
    fn start_timer(&self, name: &str) -> Timer;

    /// Get all current metrics
    fn snapshot(&self) -> MetricsSnapshot;
}

/// A timer for measuring durations
pub struct Timer {
    name: String,
    start: Instant,
    collector: Arc<dyn Metrics>,
    labels: Vec<Label>,
    stopped: bool,
}

impl Timer {
    /// Create a new timer
    pub fn new(name: impl Into<String>, collector: Arc<dyn Metrics>) -> Self {
        Self {
            name: name.into(),
            start: Instant::now(),
            collector,
            labels: Vec::new(),
            stopped: false,
        }
    }

    /// Add labels to the timer
    pub fn with_labels(mut self, labels: Vec<Label>) -> Self {
        self.labels = labels;
        self
    }

    /// Stop the timer and record the duration
    pub fn stop(mut self) -> Duration {
        self.stopped = true;
        let duration = self.start.elapsed();
        if self.labels.is_empty() {
            self.collector.histogram(&self.name, duration.as_secs_f64());
        } else {
            self.collector
                .histogram_with_labels(&self.name, duration.as_secs_f64(), &self.labels);
        }
        duration
    }

    /// Get the elapsed time without stopping
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Cancel the timer without recording (useful for error paths)
    pub fn cancel(mut self) {
        self.stopped = true;
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if !self.stopped {
            let duration = self.start.elapsed();
            if self.labels.is_empty() {
                self.collector.histogram(&self.name, duration.as_secs_f64());
            } else {
                self.collector.histogram_with_labels(
                    &self.name,
                    duration.as_secs_f64(),
                    &self.labels,
                );
            }
        }
    }
}

/// A snapshot of all metrics
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    /// All metrics keyed by name
    pub metrics: HashMap<String, MetricValue>,
}

impl MetricsSnapshot {
    /// Get a counter value
    pub fn counter(&self, name: &str) -> Option<u64> {
        match self.metrics.get(name) {
            Some(MetricValue::Counter(v)) => Some(*v),
            _ => None,
        }
    }

    /// Get a gauge value
    pub fn gauge(&self, name: &str) -> Option<i64> {
        match self.metrics.get(name) {
            Some(MetricValue::Gauge(v)) => Some(*v),
            _ => None,
        }
    }

    /// Get a histogram snapshot
    pub fn histogram(&self, name: &str) -> Option<&HistogramSnapshot> {
        match self.metrics.get(name) {
            Some(MetricValue::Histogram(h)) => Some(h),
            _ => None,
        }
    }
}

/// Simple in-memory metrics collector
#[derive(Debug, Default)]
pub struct MetricsCollector {
    counters: RwLock<HashMap<String, AtomicU64>>,
    gauges: RwLock<HashMap<String, i64>>,
    histograms: RwLock<HashMap<String, Vec<f64>>>,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a shared metrics collector
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    fn key_with_labels(name: &str, labels: &[Label]) -> String {
        if labels.is_empty() {
            name.to_string()
        } else {
            let label_str: Vec<_> = labels
                .iter()
                .map(|l| format!("{}={}", l.name, l.value))
                .collect();
            format!("{}{{{}}}", name, label_str.join(","))
        }
    }
}

impl Metrics for MetricsCollector {
    fn increment(&self, name: &str, value: u64) {
        let counters = self.counters.read();
        if let Some(counter) = counters.get(name) {
            counter.fetch_add(value, Ordering::Relaxed);
        } else {
            drop(counters);
            let mut counters = self.counters.write();
            counters
                .entry(name.to_string())
                .or_insert_with(|| AtomicU64::new(0))
                .fetch_add(value, Ordering::Relaxed);
        }
    }

    fn increment_with_labels(&self, name: &str, value: u64, labels: &[Label]) {
        let key = Self::key_with_labels(name, labels);
        self.increment(&key, value);
    }

    fn gauge(&self, name: &str, value: i64) {
        let mut gauges = self.gauges.write();
        gauges.insert(name.to_string(), value);
    }

    fn gauge_with_labels(&self, name: &str, value: i64, labels: &[Label]) {
        let key = Self::key_with_labels(name, labels);
        self.gauge(&key, value);
    }

    fn histogram(&self, name: &str, value: f64) {
        let mut histograms = self.histograms.write();
        histograms.entry(name.to_string()).or_default().push(value);
    }

    fn histogram_with_labels(&self, name: &str, value: f64, labels: &[Label]) {
        let key = Self::key_with_labels(name, labels);
        self.histogram(&key, value);
    }

    fn start_timer(&self, name: &str) -> Timer {
        // Note: This default impl uses NoopMetrics since &self can't be converted to Arc<Self>.
        // To record real metrics, use `Timer::new_with_collector(name, collector)` directly.
        Timer {
            name: name.to_string(),
            start: Instant::now(),
            collector: Arc::new(NoopMetrics),
            labels: Vec::new(),
            stopped: false,
        }
    }

    fn snapshot(&self) -> MetricsSnapshot {
        let mut metrics = HashMap::new();

        // Counters
        let counters = self.counters.read();
        for (name, counter) in counters.iter() {
            metrics.insert(
                name.clone(),
                MetricValue::Counter(counter.load(Ordering::Relaxed)),
            );
        }

        // Gauges
        let gauges = self.gauges.read();
        for (name, value) in gauges.iter() {
            metrics.insert(name.clone(), MetricValue::Gauge(*value));
        }

        // Histograms
        let histograms = self.histograms.read();
        for (name, values) in histograms.iter() {
            if values.is_empty() {
                continue;
            }

            let count = values.len() as u64;
            let sum: f64 = values.iter().sum();
            let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let mean = sum / count as f64;

            // Calculate percentiles
            let mut sorted = values.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

            let percentile = |p: f64| -> f64 {
                let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
                sorted[idx.min(sorted.len() - 1)]
            };

            let mut percentiles = HashMap::new();
            percentiles.insert("p50".to_string(), percentile(50.0));
            percentiles.insert("p90".to_string(), percentile(90.0));
            percentiles.insert("p95".to_string(), percentile(95.0));
            percentiles.insert("p99".to_string(), percentile(99.0));

            metrics.insert(
                name.clone(),
                MetricValue::Histogram(HistogramSnapshot {
                    count,
                    sum,
                    min,
                    max,
                    mean,
                    percentiles,
                }),
            );
        }

        MetricsSnapshot { metrics }
    }
}

/// No-op metrics implementation
#[derive(Debug, Default)]
pub struct NoopMetrics;

impl Metrics for NoopMetrics {
    fn increment(&self, _name: &str, _value: u64) {}
    fn increment_with_labels(&self, _name: &str, _value: u64, _labels: &[Label]) {}
    fn gauge(&self, _name: &str, _value: i64) {}
    fn gauge_with_labels(&self, _name: &str, _value: i64, _labels: &[Label]) {}
    fn histogram(&self, _name: &str, _value: f64) {}
    fn histogram_with_labels(&self, _name: &str, _value: f64, _labels: &[Label]) {}

    fn start_timer(&self, name: &str) -> Timer {
        Timer {
            name: name.to_string(),
            start: Instant::now(),
            collector: Arc::new(NoopMetrics),
            labels: Vec::new(),
            stopped: false,
        }
    }

    fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot::default()
    }
}

/// Standard connector metrics names
pub mod metric_names {
    /// Records read by source
    pub const RECORDS_READ: &str = "rivven_connect_records_read_total";
    /// Records written by sink
    pub const RECORDS_WRITTEN: &str = "rivven_connect_records_written_total";
    /// Bytes read by source
    pub const BYTES_READ: &str = "rivven_connect_bytes_read_total";
    /// Bytes written by sink
    pub const BYTES_WRITTEN: &str = "rivven_connect_bytes_written_total";
    /// Read latency histogram
    pub const READ_LATENCY: &str = "rivven_connect_read_latency_seconds";
    /// Write latency histogram
    pub const WRITE_LATENCY: &str = "rivven_connect_write_latency_seconds";
    /// Current lag (for sources)
    pub const LAG: &str = "rivven_connect_lag";
    /// Error count
    pub const ERRORS: &str = "rivven_connect_errors_total";
    /// Retries count
    pub const RETRIES: &str = "rivven_connect_retries_total";
    /// Connection state (1 = connected, 0 = disconnected)
    pub const CONNECTED: &str = "rivven_connect_connected";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collector_counter() {
        let collector = MetricsCollector::new();
        collector.increment("test_counter", 5);
        collector.increment("test_counter", 3);

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.counter("test_counter"), Some(8));
    }

    #[test]
    fn test_metrics_collector_gauge() {
        let collector = MetricsCollector::new();
        collector.gauge("test_gauge", 100);
        collector.gauge("test_gauge", 50);

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.gauge("test_gauge"), Some(50));
    }

    #[test]
    fn test_metrics_collector_histogram() {
        let collector = MetricsCollector::new();
        for i in 1..=100 {
            collector.histogram("test_histogram", i as f64);
        }

        let snapshot = collector.snapshot();
        let hist = snapshot.histogram("test_histogram").unwrap();
        assert_eq!(hist.count, 100);
        assert_eq!(hist.min, 1.0);
        assert_eq!(hist.max, 100.0);
    }

    #[test]
    fn test_metrics_with_labels() {
        let collector = MetricsCollector::new();
        let labels = vec![
            Label::new("source", "postgres"),
            Label::new("table", "users"),
        ];
        collector.increment_with_labels("records", 10, &labels);

        let snapshot = collector.snapshot();
        assert_eq!(
            snapshot.counter("records{source=postgres,table=users}"),
            Some(10)
        );
    }

    #[test]
    fn test_label() {
        let label = Label::new("env", "prod");
        assert_eq!(label.name, "env");
        assert_eq!(label.value, "prod");
    }
}
