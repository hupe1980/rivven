//! Prometheus metrics for rivven-connect
//!
//! Exposes connector metrics at `/metrics` endpoint in Prometheus format.
//! Metrics include:
//! - Events processed per connector (source/sink)
//! - Error counts
//! - Connector status (running/stopped/failed)
//! - Broker connection state

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

use crate::config::MetricsConfig;

/// Shared metrics state
#[derive(Default)]
pub struct MetricsState {
    /// Service start time for uptime calculation
    pub started_at: Option<Instant>,
    /// Source metrics by name
    pub sources: std::collections::HashMap<String, ConnectorMetrics>,
    /// Sink metrics by name  
    pub sinks: std::collections::HashMap<String, ConnectorMetrics>,
    /// Broker connection attempts
    pub broker_connection_attempts: AtomicU64,
    /// Broker connection failures
    pub broker_connection_failures: AtomicU64,
}

/// Atomic snapshot of all counter values for a single connector.
///
/// Collecting individual `AtomicU64` values one-by-one yields an inconsistent
/// view when counters are being mutated concurrently.  `ConnectorMetricsSnapshot`
/// captures every counter under a single point in time so that consumers (e.g.
/// Prometheus rendering) see a coherent picture.
#[derive(Debug, Clone, Default)]
pub struct ConnectorMetricsSnapshot {
    pub events_total: u64,
    pub errors_total: u64,
    pub bytes_total: u64,
    pub is_running: bool,
    pub rate_limited_events: u64,
    pub rate_limit_wait_ms: u64,
    pub pool_connections_created: u64,
    pub pool_acquisitions: u64,
    pub pool_reused: u64,
    pub pool_fresh: u64,
    pub pool_health_failures: u64,
    pub pool_lifetime_recycled: u64,
    pub pool_idle_recycled: u64,
    pub pool_wait_time_us: u64,
}

/// Metrics for a single connector
#[derive(Default)]
pub struct ConnectorMetrics {
    pub events_total: AtomicU64,
    pub errors_total: AtomicU64,
    pub bytes_total: AtomicU64,
    pub is_running: std::sync::atomic::AtomicBool,
    /// Rate limited events count (only for sinks)
    pub rate_limited_events: AtomicU64,
    /// Total time spent waiting for rate limiter (ms, only for sinks)
    pub rate_limit_wait_ms: AtomicU64,

    // Pool metrics (for RDBC connectors)
    /// Pool connections created
    pub pool_connections_created: AtomicU64,
    /// Pool acquisitions total
    pub pool_acquisitions: AtomicU64,
    /// Pool connections reused (cache hits)
    pub pool_reused: AtomicU64,
    /// Pool connections freshly created
    pub pool_fresh: AtomicU64,
    /// Pool health check failures
    pub pool_health_failures: AtomicU64,
    /// Pool connections recycled (lifetime expired)
    pub pool_lifetime_recycled: AtomicU64,
    /// Pool connections recycled (idle expired)
    pub pool_idle_recycled: AtomicU64,
    /// Pool total wait time (microseconds)
    pub pool_wait_time_us: AtomicU64,
}

impl ConnectorMetrics {
    /// Take an atomic snapshot of all counter values.
    ///
    /// All loads use `Ordering::SeqCst` so that the returned values form a
    /// consistent, linearisable snapshot – no counter can appear to "travel
    /// back in time" relative to another.
    pub fn snapshot(&self) -> ConnectorMetricsSnapshot {
        ConnectorMetricsSnapshot {
            events_total: self.events_total.load(Ordering::SeqCst),
            errors_total: self.errors_total.load(Ordering::SeqCst),
            bytes_total: self.bytes_total.load(Ordering::SeqCst),
            is_running: self.is_running.load(Ordering::SeqCst),
            rate_limited_events: self.rate_limited_events.load(Ordering::SeqCst),
            rate_limit_wait_ms: self.rate_limit_wait_ms.load(Ordering::SeqCst),
            pool_connections_created: self.pool_connections_created.load(Ordering::SeqCst),
            pool_acquisitions: self.pool_acquisitions.load(Ordering::SeqCst),
            pool_reused: self.pool_reused.load(Ordering::SeqCst),
            pool_fresh: self.pool_fresh.load(Ordering::SeqCst),
            pool_health_failures: self.pool_health_failures.load(Ordering::SeqCst),
            pool_lifetime_recycled: self.pool_lifetime_recycled.load(Ordering::SeqCst),
            pool_idle_recycled: self.pool_idle_recycled.load(Ordering::SeqCst),
            pool_wait_time_us: self.pool_wait_time_us.load(Ordering::SeqCst),
        }
    }
}

pub type SharedMetricsState = Arc<RwLock<MetricsState>>;

/// Start the Prometheus metrics HTTP server
pub async fn start_metrics_server(
    config: MetricsConfig,
    state: SharedMetricsState,
) -> std::io::Result<()> {
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], config.port));
    let listener = tokio::net::TcpListener::bind(addr).await?;

    tracing::info!(
        "Metrics server listening on http://0.0.0.0:{}/metrics",
        config.port
    );

    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();

        tokio::spawn(async move {
            let _ = handle_metrics_request(stream, state).await;
        });
    }
}

async fn handle_metrics_request(
    mut stream: tokio::net::TcpStream,
    state: SharedMetricsState,
) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buf = [0u8; 1024];
    // Read at least something; partial reads are fine for HTTP request line parsing
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(()); // Connection closed
    }

    let request = String::from_utf8_lossy(&buf[..n]);

    // Only handle GET /metrics
    if !request.starts_with("GET /metrics") {
        let response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
        stream.write_all(response.as_bytes()).await?;
        return Ok(());
    }

    let body = render_metrics(&state).await;
    let response = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         \r\n\
         {}",
        body.len(),
        body
    );

    stream.write_all(response.as_bytes()).await?;
    Ok(())
}

/// Render metrics in Prometheus text format
///
/// All per-connector counters are first collected into atomic snapshots so
/// that the rendered values form a consistent, point-in-time view.
async fn render_metrics(state: &SharedMetricsState) -> String {
    let state = state.read().await;

    // ── Phase 1: Take atomic snapshots of every connector ──────────────
    let source_snapshots: Vec<(&String, ConnectorMetricsSnapshot)> = state
        .sources
        .iter()
        .map(|(name, m)| (name, m.snapshot()))
        .collect();

    let sink_snapshots: Vec<(&String, ConnectorMetricsSnapshot)> = state
        .sinks
        .iter()
        .map(|(name, m)| (name, m.snapshot()))
        .collect();

    let broker_attempts = state.broker_connection_attempts.load(Ordering::SeqCst);
    let broker_failures = state.broker_connection_failures.load(Ordering::SeqCst);

    // ── Phase 2: Render from snapshots (no more atomic loads) ──────────
    let mut output = String::new();

    // Uptime
    if let Some(started_at) = state.started_at {
        let uptime = started_at.elapsed().as_secs_f64();
        output.push_str("# HELP rivven_connect_uptime_seconds Time since service started\n");
        output.push_str("# TYPE rivven_connect_uptime_seconds gauge\n");
        output.push_str(&format!("rivven_connect_uptime_seconds {:.3}\n\n", uptime));
    }

    // Source metrics
    output.push_str("# HELP rivven_connect_source_events_total Total events processed by source\n");
    output.push_str("# TYPE rivven_connect_source_events_total counter\n");
    for (name, snap) in &source_snapshots {
        output.push_str(&format!(
            "rivven_connect_source_events_total{{source=\"{}\"}} {}\n",
            name, snap.events_total
        ));
    }
    output.push('\n');

    output.push_str("# HELP rivven_connect_source_errors_total Total errors for source\n");
    output.push_str("# TYPE rivven_connect_source_errors_total counter\n");
    for (name, snap) in &source_snapshots {
        output.push_str(&format!(
            "rivven_connect_source_errors_total{{source=\"{}\"}} {}\n",
            name, snap.errors_total
        ));
    }
    output.push('\n');

    output
        .push_str("# HELP rivven_connect_source_running Whether source is running (1=yes, 0=no)\n");
    output.push_str("# TYPE rivven_connect_source_running gauge\n");
    for (name, snap) in &source_snapshots {
        let running = if snap.is_running { 1 } else { 0 };
        output.push_str(&format!(
            "rivven_connect_source_running{{source=\"{}\"}} {}\n",
            name, running
        ));
    }
    output.push('\n');

    // Sink metrics
    output.push_str("# HELP rivven_connect_sink_events_total Total events processed by sink\n");
    output.push_str("# TYPE rivven_connect_sink_events_total counter\n");
    for (name, snap) in &sink_snapshots {
        output.push_str(&format!(
            "rivven_connect_sink_events_total{{sink=\"{}\"}} {}\n",
            name, snap.events_total
        ));
    }
    output.push('\n');

    output.push_str("# HELP rivven_connect_sink_errors_total Total errors for sink\n");
    output.push_str("# TYPE rivven_connect_sink_errors_total counter\n");
    for (name, snap) in &sink_snapshots {
        output.push_str(&format!(
            "rivven_connect_sink_errors_total{{sink=\"{}\"}} {}\n",
            name, snap.errors_total
        ));
    }
    output.push('\n');

    output.push_str("# HELP rivven_connect_sink_running Whether sink is running (1=yes, 0=no)\n");
    output.push_str("# TYPE rivven_connect_sink_running gauge\n");
    for (name, snap) in &sink_snapshots {
        let running = if snap.is_running { 1 } else { 0 };
        output.push_str(&format!(
            "rivven_connect_sink_running{{sink=\"{}\"}} {}\n",
            name, running
        ));
    }
    output.push('\n');

    // Rate limiting metrics for sinks
    output.push_str(
        "# HELP rivven_connect_sink_rate_limited_events_total Events throttled by rate limiter\n",
    );
    output.push_str("# TYPE rivven_connect_sink_rate_limited_events_total counter\n");
    for (name, snap) in &sink_snapshots {
        if snap.rate_limited_events > 0 {
            output.push_str(&format!(
                "rivven_connect_sink_rate_limited_events_total{{sink=\"{}\"}} {}\n",
                name, snap.rate_limited_events
            ));
        }
    }
    output.push('\n');

    output.push_str("# HELP rivven_connect_sink_rate_limit_wait_ms_total Total time spent waiting for rate limiter\n");
    output.push_str("# TYPE rivven_connect_sink_rate_limit_wait_ms_total counter\n");
    for (name, snap) in &sink_snapshots {
        if snap.rate_limit_wait_ms > 0 {
            output.push_str(&format!(
                "rivven_connect_sink_rate_limit_wait_ms_total{{sink=\"{}\"}} {}\n",
                name, snap.rate_limit_wait_ms
            ));
        }
    }
    output.push('\n');

    // Pool metrics for RDBC connectors
    let all_snapshots: Vec<_> = source_snapshots
        .iter()
        .chain(sink_snapshots.iter())
        .collect();

    output.push_str(
        "# HELP rivven_connect_pool_connections_total Total connections created by pool\n",
    );
    output.push_str("# TYPE rivven_connect_pool_connections_total counter\n");
    for (name, snap) in &all_snapshots {
        if snap.pool_connections_created > 0 {
            output.push_str(&format!(
                "rivven_connect_pool_connections_total{{connector=\"{}\"}} {}\n",
                name, snap.pool_connections_created
            ));
        }
    }
    output.push('\n');

    output.push_str("# HELP rivven_connect_pool_acquisitions_total Total pool acquisitions\n");
    output.push_str("# TYPE rivven_connect_pool_acquisitions_total counter\n");
    for (name, snap) in &all_snapshots {
        if snap.pool_acquisitions > 0 {
            output.push_str(&format!(
                "rivven_connect_pool_acquisitions_total{{connector=\"{}\"}} {}\n",
                name, snap.pool_acquisitions
            ));
        }
    }
    output.push('\n');

    output.push_str("# HELP rivven_connect_pool_reuse_ratio Pool connection reuse ratio (0-1)\n");
    output.push_str("# TYPE rivven_connect_pool_reuse_ratio gauge\n");
    for (name, snap) in &all_snapshots {
        let total = snap.pool_reused + snap.pool_fresh;
        if total > 0 {
            let ratio = snap.pool_reused as f64 / total as f64;
            output.push_str(&format!(
                "rivven_connect_pool_reuse_ratio{{connector=\"{}\"}} {:.4}\n",
                name, ratio
            ));
        }
    }
    output.push('\n');

    output.push_str(
        "# HELP rivven_connect_pool_avg_wait_ms Average pool acquisition wait time in milliseconds\n",
    );
    output.push_str("# TYPE rivven_connect_pool_avg_wait_ms gauge\n");
    for (name, snap) in &all_snapshots {
        if snap.pool_acquisitions > 0 {
            let avg_ms = (snap.pool_wait_time_us as f64 / 1000.0) / snap.pool_acquisitions as f64;
            output.push_str(&format!(
                "rivven_connect_pool_avg_wait_ms{{connector=\"{}\"}} {:.3}\n",
                name, avg_ms
            ));
        }
    }
    output.push('\n');

    output.push_str("# HELP rivven_connect_pool_recycled_total Total connections recycled\n");
    output.push_str("# TYPE rivven_connect_pool_recycled_total counter\n");
    for (name, snap) in &all_snapshots {
        let total = snap.pool_lifetime_recycled + snap.pool_idle_recycled;
        if total > 0 {
            output.push_str(&format!(
                "rivven_connect_pool_recycled_total{{connector=\"{}\",reason=\"lifetime\"}} {}\n",
                name, snap.pool_lifetime_recycled
            ));
            output.push_str(&format!(
                "rivven_connect_pool_recycled_total{{connector=\"{}\",reason=\"idle\"}} {}\n",
                name, snap.pool_idle_recycled
            ));
        }
    }
    output.push('\n');

    output.push_str(
        "# HELP rivven_connect_pool_health_failures_total Total pool health check failures\n",
    );
    output.push_str("# TYPE rivven_connect_pool_health_failures_total counter\n");
    for (name, snap) in &all_snapshots {
        if snap.pool_health_failures > 0 {
            output.push_str(&format!(
                "rivven_connect_pool_health_failures_total{{connector=\"{}\"}} {}\n",
                name, snap.pool_health_failures
            ));
        }
    }
    output.push('\n');

    // Broker connection metrics
    output.push_str(
        "# HELP rivven_connect_broker_connection_attempts_total Total broker connection attempts\n",
    );
    output.push_str("# TYPE rivven_connect_broker_connection_attempts_total counter\n");
    output.push_str(&format!(
        "rivven_connect_broker_connection_attempts_total {}\n\n",
        broker_attempts
    ));

    output.push_str(
        "# HELP rivven_connect_broker_connection_failures_total Total broker connection failures\n",
    );
    output.push_str("# TYPE rivven_connect_broker_connection_failures_total counter\n");
    output.push_str(&format!(
        "rivven_connect_broker_connection_failures_total {}\n",
        broker_failures
    ));

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[tokio::test]
    async fn test_render_metrics() {
        let state = Arc::new(RwLock::new(MetricsState {
            started_at: Some(Instant::now()),
            sources: {
                let mut map = std::collections::HashMap::new();
                map.insert(
                    "test_source".to_string(),
                    ConnectorMetrics {
                        events_total: AtomicU64::new(100),
                        errors_total: AtomicU64::new(5),
                        bytes_total: AtomicU64::new(0),
                        is_running: AtomicBool::new(true),
                        rate_limited_events: AtomicU64::new(0),
                        rate_limit_wait_ms: AtomicU64::new(0),
                        ..Default::default()
                    },
                );
                map
            },
            sinks: {
                let mut map = std::collections::HashMap::new();
                map.insert(
                    "test_sink".to_string(),
                    ConnectorMetrics {
                        events_total: AtomicU64::new(500),
                        errors_total: AtomicU64::new(2),
                        bytes_total: AtomicU64::new(0),
                        is_running: AtomicBool::new(true),
                        rate_limited_events: AtomicU64::new(50),
                        rate_limit_wait_ms: AtomicU64::new(1234),
                        ..Default::default()
                    },
                );
                map
            },
            broker_connection_attempts: AtomicU64::new(3),
            broker_connection_failures: AtomicU64::new(1),
        }));

        let output = render_metrics(&state).await;

        assert!(output.contains("rivven_connect_uptime_seconds"));
        assert!(output.contains("rivven_connect_source_events_total{source=\"test_source\"} 100"));
        assert!(output.contains("rivven_connect_source_errors_total{source=\"test_source\"} 5"));
        assert!(output.contains("rivven_connect_source_running{source=\"test_source\"} 1"));
        assert!(output.contains("rivven_connect_broker_connection_attempts_total 3"));
        // Rate limiting metrics
        assert!(
            output.contains("rivven_connect_sink_rate_limited_events_total{sink=\"test_sink\"} 50")
        );
        assert!(output
            .contains("rivven_connect_sink_rate_limit_wait_ms_total{sink=\"test_sink\"} 1234"));
    }

    #[tokio::test]
    async fn test_pool_metrics_rendering() {
        let state = Arc::new(RwLock::new(MetricsState {
            started_at: Some(Instant::now()),
            sources: std::collections::HashMap::new(),
            sinks: {
                let mut map = std::collections::HashMap::new();
                map.insert(
                    "rdbc_sink".to_string(),
                    ConnectorMetrics {
                        events_total: AtomicU64::new(1000),
                        errors_total: AtomicU64::new(0),
                        is_running: AtomicBool::new(true),
                        // Pool metrics
                        pool_connections_created: AtomicU64::new(5),
                        pool_acquisitions: AtomicU64::new(100),
                        pool_reused: AtomicU64::new(95),
                        pool_fresh: AtomicU64::new(5),
                        pool_health_failures: AtomicU64::new(2),
                        pool_lifetime_recycled: AtomicU64::new(3),
                        pool_idle_recycled: AtomicU64::new(1),
                        pool_wait_time_us: AtomicU64::new(5000), // 5ms total
                        ..Default::default()
                    },
                );
                map
            },
            broker_connection_attempts: AtomicU64::new(0),
            broker_connection_failures: AtomicU64::new(0),
        }));

        let output = render_metrics(&state).await;

        // Pool connections
        assert!(output.contains("rivven_connect_pool_connections_total{connector=\"rdbc_sink\"} 5"));
        // Pool acquisitions
        assert!(
            output.contains("rivven_connect_pool_acquisitions_total{connector=\"rdbc_sink\"} 100")
        );
        // Reuse ratio: 95 / 100 = 0.95
        assert!(output.contains("rivven_connect_pool_reuse_ratio{connector=\"rdbc_sink\"} 0.95"));
        // Avg wait: 5000us / 100 / 1000 = 0.05ms
        assert!(output.contains("rivven_connect_pool_avg_wait_ms{connector=\"rdbc_sink\"} 0.050"));
        // Recycled counts
        assert!(output.contains(
            "rivven_connect_pool_recycled_total{connector=\"rdbc_sink\",reason=\"lifetime\"} 3"
        ));
        assert!(output.contains(
            "rivven_connect_pool_recycled_total{connector=\"rdbc_sink\",reason=\"idle\"} 1"
        ));
        // Health failures
        assert!(
            output.contains("rivven_connect_pool_health_failures_total{connector=\"rdbc_sink\"} 2")
        );
    }
}
