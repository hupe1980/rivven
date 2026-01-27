//! Health check HTTP endpoint
//!
//! Provides a simple HTTP endpoint for liveness and readiness probes.
//! Returns JSON with connector status and metrics.

use crate::config::HealthConfig;
use crate::error::ConnectorStatus;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Health status for all connectors
#[derive(Debug, Clone, Default)]
pub struct HealthState {
    /// Source connector statuses
    pub sources: HashMap<String, ConnectorHealth>,
    /// Sink connector statuses
    pub sinks: HashMap<String, ConnectorHealth>,
    /// Overall broker connection status
    pub broker_connected: bool,
    /// Startup time
    pub started_at: Option<std::time::Instant>,
}

/// Health status for a single connector
#[derive(Debug, Clone)]
pub struct ConnectorHealth {
    pub status: ConnectorStatus,
    pub events_processed: u64,
    pub errors_count: u64,
    pub last_error: Option<String>,
}

impl HealthState {
    /// Check if the system is healthy (all connectors running)
    pub fn is_healthy(&self) -> bool {
        self.broker_connected
            && self.sources.values().all(|h| {
                matches!(
                    h.status,
                    ConnectorStatus::Running | ConnectorStatus::Starting
                )
            })
            && self.sinks.values().all(|h| {
                matches!(
                    h.status,
                    ConnectorStatus::Running | ConnectorStatus::Starting
                )
            })
    }

    /// Check if the system is ready (at least one connector running)
    pub fn is_ready(&self) -> bool {
        self.broker_connected
            && (self
                .sources
                .values()
                .any(|h| h.status == ConnectorStatus::Running)
                || self
                    .sinks
                    .values()
                    .any(|h| h.status == ConnectorStatus::Running))
    }
}

/// Shared health state
pub type SharedHealthState = Arc<RwLock<HealthState>>;

/// Start the health check HTTP server
pub async fn start_health_server(
    config: HealthConfig,
    state: SharedHealthState,
) -> std::io::Result<()> {
    if !config.enabled {
        debug!("Health check endpoint disabled");
        return Ok(());
    }

    let addr: SocketAddr = format!("0.0.0.0:{}", config.port)
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let listener = TcpListener::bind(addr).await?;
    info!(
        "Health check endpoint listening on http://{}{}",
        addr, config.path
    );

    loop {
        let (mut socket, peer) = listener.accept().await?;
        let state = state.clone();
        let path = config.path.clone();

        tokio::spawn(async move {
            let mut buf = [0u8; 1024];

            match socket.read(&mut buf).await {
                Ok(n) if n > 0 => {
                    let request = String::from_utf8_lossy(&buf[..n]);

                    // Parse basic HTTP request
                    let lines: Vec<&str> = request.lines().collect();
                    if lines.is_empty() {
                        return;
                    }

                    let parts: Vec<&str> = lines[0].split_whitespace().collect();
                    if parts.len() < 2 {
                        return;
                    }

                    let method = parts[0];
                    let req_path = parts[1];

                    debug!(
                        "Health check request: {} {} from {}",
                        method, req_path, peer
                    );

                    let response = if method == "GET" && req_path == path {
                        build_health_response(&state).await
                    } else if method == "GET" && req_path == "/ready" {
                        build_ready_response(&state).await
                    } else if method == "GET" && req_path == "/live" {
                        build_live_response()
                    } else {
                        build_404_response()
                    };

                    if let Err(e) = socket.write_all(response.as_bytes()).await {
                        warn!("Failed to send health response: {}", e);
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    warn!("Health check socket error: {}", e);
                }
            }
        });
    }
}

async fn build_health_response(state: &SharedHealthState) -> String {
    let state = state.read().await;
    let is_healthy = state.is_healthy();
    let status_code = if is_healthy { 200 } else { 503 };

    let body = serde_json::json!({
        "status": if is_healthy { "healthy" } else { "unhealthy" },
        "broker_connected": state.broker_connected,
        "sources": state.sources.iter().map(|(name, h)| {
            serde_json::json!({
                "name": name,
                "status": h.status.to_string(),
                "events_processed": h.events_processed,
                "errors_count": h.errors_count,
                "last_error": h.last_error,
            })
        }).collect::<Vec<_>>(),
        "sinks": state.sinks.iter().map(|(name, h)| {
            serde_json::json!({
                "name": name,
                "status": h.status.to_string(),
                "events_processed": h.events_processed,
                "errors_count": h.errors_count,
                "last_error": h.last_error,
            })
        }).collect::<Vec<_>>(),
        "uptime_secs": state.started_at.map(|t| t.elapsed().as_secs()),
    });

    format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}",
        status_code,
        if is_healthy {
            "OK"
        } else {
            "Service Unavailable"
        },
        serde_json::to_string_pretty(&body).unwrap_or_default()
    )
}

async fn build_ready_response(state: &SharedHealthState) -> String {
    let state = state.read().await;
    let is_ready = state.is_ready();
    let status_code = if is_ready { 200 } else { 503 };

    let body = serde_json::json!({
        "ready": is_ready,
    });

    format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}",
        status_code,
        if is_ready {
            "OK"
        } else {
            "Service Unavailable"
        },
        serde_json::to_string(&body).unwrap_or_default()
    )
}

fn build_live_response() -> String {
    let body = serde_json::json!({ "alive": true });
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}",
        serde_json::to_string(&body).unwrap_or_default()
    )
}

fn build_404_response() -> String {
    "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\nNot Found"
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_state_healthy() {
        let mut state = HealthState {
            broker_connected: true,
            ..Default::default()
        };
        state.sources.insert(
            "test".to_string(),
            ConnectorHealth {
                status: ConnectorStatus::Running,
                events_processed: 100,
                errors_count: 0,
                last_error: None,
            },
        );

        assert!(state.is_healthy());
        assert!(state.is_ready());
    }

    #[test]
    fn test_health_state_unhealthy() {
        let mut state = HealthState {
            broker_connected: true,
            ..Default::default()
        };
        state.sources.insert(
            "test".to_string(),
            ConnectorHealth {
                status: ConnectorStatus::Failed,
                events_processed: 100,
                errors_count: 5,
                last_error: Some("Connection lost".to_string()),
            },
        );

        assert!(!state.is_healthy());
        assert!(!state.is_ready());
    }

    #[test]
    fn test_health_state_no_broker() {
        let state = HealthState {
            broker_connected: false,
            ..Default::default()
        };

        assert!(!state.is_healthy());
        assert!(!state.is_ready());
    }
}
