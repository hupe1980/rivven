//! Health check HTTP endpoint
//!
//! Provides a proper HTTP endpoint (via axum) for liveness and readiness probes.
//! Returns JSON with connector status and metrics.
//!
//! Kubernetes integration:
//! ```yaml
//! livenessProbe:
//!   httpGet:
//!     path: /live
//!     port: 8081
//! readinessProbe:
//!   httpGet:
//!     path: /ready
//!     port: 8081
//! ```

use crate::config::HealthConfig;
use crate::error::ConnectorStatus;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use axum::{
    extract::State as AxumState, http::StatusCode, response::IntoResponse, routing::get, Json,
    Router,
};

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

/// Start the health check HTTP server (axum-based)
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

    let health_path = config.path.clone();

    let app = Router::new()
        .route(&health_path, get(health_handler))
        .route("/ready", get(ready_handler))
        .route("/live", get(live_handler))
        .with_state(state);

    info!(
        "Health check endpoint listening on http://{}{}  (also /ready, /live)",
        addr, health_path
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .await
        .map_err(std::io::Error::other)
}

async fn health_handler(AxumState(state): AxumState<SharedHealthState>) -> impl IntoResponse {
    let state = state.read().await;
    let is_healthy = state.is_healthy();
    let status_code = if is_healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

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

    (status_code, Json(body))
}

async fn ready_handler(AxumState(state): AxumState<SharedHealthState>) -> impl IntoResponse {
    let state = state.read().await;
    let is_ready = state.is_ready();
    let status_code = if is_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let body = serde_json::json!({ "ready": is_ready });
    (status_code, Json(body))
}

async fn live_handler() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({ "alive": true })))
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
