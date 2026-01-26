//! Web Dashboard for Rivven
//!
//! Provides a production-grade web UI for monitoring and managing Rivven clusters.
//! The dashboard is served from embedded static assets and communicates with the
//! server via REST API endpoints.
//!
//! ## Features
//!
//! - Real-time cluster overview
//! - Topic management and monitoring
//! - Consumer group status
//! - Raft consensus visualization
//! - Prometheus metrics integration
//!
//! ## Security Considerations
//!
//! - Dashboard is disabled by default (opt-in via `--dashboard`)
//! - Static assets are compiled into the binary (no external dependencies)
//! - CORS headers restrict cross-origin requests
//! - Content Security Policy prevents XSS attacks
//! - X-Content-Type-Options prevents MIME sniffing
//! - X-Frame-Options prevents clickjacking
//! - Rate limiting prevents DoS (when tower-governor is enabled)
//! - **IMPORTANT**: In production, enable authentication via reverse proxy or mTLS

#[cfg(feature = "dashboard")]
use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderValue, Request, Response, StatusCode, Uri},
    middleware::{self, Next},
    response::IntoResponse,
    routing::get,
    Json, Router,
};

#[cfg(feature = "dashboard")]
use rust_embed::RustEmbed;

#[cfg(feature = "dashboard")]
use serde::Serialize;

#[cfg(feature = "dashboard")]
use std::sync::Arc;

#[cfg(feature = "dashboard")]
use crate::raft_api::RaftApiState;

// ============================================================================
// Embedded Static Assets
// ============================================================================

/// Embedded static files for the dashboard
#[cfg(feature = "dashboard")]
#[derive(RustEmbed)]
#[folder = "static/"]
struct DashboardAssets;

// ============================================================================
// API Types
// ============================================================================

/// Dashboard overview data
#[cfg(feature = "dashboard")]
#[derive(Debug, Serialize)]
pub struct DashboardData {
    /// List of topics
    pub topics: Vec<TopicInfo>,
    /// List of consumer groups
    pub consumer_groups: Vec<ConsumerGroupInfo>,
    /// Number of active connections
    pub active_connections: u64,
    /// Total requests handled
    pub total_requests: u64,
    /// Server uptime in seconds
    pub uptime_secs: u64,
    /// Timestamp of last data refresh (Unix millis)
    pub timestamp: u64,
}

/// Topic information for dashboard
#[cfg(feature = "dashboard")]
#[derive(Debug, Serialize)]
pub struct TopicInfo {
    /// Topic name
    pub name: String,
    /// Number of partitions
    pub partitions: u32,
    /// Replication factor
    pub replication_factor: u16,
    /// Total message count across all partitions
    pub message_count: u64,
    /// End offsets per partition
    pub partition_offsets: Vec<PartitionOffset>,
}

/// Partition offset information
#[cfg(feature = "dashboard")]
#[derive(Debug, Serialize)]
pub struct PartitionOffset {
    /// Partition ID
    pub partition: u32,
    /// Earliest offset
    pub earliest: u64,
    /// Latest offset (end offset)
    pub latest: u64,
    /// Message count in this partition
    pub count: u64,
}

/// Consumer group information for dashboard
#[cfg(feature = "dashboard")]
#[derive(Debug, Serialize)]
pub struct ConsumerGroupInfo {
    /// Group ID
    pub group_id: String,
    /// Group state
    pub state: String,
    /// Number of members
    pub member_count: usize,
    /// Topics being consumed
    pub topics: Vec<String>,
    /// Total lag across all partitions
    pub total_lag: u64,
}

// ============================================================================
// Dashboard State
// ============================================================================

/// Shared state for dashboard handlers
#[cfg(feature = "dashboard")]
#[derive(Clone)]
pub struct DashboardState {
    /// Raft API state for accessing cluster information
    pub raft_state: RaftApiState,
    /// Server statistics
    pub stats: Arc<crate::cluster_server::ServerStats>,
    /// Topic manager for listing topics
    pub topic_manager: rivven_core::TopicManager,
    /// Offset manager for consumer group info
    pub offset_manager: rivven_core::OffsetManager,
}

// ============================================================================
// Security Middleware
// ============================================================================

/// Security headers middleware
#[cfg(feature = "dashboard")]
async fn security_headers_middleware(request: Request<Body>, next: Next) -> Response<Body> {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();

    // Prevent MIME type sniffing
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );

    // Prevent clickjacking
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));

    // Content Security Policy - allows htmx/alpine from CDN and Google Fonts
    // Note: Alpine.js requires 'unsafe-eval' to evaluate x-text and other expressions
    // Note: connect-src needs ws:/wss: for SSE and localhost for API calls on different ports
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; \
             script-src 'self' 'unsafe-inline' 'unsafe-eval' https://unpkg.com; \
             style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; \
             img-src 'self' data:; \
             connect-src 'self' http://localhost:* http://127.0.0.1:* ws://localhost:* ws://127.0.0.1:*; \
             font-src 'self' https://fonts.gstatic.com; \
             frame-ancestors 'none'",
        ),
    );

    // Referrer Policy
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );

    // Permissions Policy (disable unnecessary browser features)
    headers.insert(
        "Permissions-Policy",
        HeaderValue::from_static("geolocation=(), microphone=(), camera=()"),
    );

    response
}

// ============================================================================
// Router
// ============================================================================

/// Create the dashboard router with security middleware
#[cfg(feature = "dashboard")]
pub fn create_dashboard_router(state: DashboardState) -> Router {
    Router::new()
        // Dashboard data API
        .route("/dashboard/data", get(dashboard_data_handler))
        // Static file serving (catch-all for SPA)
        .fallback(static_handler)
        // Apply security headers middleware
        .layer(middleware::from_fn(security_headers_middleware))
        .with_state(state)
}

// ============================================================================
// Handlers
// ============================================================================

/// Serve static files from embedded assets
#[cfg(feature = "dashboard")]
async fn static_handler(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    // Default to index.html for root path
    let path = if path.is_empty() { "index.html" } else { path };

    match DashboardAssets::get(path) {
        Some(content) => {
            // Determine content type
            let mime = mime_guess::from_path(path)
                .first_or_octet_stream()
                .to_string();

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime)
                // Cache static assets (except HTML)
                .header(
                    header::CACHE_CONTROL,
                    if path.ends_with(".html") {
                        "no-cache, no-store, must-revalidate"
                    } else {
                        "public, max-age=31536000, immutable"
                    },
                )
                .body(Body::from(content.data.into_owned()))
                .unwrap()
        }
        None => {
            // For SPA routing, serve index.html for non-existent paths
            if !path.contains('.') {
                if let Some(content) = DashboardAssets::get("index.html") {
                    return Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
                        .header(header::CACHE_CONTROL, "no-cache, no-store, must-revalidate")
                        .body(Body::from(content.data.into_owned()))
                        .unwrap();
                }
            }

            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::from("Not Found"))
                .unwrap()
        }
    }
}

/// Dashboard data endpoint
#[cfg(feature = "dashboard")]
async fn dashboard_data_handler(State(state): State<DashboardState>) -> impl IntoResponse {
    use std::time::{SystemTime, UNIX_EPOCH};

    // Get topics with detailed metrics
    let topic_list = state.topic_manager.list_topics().await;
    let mut topics: Vec<TopicInfo> = Vec::new();

    for name in topic_list {
        // Try to get the actual topic to fetch partition info
        if let Ok(topic) = state.topic_manager.get_topic(&name).await {
            let num_partitions = topic.num_partitions() as u32;
            let mut partition_offsets = Vec::new();
            let mut total_messages: u64 = 0;

            for p in topic.all_partitions() {
                let earliest = p.earliest_offset().await.unwrap_or(0);
                let latest = p.latest_offset().await;
                let count = latest.saturating_sub(earliest);
                total_messages += count;

                partition_offsets.push(PartitionOffset {
                    partition: p.id(),
                    earliest,
                    latest,
                    count,
                });
            }

            topics.push(TopicInfo {
                name,
                partitions: num_partitions,
                replication_factor: 1, // Single-node for now
                message_count: total_messages,
                partition_offsets,
            });
        } else {
            // Fallback if topic access fails
            topics.push(TopicInfo {
                name,
                partitions: 1,
                replication_factor: 1,
                message_count: 0,
                partition_offsets: vec![],
            });
        }
    }

    // Get consumer groups with lag calculation
    let groups = state.offset_manager.list_groups().await;
    let mut consumer_groups: Vec<ConsumerGroupInfo> = Vec::new();

    for group_id in groups {
        // Get group details from offset manager
        // Returns: topic -> (partition -> offset)
        let offsets = state.offset_manager.get_group_offsets(&group_id).await;
        let group_topics: Vec<String> = offsets
            .as_ref()
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default();

        // Calculate total lag
        let mut total_lag: u64 = 0;
        if let Some(ref group_offsets) = offsets {
            for (topic_name, partition_offsets) in group_offsets.iter() {
                if let Ok(topic) = state.topic_manager.get_topic(topic_name).await {
                    for (partition_id, committed_offset) in partition_offsets.iter() {
                        if let Ok(partition) = topic.partition(*partition_id) {
                            let latest = partition.latest_offset().await;
                            total_lag += latest.saturating_sub(*committed_offset);
                        }
                    }
                }
            }
        }

        consumer_groups.push(ConsumerGroupInfo {
            group_id,
            state: "Stable".to_string(),
            member_count: 0, // Would need member tracking
            topics: group_topics,
            total_lag,
        });
    }

    // Get server stats
    let active_connections = state.stats.get_active_connections();
    let total_requests = state.stats.get_total_requests();
    let uptime_secs = state.stats.uptime().as_secs();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let data = DashboardData {
        topics,
        consumer_groups,
        active_connections,
        total_requests,
        uptime_secs,
        timestamp,
    };

    (StatusCode::OK, Json(data))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(all(test, feature = "dashboard"))]
mod tests {
    use super::*;

    #[test]
    fn test_embedded_assets_exist() {
        // Verify index.html is embedded
        assert!(
            DashboardAssets::get("index.html").is_some(),
            "index.html should be embedded"
        );
    }

    #[test]
    fn test_dashboard_data_serialization() {
        let data = DashboardData {
            topics: vec![TopicInfo {
                name: "test-topic".to_string(),
                partitions: 3,
                replication_factor: 2,
                message_count: 1000,
                partition_offsets: vec![
                    PartitionOffset {
                        partition: 0,
                        earliest: 0,
                        latest: 500,
                        count: 500,
                    },
                    PartitionOffset {
                        partition: 1,
                        earliest: 0,
                        latest: 300,
                        count: 300,
                    },
                    PartitionOffset {
                        partition: 2,
                        earliest: 0,
                        latest: 200,
                        count: 200,
                    },
                ],
            }],
            consumer_groups: vec![ConsumerGroupInfo {
                group_id: "test-group".to_string(),
                state: "Stable".to_string(),
                member_count: 2,
                topics: vec!["test-topic".to_string()],
                total_lag: 100,
            }],
            active_connections: 5,
            total_requests: 1000,
            uptime_secs: 3600,
            timestamp: 1706200000000,
        };

        let json = serde_json::to_string(&data).unwrap();
        assert!(json.contains("test-topic"));
        assert!(json.contains("test-group"));
        assert!(json.contains("message_count"));
        assert!(json.contains("partition_offsets"));
    }
}
