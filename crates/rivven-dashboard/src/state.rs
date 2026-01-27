//! Global dashboard state management
//!
//! Provides reactive state using Leptos signals.

use gloo_timers::future::TimeoutFuture;
use leptos::*;
use wasm_bindgen_futures::spawn_local;

use crate::api::{
    ApiClient, ClusterNode, ConsumerGroupInfo, MetricsResponse, TopicInfo,
};

/// Global dashboard state
#[derive(Clone)]
pub struct DashboardState {
    /// Current view/route
    pub current_view: RwSignal<View>,
    /// Connection status
    pub connected: RwSignal<bool>,
    /// Loading state
    pub is_refreshing: RwSignal<bool>,
    /// Last refresh timestamp
    pub last_refresh: RwSignal<Option<u64>>,
    /// Server uptime
    pub uptime_secs: RwSignal<u64>,
    /// Server status
    pub server_status: RwSignal<ServerStatus>,
    /// Topics list
    pub topics: RwSignal<Vec<TopicInfo>>,
    /// Consumer groups
    pub consumer_groups: RwSignal<Vec<ConsumerGroupInfo>>,
    /// Cluster nodes
    pub cluster_nodes: RwSignal<Vec<ClusterNode>>,
    /// Raft/metrics data
    pub metrics: RwSignal<MetricsResponse>,
    /// Topic search filter
    pub topic_search: RwSignal<String>,
}

/// Current dashboard view
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum View {
    #[default]
    Overview,
    Topics,
    ConsumerGroups,
    Cluster,
    Metrics,
}

impl View {
    pub fn title(&self) -> &'static str {
        match self {
            View::Overview => "Dashboard",
            View::Topics => "Topics",
            View::ConsumerGroups => "Consumer Groups",
            View::Cluster => "Cluster Nodes",
            View::Metrics => "Metrics",
        }
    }

    pub fn path(&self) -> &'static str {
        match self {
            View::Overview => "/",
            View::Topics => "/topics",
            View::ConsumerGroups => "/groups",
            View::Cluster => "/cluster",
            View::Metrics => "/metrics",
        }
    }
}

/// Server status information
#[derive(Debug, Clone, Default)]
pub struct ServerStatus {
    pub status: String,
    pub node_id: Option<String>,
    pub mode: String,
    pub is_leader: Option<bool>,
    pub leader_id: Option<u64>,
    pub active_connections: u64,
    pub total_requests: u64,
}

impl DashboardState {
    /// Create new dashboard state with default values
    pub fn new() -> Self {
        Self {
            current_view: create_rw_signal(View::default()),
            connected: create_rw_signal(false),
            is_refreshing: create_rw_signal(false),
            last_refresh: create_rw_signal(None),
            uptime_secs: create_rw_signal(0),
            server_status: create_rw_signal(ServerStatus::default()),
            topics: create_rw_signal(Vec::new()),
            consumer_groups: create_rw_signal(Vec::new()),
            cluster_nodes: create_rw_signal(Vec::new()),
            metrics: create_rw_signal(MetricsResponse::default()),
            topic_search: create_rw_signal(String::new()),
        }
    }

    /// Start automatic data refresh (every 3 seconds)
    pub fn start_refresh(&self) {
        let state = self.clone();
        spawn_local(async move {
            loop {
                state.refresh_data().await;
                TimeoutFuture::new(3_000).await;
            }
        });
    }

    /// Manually trigger a data refresh
    pub async fn refresh_data(&self) {
        if self.is_refreshing.get_untracked() {
            return;
        }

        self.is_refreshing.set(true);
        let client = ApiClient::from_origin();

        // Fetch health
        match client.health().await {
            Ok(health) => {
                self.connected.set(true);
                self.server_status.update(|s| {
                    s.status = health.status;
                    s.node_id = health.node_id_str;
                    s.mode = if health.cluster_mode.unwrap_or(false) {
                        "Cluster".to_string()
                    } else {
                        "Standalone".to_string()
                    };
                    s.is_leader = health.is_leader;
                    s.leader_id = health.leader_id;
                });
            }
            Err(_) => {
                self.connected.set(false);
            }
        }

        // Fetch dashboard data
        if let Ok(data) = client.dashboard_data().await {
            self.topics.set(data.topics);
            self.consumer_groups.set(data.consumer_groups);
            self.uptime_secs.set(data.uptime_secs);
            self.server_status.update(|s| {
                s.active_connections = data.active_connections;
                s.total_requests = data.total_requests;
            });
        }

        // Fetch cluster membership
        if let Ok(membership) = client.membership().await {
            self.cluster_nodes.set(membership.nodes);
        }

        // Fetch metrics
        if let Ok(metrics) = client.metrics_json().await {
            self.metrics.set(metrics);
        }

        self.last_refresh.set(Some(js_sys::Date::now() as u64));
        self.is_refreshing.set(false);
    }

    // ========================================================================
    // Computed Values
    // ========================================================================

    /// Total partitions across all topics
    pub fn total_partitions(&self) -> impl Fn() -> u32 + Clone {
        let topics = self.topics;
        move || topics.get().iter().map(|t| t.partitions).sum()
    }

    /// Total messages across all topics
    pub fn total_messages(&self) -> impl Fn() -> u64 + Clone {
        let topics = self.topics;
        move || topics.get().iter().map(|t| t.message_count).sum()
    }

    /// Total consumer lag
    pub fn total_lag(&self) -> impl Fn() -> u64 + Clone {
        let groups = self.consumer_groups;
        move || groups.get().iter().map(|g| g.total_lag).sum()
    }

    /// Filtered topics based on search
    pub fn filtered_topics(&self) -> impl Fn() -> Vec<TopicInfo> + Clone {
        let topics = self.topics;
        let search = self.topic_search;
        move || {
            let search_str = search.get().to_lowercase();
            if search_str.is_empty() {
                topics.get()
            } else {
                topics
                    .get()
                    .into_iter()
                    .filter(|t| t.name.to_lowercase().contains(&search_str))
                    .collect()
            }
        }
    }
}

impl Default for DashboardState {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Format a number with K/M suffix
pub fn format_number(num: u64) -> String {
    if num >= 1_000_000 {
        format!("{:.1}M", num as f64 / 1_000_000.0)
    } else if num >= 1_000 {
        format!("{:.1}K", num as f64 / 1_000.0)
    } else {
        num.to_string()
    }
}

/// Format uptime in human-readable format
pub fn format_uptime(secs: u64) -> String {
    if secs == 0 {
        return "-".to_string();
    }

    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    let mins = (secs % 3600) / 60;

    if days > 0 {
        format!("{}d {}h", days, hours)
    } else if hours > 0 {
        format!("{}h {}m", hours, mins)
    } else {
        format!("{}m", mins)
    }
}

/// Get CSS class for lag indicator
pub fn lag_class(lag: u64) -> &'static str {
    if lag < 1000 {
        "good"
    } else if lag < 10000 {
        "warning"
    } else {
        "error"
    }
}

/// Format timestamp for display
pub fn format_time(timestamp: u64) -> String {
    let date = js_sys::Date::new(&js_sys::Number::from(timestamp as f64));
    format!(
        "{:02}:{:02}:{:02}",
        date.get_hours(),
        date.get_minutes(),
        date.get_seconds()
    )
}
