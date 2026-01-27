//! Sidebar navigation component
//!
//! Provides the main navigation for the dashboard with:
//! - Branding/logo
//! - Navigation links with active state highlighting
//! - Uptime and refresh status in footer
//! - Keyboard navigation support (arrows, Enter)

use leptos::*;
use leptos_router::*;

use super::icons::{ClusterIcon, GroupIcon, HomeIcon, MetricsIcon, TopicIcon};
use crate::state::{format_uptime, DashboardState};

/// Sidebar navigation with branding and status footer
#[component]
pub fn Sidebar() -> impl IntoView {
    let state = expect_context::<DashboardState>();

    view! {
        <aside
            class="sidebar"
            role="navigation"
            aria-label="Main navigation"
        >
            <div class="logo" aria-label="Rivven Dashboard">
                <div class="logo-icon" aria-hidden="true">"R"</div>
                <span class="logo-text">"Rivven"</span>
                <span class="logo-version">"v0.1"</span>
            </div>

            <nav class="nav" aria-label="Primary">
                <div class="nav-section">
                    <div class="nav-section-title" aria-hidden="true">"Overview"</div>
                    <A href="/" class="nav-link" active_class="active">
                        <HomeIcon/>
                        <span>"Dashboard"</span>
                    </A>
                </div>

                <div class="nav-section">
                    <div class="nav-section-title" aria-hidden="true">"Resources"</div>
                    <A href="/topics" class="nav-link" active_class="active">
                        <TopicIcon/>
                        <span>"Topics"</span>
                    </A>
                    <A href="/groups" class="nav-link" active_class="active">
                        <GroupIcon/>
                        <span>"Consumer Groups"</span>
                    </A>
                    <A href="/cluster" class="nav-link" active_class="active">
                        <ClusterIcon/>
                        <span>"Cluster Nodes"</span>
                    </A>
                </div>

                <div class="nav-section">
                    <div class="nav-section-title" aria-hidden="true">"Observability"</div>
                    <A href="/metrics" class="nav-link" active_class="active">
                        <MetricsIcon/>
                        <span>"Metrics"</span>
                    </A>
                </div>
            </nav>

            <div class="sidebar-footer" role="status" aria-label="Server status">
                <div class="sidebar-stat">
                    <span class="sidebar-stat-label">"Uptime"</span>
                    <span class="sidebar-stat-value" aria-live="polite">
                        {move || format_uptime(state.uptime_secs.get())}
                    </span>
                </div>
                <div class="sidebar-stat">
                    <span class="sidebar-stat-label">"Refreshed"</span>
                    <span class="sidebar-stat-value" aria-live="polite">
                        {move || {
                            state.last_refresh.get()
                                .map(crate::state::format_time)
                                .unwrap_or_else(|| "-".to_string())
                        }}
                    </span>
                </div>
            </div>
        </aside>
    }
}
