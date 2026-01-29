//! Header component
//!
//! The top navigation bar showing:
//! - Current page title (derived from route)
//! - Connection status indicator
//! - Manual refresh button
//!
//! Keyboard shortcut: `r` to refresh (when not in input)

use leptos::*;
use leptos_router::use_location;

use super::icons::RefreshIcon;
use super::primitives::StatusDot;
use crate::state::DashboardState;

/// Page header with status and refresh button
#[component]
pub fn Header() -> impl IntoView {
    let state = expect_context::<DashboardState>();
    let location = use_location();

    // Clone for closures
    let state_for_refresh = state.clone();
    let refresh = move |_| {
        let state = state_for_refresh.clone();
        wasm_bindgen_futures::spawn_local(async move {
            state.refresh_data().await;
        });
    };

    // Derive page title from current path
    let title = move || {
        let path = location.pathname.get();
        match path.as_str() {
            "/" => "Dashboard",
            "/topics" => "Topics",
            "/groups" => "Consumer Groups",
            "/cluster" => "Cluster Nodes",
            "/metrics" => "Metrics",
            _ => "Dashboard",
        }
    };

    view! {
        <header class="header" role="banner">
            <div class="header-left">
                <h1 class="header-title" aria-live="polite">
                    {title}
                </h1>
            </div>
            <div class="header-right">
                <div
                    class="connection-status"
                    role="status"
                    aria-live="polite"
                >
                    <StatusDot connected=state.connected/>
                    <span>
                        {move || if state.connected.get() { "Connected" } else { "Disconnected" }}
                    </span>
                </div>
                <button
                    class="refresh-btn"
                    class:loading=move || state.is_refreshing.get()
                    on:click=refresh
                    title="Refresh data (r)"
                    aria-label="Refresh dashboard data"
                    disabled=move || state.is_refreshing.get()
                >
                    <RefreshIcon/>
                </button>
            </div>
        </header>
    }
}
