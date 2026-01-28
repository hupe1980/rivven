//! Rivven Dashboard - Leptos-based WebAssembly UI
//!
//! A modern, reactive dashboard for monitoring and managing Rivven clusters.
//! Built with Leptos framework and compiled to WebAssembly.
//!
//! ## Features
//!
//! - **Real-time Updates**: Reactive data binding with automatic refresh
//! - **Cluster Monitoring**: View nodes, leader status, Raft metrics
//! - **Topic Management**: Browse topics, partitions, and message counts
//! - **Consumer Groups**: Monitor consumer lag and group membership
//! - **Zero JavaScript**: 100% Rust compiled to WASM
//! - **Air-Gapped**: No external network requests (fonts, CDNs, etc.)
//!
//! ## Configuration
//!
//! The dashboard supports configuration injection for air-gapped deployments:
//!
//! ```html
//! <!-- Server injects config via meta tags -->
//! <meta name="rivven:api-url" content="http://rivven.local:8080">
//! <meta name="rivven:version" content="0.0.1">
//! ```
//!
//! Or via JavaScript:
//!
//! ```javascript
//! window.__RIVVEN_CONFIG__ = {
//!     api_url: "http://rivven.local:8080",
//!     version: "0.0.1"
//! };
//! ```
//!
//! ## Architecture
//!
//! The dashboard runs entirely in the browser via WebAssembly:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                     Browser                              │
//! │  ┌─────────────────────────────────────────────────┐    │
//! │  │           rivven-dashboard (WASM)               │    │
//! │  │  ┌─────────┐  ┌──────────┐  ┌──────────────┐   │    │
//! │  │  │ Leptos  │→ │ gloo-net │→ │ REST API     │   │    │
//! │  │  │ UI      │  │ HTTP     │  │ /health      │   │    │
//! │  │  └─────────┘  └──────────┘  │ /metrics     │   │    │
//! │  │                             │ /dashboard/* │   │    │
//! │  └─────────────────────────────┴──────────────┘───┘    │
//! └──────────────────────────┬──────────────────────────────┘
//!                            │ HTTP (same-origin only)
//! ┌──────────────────────────▼──────────────────────────────┐
//! │                      rivvend                            │
//! │             (serves WASM + REST API)                    │
//! └─────────────────────────────────────────────────────────┘
//! ```

pub mod api;
pub mod components;
pub mod config;
pub mod state;

use leptos::*;
use leptos_router::*;

use components::{
    cluster::ClusterView, consumer_groups::ConsumerGroupsView, header::Header, metrics::MetricsView,
    overview::OverviewView, sidebar::Sidebar, topics::TopicsView,
};
use state::DashboardState;

/// Main dashboard application component
#[component]
pub fn App() -> impl IntoView {
    // Initialize panic hook for better error messages
    console_error_panic_hook::set_once();

    // Create global state
    let state = DashboardState::new();
    provide_context(state.clone());

    // Start automatic data refresh
    state.start_refresh();

    view! {
        <Router>
            <div class="app">
                <Sidebar/>
                <main class="main">
                    <Header/>
                    <div class="content">
                        <Routes>
                            <Route path="/" view=OverviewView/>
                            <Route path="/topics" view=TopicsView/>
                            <Route path="/groups" view=ConsumerGroupsView/>
                            <Route path="/cluster" view=ClusterView/>
                            <Route path="/metrics" view=MetricsView/>
                        </Routes>
                    </div>
                </main>
            </div>
        </Router>
    }
}

/// Mount the application to the DOM
#[wasm_bindgen::prelude::wasm_bindgen(start)]
pub fn main() {
    mount_to_body(|| view! { <App/> });
}
