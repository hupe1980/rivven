//! Metrics view component
//!
//! Displays real-time metrics and links to Prometheus/JSON endpoints:
//! - Quick stats cards (connections, requests, messages, lag)
//! - Prometheus endpoint configuration info
//! - Raw metrics data grid
//!
//! Uses reusable primitives for consistent UI.

use leptos::*;

use super::icons::MetricsIcon;
use super::primitives::{EmptyState, StatCard};
use crate::state::{format_number, DashboardState};

/// Metrics view with stats and Prometheus links
#[component]
pub fn MetricsView() -> impl IntoView {
    // State used by child components via context
    let _state = expect_context::<DashboardState>();

    view! {
        <div class="view active" role="main" aria-label="Metrics">
            // Quick stats
            <MetricsStatsGrid/>

            // Prometheus link
            <PrometheusCard/>

            // Raw metrics display
            <MetricsDataCard/>
        </div>
    }
}

/// Stats cards showing key metrics
#[component]
fn MetricsStatsGrid() -> impl IntoView {
    let state = expect_context::<DashboardState>();
    let total_messages = state.total_messages();
    let total_lag = state.total_lag();
    let server_status = state.server_status;

    view! {
        <div class="stats-grid" role="region" aria-label="Key metrics">
            <StatCard
                label="Active Connections"
                value=Signal::derive(move || format_number(server_status.get().active_connections))
                color="blue"
                icon=view! { <ConnectionIcon/> }
            />
            <StatCard
                label="Total Requests"
                value=Signal::derive(move || format_number(server_status.get().total_requests))
                color="green"
                icon=view! { <RequestIcon/> }
            />
            <StatCard
                label="Total Messages"
                value=Signal::derive(move || format_number(total_messages()))
                color="purple"
                icon=view! { <MetricsIcon/> }
            />
            <StatCard
                label="Consumer Lag"
                value=Signal::derive(move || format_number(total_lag()))
                color="orange"
                icon=view! { <ClockIcon/> }
            />
        </div>
    }
}

/// Prometheus endpoint information card
#[component]
fn PrometheusCard() -> impl IntoView {
    view! {
        <div class="table-card" role="region" aria-label="Prometheus configuration">
            <div class="table-header">
                <div class="table-title">"Prometheus Metrics"</div>
            </div>
            <div class="card-body prometheus-info">
                <p class="info-text">
                    "Rivven exposes Prometheus-compatible metrics at the "
                    <code>"/metrics"</code>
                    " endpoint. Configure your Prometheus server to scrape this endpoint for comprehensive monitoring."
                </p>
                <div class="metrics-links">
                    <a
                        href="/metrics"
                        target="_blank"
                        rel="noopener noreferrer"
                        class="btn btn-primary"
                        aria-label="Open Prometheus metrics in new tab"
                    >
                        <ExternalLinkIcon/>
                        "Open Prometheus Metrics"
                    </a>
                    <a
                        href="/metrics/json"
                        target="_blank"
                        rel="noopener noreferrer"
                        class="btn btn-secondary"
                        aria-label="View JSON metrics in new tab"
                    >
                        <CodeIcon/>
                        "View JSON Metrics"
                    </a>
                </div>
            </div>
        </div>
    }
}

/// Raw metrics data display
#[component]
fn MetricsDataCard() -> impl IntoView {
    let state = expect_context::<DashboardState>();

    view! {
        <div class="table-card" role="region" aria-label="Metrics data">
            <div class="table-header">
                <div class="table-title">"Metrics Data"</div>
            </div>
            <div class="card-body">
                <Show
                    when=move || !state.metrics.get().metrics.is_empty()
                    fallback=|| view! {
                        <EmptyState
                            title="Loading metrics..."
                            description="Metrics data is being fetched"
                        />
                    }
                >
                    <div class="metrics-grid" role="list" aria-label="Metrics list">
                        <For
                            each=move || {
                                let mut metrics: Vec<_> = state.metrics.get().metrics.into_iter().collect();
                                metrics.sort_by(|a, b| a.0.cmp(&b.0));
                                metrics
                            }
                            key=|(k, _)| k.clone()
                            children=move |(key, value)| {
                                view! {
                                    <div class="metric-item" role="listitem">
                                        <span class="metric-name mono" title=key.clone()>{key}</span>
                                        <span class="metric-value mono">{value.to_string()}</span>
                                    </div>
                                }
                            }
                        />
                    </div>
                </Show>
            </div>
        </div>
    }
}

// ============================================================================
// Local Icons (not reused elsewhere)
// ============================================================================

#[component]
fn ConnectionIcon() -> impl IntoView {
    view! {
        <svg class="icon" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" aria-hidden="true">
            <path stroke-linecap="round" stroke-linejoin="round" d="M12 21a9.004 9.004 0 0 0 8.716-6.747M12 21a9.004 9.004 0 0 1-8.716-6.747M12 21c2.485 0 4.5-4.03 4.5-9S14.485 3 12 3m0 18c-2.485 0-4.5-4.03-4.5-9S9.515 3 12 3m0 0a8.997 8.997 0 0 1 7.843 4.582M12 3a8.997 8.997 0 0 0-7.843 4.582m15.686 0A11.953 11.953 0 0 1 12 10.5c-2.998 0-5.74-1.1-7.843-2.918m15.686 0A8.959 8.959 0 0 1 21 12c0 .778-.099 1.533-.284 2.253m0 0A17.919 17.919 0 0 1 12 16.5c-3.162 0-6.133-.815-8.716-2.247m0 0A9.015 9.015 0 0 1 3 12c0-1.605.42-3.113 1.157-4.418"/>
        </svg>
    }
}

#[component]
fn RequestIcon() -> impl IntoView {
    view! {
        <svg class="icon" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" aria-hidden="true">
            <path stroke-linecap="round" stroke-linejoin="round" d="M7.5 14.25v2.25m3-4.5v4.5m3-6.75v6.75m3-9v9M6 20.25h12A2.25 2.25 0 0 0 20.25 18V6A2.25 2.25 0 0 0 18 3.75H6A2.25 2.25 0 0 0 3.75 6v12A2.25 2.25 0 0 0 6 20.25Z"/>
        </svg>
    }
}

#[component]
fn ClockIcon() -> impl IntoView {
    view! {
        <svg class="icon" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" aria-hidden="true">
            <path stroke-linecap="round" stroke-linejoin="round" d="M12 6v6h4.5m4.5 0a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z"/>
        </svg>
    }
}

#[component]
fn ExternalLinkIcon() -> impl IntoView {
    view! {
        <svg class="icon icon-sm" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" aria-hidden="true">
            <path stroke-linecap="round" stroke-linejoin="round" d="M13.5 6H5.25A2.25 2.25 0 0 0 3 8.25v10.5A2.25 2.25 0 0 0 5.25 21h10.5A2.25 2.25 0 0 0 18 18.75V10.5m-10.5 6L21 3m0 0h-5.25M21 3v5.25"/>
        </svg>
    }
}

#[component]
fn CodeIcon() -> impl IntoView {
    view! {
        <svg class="icon icon-sm" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" aria-hidden="true">
            <path stroke-linecap="round" stroke-linejoin="round" d="M17.25 6.75 22.5 12l-5.25 5.25m-10.5 0L1.5 12l5.25-5.25m7.5-3-4.5 16.5"/>
        </svg>
    }
}
