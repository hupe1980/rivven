//! Cluster view component
//!
//! Displays cluster information including:
//! - Server health and status
//! - Connection statistics
//! - Cluster node table (leader/follower, voter/learner)
//!
//! Uses reusable primitives for consistent UI.

use leptos::*;

use super::primitives::{Badge, BadgeVariant, EmptyState, InfoRow};
use crate::state::{format_number, format_uptime, DashboardState};

/// Cluster nodes view
#[component]
pub fn ClusterView() -> impl IntoView {
    // State used by child components via context
    let _state = expect_context::<DashboardState>();

    view! {
        <div class="view active" role="main" aria-label="Cluster">
            // Server info cards
            <div class="info-grid">
                <ServerInfoCard/>
                <ConnectionStatsCard/>
            </div>

            // Cluster nodes table
            <ClusterNodesTable/>
        </div>
    }
}

/// Server information card
#[component]
fn ServerInfoCard() -> impl IntoView {
    let state = expect_context::<DashboardState>();

    view! {
        <div class="table-card" role="region" aria-label="Server information">
            <div class="table-header">
                <div class="table-title">"Server Information"</div>
            </div>
            <div class="card-body">
                <InfoRow label="Status">
                    {move || {
                        let status = state.server_status.get().status.clone();
                        let variant = if status == "healthy" {
                            BadgeVariant::Success
                        } else {
                            BadgeVariant::Warning
                        };
                        view! { <Badge text=status variant=variant/> }
                    }}
                </InfoRow>
                <InfoRow label="Mode">
                    {move || state.server_status.get().mode.clone()}
                </InfoRow>
                <InfoRow label="Node ID">
                    {move || state.server_status.get().node_id.clone().unwrap_or_else(|| "-".to_string())}
                </InfoRow>
                <InfoRow label="Uptime">
                    {move || format_uptime(state.uptime_secs.get())}
                </InfoRow>
            </div>
        </div>
    }
}

/// Connection statistics card
#[component]
fn ConnectionStatsCard() -> impl IntoView {
    let state = expect_context::<DashboardState>();

    view! {
        <div class="table-card" role="region" aria-label="Connection statistics">
            <div class="table-header">
                <div class="table-title">"Connection Statistics"</div>
            </div>
            <div class="card-body">
                <InfoRow label="Active Connections">
                    <span class="mono" aria-live="polite">
                        {move || state.server_status.get().active_connections}
                    </span>
                </InfoRow>
                <InfoRow label="Total Requests">
                    <span class="mono" aria-live="polite">
                        {move || format_number(state.server_status.get().total_requests)}
                    </span>
                </InfoRow>
            </div>
        </div>
    }
}

/// Cluster nodes table
#[component]
fn ClusterNodesTable() -> impl IntoView {
    let state = expect_context::<DashboardState>();

    view! {
        <div class="table-card" role="region" aria-label="Cluster nodes">
            <div class="table-header">
                <div class="table-title">"Cluster Nodes"</div>
                <Badge
                    text=Signal::derive(move || format!("{} nodes", state.cluster_nodes.get().len()))
                    variant=BadgeVariant::Default
                />
            </div>
            <Show
                when=move || !state.cluster_nodes.get().is_empty()
                fallback=|| view! {
                    <EmptyState
                        title="Running in standalone mode"
                        description="Start additional nodes to form a cluster"
                    />
                }
            >
                <table role="table" aria-label="Cluster nodes">
                    <thead>
                        <tr>
                            <th scope="col">"Node ID"</th>
                            <th scope="col">"Address"</th>
                            <th scope="col">"Role"</th>
                            <th scope="col">"Type"</th>
                        </tr>
                    </thead>
                    <tbody>
                        <For
                            each=move || state.cluster_nodes.get()
                            key=|n| n.node_id
                            children=move |node| {
                                view! {
                                    <tr>
                                        <td class="mono">{node.node_id}</td>
                                        <td class="mono">{node.addr}</td>
                                        <td>
                                            {if node.is_leader {
                                                view! {
                                                    <Badge
                                                        text="Leader"
                                                        variant=BadgeVariant::Primary
                                                        with_dot=true
                                                    />
                                                }.into_view()
                                            } else {
                                                view! {
                                                    <Badge
                                                        text="Follower"
                                                        variant=BadgeVariant::Secondary
                                                    />
                                                }.into_view()
                                            }}
                                        </td>
                                        <td>
                                            {if node.is_voter {
                                                view! { <span class="tag tag-blue">"Voter"</span> }.into_view()
                                            } else {
                                                view! { <span class="tag tag-gray">"Learner"</span> }.into_view()
                                            }}
                                        </td>
                                    </tr>
                                }
                            }
                        />
                    </tbody>
                </table>
            </Show>
        </div>
    }
}
