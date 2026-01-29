//! Overview/Dashboard view component
//!
//! The main dashboard landing page showing:
//! - Key metrics (topics, partitions, consumer groups, messages)
//! - Server status card with health, mode, node ID, uptime
//! - Topics preview table (top 5)
//! - Consumer groups preview table (top 5)
//!
//! Uses reusable primitives for consistent UI patterns.

use leptos::*;

use super::icons::{GroupIcon, MessageIcon, PartitionIcon, TopicIcon};
use super::primitives::{Badge, BadgeVariant, EmptyState, InfoRow, LagIndicator, StatCard};
use crate::state::{format_number, format_uptime, DashboardState};

/// Main overview dashboard view
#[component]
pub fn OverviewView() -> impl IntoView {
    let state = expect_context::<DashboardState>();

    let total_partitions = state.total_partitions();
    let total_messages = state.total_messages();

    view! {
        <div class="view active" role="main" aria-label="Dashboard Overview">
            // Stats Grid
            <div class="stats-grid" role="region" aria-label="Key metrics">
                <StatCard
                    label="Topics"
                    value=Signal::derive(move || format_number(state.topics.get().len() as u64))
                    color="blue"
                    icon=view! { <TopicIcon/> }
                />
                <StatCard
                    label="Partitions"
                    value=Signal::derive(move || format_number(total_partitions() as u64))
                    color="green"
                    icon=view! { <PartitionIcon/> }
                />
                <StatCard
                    label="Consumer Groups"
                    value=Signal::derive(move || format_number(state.consumer_groups.get().len() as u64))
                    color="purple"
                    icon=view! { <GroupIcon/> }
                />
                <StatCard
                    label="Messages"
                    value=Signal::derive(move || format_number(total_messages()))
                    color="orange"
                    icon=view! { <MessageIcon/> }
                />
            </div>

            // Content Grid
            <div class="overview-grid">
                // Server Status Card
                <ServerStatusCard/>

                // Topics Preview
                <TopicsPreview/>

                // Consumer Groups Preview
                <ConsumerGroupsPreview/>
            </div>
        </div>
    }
}

/// Server status card showing health, mode, node ID, uptime
#[component]
fn ServerStatusCard() -> impl IntoView {
    let state = expect_context::<DashboardState>();

    view! {
        <div class="table-card" role="region" aria-label="Server status">
            <div class="table-header">
                <div class="table-title">"Server Status"</div>
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

/// Topics preview table showing top 5 topics
#[component]
fn TopicsPreview() -> impl IntoView {
    let state = expect_context::<DashboardState>();

    view! {
        <div class="table-card" role="region" aria-label="Topics preview">
            <div class="table-header">
                <div class="table-title">"Topics"</div>
                <a href="/topics" class="btn btn-secondary">"View All"</a>
            </div>
            <Show
                when=move || !state.topics.get().is_empty()
                fallback=|| view! { <EmptyState title="No topics" description="Create your first topic to get started"/> }
            >
                <table role="table" aria-label="Topics list">
                    <thead>
                        <tr>
                            <th scope="col">"Name"</th>
                            <th scope="col">"Partitions"</th>
                            <th scope="col">"Messages"</th>
                        </tr>
                    </thead>
                    <tbody>
                        <For
                            each=move || state.topics.get().into_iter().take(5)
                            key=|t| t.name.clone()
                            children=move |topic| view! {
                                <tr>
                                    <td class="mono">{topic.name}</td>
                                    <td>{topic.partitions}</td>
                                    <td class="mono">{format_number(topic.message_count)}</td>
                                </tr>
                            }
                        />
                    </tbody>
                </table>
            </Show>
        </div>
    }
}

/// Consumer groups preview table showing top 5 groups
#[component]
fn ConsumerGroupsPreview() -> impl IntoView {
    let state = expect_context::<DashboardState>();

    view! {
        <div class="table-card" role="region" aria-label="Consumer groups preview">
            <div class="table-header">
                <div class="table-title">"Consumer Groups"</div>
                <a href="/groups" class="btn btn-secondary">"View All"</a>
            </div>
            <Show
                when=move || !state.consumer_groups.get().is_empty()
                fallback=|| view! { <EmptyState title="No consumer groups" description="Consumer groups appear when clients connect"/> }
            >
                <table role="table" aria-label="Consumer groups list">
                    <thead>
                        <tr>
                            <th scope="col">"Group ID"</th>
                            <th scope="col">"State"</th>
                            <th scope="col">"Lag"</th>
                        </tr>
                    </thead>
                    <tbody>
                        <For
                            each=move || state.consumer_groups.get().into_iter().take(5)
                            key=|g| g.group_id.clone()
                            children=move |group| {
                                let lag = group.total_lag;
                                view! {
                                    <tr>
                                        <td class="mono">{group.group_id}</td>
                                        <td>
                                            <Badge text=group.state variant=BadgeVariant::Success with_dot=true/>
                                        </td>
                                        <td>
                                            <LagIndicator lag=lag/>
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
