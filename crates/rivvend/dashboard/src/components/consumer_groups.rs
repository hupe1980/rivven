//! Consumer groups view component
//!
//! Displays consumer groups with:
//! - Group ID, state, member count
//! - Subscribed topics
//! - Lag monitoring with visual indicator
//! - Summary statistics
//!
//! Uses reusable primitives for consistent UI.

use leptos::*;

use super::primitives::{Badge, BadgeVariant, EmptyState, LagIndicator};
use crate::state::{format_number, DashboardState};

/// Consumer groups list view
#[component]
pub fn ConsumerGroupsView() -> impl IntoView {
    let state = expect_context::<DashboardState>();

    view! {
        <div class="view active" role="main" aria-label="Consumer Groups">
            <div class="table-card" role="region" aria-label="Consumer groups list">
                <div class="table-header">
                    <div class="table-title">"Consumer Groups"</div>
                    <Badge
                        text=Signal::derive(move || format!("{} groups", state.consumer_groups.get().len()))
                        variant=BadgeVariant::Default
                    />
                </div>
                <Show
                    when=move || !state.consumer_groups.get().is_empty()
                    fallback=|| view! {
                        <EmptyState
                            title="No consumer groups"
                            description="Consumer groups will appear here when consumers connect"
                        />
                    }
                >
                    <table role="table" aria-label="Consumer groups">
                        <thead>
                            <tr>
                                <th scope="col">"Group ID"</th>
                                <th scope="col">"State"</th>
                                <th scope="col">"Members"</th>
                                <th scope="col">"Topics"</th>
                                <th scope="col">"Total Lag"</th>
                            </tr>
                        </thead>
                        <tbody>
                            <For
                                each=move || state.consumer_groups.get()
                                key=|g| g.group_id.clone()
                                children=move |group| {
                                    let lag = group.total_lag;
                                    view! {
                                        <tr>
                                            <td class="mono">{group.group_id}</td>
                                            <td>
                                                <Badge
                                                    text=group.state
                                                    variant=BadgeVariant::Success
                                                    with_dot=true
                                                />
                                            </td>
                                            <td>{group.member_count}</td>
                                            <td>
                                                <div class="topic-tags" role="list" aria-label="Subscribed topics">
                                                    {group.topics.into_iter().map(|t| view! {
                                                        <span class="tag" role="listitem">{t}</span>
                                                    }).collect_view()}
                                                </div>
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

            // Summary stats
            <ConsumerGroupStats/>
        </div>
    }
}

/// Summary statistics for consumer groups
#[component]
fn ConsumerGroupStats() -> impl IntoView {
    let state = expect_context::<DashboardState>();
    let total_lag = state.total_lag();
    let consumer_groups = state.consumer_groups;

    view! {
        <div class="stats-row" role="region" aria-label="Consumer group statistics">
            <div class="stat-mini">
                <span class="stat-mini-label">"Total Groups"</span>
                <span class="stat-mini-value" aria-live="polite">
                    {move || consumer_groups.get().len()}
                </span>
            </div>
            <div class="stat-mini">
                <span class="stat-mini-label">"Total Lag"</span>
                <span class="stat-mini-value" aria-live="polite">
                    {move || format_number(total_lag())}
                </span>
            </div>
            <div class="stat-mini">
                <span class="stat-mini-label">"Total Members"</span>
                <span class="stat-mini-value" aria-live="polite">
                    {move || consumer_groups.get().iter().map(|g| g.member_count).sum::<usize>()}
                </span>
            </div>
        </div>
    }
}
