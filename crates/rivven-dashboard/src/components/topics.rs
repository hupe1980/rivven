//! Topics view component
//!
//! Displays a searchable list of all topics with:
//! - Name, partition count, replication factor
//! - Message count and offset ranges
//! - Search filtering (keyboard shortcut: ⌘K)
//!
//! Uses reusable primitives for consistent UI.

use leptos::*;

use super::primitives::{Badge, BadgeVariant, EmptyState, SearchInput};
use crate::state::{format_number, DashboardState};

/// Topics list view with search
#[component]
pub fn TopicsView() -> impl IntoView {
    let state = expect_context::<DashboardState>();
    let topic_search = state.topic_search;
    // Get the filtering closure once - it captures reactive signals internally
    let get_filtered_topics = state.filtered_topics();

    view! {
        <div class="view active" role="main" aria-label="Topics">
            // Search bar
            <SearchInput
                value=topic_search
                placeholder="Search topics..."
            />

            // Topics table
            <div class="table-card" role="region" aria-label="Topics list">
                <div class="table-header">
                    <div class="table-title">"All Topics"</div>
                    <Badge
                        text=Signal::derive({
                            let f = get_filtered_topics.clone();
                            move || format!("{} topics", f().len())
                        })
                        variant=BadgeVariant::Default
                    />
                </div>
                <Show
                    when={
                        let f = get_filtered_topics.clone();
                        move || !f().is_empty()
                    }
                    fallback=|| view! {
                        <EmptyState
                            title="No topics found"
                            description="Try adjusting your search or create a new topic"
                        />
                    }
                >
                    <table role="table" aria-label="Topics">
                        <thead>
                            <tr>
                                <th scope="col">"Name"</th>
                                <th scope="col">"Partitions"</th>
                                <th scope="col">"Replication"</th>
                                <th scope="col">"Messages"</th>
                                <th scope="col">"Offset Range"</th>
                            </tr>
                        </thead>
                        <tbody>
                            <For
                                each={
                                    let f = get_filtered_topics.clone();
                                    move || f()
                                }
                                key=|t| t.name.clone()
                                children=move |topic| {
                                    let earliest: u64 = topic.partition_offsets.iter().map(|p| p.earliest).min().unwrap_or(0);
                                    let latest: u64 = topic.partition_offsets.iter().map(|p| p.latest).max().unwrap_or(0);
                                    view! {
                                        <tr>
                                            <td>
                                                <span class="mono topic-name">{topic.name}</span>
                                            </td>
                                            <td>
                                                <Badge
                                                    text=topic.partitions.to_string()
                                                    variant=BadgeVariant::Info
                                                />
                                            </td>
                                            <td>{topic.replication_factor}</td>
                                            <td class="mono">{format_number(topic.message_count)}</td>
                                            <td class="mono">
                                                <span class="offset-range" title="Earliest → Latest offset">
                                                    {format!("{} → {}", earliest, latest)}
                                                </span>
                                            </td>
                                        </tr>
                                    }
                                }
                            />
                        </tbody>
                    </table>
                </Show>
            </div>
        </div>
    }
}
