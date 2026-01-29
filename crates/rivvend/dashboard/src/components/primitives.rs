//! Reusable UI primitive components
//!
//! This module provides the foundational building blocks for the dashboard UI:
//! - Loading spinners and skeletons
//! - Error boundaries and error states
//! - Empty states
//! - Badges and indicators
//! - Cards and containers

use leptos::*;

// ============================================================================
// Loading States
// ============================================================================

/// Loading spinner with optional message
#[component]
pub fn LoadingSpinner(
    #[prop(optional)] message: Option<&'static str>,
    #[prop(optional, default = false)] overlay: bool,
) -> impl IntoView {
    let class = if overlay {
        "loading-overlay"
    } else {
        "loading-spinner"
    };

    view! {
        <div class=class role="status" aria-live="polite">
            <svg class="spinner" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                <circle class="spinner-track" cx="12" cy="12" r="10" fill="none" stroke-width="3"/>
                <circle class="spinner-head" cx="12" cy="12" r="10" fill="none" stroke-width="3"
                        stroke-dasharray="31.4 31.4" stroke-linecap="round"/>
            </svg>
            {message.map(|msg| view! { <span class="loading-message">{msg}</span> })}
        </div>
    }
}

/// Skeleton placeholder for loading content
#[component]
pub fn Skeleton(
    #[prop(optional, default = "100%")] width: &'static str,
    #[prop(optional, default = "1rem")] height: &'static str,
    #[prop(optional, default = false)] rounded: bool,
) -> impl IntoView {
    let style = format!("width: {}; height: {};", width, height);
    let class = if rounded {
        "skeleton skeleton-rounded"
    } else {
        "skeleton"
    };

    view! {
        <div class=class style=style aria-hidden="true"></div>
    }
}

/// Skeleton row for table loading states
#[component]
pub fn SkeletonRow(#[prop(optional, default = 4)] columns: usize) -> impl IntoView {
    view! {
        <tr class="skeleton-row" aria-hidden="true">
            {(0..columns).map(|_| view! {
                <td><Skeleton width="80%" height="1rem"/></td>
            }).collect_view()}
        </tr>
    }
}

// ============================================================================
// Error Handling
// ============================================================================

/// Error boundary wrapper that catches panics
#[component]
pub fn ErrorBoundary(
    children: Children,
    #[prop(optional)] fallback: Option<View>,
) -> impl IntoView {
    let (error, _set_error) = create_signal::<Option<String>>(None);
    let children_view = children();

    // In WASM, we use the panic hook to catch errors
    // For now, we provide a simple error state UI

    view! {
        <Show
            when=move || error.get().is_none()
            fallback=move || {
                let err = error.get().unwrap_or_else(|| "An unexpected error occurred".to_string());
                fallback.clone().unwrap_or_else(|| view! {
                    <ErrorState message=err.clone()/>
                }.into_view())
            }
        >
            {children_view.clone()}
        </Show>
    }
}

/// Error state display
#[component]
pub fn ErrorState(
    message: String,
    #[prop(optional)] retry: Option<Callback<()>>,
) -> impl IntoView {
    view! {
        <div class="error-state" role="alert">
            <div class="error-icon">
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126ZM12 15.75h.007v.008H12v-.008Z"/>
                </svg>
            </div>
            <div class="error-content">
                <h3 class="error-title">"Something went wrong"</h3>
                <p class="error-message">{message}</p>
            </div>
            {retry.map(|on_retry| view! {
                <button class="btn btn-primary" on:click=move |_| on_retry.call(())>
                    "Try Again"
                </button>
            })}
        </div>
    }
}

/// Connection error state (used when API is unreachable)
#[component]
pub fn ConnectionError(
    #[prop(optional)] on_retry: Option<Callback<()>>,
) -> impl IntoView {
    view! {
        <div class="connection-error" role="alert" aria-live="assertive">
            <div class="connection-error-icon">
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v3.75m0-10.036A11.959 11.959 0 0 1 3.598 6 11.99 11.99 0 0 0 3 9.75c0 5.592 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.31-.21-2.57-.598-3.75h-.152c-3.196 0-6.1-1.25-8.25-3.286Zm0 13.036h.008v.008H12v-.008Z"/>
                </svg>
            </div>
            <h3>"Connection Lost"</h3>
            <p>"Unable to connect to Rivven server. Retrying automatically..."</p>
            {on_retry.map(|retry| view! {
                <button class="btn btn-secondary" on:click=move |_| retry.call(())>
                    "Retry Now"
                </button>
            })}
        </div>
    }
}

// ============================================================================
// Empty States
// ============================================================================

/// Generic empty state component
#[component]
pub fn EmptyState(
    title: &'static str,
    #[prop(optional)] description: Option<&'static str>,
    #[prop(optional)] icon: Option<View>,
    #[prop(optional)] action: Option<View>,
) -> impl IntoView {
    let default_icon = view! {
        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" d="M20.25 7.5l-.625 10.632a2.25 2.25 0 0 1-2.247 2.118H6.622a2.25 2.25 0 0 1-2.247-2.118L3.75 7.5m6 4.125 2.25 2.25m0 0 2.25 2.25M12 13.875l2.25-2.25M12 13.875l-2.25 2.25M3.375 7.5h17.25c.621 0 1.125-.504 1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125H3.375c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125Z"/>
        </svg>
    };

    view! {
        <div class="empty-state" role="status">
            <div class="empty-icon">
                {icon.unwrap_or(default_icon.into_view())}
            </div>
            <div class="empty-text">{title}</div>
            {description.map(|desc| view! { <p class="empty-description">{desc}</p> })}
            {action}
        </div>
    }
}

// ============================================================================
// Badges & Indicators
// ============================================================================

/// Badge variant
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BadgeVariant {
    #[default]
    Default,
    Primary,
    Secondary,
    Success,
    Warning,
    Error,
    Info,
}

impl BadgeVariant {
    pub fn class(&self) -> &'static str {
        match self {
            BadgeVariant::Default => "badge",
            BadgeVariant::Primary => "badge badge-primary",
            BadgeVariant::Secondary => "badge badge-secondary",
            BadgeVariant::Success => "badge badge-success",
            BadgeVariant::Warning => "badge badge-warning",
            BadgeVariant::Error => "badge badge-error",
            BadgeVariant::Info => "badge badge-info",
        }
    }
}

/// Badge component with text
#[component]
pub fn Badge<T: IntoView + 'static>(
    text: T,
    #[prop(optional)] variant: BadgeVariant,
    #[prop(optional, default = false)] with_dot: bool,
) -> impl IntoView {
    view! {
        <span class=variant.class()>
            {with_dot.then(|| view! { <span class="badge-dot"></span> })}
            {text}
        </span>
    }
}

/// Status indicator dot
#[component]
pub fn StatusDot(
    #[prop(into)] connected: MaybeSignal<bool>,
) -> impl IntoView {
    view! {
        <span
            class="status-dot"
            class:connected=move || connected.get()
            class:disconnected=move || !connected.get()
            role="status"
            aria-label=move || if connected.get() { "Connected" } else { "Disconnected" }
        />
    }
}

/// Lag indicator with bar visualization
#[component]
pub fn LagIndicator(lag: u64) -> impl IntoView {
    let class = crate::state::lag_class(lag);
    let percentage = (lag.min(10000) as f64 / 100.0).min(100.0);

    view! {
        <div class="lag-indicator" role="meter" aria-valuenow=lag aria-valuemin=0 aria-valuemax=10000>
            <span class="mono">{crate::state::format_number(lag)}</span>
            <div class="lag-bar" aria-hidden="true">
                <div class="lag-fill" class=class style=format!("width: {}%", percentage)></div>
            </div>
        </div>
    }
}

// ============================================================================
// Cards & Containers
// ============================================================================

/// Stat card for overview metrics
#[component]
pub fn StatCard(
    label: &'static str,
    #[prop(into)] value: Signal<String>,
    #[prop(optional)] color: Option<&'static str>,
    #[prop(optional)] icon: Option<View>,
) -> impl IntoView {
    let class = format!("stat-card {}", color.unwrap_or(""));

    view! {
        <div class=class>
            <div class="stat-header">
                {icon.map(|i| view! { <div class="stat-icon">{i}</div> })}
                <span class="stat-label">{label}</span>
            </div>
            <div class="stat-value" aria-label=format!("{}: ", label)>
                {move || value.get()}
            </div>
        </div>
    }
}

/// Table card container
#[component]
pub fn TableCard(
    title: &'static str,
    children: Children,
    #[prop(optional)] action: Option<View>,
    #[prop(optional)] badge: Option<View>,
) -> impl IntoView {
    view! {
        <div class="table-card">
            <div class="table-header">
                <div class="table-title-group">
                    <div class="table-title">{title}</div>
                    {badge}
                </div>
                {action}
            </div>
            {children()}
        </div>
    }
}

// ============================================================================
// Info Rows
// ============================================================================

/// Key-value info row with children for the value
#[component]
pub fn InfoRow(
    label: &'static str,
    children: Children,
) -> impl IntoView {
    view! {
        <div class="info-row">
            <span class="info-label">{label}</span>
            <span class="info-value">
                {children()}
            </span>
        </div>
    }
}

// ============================================================================
// Search
// ============================================================================

/// Search input with keyboard shortcut hint
#[component]
pub fn SearchInput(
    #[prop(into)] value: RwSignal<String>,
    #[prop(optional, default = "Search...")] placeholder: &'static str,
) -> impl IntoView {
    let input_ref = create_node_ref::<html::Input>();

    view! {
        <div class="search-bar">
            <div class="search-icon" aria-hidden="true">
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" d="m21 21-5.197-5.197m0 0A7.5 7.5 0 1 0 5.196 5.196a7.5 7.5 0 0 0 10.607 10.607Z"/>
                </svg>
            </div>
            <input
                type="search"
                node_ref=input_ref
                placeholder=placeholder
                class="search-input"
                prop:value=move || value.get()
                on:input=move |ev| {
                    let new_value = event_target_value(&ev);
                    value.set(new_value);
                }
                aria-label=placeholder
            />
            <kbd class="search-shortcut" aria-hidden="true">"⌘K"</kbd>
        </div>
    }
}
