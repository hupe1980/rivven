# Rivven Dashboard

A modern, reactive dashboard for monitoring and managing Rivven clusters, built with [Leptos](https://leptos.dev/) and compiled to WebAssembly.

## Features

- **100% Rust** — Zero JavaScript, compiled to WASM
- **Air-Gapped** — No external fonts, CDNs, or network requests
- **Real-time Updates** — Reactive data binding with 3-second auto-refresh
- **Cluster Monitoring** — View nodes, leader status, Raft metrics
- **Topic Management** — Browse topics, partitions, and message counts
- **Consumer Groups** — Monitor consumer lag and group membership
- **Dark Theme** — Beautiful, modern UI optimized for operations

## Air-Gapped Support

The dashboard is designed for air-gapped environments:

- ✅ **System fonts** — Uses OS font stack (no Google Fonts)
- ✅ **No CDN** — All assets served from rivvend
- ✅ **Same-origin API** — No CORS or external calls
- ✅ **Embedded WASM** — Bundled in server binary

### Configuration Injection

Pass the API URL at startup via meta tags:

```html
<!-- Server injects these when serving index.html -->
<meta name="rivven:api-url" content="http://rivven.internal:8080">
<meta name="rivven:version" content="0.0.1">
```

Or via JavaScript for reverse proxy setups:

```html
<script>
  window.__RIVVEN_CONFIG__ = {
    api_url: "https://rivven.example.com/api"
  };
</script>
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Browser                                  │
│  ┌─────────────────────────────────────────────────────┐    │
│  │           rivven-dashboard (WASM)                   │    │
│  │  ┌─────────┐  ┌──────────┐  ┌──────────────┐       │    │
│  │  │ Leptos  │→ │ gloo-net │→ │ REST API     │       │    │
│  │  │ UI      │  │ HTTP     │  │ /health      │       │    │
│  │  └─────────┘  └──────────┘  │ /metrics     │       │    │
│  │                             │ /dashboard/* │       │    │
│  └─────────────────────────────┴──────────────┘───────┘    │
└──────────────────────────┬──────────────────────────────────┘
                           │ HTTP
┌──────────────────────────▼──────────────────────────────────┐
│                      rivvend                                │
│             (serves WASM + REST API)                        │
└─────────────────────────────────────────────────────────────┘
```

## Building

### Prerequisites

Install [trunk](https://trunkrs.dev/):

```bash
cargo install trunk
rustup target add wasm32-unknown-unknown
```

### Development

```bash
# Start development server with hot reload
cd crates/rivven-dashboard
trunk serve --port 8081

# Make sure rivvend is running on port 8080
rivvend --data-dir ./data
```

### Production Build

```bash
# Build optimized WASM bundle
trunk build --release

# Output in dist/
ls dist/
# index.html  rivven_dashboard_bg.wasm  rivven_dashboard.js  styles.css
```

### Embed in rivvend

After building, copy the `dist/` contents to `rivvend/static/`:

```bash
# Build dashboard
cd crates/rivven-dashboard
trunk build --release

# Copy to server static files
cp -r dist/* ../rivvend/static/
```

Then rebuild rivvend to embed the new assets:

```bash
cargo build -p rivvend --release
```

## Views

### Overview
- Server status and uptime
- Quick stats (topics, partitions, consumer groups, messages)
- Topics preview with partition counts
- Consumer groups preview with lag indicators

### Topics
- Searchable topic list
- Partition counts and replication factor
- Message counts per topic
- Offset ranges (earliest → latest)

### Consumer Groups
- Group state and member count
- Topic subscriptions
- Total lag with visual indicators
- Summary statistics

### Cluster
- Node listing with leader/follower roles
- Voter/learner status
- Server connection statistics
- Uptime and request counts

### Metrics
- Quick stats overview
- Prometheus endpoint links
- Raw metrics display

## Technology Stack

- **[Leptos](https://leptos.dev/)** — Rust web framework with fine-grained reactivity
- **[gloo-net](https://gloo-rs.web.app/)** — HTTP client for WASM
- **[leptos_router](https://docs.rs/leptos_router)** — Client-side routing
- **[trunk](https://trunkrs.dev/)** — WASM bundler and dev server

## Component Architecture

The dashboard uses a modular component architecture with reusable primitives:

### Primitives (`components/primitives.rs`)

Reusable UI building blocks for consistency across views:

| Component | Description |
|-----------|-------------|
| `LoadingSpinner` | Animated loading indicator |
| `Skeleton` / `SkeletonRow` | Content placeholders |
| `ErrorBoundary` | Error handling wrapper with retry |
| `ErrorState` | Error message display |
| `ConnectionError` | Offline/disconnected state |
| `EmptyState` | No data available message |
| `Badge` | Status indicators with variants (Default, Info, Success, Warning, Error) |
| `StatusDot` | Colored connection indicator |
| `LagIndicator` | Consumer lag display with severity colors |
| `StatCard` | Metric card with value and label |
| `TableCard` | Styled table container |
| `InfoRow` | Label-value pair display |
| `SearchInput` | Search field with keyboard shortcut hint |

### Icons (`components/icons.rs`)

Centralized SVG icons for air-gapped deployment (no external icon fonts):

- Navigation: `HomeIcon`, `TopicIcon`, `GroupIcon`, `ClusterIcon`, `MetricsIcon`
- Actions: `RefreshIcon`, `SearchIcon`, `CopyIcon`, `CheckIcon`
- Status: `WarningIcon`, `ErrorIcon`, `SuccessIcon`, `InfoIcon`
- Decorative: `PartitionIcon`, `MessageIcon`, `EmptyBoxIcon`, `KeyboardIcon`, `LeaderIcon`

All icons support `IconSize` (Small 16px, Medium 20px, Large 24px) and include `aria-hidden="true"` for accessibility.

### Usage Example

```rust
use crate::components::primitives::{Badge, BadgeVariant, StatCard, EmptyState};
use crate::components::icons::{TopicIcon, IconSize};

view! {
    <StatCard
        title="Topics"
        value=Signal::derive(move || topics.get().len().to_string())
        icon=view! { <TopicIcon size=IconSize::Medium /> }
    />
    
    <Badge text="Active" variant=BadgeVariant::Success />
    
    <Show
        when=move || !data.is_empty()
        fallback=|| view! { <EmptyState title="No data" description="Check back later" /> }
    >
        // ... content
    </Show>
}
```

## Why Leptos over htmx/Alpine.js?

The previous dashboard used htmx and Alpine.js — solid choices for server-rendered apps. However, Leptos offers:

1. **Type Safety** — Compile-time guarantees for UI logic
2. **Single Language** — Rust everywhere (server + client)
3. **Fine-Grained Reactivity** — Only updates what changes
4. **No JavaScript Runtime** — Smaller bundle, faster execution
5. **Rust Ecosystem** — Share types with server, use cargo tools

## Documentation

- [Online Docs](https://hupe1980.github.io/rivven/docs/dashboard) — User guide

## License

Apache-2.0
