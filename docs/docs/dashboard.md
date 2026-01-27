---
layout: default
title: Dashboard
nav_order: 12
---

# Rivven Dashboard

The Rivven Dashboard provides a **modern, reactive web interface** for monitoring and managing your Rivven cluster. Built entirely in Rust with Leptos and compiled to WebAssembly, it runs directly in the browser with zero JavaScript dependencies.

## Features

| Feature | Description |
|---------|-------------|
| **100% Rust** | No JavaScript, TypeScript, or npm |
| **Air-Gapped** | No external fonts, CDNs, or API calls |
| **Real-time Updates** | Automatic 3-second refresh cycle |
| **Cluster Monitoring** | Raft leader, nodes, terms |
| **Topic Management** | Browse topics, partitions, messages |
| **Consumer Groups** | Monitor lag and group membership |
| **Prometheus Metrics** | Built-in metrics viewer |
| **Dark Theme** | Operations-optimized UI |

## Air-Gapped Support

The dashboard is designed for **air-gapped environments**:

- ✅ **System fonts** — Uses OS font stack (no Google Fonts)
- ✅ **No CDN** — All assets served from rivven-server
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

**Priority order:**
1. `<meta name="rivven:api-url">` (highest)
2. `window.__RIVVEN_CONFIG__.api_url`
3. Current window origin (default)

## Accessing the Dashboard

When running rivven-server with the embedded dashboard:

```bash
# Start server
rivvend --data-dir ./data

# Dashboard available at:
# http://localhost:8080/
```

## Views

### Overview

The landing page provides cluster health at a glance:

- **Server Status**: Healthy/Unhealthy badge
- **Topics Count**: Total topics in cluster
- **Partitions Count**: Aggregate partition count
- **Consumer Groups**: Active consumer groups
- **Total Messages**: Messages across all topics
- **Top Topics**: Quick view of busiest topics
- **Top Groups**: Consumer groups with highest lag

### Topics

Detailed topic information:

| Column | Description |
|--------|-------------|
| Name | Topic identifier |
| Partitions | Number of partitions |
| Replication | Replication factor |
| Messages | Total message count |
| Offsets | Earliest → Latest offset range |

**Features:**
- Full-text search across topic names
- Click to expand partition details

### Consumer Groups

Monitor consumer group health:

| Column | Description |
|--------|-------------|
| Group ID | Consumer group identifier |
| State | Stable / Rebalancing / Empty |
| Members | Active member count |
| Lag | Total lag across all partitions |
| Topics | Subscribed topic list |

**Lag Indicators:**
- 🟢 Green: Lag < 100
- 🟡 Yellow: 100 ≤ Lag < 10,000
- 🔴 Red: Lag ≥ 10,000

### Cluster

Raft cluster status and node information:

| Component | Data |
|-----------|------|
| Cluster ID | Unique cluster identifier |
| Leader | Current Raft leader node |
| Term | Current Raft election term |
| Node Count | Total nodes in cluster |

**Node Status:**
- 🟢 Connected: Node is reachable
- 🔴 Disconnected: Node unreachable
- 👑 Leader: Current Raft leader
- ⚙️ Follower: Raft follower node

### Metrics

Prometheus-compatible metrics display:

- Raw metrics text viewer
- Copy-to-clipboard functionality
- Metrics endpoint URL for Grafana integration

## Configuration

### Enabling the Dashboard

```yaml
# rivven.yaml
dashboard:
  enabled: true
  # Uses same port as API (default: 8080)
```

### Authentication

The dashboard inherits authentication from the server:

```yaml
auth:
  enabled: true
  type: basic
  users:
    - username: admin
      password_hash: "$argon2id$..."

# Dashboard requires same credentials
```

### CSP Headers

The dashboard enforces Content Security Policy:

```
Content-Security-Policy: 
  default-src 'self'; 
  script-src 'self' 'wasm-unsafe-eval'; 
  style-src 'self' 'unsafe-inline'; 
  connect-src 'self'
```

## Building from Source

### Prerequisites

```bash
# Install trunk (WASM bundler)
cargo install trunk

# Add WASM target
rustup target add wasm32-unknown-unknown
```

### Development Build

```bash
# Start dashboard dev server with hot reload
cd crates/rivven-dashboard
trunk serve --port 8081

# In another terminal, start rivven-server
cargo run -p rivven-server -- --data-dir ./data
```

### Production Build

```bash
# Using justfile recipes
just dashboard-build

# Or manually
cd crates/rivven-dashboard
trunk build --release

# Output in dist/
ls dist/
# index.html  rivven_dashboard_bg.wasm  rivven_dashboard.js  styles.css
```

### Embedding in Server

The dashboard assets must be copied to `rivven-server/static/` before building the server:

```bash
# Step 1: Build dashboard
cd crates/rivven-dashboard
trunk build --release

# Step 2: Copy assets to server
cp -r dist/* ../rivven-server/static/

# Step 3: Build server with embedded dashboard
cd ../..
cargo build -p rivven-server --release --features dashboard

# Dashboard is now embedded in the binary
./target/release/rivven-server
```

The server uses `rust-embed` to compile static files into the binary at build time. This creates a single, self-contained executable with no external dependencies.

**Production deployment:**
```bash
# Just run the binary - dashboard is embedded
./rivven-server --data-dir /var/lib/rivven
# Dashboard available at http://localhost:8080/
```

## REST API

The dashboard consumes these REST endpoints:

### Health Check

```http
GET /dashboard/health
```

```json
{
  "status": "healthy",
  "uptime_secs": 3600,
  "version": "0.0.1",
  "node_id": "node-1",
  "role": "leader"
}
```

### Topics

```http
GET /dashboard/topics
```

```json
[
  {
    "name": "orders",
    "partitions": 8,
    "replication_factor": 3,
    "message_count": 1500000,
    "earliest_offset": 0,
    "latest_offset": 1500000
  }
]
```

### Consumer Groups

```http
GET /dashboard/groups
```

```json
[
  {
    "group_id": "order-processor",
    "state": "Stable",
    "members": 3,
    "total_lag": 150,
    "topics": ["orders"]
  }
]
```

### Cluster

```http
GET /dashboard/cluster
```

```json
{
  "cluster_id": "rivven-prod",
  "leader_id": "node-1",
  "term": 42,
  "nodes": [...]
}
```

## Troubleshooting

### Dashboard Not Loading

1. **Check WASM loading**: Open browser DevTools → Console
2. **Verify CSP**: Look for Content-Security-Policy violations
3. **Check API**: Ensure `/dashboard/health` returns 200

### API Connection Errors

```bash
# Verify server is running
curl http://localhost:8080/health

# Check dashboard endpoints
curl http://localhost:8080/dashboard/topics
```

### Build Errors

```bash
# Missing target
rustup target add wasm32-unknown-unknown

# Trunk not installed
cargo install trunk

# Clear build cache
cd crates/rivven-dashboard
rm -rf dist/ target/
trunk build --release
```

## Architecture

```
Browser
├── rivven-dashboard.wasm (Leptos app)
│   ├── Components
│   │   ├── primitives.rs    # Reusable UI components
│   │   ├── icons.rs         # Centralized SVG icons
│   │   ├── sidebar.rs       # Navigation
│   │   ├── header.rs        # Top bar
│   │   ├── overview.rs      # Dashboard home
│   │   ├── topics.rs        # Topic list
│   │   ├── consumer_groups.rs
│   │   ├── cluster.rs       # Raft status
│   │   └── metrics.rs       # Prometheus viewer
│   ├── State (Leptos signals)
│   └── ApiClient (gloo-net HTTP)
│
└── HTTP Requests
    │
    ▼
rivven-server
├── Static file serving (WASM, CSS, HTML)
├── Dashboard REST API (/dashboard/*)
└── Prometheus metrics (/metrics)
```

### Component Primitives

The dashboard uses reusable UI primitives for consistency:

| Primitive | Description |
|-----------|-------------|
| `LoadingSpinner` | Animated loading indicator |
| `Skeleton` | Content placeholder during load |
| `ErrorBoundary` | Error handling wrapper |
| `EmptyState` | No data message |
| `Badge` | Status indicators (Info, Success, Warning, Error) |
| `StatusDot` | Connection status indicator |
| `LagIndicator` | Consumer lag with color coding |
| `StatCard` | Metric cards with icon |
| `SearchInput` | Search with keyboard shortcut |

### SVG Icons

All icons are embedded for air-gapped operation:

- Navigation: `HomeIcon`, `TopicIcon`, `GroupIcon`, `ClusterIcon`, `MetricsIcon`
- Actions: `RefreshIcon`, `SearchIcon`, `CopyIcon`
- Status: `WarningIcon`, `ErrorIcon`, `SuccessIcon`, `InfoIcon`

## See Also

- [Architecture Overview](architecture.md)
- [Getting Started](getting-started.md)
- [Security](security.md)
