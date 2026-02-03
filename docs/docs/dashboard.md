---
layout: default
title: Dashboard
nav_order: 12
---

# Rivven Dashboard

The Rivven Dashboard provides a **lightweight, modern web interface** for monitoring and managing your Rivven cluster. Built as a single static HTML file with vanilla JavaScript, it's embedded directly into the server binary.

## Features

| Feature | Description |
|---------|-------------|
| **Zero Dependencies** | Single HTML file, no npm/build tools |
| **Air-Gapped** | No external fonts, CDNs, or API calls |
| **Real-time Updates** | Automatic 5-second refresh cycle |
| **Cluster Monitoring** | Nodes, leader status, health |
| **Topic Management** | Browse topics, partitions, messages |
| **Consumer Groups** | Monitor lag and group membership |
| **Prometheus Metrics** | Built-in metrics viewer |
| **Dark Theme** | Operations-optimized UI |

## Air-Gapped Support

The dashboard is designed for **air-gapped environments**:

- ✅ **System fonts** — Uses OS font stack (no Google Fonts)
- ✅ **No CDN** — All assets served from rivvend
- ✅ **Same-origin API** — No CORS or external calls
- ✅ **Embedded HTML** — Bundled in server binary via rust-embed

## Accessing the Dashboard

When running rivvend with the dashboard feature enabled:

```bash
# Start server with dashboard (default)
rivvend --data-dir ./data

# Dashboard available at:
# http://localhost:9094/

# Disable dashboard
rivvend --data-dir ./data --no-dashboard
```

## Views

### Overview

The landing page provides cluster health at a glance:

- **Topics Count**: Total topics in cluster
- **Consumer Groups**: Active consumer groups
- **Active Connections**: Current client connections
- **Total Requests**: Cumulative request count
- **Recent Topics**: Quick view of topics with message counts

### Topics

Detailed topic information:

| Column | Description |
|--------|-------------|
| Name | Topic identifier |
| Partitions | Number of partitions |
| Replication | Replication factor |
| Messages | Total message count |

### Consumer Groups

Monitor consumer group health:

| Column | Description |
|--------|-------------|
| Group ID | Consumer group identifier |
| State | Current group state (Stable, Empty, etc.) |
| Members | Number of active members |
| Topics | Subscribed topics |
| Lag | Total lag across all partitions |

### Cluster

View cluster topology:

- **Node Count**: Total nodes in cluster
- **Leader**: Current Raft leader node
- **Node Cards**: Per-node status with address, leader badge, voter/learner role

### Metrics

Browse Prometheus metrics organized by prefix:

- Metrics grouped by category (e.g., `rivven_broker_*`, `rivven_cdc_*`)
- Real-time values with automatic refresh

## REST API Endpoints

The dashboard consumes these JSON endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /dashboard/data` | Topics, consumer groups, stats |
| `GET /membership` | Cluster node membership |
| `GET /metrics/json` | Prometheus metrics as JSON |
| `GET /health` | Server health check |

## Security

### HTTP Headers

The dashboard applies security headers automatically:

```
Content-Security-Policy: default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; font-src 'self'; frame-ancestors 'none'
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
Referrer-Policy: strict-origin-when-cross-origin
Permissions-Policy: geolocation=(), microphone=(), camera=()
```

### Production Recommendations

1. **Enable TLS** — Use `--tls-cert` and `--tls-key` flags
2. **Reverse Proxy** — Put nginx/Caddy in front for authentication
3. **Network Isolation** — Bind dashboard to internal networks only
4. **mTLS** — Use mutual TLS for zero-trust environments

## Customization

### Modifying the Dashboard

The dashboard source is at `crates/rivvend/dashboard/index.html`. To customize:

```bash
# Edit the HTML file
vim crates/rivvend/dashboard/index.html

# Rebuild rivvend (rust-embed recompiles automatically)
cargo build -p rivvend --release
```

### Disabling the Dashboard

```bash
# At runtime
rivvend --no-dashboard

# At compile time (smaller binary)
cargo build -p rivvend --no-default-features
```

## Troubleshooting

### Dashboard Not Loading

1. **Check port**: Dashboard is on the HTTP API port (default: 9094)
2. **Check feature**: Ensure built with `dashboard` feature (default)
3. **Check browser console**: Look for JavaScript errors

### API Errors

1. **CORS issues**: Dashboard must be served from same origin as API
2. **Connection refused**: Verify rivvend is running and port is accessible
3. **Empty data**: Check that topics/consumer groups exist

### Refresh Not Working

The dashboard auto-refreshes every 5 seconds. If updates stop:

1. Check browser console for errors
2. Verify server is still running
3. Check network connectivity to API endpoints

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                     Browser                         │
├─────────────────────────────────────────────────────┤
│  dashboard/index.html (embedded in binary)          │
│  ├── Vanilla JavaScript                             │
│  ├── CSS (embedded)                                 │
│  └── fetch() → /dashboard/data, /membership, etc.  │
└─────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────┐
│                     rivvend                         │
├─────────────────────────────────────────────────────┤
│  rust-embed → serves index.html from memory         │
│  axum routes → /dashboard/data, /membership, etc.   │
└─────────────────────────────────────────────────────┘
```

## Comparison to Previous WASM Dashboard

| Aspect | Old (WASM) | New (Static HTML) |
|--------|------------|-------------------|
| Build tools | trunk, wasm-opt | None |
| Rust targets | wasm32-unknown-unknown | Native only |
| CI complexity | High (WASM build step) | Low |
| Binary size | +2MB | +30KB |
| Browser support | WASM required | Universal |
| Development | Rebuild WASM | Edit HTML |
