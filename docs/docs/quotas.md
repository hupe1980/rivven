---
layout: default
title: Per-Principal Quotas
nav_order: 11
---

# Per-Principal Quotas
{: .no_toc }

Rate limiting for multi-tenant deployments.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Rivven supports **per-principal quotas** to limit throughput on a per-user, per-client-id, or per-consumer-group basis. This prevents noisy neighbors in multi-tenant deployments and ensures fair resource allocation.

Quotas can be configured at different granularities:

| Entity Type | Description | Use Case |
|-------------|-------------|----------|
| `user` | Per authenticated principal | User-based throttling |
| `client-id` | Per client identifier | Application-based throttling |
| `consumer-group` | Per consumer group | Group throughput limits |
| `default` | Fallback for all entities | Global baseline limits |

## Quota Types

| Quota | Unit | Default | Description |
|-------|------|---------|-------------|
| `produce_bytes_rate` | bytes/sec | 50 MB/s | Producer throughput limit |
| `consume_bytes_rate` | bytes/sec | 100 MB/s | Consumer throughput limit |
| `request_rate` | requests/sec | 1000/s | Request rate limit |

## Quota Resolution Order

When checking quotas, Rivven resolves in this order:

1. **User + Client ID specific** — Most specific, highest priority
2. **User specific** — Per-user limits
3. **Client ID specific** — Per-application limits
4. **Default for entity type** — e.g., default user limits
5. **Global default** — Fallback baseline

## Protocol

### Describe Quotas

```rust
Request::DescribeQuotas {
    // Empty = all quotas, or specific entities
    entities: vec![
        ("user".to_string(), Some("alice".to_string())),
        ("client-id".to_string(), Some("app-1".to_string())),
    ],
}
```

Response:
```rust
Response::QuotasDescribed {
    entries: vec![
        QuotaEntry {
            entity_type: "user",
            entity_name: Some("alice"),
            quotas: {
                "produce_bytes_rate": 10_000_000,
                "consume_bytes_rate": 20_000_000,
                "request_rate": 500,
            },
        },
    ],
}
```

### Alter Quotas

```rust
Request::AlterQuotas {
    alterations: vec![
        QuotaAlteration {
            entity_type: "user".to_string(),
            entity_name: Some("alice".to_string()),
            quota_key: "produce_bytes_rate".to_string(),
            quota_value: Some(10_000_000), // 10 MB/s
        },
        QuotaAlteration {
            entity_type: "client-id".to_string(),
            entity_name: Some("batch-processor".to_string()),
            quota_key: "request_rate".to_string(),
            quota_value: Some(100), // 100 requests/sec
        },
    ],
}
```

Response:
```rust
Response::QuotasAltered {
    altered_count: 2,
}
```

### Throttle Response

When a client exceeds their quota, they receive a `Throttled` response:

```rust
Response::Throttled {
    throttle_time_ms: 500,
    quota_type: "produce_bytes_rate",
    entity: "user:alice",
}
```

The client should wait `throttle_time_ms` before retrying.

## CLI Usage

```bash
# List all quotas
rivven quota list

# Set user quota
rivven quota set --user alice \
  --produce-bytes-rate 10000000 \
  --consume-bytes-rate 20000000

# Set client-id quota  
rivven quota set --client-id batch-processor \
  --request-rate 100

# Set default quotas
rivven quota set --default \
  --produce-bytes-rate 50000000 \
  --consume-bytes-rate 100000000

# Remove a quota (revert to defaults)
rivven quota delete --user alice --quota produce_bytes_rate
```

## Configuration

Server-side default quotas can be configured at startup:

```yaml
# rivven.yaml
quotas:
  defaults:
    produce_bytes_rate: 52428800  # 50 MB/s
    consume_bytes_rate: 104857600  # 100 MB/s
    request_rate: 1000
```

## Client Handling

Clients should handle throttle responses gracefully:

```rust
use rivven_client::Client;

async fn publish_with_retry(client: &Client, topic: &str, value: &[u8]) -> Result<()> {
    loop {
        match client.publish(topic, value).await {
            Ok(response) => {
                // Check for throttle time in response
                if let Some(throttle_ms) = response.throttle_time_ms() {
                    if throttle_ms > 0 {
                        tracing::warn!("Throttled for {}ms", throttle_ms);
                        tokio::time::sleep(Duration::from_millis(throttle_ms)).await;
                    }
                }
                return Ok(());
            }
            Err(e) if e.is_throttled() => {
                let delay = e.throttle_time().unwrap_or(Duration::from_millis(100));
                tracing::warn!("Throttled, retrying after {:?}", delay);
                tokio::time::sleep(delay).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

## Monitoring

Quota metrics are exposed via the metrics endpoint:

```
# HELP rivven_quota_produce_violations_total Total produce quota violations
# TYPE rivven_quota_produce_violations_total counter
rivven_quota_produce_violations_total 42

# HELP rivven_quota_consume_violations_total Total consume quota violations  
# TYPE rivven_quota_consume_violations_total counter
rivven_quota_consume_violations_total 7

# HELP rivven_quota_request_violations_total Total request rate violations
# TYPE rivven_quota_request_violations_total counter
rivven_quota_request_violations_total 15

# HELP rivven_quota_throttle_time_ms_total Total throttle time in milliseconds
# TYPE rivven_quota_throttle_time_ms_total counter
rivven_quota_throttle_time_ms_total 23450
```

## Best Practices

### 1. Start with Conservative Defaults

Begin with generous default quotas and tighten based on observed usage:

```yaml
quotas:
  defaults:
    produce_bytes_rate: 104857600  # 100 MB/s
    consume_bytes_rate: 209715200  # 200 MB/s
    request_rate: 5000
```

### 2. Set Quotas for Known Heavy Users

Identify applications that need higher or lower limits:

```bash
# Batch processor needs higher throughput
rivven quota set --client-id batch-etl \
  --produce-bytes-rate 500000000

# Rate-limit the test environment
rivven quota set --user test-service \
  --produce-bytes-rate 1000000 \
  --request-rate 50
```

### 3. Use Unlimited for Admin/Internal Services

```bash
# Admin tools should not be throttled
rivven quota set --user admin \
  --produce-bytes-rate unlimited \
  --consume-bytes-rate unlimited
```

### 4. Monitor and Alert

Set up alerts for quota violations:

```yaml
# Prometheus alert rule
- alert: HighQuotaViolations
  expr: rate(rivven_quota_produce_violations_total[5m]) > 10
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High produce quota violations"
```

## Internal Architecture

The quota system uses a **sliding window** algorithm for rate tracking:

1. **Window Duration**: 1 second sliding window
2. **Tracking**: Atomic counters for thread-safe updates
3. **Cleanup**: Idle entity state is cleaned up after 1 hour
4. **Resolution**: Quotas checked from most to least specific

### Enforcement Path

All three quota checks (request rate, produce bytes, consume bytes) are enforced inside `handle_with_principal()`:

- **Anonymous path**: `handle()` delegates to `handle_with_principal(request, None, None)` — quotas tracked against the default entity.
- **Authenticated path**: `AuthenticatedHandler` extracts the principal name from the auth session and delegates to `handle_with_principal(request, Some(user), client_id)` — quotas tracked against the specific user/client.

This ensures:
- No double-counting of quotas between auth and handler layers
- Consume quotas are enforced with the real principal (not just anonymous)
- All three quota types use the same enforcement point

```
┌─────────────────────────────────────────────────────┐
│                    QuotaManager                      │
├─────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │
│  │   Defaults   │  │   Configs   │  │   States    │ │
│  │  (baseline)  │  │ (per-entity)│  │(sliding win)│ │
│  └─────────────┘  └─────────────┘  └─────────────┘ │
├─────────────────────────────────────────────────────┤
│  record_produce(user, client_id, bytes) → Result    │
│  record_consume(user, client_id, bytes) → Result    │
│  record_request(user, client_id) → Result           │
└─────────────────────────────────────────────────────┘
```

## Security

- **Authorization**: `DescribeQuotas` requires `Describe` on Cluster
- **Authorization**: `AlterQuotas` requires `Alter` on Cluster (admin only)
- **Audit**: All quota changes are logged for compliance
