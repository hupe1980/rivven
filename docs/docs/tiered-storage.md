---
layout: default
title: Tiered Storage
nav_order: 13
---

# Tiered Storage
{: .no_toc }

Hot/warm/cold storage architecture for cost-effective data retention.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Rivven implements a **three-tier storage architecture** that automatically moves data between storage tiers based on age and access patterns:

| Tier | Storage | Latency | Use Case |
|:-----|:--------|:--------|:---------|
| **Hot** | In-memory + NVMe/SSD | < 1ms | Recent data, active consumers |
| **Warm** | Local disk (mmap) | 1-10ms | Medium-aged data, occasional access |
| **Cold** | Object storage | 100ms+ | Archival, compliance, replay |

This approach optimizes for both **performance** (hot data in memory) and **cost** (cold data in cheap object storage).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     TIERED STORAGE                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌─────────────┐                                               │
│   │   HOT TIER  │  In-memory LRU cache + fast SSD               │
│   │   < 1ms     │  Default: 1 GB, 1 hour max age                │
│   └──────┬──────┘                                               │
│          │ demote (age/size)                                    │
│          ▼                                                      │
│   ┌─────────────┐                                               │
│   │  WARM TIER  │  Local disk, memory-mapped                    │
│   │  1-10ms     │  Default: 100 GB, 7 days max age              │
│   └──────┬──────┘                                               │
│          │ demote (age/size)                                    │
│          ▼                                                      │
│   ┌─────────────┐                                               │
│   │  COLD TIER  │  S3, GCS, Azure Blob, MinIO                   │
│   │  100ms+     │  Unlimited retention                          │
│   └─────────────┘                                               │
│          ▲                                                      │
│          │ promote (access count > threshold)                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Configuration

### Basic Configuration

```yaml
# rivvend.yaml
storage:
  tiered:
    enabled: true
    
    hot_tier:
      max_bytes: 1073741824      # 1 GB
      max_age_secs: 3600         # 1 hour
    
    warm_tier:
      max_bytes: 107374182400    # 100 GB
      max_age_secs: 604800       # 7 days
      path: /var/lib/rivven/warm
    
    cold_tier:
      backend: s3
      bucket: my-rivven-archive
      region: us-east-1
      prefix: rivven/
    
    migration:
      interval_secs: 60
      concurrency: 4
    
    promotion:
      enabled: true
      threshold: 100             # Access count to trigger promotion
```

### High-Performance Configuration

For low-latency workloads, increase hot tier size:

```yaml
storage:
  tiered:
    hot_tier:
      max_bytes: 8589934592      # 8 GB
      max_age_secs: 7200         # 2 hours
    
    warm_tier:
      max_bytes: 536870912000    # 500 GB
    
    migration:
      interval_secs: 30          # More frequent migration
```

### Cost-Optimized Configuration

For archival workloads, minimize hot/warm tiers:

```yaml
storage:
  tiered:
    hot_tier:
      max_bytes: 268435456       # 256 MB
      max_age_secs: 900          # 15 minutes
    
    warm_tier:
      max_bytes: 10737418240     # 10 GB
      max_age_secs: 86400        # 1 day
```

---

## Cold Storage Backends

### Amazon S3

```yaml
cold_tier:
  backend: s3
  bucket: my-bucket
  region: us-east-1
  prefix: rivven/data/
  # Credentials from AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or IAM role
```

### Google Cloud Storage

```yaml
cold_tier:
  backend: gcs
  bucket: my-bucket
  prefix: rivven/data/
  # Credentials from GOOGLE_APPLICATION_CREDENTIALS or GKE workload identity
```

### Azure Blob Storage

```yaml
cold_tier:
  backend: azure
  container: my-container
  account: mystorageaccount
  prefix: rivven/data/
  # Credentials from AZURE_STORAGE_KEY or managed identity
```

### MinIO (S3-Compatible)

```yaml
cold_tier:
  backend: s3
  bucket: my-bucket
  endpoint: http://minio.local:9000
  region: us-east-1
  access_key: minioadmin
  secret_key: minioadmin
```

### Local Filesystem

For testing or single-node deployments:

```yaml
cold_tier:
  backend: local
  path: /mnt/archive/rivven
```

---

## Data Lifecycle

### Automatic Demotion

Data moves from hot → warm → cold based on:

1. **Age**: Data older than `max_age_secs` is demoted
2. **Size**: When tier exceeds `max_bytes`, oldest data is demoted

```
New message arrives
        │
        ▼
   ┌─────────┐
   │   HOT   │◄─── All writes go here
   └────┬────┘
        │ age > 1 hour OR size > 1 GB
        ▼
   ┌─────────┐
   │  WARM   │◄─── Memory-mapped for fast reads
   └────┬────┘
        │ age > 7 days OR size > 100 GB
        ▼
   ┌─────────┐
   │  COLD   │◄─── Object storage (S3/GCS/Azure)
   └─────────┘
```

### Access-Based Promotion

When `promotion.enabled: true`, frequently accessed cold data is promoted:

1. Track access count per segment
2. When count exceeds `promotion.threshold`, promote to warm tier
3. Subsequent reads benefit from lower latency

This is useful for:
- Replay scenarios (re-processing historical data)
- Analytics queries on specific time ranges
- Compliance audits

---

## Compaction

Rivven automatically compacts segments to reclaim space from deleted/expired messages:

```yaml
storage:
  tiered:
    compaction:
      threshold: 0.5   # Compact when 50% of segment is dead bytes
```

Compaction runs during tier migration to minimize I/O impact.

---

## Monitoring

### Metrics

| Metric | Description |
|:-------|:------------|
| `rivven_storage_hot_tier_bytes` | Current hot tier size |
| `rivven_storage_warm_tier_bytes` | Current warm tier size |
| `rivven_storage_cold_tier_bytes` | Current cold tier size |
| `rivven_storage_hot_tier_hits` | Cache hits in hot tier |
| `rivven_storage_hot_tier_misses` | Cache misses in hot tier |
| `rivven_storage_migrations_total` | Total segment migrations |
| `rivven_storage_promotions_total` | Total segment promotions |
| `rivven_storage_compactions_total` | Total compaction operations |

### Prometheus Example

```yaml
# Alert when hot tier is full
- alert: RivvenHotTierFull
  expr: rivven_storage_hot_tier_bytes / rivven_storage_hot_tier_max_bytes > 0.9
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Hot tier is 90% full"
    description: "Consider increasing hot_tier.max_bytes"
```

---

## Best Practices

### 1. Size Hot Tier for Working Set

The hot tier should hold your **active working set**—data that consumers are actively reading:

```yaml
# Rule of thumb: 
# hot_tier_max_bytes ≈ (messages/sec × avg_size × retention_window)
hot_tier:
  max_bytes: 4294967296  # 4 GB for 10K msg/s × 1KB × 400s window
```

### 2. Use SSDs for Warm Tier

Memory-mapped warm tier benefits from fast random reads:

```yaml
warm_tier:
  path: /mnt/nvme/rivven/warm  # NVMe SSD recommended
```

### 3. Enable Encryption for Cold Tier

Object storage should use server-side encryption:

```yaml
cold_tier:
  backend: s3
  bucket: my-bucket
  encryption: AES256  # or aws:kms
```

### 4. Set Lifecycle Policies

Configure object storage lifecycle policies for cost optimization:

```json
{
  "Rules": [{
    "ID": "MoveToGlacier",
    "Status": "Enabled",
    "Filter": {"Prefix": "rivven/"},
    "Transitions": [{
      "Days": 90,
      "StorageClass": "GLACIER"
    }]
  }]
}
```

### 5. Monitor Migration Lag

Ensure migrations keep up with data ingestion:

```yaml
# Alert if migration is falling behind
- alert: RivvenMigrationLag
  expr: rivven_storage_pending_migrations > 1000
  for: 10m
  labels:
    severity: warning
```

---

## Troubleshooting

### High Read Latency

**Symptom**: Consumer lag increasing, read latency > 100ms

**Cause**: Too many reads hitting cold tier

**Solution**:
1. Increase hot tier size
2. Enable promotion for frequently accessed data
3. Check if consumers are reading historical data

### Cold Storage Errors

**Symptom**: `ColdStorageError: connection timeout`

**Cause**: Network issues or misconfigured credentials

**Solution**:
1. Verify credentials: `aws s3 ls s3://bucket/`
2. Check network connectivity to object storage
3. Increase timeout in cold tier config

### Disk Full on Warm Tier

**Symptom**: `No space left on device`

**Cause**: Warm tier migration not keeping up

**Solution**:
1. Reduce `warm_tier.max_bytes`
2. Increase migration concurrency
3. Add disk space or move to larger volume
