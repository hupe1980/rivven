# Admin API

Rivven provides a comprehensive Admin API for programmatic cluster management, matching Kafka's administrative capabilities. These operations enable DevOps automation, infrastructure-as-code workflows, and dynamic cluster reconfiguration.

## Overview

| Operation | Description | Authorization |
|-----------|-------------|---------------|
| `AlterTopicConfig` | Modify topic configuration | `Alter` on Topic |
| `CreatePartitions` | Add partitions to a topic | `Alter` on Topic |
| `DeleteRecords` | Delete records before offset | `Delete` on Topic |
| `DescribeTopicConfigs` | Query topic configurations | `Describe` on Cluster |

## Topic Configuration

### Supported Configuration Keys

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `retention.ms` | i64 | 604800000 (7d) | Message retention time |
| `retention.bytes` | i64 | -1 (unlimited) | Max bytes per partition |
| `max.message.bytes` | i64 | 1048576 (1MB) | Maximum message size |
| `segment.bytes` | i64 | 1073741824 (1GB) | Segment file size |
| `segment.ms` | i64 | 604800000 (7d) | Segment roll time |
| `cleanup.policy` | string | "delete" | "delete", "compact", or "compact,delete" |
| `min.insync.replicas` | i32 | 1 | Minimum in-sync replicas |
| `compression.type` | string | "producer" | none, producer, lz4, zstd, snappy, gzip |

### Alter Topic Configuration

Modify configuration for an existing topic:

```rust
Request::AlterTopicConfig {
    topic: "events".to_string(),
    configs: vec![
        TopicConfigEntry {
            key: "retention.ms".to_string(),
            value: Some("86400000".to_string()), // 1 day
        },
        TopicConfigEntry {
            key: "cleanup.policy".to_string(),
            value: Some("compact".to_string()),
        },
    ],
}
```

Response:
```rust
Response::TopicConfigAltered {
    topic: "events",
    changed_count: 2,
}
```

To reset a configuration to its default:
```rust
TopicConfigEntry {
    key: "retention.ms".to_string(),
    value: None, // Reset to default
}
```

### Describe Topic Configurations

Query current configuration for topics:

```rust
Request::DescribeTopicConfigs {
    topics: vec!["events".to_string(), "orders".to_string()],
}
```

Response:
```rust
Response::TopicConfigsDescribed {
    configs: vec![
        TopicConfigDescription {
            topic: "events",
            configs: {
                "retention.ms": TopicConfigValue {
                    value: "86400000",
                    is_default: false,
                    is_read_only: false,
                    is_sensitive: false,
                },
                "cleanup.policy": TopicConfigValue {
                    value: "compact",
                    is_default: false,
                    is_read_only: false,
                    is_sensitive: false,
                },
                // ... other configs
            },
        },
    ],
}
```

## Partition Management

### Create Partitions

Add partitions to an existing topic. Partition count can only increase, never decrease.

```rust
Request::CreatePartitions {
    topic: "events".to_string(),
    new_partition_count: 12, // Increase from current count
    assignments: vec![], // Auto-assign to brokers
}
```

Response:
```rust
Response::PartitionsCreated {
    topic: "events",
    new_partition_count: 12,
}
```

**Note**: When increasing partitions, existing messages stay in their original partitions. New messages will be distributed across all partitions. This may affect ordering guarantees for keys that were previously assigned to specific partitions.

## Record Management

### Delete Records

Delete records before a given offset (log truncation). This is useful for:
- GDPR compliance (right to be forgotten)
- Reducing storage costs
- Clearing test data

```rust
Request::DeleteRecords {
    topic: "events".to_string(),
    partition_offsets: vec![
        (0, 1000), // Delete all records before offset 1000 in partition 0
        (1, 500),  // Delete all records before offset 500 in partition 1
    ],
}
```

Response:
```rust
Response::RecordsDeleted {
    topic: "events",
    results: vec![
        DeleteRecordsResult {
            partition: 0,
            low_watermark: 1000, // New earliest available offset
            error: None,
        },
        DeleteRecordsResult {
            partition: 1,
            low_watermark: 500,
            error: None,
        },
    ],
}
```

**Warning**: Deleted records cannot be recovered. Ensure proper backups before deletion.

## CLI Usage

```bash
# Alter topic configuration
rivvenctl topic config set events \
  --config retention.ms=86400000 \
  --config cleanup.policy=compact

# Describe topic configuration
rivvenctl topic config describe events

# Create partitions
rivvenctl topic partitions create events --count 12

# Delete records
rivvenctl topic records delete events \
  --partition 0 --before-offset 1000 \
  --partition 1 --before-offset 500
```

## Use Cases

### 1. Retention Tuning

Adjust retention for high-volume topics:

```bash
# Short retention for transient data
rivvenctl topic config set clickstream \
  --config retention.ms=3600000 \
  --config retention.bytes=10737418240

# Long retention for audit logs
rivvenctl topic config set audit-logs \
  --config retention.ms=31536000000 \
  --config cleanup.policy=delete
```

### 2. Log Compaction for CDC

Enable compaction for changelog topics:

```bash
rivvenctl topic config set users-changelog \
  --config cleanup.policy=compact \
  --config min.insync.replicas=2
```

### 3. Scaling Partitions

Increase partitions for higher throughput:

```bash
# Check current partition count
rivvenctl topic describe events

# Increase partitions
rivvenctl topic partitions create events --count 24
```

### 4. GDPR Compliance

Delete user data on request:

```bash
# Find offsets containing user data (application-specific)
# Then delete records up to and including those offsets
rivvenctl topic records delete user-events \
  --partition 0 --before-offset 5000
```

## Error Handling

### Common Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `UNKNOWN_TOPIC_OR_PARTITION` | Topic or partition doesn't exist | Verify topic name |
| `INVALID_CONFIG` | Invalid configuration key or value | Check config key names and value types |
| `INVALID_PARTITIONS` | New count ≤ current count | Partition count can only increase |
| `OFFSET_OUT_OF_RANGE` | Target offset exceeds latest | Use valid offset within bounds |
| `AUTHORIZATION_FAILED` | Insufficient permissions | Request `Alter` or `Delete` permission |

## Best Practices

### 1. Use Infrastructure-as-Code

Store topic configurations in version control:

```yaml
# topics.yaml
topics:
  - name: events
    partitions: 12
    config:
      retention.ms: 86400000
      cleanup.policy: compact
      min.insync.replicas: 2
```

### 2. Monitor Configuration Changes

Set up alerts for configuration changes:

```yaml
# Prometheus alert
- alert: TopicConfigChanged
  expr: increase(rivven_admin_config_changes_total[1h]) > 0
  annotations:
    summary: "Topic configuration was modified"
```

### 3. Test in Non-Production First

Always test configuration changes in staging before applying to production.

### 4. Document Changes

Log all administrative changes for audit purposes:

```bash
# The Admin API logs all changes
INFO Altered 2 configuration(s) for topic 'events'
```

## Security

- **Authorization**: All Admin API operations require appropriate ACL permissions
- **Audit Logging**: All changes are logged with timestamp and principal
- **Rate Limiting**: Admin operations are subject to request rate quotas
