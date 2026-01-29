---
layout: docs
title: Testing
description: Comprehensive testing strategy and integration test documentation
---

# Testing

Rivven uses a multi-layered testing strategy to ensure reliability, performance, and security.

## Test Structure

```
Tests: 1552+ passing
├── Unit Tests       (~1300) - Per-crate tests in src/
├── Integration      (~150)  - rivven-integration-tests/
├── Property-based   (~50)   - proptest/quickcheck
├── Benchmarks       (~50)   - criterion micro-benchmarks
└── Feature-gated    (~113)  - Optional feature tests
```

## Running Tests

### Quick Start

```bash
# Run all tests
cargo test --workspace

# Run with all features enabled
cargo test --workspace --all-features

# Run specific crate tests
cargo test -p rivven-core
cargo test -p rivven-cdc
cargo test -p rivven-integration-tests

# Run integration tests only
cargo test -p rivven-integration-tests --test '*'
```

### Integration Tests

Integration tests require Docker for testcontainers:

```bash
# Ensure Docker is running
docker info

# Run integration tests
cargo test -p rivven-integration-tests

# Run specific test categories
cargo test -p rivven-integration-tests --test cdc_postgres
cargo test -p rivven-integration-tests --test cdc_mysql
cargo test -p rivven-integration-tests --test cdc_mariadb
cargo test -p rivven-integration-tests --test client_protocol
cargo test -p rivven-integration-tests --test e2e_pipeline
cargo test -p rivven-integration-tests --test cluster_consensus
cargo test -p rivven-integration-tests --test security
cargo test -p rivven-integration-tests --test chaos

# Run with verbose output
RUST_LOG=info cargo test -p rivven-integration-tests -- --nocapture
```

## Integration Test Categories

### CDC PostgreSQL Tests (`cdc_postgres.rs`)

Tests PostgreSQL Change Data Capture functionality using testcontainers:

| Test | Description |
|------|-------------|
| `postgres_container_starts_and_connects` | Container lifecycle and connectivity |
| `postgres_creates_test_schema` | Schema and table creation |
| `postgres_insert_generates_changes` | INSERT operation capture |
| `postgres_update_generates_changes` | UPDATE operation capture |
| `postgres_delete_generates_changes` | DELETE operation capture |
| `postgres_replication_slot_lifecycle` | Slot creation and cleanup |
| `postgres_multiple_table_subscriptions` | Multi-table CDC |
| `postgres_transaction_boundaries` | Transaction atomicity |
| `postgres_jsonb_data_types` | Complex type handling |
| `postgres_large_batch_operations` | Bulk operation performance |
| `postgres_concurrent_connections` | Connection pool behavior |
| `postgres_checkpoint_and_resume` | Checkpoint persistence |
| `postgres_schema_change_detection` | DDL tracking |
| `postgres_replica_identity_full` | Full row capture |
| `postgres_heartbeat_keepalive` | Connection health |

### CDC MySQL Tests (`cdc_mysql.rs`)

Tests MySQL Change Data Capture functionality using binary log replication:

| Test | Description |
|------|-------------|
| `mysql_container_starts` | Container lifecycle and connectivity |
| `mysql_creates_test_schema` | Schema and table creation |
| `mysql_binlog_enabled` | ROW format, FULL image verification |
| `mysql_gtid_enabled` | GTID mode configuration |
| `mysql_insert_generates_binlog` | INSERT binlog events |
| `mysql_update_generates_binlog` | UPDATE binlog events |
| `mysql_delete_generates_binlog` | DELETE binlog events |
| `mysql_transaction_binlog` | Transaction boundaries |
| `mysql_large_batch` | Bulk operation performance |
| `mysql_foreign_keys` | Relationship handling |
| `mysql_json_columns` | JSON data type support |
| `mysql_concurrent_connections` | Connection pool behavior |
| `mysql_gtid_tracking` | GTID position tracking |
| `mysql_unicode_support` | Character set handling |
| `mysql_binlog_events` | SHOW BINLOG EVENTS |

### CDC MariaDB Tests (`cdc_mariadb.rs`)

Tests MariaDB Change Data Capture functionality with MariaDB-specific features:

| Test | Description |
|------|-------------|
| `mariadb_container_starts` | Container lifecycle and connectivity |
| `mariadb_version` | MariaDB version detection |
| `mariadb_creates_test_schema` | Schema and table creation |
| `mariadb_binlog_enabled` | ROW format, FULL image verification |
| `mariadb_insert_generates_binlog` | INSERT binlog events |
| `mariadb_update_generates_binlog` | UPDATE binlog events |
| `mariadb_delete_generates_binlog` | DELETE binlog events |
| `mariadb_transaction_binlog` | Transaction boundaries |
| `mariadb_json_columns` | JSON data type support |
| `mariadb_gtid_tracking` | MariaDB GTID format |
| `mariadb_large_batch` | Bulk operation performance |
| `mariadb_concurrent_connections` | Connection pool behavior |
| `mariadb_foreign_keys` | Relationship handling |
| `mariadb_unicode_support` | Character set handling |
| `mariadb_rollback` | Transaction rollback |
| `mariadb_storage_engines` | InnoDB verification |
| `mariadb_cdc_system_variables` | Server configuration |

### Client Protocol Tests (`client_protocol.rs`)

Tests the client-server wire protocol:

| Test | Description |
|------|-------------|
| `client_connects_to_broker` | Basic connectivity |
| `client_creates_and_lists_topics` | Topic management |
| `client_publishes_single_message` | Single message produce |
| `client_publishes_batch_messages` | Batch produce |
| `client_consumes_from_beginning` | Historical consumption |
| `client_consumes_from_latest` | Live consumption |
| `client_round_trip_latency` | Latency measurement |
| `client_handles_partitioned_topic` | Partition routing |
| `client_preserves_message_ordering` | Order guarantees |
| `client_handles_large_messages` | Size limits (64MB) |
| `client_concurrent_producers` | Producer scaling |
| `client_concurrent_consumers` | Consumer scaling |
| `client_throughput_benchmark` | Performance baseline |
| `client_handles_topic_not_found` | Error handling |
| `client_reconnects_after_disconnect` | Resilience |

### End-to-End Pipeline Tests (`e2e_pipeline.rs`)

Tests complete data pipelines:

| Test | Description |
|------|-------------|
| `e2e_basic_produce_consume_pipeline` | Simple pipeline |
| `e2e_multi_topic_pipeline` | Cross-topic routing |
| `e2e_fan_out_pattern` | One-to-many distribution |
| `e2e_partitioned_processing` | Parallel processing |
| `e2e_consumer_group_coordination` | Group rebalancing |
| `e2e_cdc_to_broker_pipeline` | CDC → Broker flow |
| `e2e_exactly_once_simulation` | Deduplication |
| `e2e_message_ordering_across_partitions` | Cross-partition order |
| `e2e_data_integrity_verification` | Content validation |
| `e2e_pipeline_throughput` | Pipeline performance |

### Cluster Consensus Tests (`cluster_consensus.rs`)

Tests distributed cluster behavior:

| Test | Description |
|------|-------------|
| `cluster_single_node_operations` | Single-node mode |
| `cluster_forms_with_three_nodes` | Cluster formation |
| `cluster_data_consistency_across_nodes` | Replication |
| `cluster_handles_concurrent_writes` | Write conflicts |
| `cluster_maintains_message_ordering` | Order preservation |
| `cluster_partition_distribution` | Load balancing |
| `cluster_metadata_replication` | State sync |
| `cluster_consumer_group_coordination` | Distributed groups |
| `cluster_handles_node_rejoin` | Node recovery |
| `cluster_leader_election` | Raft election |
| `cluster_handles_network_partition` | Split-brain |
| `cluster_consistent_reads` | Read consistency |

### Security Tests (`security.rs`)

Tests security boundaries and attack resistance:

| Test | Description |
|------|-------------|
| `security_default_configuration_secure` | Secure defaults |
| `security_topic_name_validation` | Input sanitization |
| `security_message_key_validation` | Key validation |
| `security_rejects_oversized_messages` | Size limits |
| `security_connection_limits` | DoS protection |
| `security_topic_isolation` | Tenant isolation |
| `security_message_integrity` | Data integrity |
| `security_concurrent_access_safety` | Race conditions |
| `security_empty_inputs_handled` | Edge cases |
| `security_unicode_normalization` | Unicode attacks |
| `security_binary_data_safe` | Binary payloads |
| `security_rapid_operations_stable` | Timing attacks |
| `security_resource_cleanup` | Leak prevention |
| `security_error_messages_safe` | Info disclosure |
| `security_consistent_behavior` | Predictability |

### Chaos Engineering Tests (`chaos.rs`)

Tests system resilience under failure conditions:

| Test | Description |
|------|-------------|
| `chaos_broker_restart_recovery` | Process restart |
| `chaos_client_reconnection` | Client resilience |
| `chaos_connection_churn` | Rapid connect/disconnect |
| `chaos_concurrent_topic_creation` | Race conditions |
| `chaos_high_load_degradation` | Graceful degradation |
| `chaos_connection_timeout_handling` | Timeout recovery |
| `chaos_message_flood_handling` | Backpressure |
| `chaos_memory_pressure_handling` | OOM resistance |
| `chaos_rapid_topic_lifecycle` | Fast create/delete |
| `chaos_concurrent_consumer_groups` | Group scaling |
| `chaos_network_delay_simulation` | Latency tolerance |
| `chaos_partial_failure_recovery` | Partial outages |

## Test Infrastructure

### Fixtures (`fixtures.rs`)

The test infrastructure provides reusable fixtures:

```rust
use rivven_integration_tests::fixtures::*;

// Embedded broker for fast tests
let broker = TestBroker::start().await?;
let client = broker.client().await?;

// PostgreSQL container for CDC tests
let pg = TestPostgres::start().await?;
let conn = pg.connect().await?;

// Multi-node cluster for consensus tests
let cluster = TestCluster::start(3).await?;
```

### Helpers (`helpers.rs`)

Common test utilities:

```rust
use rivven_integration_tests::helpers::*;

// Initialize tracing for test debugging
init_tracing();

// Wait for condition with timeout
wait_for(|| async { broker.is_ready() }, Duration::from_secs(30)).await?;

// Generate unique test identifiers
let topic = unique_topic_name("test");
let group = unique_group_id("consumer");
```

### Test Data Generation

```rust
use rivven_integration_tests::fixtures::test_data::*;

// Generate test events
let event = TestEvent::new("order.created", json!({"id": 123}));

// Generate message batches
let messages = generate_messages(1000);
let large_batch = generate_large_messages(100, 1024 * 1024);
```

## Property-Based Testing

Property-based tests verify invariants across random inputs:

```bash
# Run property tests
cargo test -p rivven-core -- --test-threads=1 proptest

# Increase case count for thorough testing
PROPTEST_CASES=10000 cargo test -p rivven-core proptest
```

Example properties tested:
- Message serialization roundtrips
- Partition assignment determinism
- Compression ratio bounds
- Offset ordering invariants

## Benchmarks

```bash
# Run all benchmarks
cargo bench --workspace

# Run specific benchmarks
cargo bench -p rivven-core -- compression
cargo bench -p rivven-cdc -- throughput

# Generate HTML reports
cargo bench --workspace -- --save-baseline main
```

Key benchmarks:
- Message serialization throughput
- Compression ratios (LZ4/Zstd)
- WAL append latency
- Consumer group rebalancing
- CDC change processing

## Coverage

Generate coverage reports with `cargo-tarpaulin`:

```bash
# Install tarpaulin
cargo install cargo-tarpaulin

# Generate coverage report
cargo tarpaulin --workspace --out Html

# View report
open tarpaulin-report.html
```

Current coverage targets:
- Core crates: >80%
- CDC crate: >75%
- Protocol crate: >90%

## CI Integration

Tests run automatically in GitHub Actions:

```yaml
# .github/workflows/ci.yml
- name: Run tests
  run: cargo test --workspace --all-features

- name: Run integration tests
  run: cargo test -p rivven-integration-tests
  env:
    DOCKER_HOST: unix:///var/run/docker.sock
```

### Required CI Secrets

For integration tests with external services:

| Secret | Purpose |
|--------|---------|
| `DOCKER_HOST` | Docker socket for testcontainers |
| `AWS_ACCESS_KEY_ID` | S3 sink tests (optional) |
| `GCP_SERVICE_ACCOUNT` | GCS/BigQuery tests (optional) |

## Writing New Tests

### Unit Test Guidelines

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_does_expected_thing() {
        // Arrange
        let input = create_input();
        
        // Act
        let result = function_under_test(input);
        
        // Assert
        assert_eq!(result, expected);
    }
}
```

### Integration Test Guidelines

```rust
use rivven_integration_tests::prelude::*;

#[tokio::test]
async fn test_feature_works_end_to_end() {
    // Use fixtures for setup
    let broker = TestBroker::start().await.unwrap();
    let client = broker.client().await.unwrap();
    
    // Create unique resources to avoid test interference
    let topic = unique_topic_name("feature");
    client.create_topic(&topic, 3).await.unwrap();
    
    // Test the feature
    client.produce(&topic, "key", "value").await.unwrap();
    
    // Verify results
    let messages = client.consume(&topic, 0, 1).await.unwrap();
    assert_eq!(messages.len(), 1);
    
    // Cleanup happens automatically via Drop
}
```

### Test Naming Conventions

- Unit tests: `test_<function>_<scenario>_<expected>`
- Integration tests: `<category>_<feature>_<behavior>`
- Property tests: `prop_<invariant>`
- Benchmarks: `bench_<operation>_<variant>`

## Troubleshooting

### Docker Issues

```bash
# Check Docker is running
docker info

# Check for port conflicts
lsof -i :5432  # PostgreSQL
lsof -i :9092  # Broker

# Clean up orphaned containers
docker container prune -f
docker volume prune -f
```

### Test Timeouts

```bash
# Increase test timeout
RUST_TEST_TIME_UNIT=60000 cargo test

# Run tests serially to reduce contention
cargo test -- --test-threads=1
```

### Debugging Failures

```bash
# Enable trace logging
RUST_LOG=trace cargo test -- --nocapture

# Run specific failing test
cargo test -p rivven-integration-tests test_name -- --exact --nocapture
```

## See Also

- [Architecture](architecture.md) — System design
- [CDC Configuration](cdc-configuration.md) — CDC setup
- [Security](security.md) — Security features
- [Getting Started](getting-started.md) — Quick start guide
