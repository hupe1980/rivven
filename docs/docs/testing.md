---
layout: docs
title: Testing
description: Comprehensive testing strategy and integration test documentation
---

# Testing

Rivven uses a multi-layered testing strategy to ensure reliability, performance, and security.

## Test Structure

```
Tests: 1610+ passing
├── Unit Tests       (~1300) - Per-crate tests in src/
├── Integration      (~210)  - rivven-integration-tests/
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
cargo test -p rivven-integration-tests --test stress
cargo test -p rivven-integration-tests --test consumer_groups
cargo test -p rivven-integration-tests --test connect_e2e
cargo test -p rivven-integration-tests --test durability
cargo test -p rivven-integration-tests --test rbac
cargo test -p rivven-integration-tests --test tls

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

### Stress Tests (`stress.rs`)

Tests system behavior under high-load conditions:

| Test | Description |
|------|-------------|
| `test_sustained_throughput` | Continuous message production (10K messages/sec) |
| `test_concurrent_producers` | 10 parallel producers writing simultaneously |
| `test_burst_traffic` | Handle sudden traffic spikes (500 concurrent writes) |
| `test_large_messages` | Large payload handling (1MB messages) |
| `test_memory_stability` | Memory doesn't grow unbounded under load |
| `test_many_topics` | Handle 100+ topics without degradation |
| `test_concurrent_read_write` | Simultaneous produce and consume |
| `test_recovery_after_high_load` | System stabilizes after load spike |
| `soak_test_extended_load` | Extended duration test (ignored by default) |

```bash
# Run stress tests
cargo test -p rivven-integration-tests --test stress

# Run with soak test (takes ~1 minute)
cargo test -p rivven-integration-tests --test stress -- --ignored
```

### Consumer Group Tests (`consumer_groups.rs`)

Tests consumer group coordination and delivery guarantees:

| Test | Description |
|------|-------------|
| `test_consumer_group_basic` | Basic group formation and consumption |
| `test_offset_commit_recovery` | Offsets persist across reconnects |
| `test_parallel_consumers` | Multiple consumers in same group |
| `test_multiple_consumer_groups` | Independent groups on same topic |
| `test_consumer_lag` | Lag measurement and tracking |
| `test_consumer_session_timeout` | Session expiry handling |
| `test_at_least_once_delivery` | No messages lost on restart |
| `test_ordered_delivery_within_partition` | Message ordering preserved |
| `test_keyed_messages` | Consistent partition routing by key |

### Connector End-to-End Tests (`connect_e2e.rs`)

Tests complete connector pipelines with simulated sources and sinks:

| Test | Description |
|------|-------------|
| `test_source_to_broker_basic` | Source connector → broker ingestion |
| `test_broker_to_sink_basic` | Broker → sink connector delivery |
| `test_full_connect_pipeline` | Source → broker → sink flow |
| `test_connector_error_handling` | Failed record error routing |
| `test_connector_batching` | Batch accumulation and delivery |
| `test_connector_fan_in` | Multiple sources → single topic |
| `test_connector_fan_out` | Single topic → multiple sinks |
| `test_connector_retry_recovery` | Transient failure recovery |
| `test_connector_with_schema` | Schema-aware transformations |

### Durability Tests (`durability.rs`)

Tests data persistence and recovery across broker restarts:

| Test | Description |
|------|-------------|
| `test_no_data_loss_on_restart_with_persistence` | Messages survive broker restart |
| `test_multi_topic_durability` | Multiple topics persist correctly |
| `test_offset_preservation_across_restart` | Consumer offsets preserved |
| `test_crash_recovery` | Recovery from unclean shutdown |

### RBAC Integration Tests (`rbac.rs`)

Real end-to-end tests for authentication and authorization at the broker level using `TestSecureBroker`:

**Authentication Tests:**

| Test | Description |
|------|-------------|
| `test_unauthenticated_request_rejected` | Verifies broker rejects requests without auth |
| `test_authentication_success` | Valid credentials authenticate successfully |
| `test_authentication_failure_invalid_password` | Wrong password is rejected |
| `test_authentication_failure_unknown_user` | Unknown user is rejected |
| `test_scram_authentication` | SCRAM-SHA-256 challenge-response auth works |

**Authorization Tests:**

| Test | Description |
|------|-------------|
| `test_admin_can_create_topic` | Admin role can create topics |
| `test_producer_can_publish` | Producer role can publish messages |
| `test_consumer_can_consume` | Consumer role can consume messages |
| `test_readonly_can_describe` | Read-only role permissions verified |
| `test_unauthorized_publish_denied` | Write denied without producer role |
| `test_unauthorized_topic_creation_denied` | Create denied without admin role |

**Session Management Tests:**

| Test | Description |
|------|-------------|
| `test_session_persistence` | Session persists across multiple requests |
| `test_concurrent_sessions` | Multiple users have concurrent sessions |
| `test_custom_users` | Custom user creation and authentication |
| `test_auth_lockout_after_failures` | Account lockout after failed attempts |

**Edge Case Tests:**

| Test | Description |
|------|-------------|
| `test_empty_credentials` | Empty username/password rejected |
| `test_special_characters_in_credentials` | Special chars in password work |

```bash
# Run RBAC tests
cargo test -p rivven-integration-tests --test rbac
```

### TLS Integration Tests (`tls.rs`)

Tests TLS/mTLS functionality at the broker level using self-signed certificates:

**Basic TLS Connection Tests:**

| Test | Description |
|------|-------------|
| `test_tls_connection_self_signed` | TLS with self-signed certificates |
| `test_plaintext_to_tls_broker_fails` | Plaintext requests rejected by TLS broker |
| `test_tls_client_to_plaintext_broker_fails` | TLS client fails to connect to plaintext broker |

**Message Transmission Tests:**

| Test | Description |
|------|-------------|
| `test_tls_publish_consume` | Publish/consume messages over TLS |
| `test_tls_large_message` | 1MB message over TLS |
| `test_tls_binary_data` | All 256 byte values transmitted correctly |

**mTLS Mode Tests:**

| Test | Description |
|------|-------------|
| `test_mtls_disabled_accepts_any_client` | mTLS disabled mode accepts any client |

**Connection Tests:**

| Test | Description |
|------|-------------|
| `test_tls_concurrent_connections` | Multiple concurrent TLS connections |
| `test_tls_connection_persistence` | Long-lived TLS connections |
| `test_tls_connection_cycling` | Rapid connect/disconnect cycles |
| `test_tls_connection_timeout` | Connection timeout handling |

**TLS Configuration Tests:**

| Test | Description |
|------|-------------|
| `test_tls_server_name_indication` | SNI with custom server name |
| `test_tls_with_authentication` | TLS + authentication combined |
| `test_tls_encryption_active` | Verify encrypted transmission |
| `test_tls_multiple_topics` | Multiple topics over single connection |

```bash
# Run TLS tests
cargo test -p rivven-integration-tests --test tls

# Run observability tests
cargo test -p rivven-integration-tests --test observability
```

### Observability Tests (`observability.rs`)

Tests for metrics, monitoring, and health endpoints:

**Health Endpoint Tests:**

| Test | Description |
|------|-------------|
| `test_health_endpoint_returns_healthy` | Health endpoint returns 200 OK with "healthy" status |
| `test_health_endpoint_json_format` | Health response is valid JSON with expected fields |

**Prometheus Metrics Tests:**

| Test | Description |
|------|-------------|
| `test_prometheus_metrics_endpoint_accessible` | Metrics endpoint accessible with correct Content-Type |
| `test_prometheus_metrics_format` | Metrics follow Prometheus text format (HELP, TYPE) |
| `test_prometheus_counter_metrics` | Counters correctly typed with `_total` suffix |
| `test_prometheus_gauge_metrics` | Gauges correctly typed |
| `test_prometheus_histogram_metrics` | Histograms have `_bucket`, `_sum`, `_count` |

**JSON Metrics Tests:**

| Test | Description |
|------|-------------|
| `test_json_metrics_endpoint_accessible` | JSON metrics endpoint accessible |
| `test_json_metrics_format` | JSON response has expected fields |

**Metrics Accuracy Tests:**

| Test | Description |
|------|-------------|
| `test_metrics_update_after_publish` | Metrics increment after publishing messages |
| `test_partition_count_metric_accuracy` | Partition gauge reflects actual count |

**Performance Tests:**

| Test | Description |
|------|-------------|
| `test_metrics_endpoint_latency` | Metrics endpoint responds in < 100ms |
| `test_health_endpoint_latency` | Health endpoint responds in < 50ms |
| `test_metrics_concurrent_access` | Handles concurrent requests |

**Edge Case Tests:**

| Test | Description |
|------|-------------|
| `test_unknown_endpoint_returns_404` | Unknown paths return 404 |
| `test_metrics_endpoint_idempotent` | Multiple requests return consistent format |
| `test_metrics_naming_conventions` | Metrics follow rivven_ naming convention |

```bash
# Run observability tests
cargo test -p rivven-integration-tests --test observability
```

### Compression Tests (`compression.rs`)

Tests for LZ4 and Zstd compression algorithms:

**Basic Compression Tests:**

| Test | Description |
|------|-------------|
| `test_lz4_compression_transparent` | LZ4 compression/decompression via broker |
| `test_zstd_compression_transparent` | Zstd compression/decompression via broker |
| `test_small_payload_no_compression` | Small payloads bypass compression threshold |

**Compression Ratio Tests:**

| Test | Description |
|------|-------------|
| `test_compression_ratio_text_data` | Text data compresses to < 50% with LZ4 |
| `test_zstd_better_ratio_than_lz4` | Zstd achieves better ratios for redundant data |
| `test_incompressible_data_handling` | Random data handled gracefully |

**Throughput Tests:**

| Test | Description |
|------|-------------|
| `test_lz4_throughput` | LZ4 achieves > 100 MB/s throughput |
| `test_zstd_throughput` | Zstd achieves > 50 MB/s throughput |

**Large Payload Tests:**

| Test | Description |
|------|-------------|
| `test_large_payload_compression` | 10MB payload compression/decompression |
| `test_binary_data_compression` | All 256 byte values handled correctly |

**Multi-Message Tests:**

| Test | Description |
|------|-------------|
| `test_mixed_compressibility_batch` | Batch with varying compressibility levels |
| `test_concurrent_compression` | Multiple producers with compression |

**Compression Level Tests:**

| Test | Description |
|------|-------------|
| `test_lz4_compression_levels` | LZ4 Fast/Default/Best levels work correctly |
| `test_zstd_compression_levels` | Zstd levels achieve progressively better ratios |

**Edge Case Tests:**

| Test | Description |
|------|-------------|
| `test_empty_payload` | Empty data handled correctly |
| `test_single_byte_payload` | Single byte compression works |
| `test_threshold_boundary` | Boundary around min_size threshold |
| `test_compression_idempotency` | Same input produces same compressed output |

```bash
# Run compression tests
cargo test -p rivven-integration-tests --test compression
```

## Test Infrastructure

### Fixtures (`fixtures.rs`)

The test infrastructure provides reusable fixtures:

```rust
use rivven_integration_tests::fixtures::*;

// Embedded broker for fast tests
let broker = TestBroker::start().await?;
let addr = broker.connection_string();

// Secure broker with authentication enabled (for RBAC tests)
let secure_broker = TestSecureBroker::start().await?;
let admin = secure_broker.admin_credentials();

// TLS broker with self-signed certificates (for TLS tests)
let tls_broker = TestTlsBroker::start().await?;
let client_tls_config = tls_broker.client_tls_config();

// Metrics broker with observability endpoints (for observability tests)
let metrics_broker = TestMetricsBroker::start().await?;
let metrics_url = metrics_broker.metrics_url();      // /metrics (Prometheus)
let json_url = metrics_broker.json_metrics_url();    // /metrics/json
let health_url = metrics_broker.health_url();        // /health

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

// Initialize tracing for test debugging (also initializes TLS crypto provider)
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
