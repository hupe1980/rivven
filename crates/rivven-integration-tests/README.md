# rivven-integration-tests

Integration and end-to-end tests for the Rivven distributed event streaming platform.

## Overview

This crate contains comprehensive integration tests using [testcontainers](https://github.com/testcontainers/testcontainers-rs) for realistic testing with actual database instances and multi-node cluster scenarios.

## Test Categories

| File | Tests | Passed | Ignored | Description |
|------|-------|--------|---------|-------------|
| `cdc_postgres.rs` | 16 | 15 | 1 | PostgreSQL CDC with logical replication |
| `cdc_mysql.rs` | 16 | 15 | 1 | MySQL CDC with binary log replication |
| `cdc_mariadb.rs` | 15 | 14 | 1 | MariaDB CDC with binlog and GTID |
| `client_protocol.rs` | 16 | 14 | 2 | Client-server wire protocol conformance |
| `e2e_pipeline.rs` | 9 | 8 | 1 | End-to-end data pipeline scenarios |
| `cluster_consensus.rs` | 12 | 10 | 2 | Cluster formation and Raft consensus |
| `security.rs` | 14 | 11 | 3 | Security boundaries and attack resistance |
| `chaos.rs` | 12 | 11 | 1 | Chaos engineering and failure recovery |

**Total: 110 tests (98 passed + 12 ignored)**

## Prerequisites

- **Docker**: Required for testcontainers
- **Rust 1.75+**: For async test support

```bash
# Verify Docker is running
docker info
```

## Running Tests

```bash
# Run all integration tests
cargo test -p rivven-integration-tests

# Run specific test file
cargo test -p rivven-integration-tests --test cdc_postgres
cargo test -p rivven-integration-tests --test cdc_mysql
cargo test -p rivven-integration-tests --test cdc_mariadb
cargo test -p rivven-integration-tests --test security

# Run with logging
RUST_LOG=info cargo test -p rivven-integration-tests -- --nocapture

# Run specific test
cargo test -p rivven-integration-tests postgres_container_starts -- --exact
```

## Test Infrastructure

### Fixtures

The `fixtures` module provides reusable test infrastructure:

```rust
use rivven_integration_tests::fixtures::*;

// Embedded broker (fast, in-memory)
let broker = TestBroker::start().await?;
let client = broker.client().await?;

// PostgreSQL container (testcontainers)
let pg = TestPostgres::start().await?;
let conn = pg.connect().await?;

// MySQL container (testcontainers)
let mysql = TestMysql::start().await?;
let pool = mysql.pool();

// MariaDB container (testcontainers)
let mariadb = TestMariadb::start().await?;
let pool = mariadb.pool();

// Multi-node cluster
let cluster = TestCluster::start(3).await?;
```

### Helpers

The `helpers` module provides test utilities:

```rust
use rivven_integration_tests::helpers::*;

init_tracing();  // Enable test logging
let topic = unique_topic_name("test");  // Unique identifiers
wait_for(|| condition, timeout).await?;  // Async polling
```

### Test Data

Generate test payloads:

```rust
use rivven_integration_tests::fixtures::test_data::*;

let event = TestEvent::new("order.created", json!({"id": 123}));
let messages = generate_messages(1000);
```

## Architecture

```
rivven-integration-tests/
├── src/
│   ├── lib.rs           # Crate root, prelude exports
│   ├── fixtures.rs      # TestBroker, TestCluster, TestPostgres, TestMysql, TestMariadb
│   ├── helpers.rs       # Test utilities
│   └── mocks.rs         # Mock implementations
└── tests/
    ├── cdc_postgres.rs       # PostgreSQL CDC tests
    ├── cdc_mysql.rs          # MySQL CDC tests
    ├── cdc_mariadb.rs        # MariaDB CDC tests
    ├── client_protocol.rs    # Protocol conformance tests
    ├── e2e_pipeline.rs       # Pipeline integration tests
    ├── cluster_consensus.rs  # Cluster behavior tests
    ├── security.rs           # Security boundary tests
    └── chaos.rs              # Chaos engineering tests
```

## Writing Tests

### Basic Pattern

```rust
use rivven_integration_tests::prelude::*;

#[tokio::test]
async fn test_feature_works() {
    // Setup
    let broker = TestBroker::start().await.unwrap();
    let client = broker.client().await.unwrap();
    let topic = unique_topic_name("feature");
    
    // Execute
    client.create_topic(&topic, 1).await.unwrap();
    client.produce(&topic, "key", "value").await.unwrap();
    
    // Verify
    let messages = client.consume(&topic, 0, 1).await.unwrap();
    assert_eq!(messages.len(), 1);
}
```

### CDC Test Pattern

```rust
#[tokio::test]
async fn test_cdc_captures_changes() {
    let pg = TestPostgres::start().await.unwrap();
    let conn = pg.connect().await.unwrap();
    
    // Setup CDC
    conn.execute("CREATE TABLE test (id INT PRIMARY KEY)", &[]).await.unwrap();
    
    // Make changes
    conn.execute("INSERT INTO test VALUES (1)", &[]).await.unwrap();
    
    // Verify CDC captured the change
    // ...
}
```

### Cluster Test Pattern

```rust
#[tokio::test]
async fn test_cluster_replicates_data() {
    let cluster = TestCluster::start(3).await.unwrap();
    
    // Write to leader
    let leader = cluster.leader().await.unwrap();
    leader.produce("topic", "key", "value").await.unwrap();
    
    // Verify replication to followers
    for follower in cluster.followers() {
        let messages = follower.consume("topic", 0, 1).await.unwrap();
        assert_eq!(messages.len(), 1);
    }
}
```

## Test Categories Explained

### CDC PostgreSQL

Tests the Change Data Capture connector for PostgreSQL:
- Container lifecycle management
- Logical replication slot handling
- INSERT/UPDATE/DELETE operation capture
- Transaction boundary tracking
- JSONB and complex type support
- Schema change detection
- Checkpoint and resume

### Client Protocol

Tests the client-server communication protocol:
- Connection establishment
- Topic CRUD operations
- Message produce/consume
- Partitioning and routing
- Error handling
- Reconnection logic

### End-to-End Pipeline

Tests complete data flow scenarios:
- Simple produce/consume pipelines
- Multi-topic routing
- Fan-out patterns
- Consumer group coordination
- Exactly-once delivery simulation

### Cluster Consensus

Tests distributed cluster behavior:
- Single-node operation
- Multi-node cluster formation
- Raft leader election
- Data replication consistency
- Node failure and recovery
- Network partition handling

### Security

Tests security boundaries:
- Input validation and sanitization
- Message size limits
- Connection limits
- Topic isolation
- Error message safety
- Resource cleanup

### Chaos Engineering

Tests system resilience:
- Broker restart recovery
- Connection churn
- High load degradation
- Memory pressure handling
- Concurrent operations
- Timeout handling

## Dependencies

```toml
[dependencies]
testcontainers = "0.23"
testcontainers-modules = { version = "0.11", features = ["postgres"] }
tokio = { version = "1", features = ["full", "test-util"] }
tokio-postgres = "0.7"
```

## Troubleshooting

### Docker Connection Issues

```bash
# Check Docker socket
ls -la /var/run/docker.sock

# On macOS with Docker Desktop
export DOCKER_HOST=unix:///var/run/docker.sock
```

### Port Conflicts

```bash
# Check for port usage
lsof -i :5432  # PostgreSQL
lsof -i :9092  # Broker default
```

### Test Timeouts

```bash
# Increase timeout
RUST_TEST_TIME_UNIT=60000 cargo test -p rivven-integration-tests
```

### Container Cleanup

```bash
# Remove orphaned test containers
docker ps -a | grep testcontainers | awk '{print $1}' | xargs docker rm -f
```

## CI/CD

Tests run in GitHub Actions with Docker support:

```yaml
- name: Integration Tests
  run: cargo test -p rivven-integration-tests
  env:
    DOCKER_HOST: unix:///var/run/docker.sock
```

## See Also

- [Testing Documentation](../../docs/docs/testing.md)
- [CDC Architecture](../../docs/docs/cdc.md)
- [Security](../../docs/docs/security.md)
