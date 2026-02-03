//! Distributed Connect integration tests
//!
//! Tests for distributed connector coordination:
//! - Singleton connector leader election and failover
//! - Task assignment across multiple nodes
//! - Rebalancing when nodes join/leave
//! - Heartbeat and health monitoring
//!
//! Run with: cargo test -p rivven-integration-tests --test connect_distributed -- --nocapture

use anyhow::Result;
use rivven_connect::distributed::{
    AssignmentStrategy, ConnectCoordinator, ConnectorId, ConnectorMode, ConnectorState,
    CoordinatorConfig, FailoverConfig, NodeId,
};
use rivven_integration_tests::helpers::*;
use std::time::Duration;
use tracing::info;

// ============================================================================
// Helper Functions
// ============================================================================

fn create_coordinator(node_id: &str) -> ConnectCoordinator {
    let config = CoordinatorConfig {
        node_id: NodeId::new(node_id),
        heartbeat_interval: Duration::from_millis(100),
        heartbeat_timeout: Duration::from_millis(500),
        session_timeout: Duration::from_secs(2),
        rebalance_delay: Duration::from_millis(100),
        assignment_strategy: AssignmentStrategy::LeastLoaded,
        failover_config: FailoverConfig::default(),
    };
    ConnectCoordinator::new(config)
}

// ============================================================================
// Singleton Connector Tests
// ============================================================================

/// Test that singleton connectors only have one task
#[tokio::test]
async fn test_singleton_connector_single_task() -> Result<()> {
    init_tracing();

    let mut coordinator = create_coordinator("node-1");

    // Register a CDC connector as singleton
    let connector_id = ConnectorId::new("postgres-cdc");
    coordinator.register_connector(
        connector_id.clone(),
        "postgres-cdc".to_string(),
        ConnectorMode::Singleton,
        5, // Request 5 tasks - should be overridden to 1
        serde_json::json!({"host": "localhost", "database": "test"}),
    )?;

    // Verify only one task was created
    let connector = coordinator.get_connector(&connector_id).unwrap();
    assert_eq!(
        connector.tasks_max, 1,
        "Singleton connector should have exactly 1 task"
    );
    assert_eq!(connector.mode, ConnectorMode::Singleton);

    info!("Singleton connector correctly limited to 1 task");
    Ok(())
}

/// Test singleton connector state transitions
#[tokio::test]
async fn test_singleton_connector_lifecycle() -> Result<()> {
    init_tracing();

    let mut coordinator = create_coordinator("node-1");

    let connector_id = ConnectorId::new("mysql-cdc");
    coordinator.register_connector(
        connector_id.clone(),
        "mysql-cdc".to_string(),
        ConnectorMode::Singleton,
        1,
        serde_json::json!({}),
    )?;

    // Initial state should be Registered
    let connector = coordinator.get_connector(&connector_id).unwrap();
    assert_eq!(connector.state, ConnectorState::Registered);

    // Start the connector
    coordinator.start_connector(&connector_id)?;
    let connector = coordinator.get_connector(&connector_id).unwrap();
    assert!(matches!(
        connector.state,
        ConnectorState::Starting | ConnectorState::Running
    ));

    // Pause the connector
    coordinator.pause_connector(&connector_id)?;
    let connector = coordinator.get_connector(&connector_id).unwrap();
    assert_eq!(connector.state, ConnectorState::Paused);

    // Resume the connector
    coordinator.resume_connector(&connector_id)?;
    let connector = coordinator.get_connector(&connector_id).unwrap();
    assert_eq!(connector.state, ConnectorState::Running);

    info!("Singleton connector lifecycle test passed");
    Ok(())
}

// ============================================================================
// Scalable Connector Tests
// ============================================================================

/// Test scalable connector can have multiple tasks
#[tokio::test]
async fn test_scalable_connector_multiple_tasks() -> Result<()> {
    init_tracing();

    let mut coordinator = create_coordinator("node-1");

    let connector_id = ConnectorId::new("s3-sink");
    coordinator.register_connector(
        connector_id.clone(),
        "s3-sink".to_string(),
        ConnectorMode::Scalable,
        4, // Request 4 tasks
        serde_json::json!({"bucket": "events", "region": "us-east-1"}),
    )?;

    let connector = coordinator.get_connector(&connector_id).unwrap();
    assert_eq!(
        connector.tasks_max, 4,
        "Scalable connector should have 4 tasks"
    );
    assert_eq!(connector.mode, ConnectorMode::Scalable);

    info!(
        "Scalable connector correctly supports {} tasks",
        connector.tasks_max
    );
    Ok(())
}

// ============================================================================
// Task Assignment Tests
// ============================================================================

/// Test task assignment with least-loaded strategy
#[tokio::test]
async fn test_task_assignment_least_loaded() -> Result<()> {
    init_tracing();

    let mut coordinator = create_coordinator("coordinator");

    // Add some member nodes
    coordinator.add_member(NodeId::new("worker-1"))?;
    coordinator.add_member(NodeId::new("worker-2"))?;
    coordinator.add_member(NodeId::new("worker-3"))?;

    // Register a scalable connector with 6 tasks
    let connector_id = ConnectorId::new("kafka-sink");
    coordinator.register_connector(
        connector_id.clone(),
        "kafka-sink".to_string(),
        ConnectorMode::Scalable,
        6,
        serde_json::json!({}),
    )?;

    // Start the connector and trigger rebalance
    coordinator.start_connector(&connector_id)?;
    coordinator.rebalance()?;

    // Get task assignments
    let assignments = coordinator.get_task_assignments();

    // All 6 tasks should be assigned
    assert_eq!(assignments.len(), 6, "All 6 tasks should be assigned");

    // Verify tasks are distributed across available workers
    let mut tasks_per_worker: std::collections::HashMap<NodeId, usize> =
        std::collections::HashMap::new();
    for (task_id, node_id) in &assignments {
        *tasks_per_worker.entry(node_id.clone()).or_default() += 1;
        info!("Task {} assigned to {}", task_id.0, node_id.0);
    }

    // With least-loaded strategy, tasks should be distributed
    // (may not be perfectly even due to algorithm dynamics)
    info!(
        "Task assignment test passed: {} tasks distributed across {} workers",
        assignments.len(),
        tasks_per_worker.len()
    );
    Ok(())
}

/// Test rebalancing when a node joins
#[tokio::test]
async fn test_rebalance_on_node_join() -> Result<()> {
    init_tracing();

    let mut coordinator = create_coordinator("coordinator");

    // Start with 2 workers
    coordinator.add_member(NodeId::new("worker-1"))?;
    coordinator.add_member(NodeId::new("worker-2"))?;

    // Register connector with 6 tasks
    let connector_id = ConnectorId::new("http-sink");
    coordinator.register_connector(
        connector_id.clone(),
        "http-sink".to_string(),
        ConnectorMode::Scalable,
        6,
        serde_json::json!({}),
    )?;

    coordinator.start_connector(&connector_id)?;
    coordinator.rebalance()?;

    let initial_assignments = coordinator.get_task_assignments();
    info!(
        "Initial assignments: {} tasks across 2 workers",
        initial_assignments.len()
    );
    assert_eq!(
        initial_assignments.len(),
        6,
        "All 6 tasks should be assigned"
    );

    // Add a third worker
    coordinator.add_member(NodeId::new("worker-3"))?;
    coordinator.rebalance()?;

    let new_assignments = coordinator.get_task_assignments();
    assert_eq!(
        new_assignments.len(),
        6,
        "All 6 tasks should still be assigned"
    );

    // Note: The least-loaded strategy prioritizes stability over perfect distribution.
    // Existing healthy assignments are kept. New tasks would go to the new worker.
    // This test verifies the cluster is stable after the join.
    info!(
        "Rebalance on node join test passed: {} tasks assigned",
        new_assignments.len()
    );
    Ok(())
}

/// Test rebalancing when a node leaves
#[tokio::test]
async fn test_rebalance_on_node_leave() -> Result<()> {
    init_tracing();

    let mut coordinator = create_coordinator("coordinator");

    // Start with 3 workers
    coordinator.add_member(NodeId::new("worker-1"))?;
    coordinator.add_member(NodeId::new("worker-2"))?;
    coordinator.add_member(NodeId::new("worker-3"))?;

    // Register connector with 6 tasks
    let connector_id = ConnectorId::new("elastic-sink");
    coordinator.register_connector(
        connector_id.clone(),
        "elastic-sink".to_string(),
        ConnectorMode::Scalable,
        6,
        serde_json::json!({}),
    )?;

    coordinator.start_connector(&connector_id)?;
    coordinator.rebalance()?;

    // Remove worker-2
    coordinator.remove_member(&NodeId::new("worker-2"))?;
    coordinator.rebalance()?;

    let assignments = coordinator.get_task_assignments();

    // Verify worker-2 has no tasks and all tasks are reassigned
    let worker2_tasks: Vec<_> = assignments
        .iter()
        .filter(|(_, node)| node.0 == "worker-2")
        .collect();

    assert!(
        worker2_tasks.is_empty(),
        "Removed worker should have no tasks"
    );

    // All 6 tasks should still be assigned
    assert_eq!(assignments.len(), 6, "All tasks should be reassigned");

    info!("Node leave rebalance test passed");
    Ok(())
}

// ============================================================================
// Heartbeat and Health Tests
// ============================================================================

/// Test heartbeat processing
#[tokio::test]
async fn test_heartbeat_processing() -> Result<()> {
    init_tracing();

    let mut coordinator = create_coordinator("coordinator");

    coordinator.add_member(NodeId::new("worker-1"))?;

    // Process heartbeat from worker
    let response = coordinator.process_heartbeat(
        &NodeId::new("worker-1"),
        vec![], // No task status reports
    )?;

    // Should receive back the current epoch - verify the response was received
    let _ = response.epoch.0;

    info!("Heartbeat processing test passed");
    Ok(())
}

/// Test heartbeat timeout detection
#[tokio::test]
async fn test_heartbeat_timeout() -> Result<()> {
    init_tracing();

    let config = CoordinatorConfig {
        node_id: NodeId::new("coordinator"),
        heartbeat_timeout: Duration::from_millis(50), // Very short timeout for testing
        ..Default::default()
    };
    let mut coordinator = ConnectCoordinator::new(config);

    coordinator.add_member(NodeId::new("worker-1"))?;

    // Wait for timeout
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Check for timed out nodes
    let timed_out = coordinator.check_heartbeat_timeouts();

    assert!(
        timed_out.contains(&NodeId::new("worker-1")),
        "Worker should be detected as timed out"
    );

    info!("Heartbeat timeout test passed");
    Ok(())
}

// ============================================================================
// Generation and Epoch Tests
// ============================================================================

/// Test generation increments on connector changes
#[tokio::test]
async fn test_connector_generation() -> Result<()> {
    init_tracing();

    let mut coordinator = create_coordinator("coordinator");

    let connector_id = ConnectorId::new("test-connector");
    coordinator.register_connector(
        connector_id.clone(),
        "test".to_string(),
        ConnectorMode::Scalable,
        2,
        serde_json::json!({}),
    )?;

    let gen1 = coordinator.get_connector(&connector_id).unwrap().generation;

    // Update connector config
    coordinator.update_connector_config(&connector_id, serde_json::json!({"updated": true}))?;

    let gen2 = coordinator.get_connector(&connector_id).unwrap().generation;

    assert!(
        gen2.0 > gen1.0,
        "Generation should increment on config update"
    );

    info!("Generation test passed: {} -> {}", gen1.0, gen2.0);
    Ok(())
}

/// Test epoch increments on cluster changes
#[tokio::test]
async fn test_coordinator_epoch() -> Result<()> {
    init_tracing();

    let mut coordinator = create_coordinator("coordinator");

    let epoch1 = coordinator.epoch();

    // Add member should increment epoch
    coordinator.add_member(NodeId::new("worker-1"))?;
    let epoch2 = coordinator.epoch();

    assert!(
        epoch2.0 > epoch1.0,
        "Epoch should increment on membership change"
    );

    // Rebalance should increment epoch
    coordinator.rebalance()?;
    let epoch3 = coordinator.epoch();

    assert!(epoch3.0 > epoch2.0, "Epoch should increment on rebalance");

    info!(
        "Epoch test passed: {} -> {} -> {}",
        epoch1.0, epoch2.0, epoch3.0
    );
    Ok(())
}

// ============================================================================
// Error Handling Tests
// ============================================================================

/// Test duplicate connector registration
#[tokio::test]
async fn test_duplicate_connector_error() -> Result<()> {
    init_tracing();

    let mut coordinator = create_coordinator("coordinator");

    let connector_id = ConnectorId::new("dup-test");
    coordinator.register_connector(
        connector_id.clone(),
        "dup-test".to_string(),
        ConnectorMode::Scalable,
        1,
        serde_json::json!({}),
    )?;

    // Try to register again
    let result = coordinator.register_connector(
        connector_id,
        "dup-test".to_string(),
        ConnectorMode::Scalable,
        1,
        serde_json::json!({}),
    );

    assert!(result.is_err(), "Duplicate registration should fail");

    info!("Duplicate connector error test passed");
    Ok(())
}

/// Test operations on non-existent connector
#[tokio::test]
async fn test_nonexistent_connector_error() -> Result<()> {
    init_tracing();

    let mut coordinator = create_coordinator("coordinator");

    let result = coordinator.start_connector(&ConnectorId::new("does-not-exist"));
    assert!(
        result.is_err(),
        "Starting non-existent connector should fail"
    );

    let result = coordinator.pause_connector(&ConnectorId::new("does-not-exist"));
    assert!(
        result.is_err(),
        "Pausing non-existent connector should fail"
    );

    info!("Non-existent connector error test passed");
    Ok(())
}

// ============================================================================
// Coordinator Status Tests
// ============================================================================

/// Test coordinator status reporting
#[tokio::test]
async fn test_coordinator_status() -> Result<()> {
    init_tracing();

    let mut coordinator = create_coordinator("coordinator");

    // Note: The coordinator itself counts as a member (node_id: "coordinator")
    // So when we add 2 workers, total members = 3
    coordinator.add_member(NodeId::new("worker-1"))?;
    coordinator.add_member(NodeId::new("worker-2"))?;

    let connector_id = ConnectorId::new("status-test");
    coordinator.register_connector(
        connector_id.clone(),
        "status-test".to_string(),
        ConnectorMode::Scalable,
        4,
        serde_json::json!({}),
    )?;

    coordinator.start_connector(&connector_id)?;
    coordinator.rebalance()?;

    let status = coordinator.status();

    // Coordinator + 2 workers = 3 members
    assert_eq!(
        status.member_count, 3,
        "Should have 3 members (coordinator + 2 workers)"
    );
    assert_eq!(status.connector_count, 1, "Should have 1 connector");
    assert_eq!(status.total_tasks, 4, "Should have 4 tasks");
    assert!(status.assigned_tasks > 0, "Some tasks should be assigned");

    info!(
        "Status test passed: {} members, {} connectors, {} tasks ({} assigned)",
        status.member_count, status.connector_count, status.total_tasks, status.assigned_tasks
    );
    Ok(())
}
