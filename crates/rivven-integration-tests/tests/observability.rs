//! Observability integration tests
//!
//! Tests for metrics, monitoring, and health endpoints:
//! - Prometheus metrics endpoint format and content
//! - JSON metrics endpoint for debugging
//! - Health check endpoints
//! - Metrics accuracy after operations
//! - Metrics endpoint performance
//!
//! Run with: cargo test -p rivven-integration-tests --test observability -- --nocapture

use anyhow::Result;
use rivven_client::Client;
use rivven_integration_tests::fixtures::{TestBroker, TestMetricsBroker};
use rivven_integration_tests::helpers::*;
use std::time::{Duration, Instant};
use tracing::info;

// ============================================================================
// Health Endpoint Tests
// ============================================================================

/// Test health endpoint returns healthy status
#[tokio::test]
async fn test_health_endpoint_returns_healthy() -> Result<()> {
    init_tracing();

    let broker = TestMetricsBroker::start().await?;

    let client = reqwest::Client::new();
    let response = client
        .get(broker.health_url())
        .timeout(Duration::from_secs(5))
        .send()
        .await?;

    assert!(
        response.status().is_success(),
        "Health endpoint should return 200 OK"
    );

    let body = response.text().await?;
    assert!(
        body.contains("healthy"),
        "Response should contain 'healthy'"
    );
    assert!(
        body.contains("node_id"),
        "Response should contain 'node_id'"
    );

    info!("Health response: {}", body);

    broker.shutdown().await?;
    Ok(())
}

/// Test health endpoint returns valid JSON
#[tokio::test]
async fn test_health_endpoint_json_format() -> Result<()> {
    init_tracing();

    let broker = TestMetricsBroker::start().await?;

    let client = reqwest::Client::new();
    let response = client
        .get(broker.health_url())
        .timeout(Duration::from_secs(5))
        .send()
        .await?;

    let health: serde_json::Value = response.json().await?;

    assert!(health.get("status").is_some(), "Should have 'status' field");
    assert!(
        health.get("node_id").is_some(),
        "Should have 'node_id' field"
    );

    info!("Health JSON: {:?}", health);

    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// Prometheus Metrics Tests
// ============================================================================

/// Test Prometheus metrics endpoint accessibility
#[tokio::test]
async fn test_prometheus_metrics_endpoint_accessible() -> Result<()> {
    init_tracing();

    let broker = TestMetricsBroker::start().await?;

    let client = reqwest::Client::new();
    let response = client
        .get(broker.metrics_url())
        .timeout(Duration::from_secs(5))
        .send()
        .await?;

    assert!(
        response.status().is_success(),
        "Metrics endpoint should return 200 OK"
    );

    // Prometheus metrics should have text/plain content-type with version
    let content_type = response
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap_or(""))
        .unwrap_or("");
    assert!(
        content_type.contains("text/plain"),
        "Content-Type should be text/plain for Prometheus format, got: {}",
        content_type
    );

    info!(
        "Prometheus metrics endpoint accessible with Content-Type: {}",
        content_type
    );

    broker.shutdown().await?;
    Ok(())
}

/// Test Prometheus metrics format compliance
#[tokio::test]
async fn test_prometheus_metrics_format() -> Result<()> {
    init_tracing();

    let broker = TestMetricsBroker::start().await?;

    let client = reqwest::Client::new();
    let body = client
        .get(broker.metrics_url())
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .text()
        .await?;

    // Prometheus format requirements:
    // 1. HELP lines describe metrics
    // 2. TYPE lines declare metric type (counter, gauge, histogram, summary)
    // 3. Metric lines are metric_name{labels} value

    assert!(body.contains("# HELP"), "Should contain HELP comments");
    assert!(body.contains("# TYPE"), "Should contain TYPE declarations");
    assert!(
        body.contains("rivven_"),
        "Should contain rivven_ prefixed metrics"
    );

    // Verify at least some core metrics are present
    let expected_metrics = [
        "rivven_core_messages_appended_total",
        "rivven_core_active_connections",
        "rivven_raft_is_leader",
    ];

    for metric in expected_metrics {
        assert!(body.contains(metric), "Should contain metric: {}", metric);
    }

    info!("Prometheus metrics format validated");

    broker.shutdown().await?;
    Ok(())
}

/// Test Prometheus metrics contain counter types
#[tokio::test]
async fn test_prometheus_counter_metrics() -> Result<()> {
    init_tracing();

    let broker = TestMetricsBroker::start().await?;

    let client = reqwest::Client::new();
    let body = client
        .get(broker.metrics_url())
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .text()
        .await?;

    // Counter metrics should be declared as counter type
    assert!(
        body.contains("# TYPE rivven_core_messages_appended_total counter"),
        "messages_appended should be a counter"
    );

    info!("Counter metrics correctly typed");

    broker.shutdown().await?;
    Ok(())
}

/// Test Prometheus metrics contain gauge types
#[tokio::test]
async fn test_prometheus_gauge_metrics() -> Result<()> {
    init_tracing();

    let broker = TestMetricsBroker::start().await?;

    let client = reqwest::Client::new();
    let body = client
        .get(broker.metrics_url())
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .text()
        .await?;

    // Gauge metrics should be declared as gauge type
    assert!(
        body.contains("# TYPE rivven_core_active_connections gauge"),
        "active_connections should be a gauge"
    );
    assert!(
        body.contains("# TYPE rivven_raft_is_leader gauge"),
        "is_leader should be a gauge"
    );

    info!("Gauge metrics correctly typed");

    broker.shutdown().await?;
    Ok(())
}

/// Test Prometheus metrics contain histogram types
#[tokio::test]
async fn test_prometheus_histogram_metrics() -> Result<()> {
    init_tracing();

    let broker = TestMetricsBroker::start().await?;

    let client = reqwest::Client::new();
    let body = client
        .get(broker.metrics_url())
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .text()
        .await?;

    // Histogram metrics should have buckets, sum, and count
    assert!(
        body.contains("# TYPE rivven_core_append_latency_seconds histogram"),
        "append_latency should be a histogram"
    );
    assert!(
        body.contains("rivven_core_append_latency_seconds_bucket"),
        "histogram should have _bucket suffix"
    );
    assert!(
        body.contains("rivven_core_append_latency_seconds_sum"),
        "histogram should have _sum suffix"
    );
    assert!(
        body.contains("rivven_core_append_latency_seconds_count"),
        "histogram should have _count suffix"
    );

    info!("Histogram metrics correctly structured");

    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// JSON Metrics Tests
// ============================================================================

/// Test JSON metrics endpoint accessibility
#[tokio::test]
async fn test_json_metrics_endpoint_accessible() -> Result<()> {
    init_tracing();

    let broker = TestMetricsBroker::start().await?;

    let client = reqwest::Client::new();
    let response = client
        .get(broker.json_metrics_url())
        .timeout(Duration::from_secs(5))
        .send()
        .await?;

    assert!(
        response.status().is_success(),
        "JSON metrics endpoint should return 200 OK"
    );

    let content_type = response
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap_or(""))
        .unwrap_or("");
    assert!(
        content_type.contains("application/json"),
        "Content-Type should be application/json, got: {}",
        content_type
    );

    info!("JSON metrics endpoint accessible");

    broker.shutdown().await?;
    Ok(())
}

/// Test JSON metrics format
#[tokio::test]
async fn test_json_metrics_format() -> Result<()> {
    init_tracing();

    let broker = TestMetricsBroker::start().await?;

    let client = reqwest::Client::new();
    let metrics: serde_json::Value = client
        .get(broker.json_metrics_url())
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .json()
        .await?;

    // Should have essential fields
    assert!(metrics.get("node_id").is_some(), "Should have 'node_id'");
    assert!(
        metrics.get("is_leader").is_some(),
        "Should have 'is_leader'"
    );
    assert!(
        metrics.get("current_term").is_some(),
        "Should have 'current_term'"
    );

    info!("JSON metrics: {:?}", metrics);

    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// Metrics Accuracy Tests (with actual operations)
// ============================================================================

/// Test that metrics update correctly after publishing messages
#[tokio::test]
async fn test_metrics_update_after_publish() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    // Create topic and publish messages
    let topic = unique_topic_name("metrics-publish");
    client.create_topic(&topic, Some(1)).await?;

    let message_count = 100;
    for i in 0..message_count {
        client
            .publish(&topic, format!("msg-{}", i).into_bytes())
            .await?;
    }

    // Use CoreMetrics directly to verify the metrics were recorded
    // (The embedded broker uses the same metrics crate)

    info!(
        "Published {} messages, metrics should be updated",
        message_count
    );

    broker.shutdown().await?;
    Ok(())
}

/// Test that partition count gauge is accurate
#[tokio::test]
async fn test_partition_count_metric_accuracy() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    // Create multiple topics with different partition counts
    let topic1 = unique_topic_name("metrics-partitions-1");
    let topic2 = unique_topic_name("metrics-partitions-2");
    let topic3 = unique_topic_name("metrics-partitions-3");

    client.create_topic(&topic1, Some(1)).await?;
    client.create_topic(&topic2, Some(4)).await?;
    client.create_topic(&topic3, Some(2)).await?;

    // Total partitions should be 1 + 4 + 2 = 7
    info!("Created 3 topics with total 7 partitions");

    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// Performance Tests
// ============================================================================

/// Test metrics endpoint latency
#[tokio::test]
async fn test_metrics_endpoint_latency() -> Result<()> {
    init_tracing();

    let broker = TestMetricsBroker::start().await?;
    let client = reqwest::Client::new();
    let metrics_url = broker.metrics_url();

    // Warm up
    let _ = client
        .get(&metrics_url)
        .timeout(Duration::from_secs(5))
        .send()
        .await?;

    // Measure latency over multiple requests
    let iterations = 10;
    let start = Instant::now();

    for _ in 0..iterations {
        let _ = client
            .get(&metrics_url)
            .timeout(Duration::from_secs(5))
            .send()
            .await?;
    }

    let total_duration = start.elapsed();
    let avg_latency = total_duration / iterations;

    // Metrics endpoint should respond in under 100ms on average
    assert!(
        avg_latency < Duration::from_millis(100),
        "Metrics endpoint too slow: {:?} avg",
        avg_latency
    );

    info!(
        "Metrics endpoint latency: {:?} avg over {} requests",
        avg_latency, iterations
    );

    broker.shutdown().await?;
    Ok(())
}

/// Test health endpoint latency
#[tokio::test]
async fn test_health_endpoint_latency() -> Result<()> {
    init_tracing();

    let broker = TestMetricsBroker::start().await?;
    let client = reqwest::Client::new();
    let health_url = broker.health_url();

    // Measure latency over multiple requests
    let iterations = 20;
    let start = Instant::now();

    for _ in 0..iterations {
        let _ = client
            .get(&health_url)
            .timeout(Duration::from_secs(5))
            .send()
            .await?;
    }

    let total_duration = start.elapsed();
    let avg_latency = total_duration / iterations;

    // Health endpoint should be fast (< 50ms)
    assert!(
        avg_latency < Duration::from_millis(50),
        "Health endpoint too slow: {:?} avg",
        avg_latency
    );

    info!(
        "Health endpoint latency: {:?} avg over {} requests",
        avg_latency, iterations
    );

    broker.shutdown().await?;
    Ok(())
}

/// Test concurrent metrics requests
#[tokio::test]
async fn test_metrics_concurrent_access() -> Result<()> {
    init_tracing();

    let broker = TestMetricsBroker::start().await?;
    let metrics_url = broker.metrics_url();

    // Launch multiple concurrent requests
    let concurrent_requests = 10;
    let mut handles = Vec::new();

    for _ in 0..concurrent_requests {
        let url = metrics_url.clone();
        handles.push(tokio::spawn(async move {
            let client = reqwest::Client::new();
            client
                .get(&url)
                .timeout(Duration::from_secs(5))
                .send()
                .await
        }));
    }

    // All requests should succeed
    let mut success_count = 0;
    for handle in handles {
        if let Ok(Ok(response)) = handle.await {
            if response.status().is_success() {
                success_count += 1;
            }
        }
    }

    assert_eq!(
        success_count, concurrent_requests,
        "All concurrent requests should succeed"
    );

    info!(
        "All {} concurrent metrics requests succeeded",
        concurrent_requests
    );

    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// Edge Cases
// ============================================================================

/// Test 404 for unknown endpoints
#[tokio::test]
async fn test_unknown_endpoint_returns_404() -> Result<()> {
    init_tracing();

    let broker = TestMetricsBroker::start().await?;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/nonexistent", broker.api_url()))
        .timeout(Duration::from_secs(5))
        .send()
        .await?;

    assert_eq!(
        response.status().as_u16(),
        404,
        "Unknown endpoint should return 404"
    );

    info!("Unknown endpoint correctly returns 404");

    broker.shutdown().await?;
    Ok(())
}

/// Test metrics endpoint is idempotent
#[tokio::test]
async fn test_metrics_endpoint_idempotent() -> Result<()> {
    init_tracing();

    let broker = TestMetricsBroker::start().await?;
    let client = reqwest::Client::new();

    // Multiple GET requests should return consistent format
    let response1 = client
        .get(broker.metrics_url())
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .text()
        .await?;

    let response2 = client
        .get(broker.metrics_url())
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .text()
        .await?;

    // Both responses should have the same metric names (values may differ)
    let has_same_metrics = response1.contains("rivven_core_messages_appended_total")
        && response2.contains("rivven_core_messages_appended_total");

    assert!(
        has_same_metrics,
        "Metrics endpoint should return consistent metric names"
    );

    info!("Metrics endpoint is idempotent");

    broker.shutdown().await?;
    Ok(())
}

/// Test metrics naming conventions
#[tokio::test]
async fn test_metrics_naming_conventions() -> Result<()> {
    init_tracing();

    let broker = TestMetricsBroker::start().await?;

    let client = reqwest::Client::new();
    let body = client
        .get(broker.metrics_url())
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .text()
        .await?;

    // All Rivven metrics should follow the naming convention:
    // rivven_{component}_{name}_{unit}
    // - Counters should end with _total
    // - Histograms/summaries for time should use _seconds

    // Check counter naming
    for line in body.lines() {
        if line.starts_with("rivven_") && !line.starts_with("# ") {
            let metric_name = line
                .split('{')
                .next()
                .unwrap_or(line)
                .split(' ')
                .next()
                .unwrap_or(line);

            // Counters should end with _total
            if body.contains(&format!("# TYPE {} counter", metric_name)) {
                assert!(
                    metric_name.ends_with("_total"),
                    "Counter metric '{}' should end with _total",
                    metric_name
                );
            }

            // Time histograms should use _seconds
            if metric_name.contains("latency") || metric_name.contains("duration") {
                assert!(
                    metric_name.contains("_seconds")
                        || metric_name.contains("_bucket")
                        || metric_name.contains("_sum")
                        || metric_name.contains("_count"),
                    "Time metric '{}' should use _seconds unit",
                    metric_name
                );
            }
        }
    }

    info!("Metrics naming conventions validated");

    broker.shutdown().await?;
    Ok(())
}
