//! Connect framework end-to-end tests
//!
//! Tests for complete connector pipelines:
//! - Source connector → Broker → Sink connector flows
//! - Connector lifecycle management
//! - Schema propagation through pipelines
//! - Error handling and recovery
//!
//! Run with: cargo test -p rivven-integration-tests --test connect_e2e -- --nocapture

use anyhow::Result;
use bytes::Bytes;
use rivven_client::Client;
use rivven_core::Config;
use rivven_integration_tests::fixtures::TestBroker;
use rivven_integration_tests::helpers::*;
use std::collections::HashMap;
use tracing::info;

/// Test simple source to broker flow
#[tokio::test]
async fn test_source_to_broker_basic() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("connect-source-basic");
    client.create_topic(&topic, Some(2)).await?;

    // Simulate a source connector producing data
    let records = vec![
        ("key1", r#"{"id": 1, "name": "Alice"}"#),
        ("key2", r#"{"id": 2, "name": "Bob"}"#),
        ("key3", r#"{"id": 3, "name": "Charlie"}"#),
    ];

    info!(
        "Simulating source connector producing {} records",
        records.len()
    );

    for (key, value) in &records {
        // In a real connector, this would come from the source system
        let msg = Bytes::from(format!("{}:{}", key, value).into_bytes());
        // Hash key to determine partition (simple simulation)
        let partition = (key.as_bytes()[key.len() - 1] as u32) % 2;
        client
            .publish_to_partition(&topic, partition, None::<Bytes>, msg)
            .await?;
    }

    // Verify data landed in broker
    let mut total = 0;
    for partition in 0..2 {
        let messages = client.consume(&topic, partition, 0, 100).await?;
        total += messages.len();
    }

    assert_eq!(total, records.len());
    info!(
        "Source to broker test passed: {} records transferred",
        total
    );

    broker.shutdown().await?;
    Ok(())
}

/// Test broker to sink flow
#[tokio::test]
async fn test_broker_to_sink_basic() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut producer = Client::connect(&broker.connection_string()).await?;
    let mut sink_client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("connect-sink-basic");
    producer.create_topic(&topic, Some(1)).await?;

    // Produce some records to the topic
    let records = vec![
        r#"{"id": 1, "action": "create"}"#,
        r#"{"id": 2, "action": "update"}"#,
        r#"{"id": 3, "action": "delete"}"#,
    ];

    for record in &records {
        producer
            .publish(&topic, Bytes::from(record.as_bytes().to_vec()))
            .await?;
    }

    info!(
        "Produced {} records, simulating sink connector",
        records.len()
    );

    // Simulate sink connector consuming and "writing" to destination
    let messages = sink_client.consume(&topic, 0, 0, 100).await?;

    // Simulate sink processing (validation)
    let mut sink_processed: Vec<serde_json::Value> = Vec::new();
    for msg in &messages {
        let parsed: serde_json::Value = serde_json::from_slice(&msg.value)?;
        sink_processed.push(parsed);
    }

    assert_eq!(sink_processed.len(), records.len());
    assert_eq!(sink_processed[0]["action"], "create");
    assert_eq!(sink_processed[1]["action"], "update");
    assert_eq!(sink_processed[2]["action"], "delete");

    info!(
        "Broker to sink test passed: {} records processed",
        sink_processed.len()
    );

    broker.shutdown().await?;
    Ok(())
}

/// Test full source → broker → sink pipeline
#[tokio::test]
async fn test_full_connect_pipeline() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let broker_addr = broker.connection_string();

    let source_topic = unique_topic_name("connect-pipeline-source");
    let sink_topic = unique_topic_name("connect-pipeline-sink");

    // Setup topics
    {
        let mut client = Client::connect(&broker_addr).await?;
        client.create_topic(&source_topic, Some(2)).await?;
        client.create_topic(&sink_topic, Some(2)).await?;
    }

    // Phase 1: Source connector produces data
    info!("Phase 1: Source connector producing data");
    {
        let mut source_client = Client::connect(&broker_addr).await?;

        for i in 0..100 {
            let record = serde_json::json!({
                "id": i,
                "timestamp": chrono::Utc::now().timestamp_millis(),
                "source": "source-connector",
                "data": format!("record-{}", i)
            });
            let partition = (i % 2) as u32;
            source_client
                .publish_to_partition(
                    &source_topic,
                    partition,
                    None::<Bytes>,
                    Bytes::from(record.to_string().into_bytes()),
                )
                .await?;
        }
        info!("Source connector produced 100 records");
    }

    // Phase 2: Stream processing (transformation)
    info!("Phase 2: Stream processing / transformation");
    {
        let mut reader = Client::connect(&broker_addr).await?;
        let mut writer = Client::connect(&broker_addr).await?;

        for partition in 0..2 {
            let messages = reader.consume(&source_topic, partition, 0, 100).await?;

            for msg in messages {
                // Transform the record
                let mut record: serde_json::Value = serde_json::from_slice(&msg.value)?;
                record["processed"] = serde_json::json!(true);
                record["processor"] = serde_json::json!("stream-processor");

                writer
                    .publish_to_partition(
                        &sink_topic,
                        partition,
                        None::<Bytes>,
                        Bytes::from(record.to_string().into_bytes()),
                    )
                    .await?;
            }
        }
        info!("Stream processor transformed all records");
    }

    // Phase 3: Sink connector consumes transformed data
    info!("Phase 3: Sink connector consuming transformed data");
    {
        let mut sink_client = Client::connect(&broker_addr).await?;
        let mut total_processed = 0;

        for partition in 0..2 {
            let messages = sink_client.consume(&sink_topic, partition, 0, 100).await?;

            for msg in &messages {
                let record: serde_json::Value = serde_json::from_slice(&msg.value)?;
                assert_eq!(record["processed"], true);
                assert_eq!(record["processor"], "stream-processor");
                total_processed += 1;
            }
        }

        assert_eq!(total_processed, 100);
        info!("Sink connector processed {} records", total_processed);
    }

    info!("Full connect pipeline test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test connector with multiple workers (fan-out)
#[tokio::test]
async fn test_connector_fan_out() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let broker_addr = broker.connection_string();

    let input_topic = unique_topic_name("connect-fanout-in");
    let output_topics = vec![
        unique_topic_name("connect-fanout-type-a"),
        unique_topic_name("connect-fanout-type-b"),
        unique_topic_name("connect-fanout-type-c"),
    ];

    // Setup
    {
        let mut client = Client::connect(&broker_addr).await?;
        client.create_topic(&input_topic, Some(1)).await?;
        for topic in &output_topics {
            client.create_topic(topic, Some(1)).await?;
        }
    }

    // Produce mixed type records
    info!("Producing mixed type records");
    {
        let mut client = Client::connect(&broker_addr).await?;
        let types = ["type-a", "type-b", "type-c"];

        for i in 0..30 {
            let record_type = types[i % 3];
            let record = serde_json::json!({
                "id": i,
                "type": record_type,
                "data": format!("data-{}", i)
            });
            client
                .publish(&input_topic, Bytes::from(record.to_string().into_bytes()))
                .await?;
        }
    }

    // Fan-out: Route records to appropriate topics based on type
    info!("Fanning out records by type");
    {
        let mut reader = Client::connect(&broker_addr).await?;
        let mut writer = Client::connect(&broker_addr).await?;

        let messages = reader.consume(&input_topic, 0, 0, 100).await?;

        for msg in messages {
            let record: serde_json::Value = serde_json::from_slice(&msg.value)?;
            let record_type = record["type"].as_str().unwrap_or("unknown");

            let target_topic = match record_type {
                "type-a" => &output_topics[0],
                "type-b" => &output_topics[1],
                "type-c" => &output_topics[2],
                _ => continue,
            };

            writer.publish(target_topic, msg.value.clone()).await?;
        }
    }

    // Verify fan-out
    info!("Verifying fan-out results");
    {
        let mut client = Client::connect(&broker_addr).await?;

        for (i, topic) in output_topics.iter().enumerate() {
            let messages = client.consume(topic, 0, 0, 100).await?;
            assert_eq!(messages.len(), 10, "Each type topic should have 10 records");

            // Verify all records in this topic have the correct type
            let expected_type = format!("type-{}", (b'a' + i as u8) as char);
            for msg in &messages {
                let record: serde_json::Value = serde_json::from_slice(&msg.value)?;
                assert_eq!(record["type"].as_str().unwrap(), expected_type);
            }
        }
    }

    info!("Connector fan-out test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test connector with aggregation (fan-in)
#[tokio::test]
async fn test_connector_fan_in() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let broker_addr = broker.connection_string();

    let input_topics = vec![
        unique_topic_name("connect-fanin-src1"),
        unique_topic_name("connect-fanin-src2"),
        unique_topic_name("connect-fanin-src3"),
    ];
    let output_topic = unique_topic_name("connect-fanin-out");

    // Setup
    {
        let mut client = Client::connect(&broker_addr).await?;
        for topic in &input_topics {
            client.create_topic(topic, Some(1)).await?;
        }
        client.create_topic(&output_topic, Some(1)).await?;
    }

    // Produce to multiple source topics
    info!("Producing to multiple source topics");
    {
        let mut client = Client::connect(&broker_addr).await?;

        for (i, topic) in input_topics.iter().enumerate() {
            for j in 0..10 {
                let record = serde_json::json!({
                    "source": format!("source-{}", i),
                    "id": j,
                    "data": format!("data-{}-{}", i, j)
                });
                client
                    .publish(topic, Bytes::from(record.to_string().into_bytes()))
                    .await?;
            }
        }
    }

    // Fan-in: Aggregate all sources into one topic
    info!("Aggregating from multiple sources");
    {
        let mut reader = Client::connect(&broker_addr).await?;
        let mut writer = Client::connect(&broker_addr).await?;

        for topic in &input_topics {
            let messages = reader.consume(topic, 0, 0, 100).await?;

            for msg in messages {
                // Add aggregation metadata
                let mut record: serde_json::Value = serde_json::from_slice(&msg.value)?;
                record["aggregated_from"] = serde_json::json!(topic);
                record["aggregated_at"] = serde_json::json!(chrono::Utc::now().timestamp_millis());

                writer
                    .publish(&output_topic, Bytes::from(record.to_string().into_bytes()))
                    .await?;
            }
        }
    }

    // Verify aggregation
    info!("Verifying aggregation results");
    {
        let mut client = Client::connect(&broker_addr).await?;
        let messages = client.consume(&output_topic, 0, 0, 100).await?;

        assert_eq!(messages.len(), 30, "Should have 30 aggregated records");

        // Verify sources are mixed
        let mut source_counts: HashMap<String, usize> = HashMap::new();
        for msg in &messages {
            let record: serde_json::Value = serde_json::from_slice(&msg.value)?;
            let source = record["source"].as_str().unwrap().to_string();
            *source_counts.entry(source).or_insert(0) += 1;
        }

        assert_eq!(source_counts.len(), 3, "Should have 3 different sources");
        for count in source_counts.values() {
            assert_eq!(*count, 10, "Each source should contribute 10 records");
        }
    }

    info!("Connector fan-in test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test connector error handling
#[tokio::test]
async fn test_connector_error_handling() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let broker_addr = broker.connection_string();

    let input_topic = unique_topic_name("connect-error-in");
    let output_topic = unique_topic_name("connect-error-out");
    let dlq_topic = unique_topic_name("connect-error-dlq"); // Dead letter queue

    // Setup
    {
        let mut client = Client::connect(&broker_addr).await?;
        client.create_topic(&input_topic, Some(1)).await?;
        client.create_topic(&output_topic, Some(1)).await?;
        client.create_topic(&dlq_topic, Some(1)).await?;
    }

    // Produce some valid and invalid records
    info!("Producing mix of valid and invalid records");
    {
        let mut client = Client::connect(&broker_addr).await?;

        // Valid JSON
        client
            .publish(&input_topic, Bytes::from(r#"{"id": 1, "valid": true}"#))
            .await?;
        // Invalid JSON
        client
            .publish(&input_topic, Bytes::from(b"not valid json".to_vec()))
            .await?;
        // Valid JSON
        client
            .publish(&input_topic, Bytes::from(r#"{"id": 2, "valid": true}"#))
            .await?;
        // Missing required field (simulated error)
        client
            .publish(&input_topic, Bytes::from(r#"{"invalid_schema": true}"#))
            .await?;
        // Valid JSON
        client
            .publish(&input_topic, Bytes::from(r#"{"id": 3, "valid": true}"#))
            .await?;
    }

    // Process with error handling
    info!("Processing with error handling (DLQ pattern)");
    let mut success_count = 0;
    let mut error_count = 0;
    {
        let mut reader = Client::connect(&broker_addr).await?;
        let mut writer = Client::connect(&broker_addr).await?;

        let messages = reader.consume(&input_topic, 0, 0, 100).await?;

        for msg in messages {
            // Try to process
            match serde_json::from_slice::<serde_json::Value>(&msg.value) {
                Ok(record) => {
                    // Check for required field
                    if record.get("id").is_some() {
                        // Success - write to output
                        writer.publish(&output_topic, msg.value.clone()).await?;
                        success_count += 1;
                    } else {
                        // Schema error - write to DLQ
                        let dlq_record = serde_json::json!({
                            "error": "missing required field: id",
                            "original": record
                        });
                        writer
                            .publish(&dlq_topic, Bytes::from(dlq_record.to_string().into_bytes()))
                            .await?;
                        error_count += 1;
                    }
                }
                Err(e) => {
                    // Parse error - write to DLQ
                    let dlq_record = serde_json::json!({
                        "error": format!("JSON parse error: {}", e),
                        "original_bytes": String::from_utf8_lossy(&msg.value)
                    });
                    writer
                        .publish(&dlq_topic, Bytes::from(dlq_record.to_string().into_bytes()))
                        .await?;
                    error_count += 1;
                }
            }
        }
    }

    // Verify results
    info!("Verifying error handling results");
    {
        let mut client = Client::connect(&broker_addr).await?;

        let output_msgs = client.consume(&output_topic, 0, 0, 100).await?;
        let dlq_msgs = client.consume(&dlq_topic, 0, 0, 100).await?;

        assert_eq!(output_msgs.len(), 3, "Should have 3 successful records");
        assert_eq!(dlq_msgs.len(), 2, "Should have 2 DLQ records");
        assert_eq!(success_count, 3);
        assert_eq!(error_count, 2);
    }

    info!(
        "Connector error handling test passed: {} success, {} errors",
        success_count, error_count
    );
    broker.shutdown().await?;
    Ok(())
}

/// Test connector with schema registry
#[tokio::test]
async fn test_connector_with_schema() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("connect-schema");
    client.create_topic(&topic, Some(1)).await?;

    // Define expected schema
    let _schema = serde_json::json!({
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": "string"}
        ]
    });

    info!("Testing connector with schema validation");

    // Produce schema-compliant records
    let valid_records = vec![
        serde_json::json!({"id": 1, "name": "Alice", "email": "alice@example.com"}),
        serde_json::json!({"id": 2, "name": "Bob", "email": "bob@example.com"}),
        serde_json::json!({"id": 3, "name": "Charlie", "email": "charlie@example.com"}),
    ];

    for record in &valid_records {
        // Validate against schema (simplified)
        assert!(record.get("id").and_then(|v| v.as_i64()).is_some());
        assert!(record.get("name").and_then(|v| v.as_str()).is_some());
        assert!(record.get("email").and_then(|v| v.as_str()).is_some());

        client
            .publish(&topic, Bytes::from(record.to_string().into_bytes()))
            .await?;
    }

    // Consume and validate
    let messages = client.consume(&topic, 0, 0, 100).await?;
    assert_eq!(messages.len(), valid_records.len());

    for (i, msg) in messages.iter().enumerate() {
        let record: serde_json::Value = serde_json::from_slice(&msg.value)?;
        assert_eq!(record["id"], valid_records[i]["id"]);
        assert_eq!(record["name"], valid_records[i]["name"]);
        assert_eq!(record["email"], valid_records[i]["email"]);
    }

    info!("Connector with schema test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test connector retry and recovery
#[tokio::test]
async fn test_connector_retry_recovery() -> Result<()> {
    init_tracing();

    let config = Config {
        enable_persistence: true,
        data_dir: format!("/tmp/rivven-connect-retry-{}", uuid::Uuid::new_v4()),
        ..Default::default()
    };

    let topic = unique_topic_name("connect-retry");

    // Phase 1: Produce messages
    info!("Phase 1: Source connector producing messages");
    {
        let broker = TestBroker::start_with_config(config.clone()).await?;
        let mut client = Client::connect(&broker.connection_string()).await?;

        client.create_topic(&topic, Some(1)).await?;

        for i in 0..50 {
            let record = serde_json::json!({
                "id": i,
                "data": format!("retry-test-{}", i)
            });
            client
                .publish(&topic, Bytes::from(record.to_string().into_bytes()))
                .await?;
        }

        info!("Produced 50 records");
        broker.shutdown().await?;
    }

    // Phase 2: Simulate connector consuming partially then "crashing"
    let consumed_offset = 20u64;
    info!("Phase 2: Simulating partial consumption (20 records) before crash");
    {
        let broker = TestBroker::start_with_config(config.clone()).await?;
        let mut client = Client::connect(&broker.connection_string()).await?;

        let messages = client.consume(&topic, 0, 0, 20).await?;
        assert_eq!(messages.len(), 20);

        info!("Consumed {} records, simulating crash", messages.len());
        // No commit - simulate crash
        broker.shutdown().await?;
    }

    // Phase 3: Connector restarts and retries from checkpoint
    info!("Phase 3: Connector retry from last checkpoint");
    {
        let broker = TestBroker::start_with_config(config.clone()).await?;
        let mut client = Client::connect(&broker.connection_string()).await?;

        // Resume from committed offset
        let messages = client.consume(&topic, 0, consumed_offset, 100).await?;
        assert_eq!(messages.len(), 30, "Should consume remaining 30 records");

        // Verify first message is at expected offset
        let first: serde_json::Value = serde_json::from_slice(&messages[0].value)?;
        assert_eq!(first["id"], 20);

        info!("Successfully resumed from offset {}", consumed_offset);
        broker.shutdown().await?;
    }

    info!("Connector retry recovery test passed");
    let _ = tokio::fs::remove_dir_all(&config.data_dir).await;
    Ok(())
}

/// Test connector batching
#[tokio::test]
async fn test_connector_batching() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("connect-batch");
    client.create_topic(&topic, Some(1)).await?;

    let total_records = 1000;
    let batch_size = 100;

    info!(
        "Testing connector batching: {} records in batches of {}",
        total_records, batch_size
    );

    // Produce in batches (simulating efficient connector)
    let start = std::time::Instant::now();
    for batch in 0..(total_records / batch_size) {
        for i in 0..batch_size {
            let record_id = batch * batch_size + i;
            let record = serde_json::json!({
                "id": record_id,
                "batch": batch
            });
            client
                .publish(&topic, Bytes::from(record.to_string().into_bytes()))
                .await?;
        }
    }
    let produce_time = start.elapsed();
    info!("Produced {} records in {:?}", total_records, produce_time);

    // Consume in batches
    let start = std::time::Instant::now();
    let mut consumed = 0;
    let mut offset = 0u64;

    while consumed < total_records {
        let messages = client.consume(&topic, 0, offset, batch_size).await?;
        if messages.is_empty() {
            break;
        }
        consumed += messages.len();
        offset = messages.last().unwrap().offset + 1;
    }

    let consume_time = start.elapsed();
    info!("Consumed {} records in {:?}", consumed, consume_time);

    assert_eq!(consumed, total_records);

    info!("Connector batching test passed");
    broker.shutdown().await?;
    Ok(())
}
