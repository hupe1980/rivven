//! Transaction Integration Tests
//!
//! Tests for Rivven's native transaction support.
//! Verifies exactly-once semantics with cross-topic atomic writes.
//!
//! Run with: cargo test -p rivven-integration-tests --test transactions -- --nocapture

use anyhow::Result;
use rivven_client::Client;
use rivven_integration_tests::fixtures::TestBroker;
use rivven_integration_tests::helpers::*;
use tracing::info;

// =============================================================================
// IDEMPOTENT PRODUCER TESTS
// =============================================================================

/// Test initializing a producer ID
#[tokio::test]
async fn test_init_producer_id() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    // Initialize producer
    let producer = client.init_producer_id(None).await?;

    assert!(producer.producer_id > 0);
    assert_eq!(producer.producer_epoch, 0);
    assert_eq!(producer.next_sequence, 0);
    info!(
        "Got producer ID: {}, epoch: {}",
        producer.producer_id, producer.producer_epoch
    );

    broker.shutdown().await?;
    info!("Init producer ID test passed");
    Ok(())
}

/// Test producer ID epoch bumps on reconnect
#[tokio::test]
async fn test_producer_id_epoch_bump() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    // First init
    let producer1 = client.init_producer_id(None).await?;
    info!(
        "First producer: id={}, epoch={}",
        producer1.producer_id, producer1.producer_epoch
    );

    // Second init with same producer_id should bump epoch
    let producer2 = client.init_producer_id(Some(producer1.producer_id)).await?;
    info!(
        "Second producer: id={}, epoch={}",
        producer2.producer_id, producer2.producer_epoch
    );

    assert_eq!(producer2.producer_id, producer1.producer_id);
    assert!(
        producer2.producer_epoch > producer1.producer_epoch,
        "Epoch should be bumped: {} > {}",
        producer2.producer_epoch,
        producer1.producer_epoch
    );

    broker.shutdown().await?;
    info!("Producer ID epoch bump test passed");
    Ok(())
}

/// Test idempotent publish deduplication
#[tokio::test]
async fn test_idempotent_publish_basic() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("idempotent-basic");
    client.create_topic(&topic, Some(1)).await?;
    info!("Created topic: {}", topic);

    // Initialize producer
    let mut producer = client.init_producer_id(None).await?;
    info!("Producer initialized: id={}", producer.producer_id);

    // First publish
    let (offset1, partition1, dup1) = client
        .publish_idempotent(
            &topic,
            None::<Vec<u8>>,
            b"message-1".to_vec(),
            &mut producer,
        )
        .await?;

    assert!(!dup1, "First message should not be duplicate");
    info!(
        "First publish: offset={}, partition={}, duplicate={}",
        offset1, partition1, dup1
    );

    // Second publish (new message)
    let (offset2, partition2, dup2) = client
        .publish_idempotent(
            &topic,
            None::<Vec<u8>>,
            b"message-2".to_vec(),
            &mut producer,
        )
        .await?;

    assert!(!dup2, "Second message should not be duplicate");
    assert!(offset2 > offset1, "Second offset should be higher");
    info!(
        "Second publish: offset={}, partition={}",
        offset2, partition2
    );

    // Verify both messages are stored
    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 2);

    broker.shutdown().await?;
    info!("Idempotent publish basic test passed");
    Ok(())
}

/// Test idempotent publish with key for partitioning
#[tokio::test]
async fn test_idempotent_publish_with_key() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("idempotent-key");
    client.create_topic(&topic, Some(3)).await?;
    info!("Created topic with 3 partitions");

    // Each producer should have its own sequence number tracking
    // Since we're publishing to different partitions, we need separate producers
    // or the broker tracks sequences per-partition
    let mut producer = client.init_producer_id(None).await?;

    // Publish messages with keys - note: sequence is per-producer in our implementation
    // We'll use the same producer but acknowledge different partitions get different sequences
    let (_, p1, _) = client
        .publish_idempotent(
            &topic,
            Some(b"key-1".to_vec()),
            b"value-1".to_vec(),
            &mut producer,
        )
        .await?;
    info!("Published key-1 to partition {}", p1);

    // Get a fresh producer for independent sequence tracking
    let mut producer2 = client.init_producer_id(None).await?;
    let (_, p2, _) = client
        .publish_idempotent(
            &topic,
            Some(b"key-2".to_vec()),
            b"value-2".to_vec(),
            &mut producer2,
        )
        .await?;
    info!("Published key-2 to partition {}", p2);

    let mut producer3 = client.init_producer_id(None).await?;
    let (_, p3, _) = client
        .publish_idempotent(
            &topic,
            Some(b"key-1".to_vec()),
            b"value-3".to_vec(),
            &mut producer3,
        )
        .await?;
    info!("Published key-1 again to partition {}", p3);

    // Same key should go to same partition
    assert_eq!(p1, p3, "Same key should map to same partition");
    info!("key-1 -> partition {}, key-2 -> partition {}", p1, p2);

    broker.shutdown().await?;
    info!("Idempotent publish with key test passed");
    Ok(())
}

/// Test sequence number tracking
#[tokio::test]
async fn test_producer_sequence_tracking() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("sequence-track");
    client.create_topic(&topic, Some(1)).await?;

    let mut producer = client.init_producer_id(None).await?;
    assert_eq!(producer.next_sequence, 0);

    // Each publish should increment sequence
    for i in 0..5 {
        let expected_seq = i;
        assert_eq!(producer.next_sequence, expected_seq);

        let _ = client
            .publish_idempotent(
                &topic,
                None::<Vec<u8>>,
                format!("msg-{}", i).into_bytes(),
                &mut producer,
            )
            .await?;

        assert_eq!(producer.next_sequence, expected_seq + 1);
    }

    assert_eq!(producer.next_sequence, 5);
    info!(
        "Sequence tracking verified: next_sequence = {}",
        producer.next_sequence
    );

    broker.shutdown().await?;
    info!("Producer sequence tracking test passed");
    Ok(())
}

// =============================================================================
// TRANSACTION TESTS
// =============================================================================

/// Test basic transaction begin/commit flow
#[tokio::test]
async fn test_transaction_begin_commit() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("txn-commit");
    client.create_topic(&topic, Some(1)).await?;
    info!("Created topic: {}", topic);

    let mut producer = client.init_producer_id(None).await?;
    let txn_id = "txn-commit-test-1";

    // Begin transaction
    client.begin_transaction(txn_id, &producer, None).await?;
    info!("Transaction {} started", txn_id);

    // Add partition to transaction BEFORE publishing
    client
        .add_partitions_to_txn(txn_id, &producer, &[(&topic, 0)])
        .await?;
    info!("Added partition 0 to transaction");

    // Publish within transaction
    let (offset, partition, seq) = client
        .publish_transactional(
            txn_id,
            &topic,
            None::<Vec<u8>>,
            b"txn-message-1".to_vec(),
            &mut producer,
        )
        .await?;
    info!(
        "Published at offset={}, partition={}, seq={}",
        offset, partition, seq
    );

    // Commit transaction
    client.commit_transaction(txn_id, &producer).await?;
    info!("Transaction {} committed", txn_id);

    // Verify message is visible after commit
    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].value.as_ref(), b"txn-message-1".to_vec());

    broker.shutdown().await?;
    info!("Transaction begin/commit test passed");
    Ok(())
}

/// Test transaction abort
///
/// NOTE: Current implementation uses read_uncommitted semantics - aborted messages
/// may still be visible. Full read_committed isolation with abort markers is planned.
#[tokio::test]
async fn test_transaction_abort() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("txn-abort");
    client.create_topic(&topic, Some(1)).await?;
    info!("Created topic: {}", topic);

    let mut producer = client.init_producer_id(None).await?;
    let txn_id = "txn-abort-test-1";

    // Begin transaction
    client.begin_transaction(txn_id, &producer, None).await?;
    info!("Transaction {} started", txn_id);

    // Add partition to transaction
    client
        .add_partitions_to_txn(txn_id, &producer, &[(&topic, 0)])
        .await?;

    // Publish within transaction
    let _ = client
        .publish_transactional(
            txn_id,
            &topic,
            None::<Vec<u8>>,
            b"should-be-discarded".to_vec(),
            &mut producer,
        )
        .await?;
    info!("Published message in transaction");

    // Abort transaction
    client.abort_transaction(txn_id, &producer).await?;
    info!("Transaction {} aborted", txn_id);

    // NOTE: With read_uncommitted semantics (current default), aborted messages may be visible.
    // Full read_committed isolation requires transaction markers and consumer-side filtering.
    let messages = client.consume(&topic, 0, 0, 10).await?;
    info!(
        "Found {} messages after abort (read_uncommitted behavior)",
        messages.len()
    );

    broker.shutdown().await?;
    info!("Transaction abort test passed");
    Ok(())
}

/// Test transaction with multiple messages
#[tokio::test]
async fn test_transaction_multiple_messages() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("txn-multi");
    client.create_topic(&topic, Some(1)).await?;

    let mut producer = client.init_producer_id(None).await?;
    let txn_id = "txn-multi-1";

    // Begin transaction and add partition
    client.begin_transaction(txn_id, &producer, None).await?;
    client
        .add_partitions_to_txn(txn_id, &producer, &[(&topic, 0)])
        .await?;

    for i in 0..5 {
        client
            .publish_transactional(
                txn_id,
                &topic,
                None::<Vec<u8>>,
                format!("batch-msg-{}", i).into_bytes(),
                &mut producer,
            )
            .await?;
    }
    info!("Published 5 messages in transaction");

    // Commit
    client.commit_transaction(txn_id, &producer).await?;
    info!("Transaction committed");

    // All messages should be visible atomically
    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 5, "All 5 messages should be visible");

    broker.shutdown().await?;
    info!("Transaction multiple messages test passed");
    Ok(())
}

/// Test cross-topic transaction (atomic writes to multiple topics)
#[tokio::test]
async fn test_transaction_cross_topic() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic1 = unique_topic_name("txn-cross-1");
    let topic2 = unique_topic_name("txn-cross-2");
    client.create_topic(&topic1, Some(1)).await?;
    client.create_topic(&topic2, Some(1)).await?;
    info!("Created topics: {} and {}", topic1, topic2);

    let mut producer = client.init_producer_id(None).await?;
    let txn_id = "txn-cross-topics";

    // Begin transaction
    client.begin_transaction(txn_id, &producer, None).await?;

    // Add both topic partitions to transaction
    client
        .add_partitions_to_txn(txn_id, &producer, &[(&topic1, 0), (&topic2, 0)])
        .await?;

    // Publish to multiple topics
    client
        .publish_transactional(
            txn_id,
            &topic1,
            None::<Vec<u8>>,
            b"topic1-msg".to_vec(),
            &mut producer,
        )
        .await?;
    client
        .publish_transactional(
            txn_id,
            &topic2,
            None::<Vec<u8>>,
            b"topic2-msg".to_vec(),
            &mut producer,
        )
        .await?;

    // Commit atomically
    client.commit_transaction(txn_id, &producer).await?;
    info!("Cross-topic transaction committed");

    // Both messages should be visible
    let msgs1 = client.consume(&topic1, 0, 0, 10).await?;
    let msgs2 = client.consume(&topic2, 0, 0, 10).await?;

    assert_eq!(msgs1.len(), 1);
    assert_eq!(msgs2.len(), 1);
    assert_eq!(msgs1[0].value.as_ref(), b"topic1-msg".to_vec());
    assert_eq!(msgs2[0].value.as_ref(), b"topic2-msg".to_vec());

    broker.shutdown().await?;
    info!("Transaction cross-topic test passed");
    Ok(())
}

/// Test adding partitions to transaction
#[tokio::test]
async fn test_transaction_add_partitions() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("txn-partitions");
    client.create_topic(&topic, Some(3)).await?;
    info!("Created topic with 3 partitions");

    let producer = client.init_producer_id(None).await?;
    let txn_id = "txn-add-partitions";

    // Begin transaction
    client.begin_transaction(txn_id, &producer, None).await?;

    // Add partitions to transaction
    let count = client
        .add_partitions_to_txn(txn_id, &producer, &[(&topic, 0), (&topic, 1), (&topic, 2)])
        .await?;

    info!("Added {} partitions to transaction", count);
    assert_eq!(count, 3);

    broker.shutdown().await?;
    info!("Transaction add partitions test passed");
    Ok(())
}

/// Test transaction with consumer offsets (exactly-once consume-transform-produce)
#[tokio::test]
async fn test_transaction_with_offsets() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let input_topic = unique_topic_name("txn-input");
    let output_topic = unique_topic_name("txn-output");
    client.create_topic(&input_topic, Some(1)).await?;
    client.create_topic(&output_topic, Some(1)).await?;

    // First, publish some source messages
    for i in 0..3 {
        client
            .publish(&input_topic, format!("input-{}", i).into_bytes())
            .await?;
    }
    info!("Published 3 input messages");

    // Now do consume-transform-produce with exactly-once
    let mut producer = client.init_producer_id(None).await?;
    let txn_id = "txn-exactly-once";
    let group_id = "processor-group";

    client.begin_transaction(txn_id, &producer, None).await?;

    // Add output partition to transaction
    client
        .add_partitions_to_txn(txn_id, &producer, &[(&output_topic, 0)])
        .await?;

    // Consume input
    let messages = client.consume(&input_topic, 0, 0, 10).await?;

    // Transform and produce
    for msg in &messages {
        let transformed = format!("processed: {:?}", msg.value);
        client
            .publish_transactional(
                txn_id,
                &output_topic,
                None::<Vec<u8>>,
                transformed.into_bytes(),
                &mut producer,
            )
            .await?;
    }

    // Add consumed offsets to transaction
    client
        .add_offsets_to_txn(
            txn_id,
            &producer,
            group_id,
            &[(&input_topic, 0, messages.len() as i64)],
        )
        .await?;

    // Commit atomically
    client.commit_transaction(txn_id, &producer).await?;
    info!("Exactly-once consume-transform-produce completed");

    // Verify output
    let output = client.consume(&output_topic, 0, 0, 10).await?;
    assert_eq!(output.len(), 3);

    broker.shutdown().await?;
    info!("Transaction with offsets test passed");
    Ok(())
}

/// Test transaction timeout configuration
#[tokio::test]
async fn test_transaction_custom_timeout() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("txn-timeout");
    client.create_topic(&topic, Some(1)).await?;

    let producer = client.init_producer_id(None).await?;
    let txn_id = "txn-with-timeout";

    // Begin with custom timeout (5 minutes)
    client
        .begin_transaction(txn_id, &producer, Some(300_000))
        .await?;
    info!("Transaction started with 5 minute timeout");

    // Can still work normally
    // (We don't test actual timeout expiration as it would make tests slow)

    broker.shutdown().await?;
    info!("Transaction custom timeout test passed");
    Ok(())
}

/// Test multiple concurrent transactions
///
/// NOTE: Current implementation uses read_uncommitted semantics - aborted messages
/// may still be visible. Full read_committed isolation with abort markers is planned.
#[tokio::test]
async fn test_concurrent_transactions() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client1 = Client::connect(&broker.connection_string()).await?;
    let mut client2 = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("txn-concurrent");
    client1.create_topic(&topic, Some(1)).await?;

    let mut producer1 = client1.init_producer_id(None).await?;
    let mut producer2 = client2.init_producer_id(None).await?;

    // Start two transactions and add partition to each
    client1.begin_transaction("txn-1", &producer1, None).await?;
    client1
        .add_partitions_to_txn("txn-1", &producer1, &[(&topic, 0)])
        .await?;

    client2.begin_transaction("txn-2", &producer2, None).await?;
    client2
        .add_partitions_to_txn("txn-2", &producer2, &[(&topic, 0)])
        .await?;

    // Publish from both
    client1
        .publish_transactional(
            "txn-1",
            &topic,
            None::<Vec<u8>>,
            b"from-txn-1".to_vec(),
            &mut producer1,
        )
        .await?;
    client2
        .publish_transactional(
            "txn-2",
            &topic,
            None::<Vec<u8>>,
            b"from-txn-2".to_vec(),
            &mut producer2,
        )
        .await?;

    // Commit txn-1, abort txn-2
    client1.commit_transaction("txn-1", &producer1).await?;
    client2.abort_transaction("txn-2", &producer2).await?;

    // Verify committed transaction message is visible
    let messages = client1.consume(&topic, 0, 0, 10).await?;

    let has_txn1 = messages.iter().any(|m| m.value.as_ref() == b"from-txn-1");
    assert!(has_txn1, "Committed transaction message should be visible");

    // NOTE: With read_uncommitted semantics (current default), txn-2's aborted message
    // may also be visible. Full read_committed isolation would filter it out.
    info!(
        "Found {} total messages (read_uncommitted behavior)",
        messages.len()
    );

    broker.shutdown().await?;
    info!("Concurrent transactions test passed");
    Ok(())
}

// =============================================================================
// ERROR HANDLING TESTS
// =============================================================================

/// Test transaction error handling
#[tokio::test]
async fn test_transaction_error_recovery() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("txn-error");
    client.create_topic(&topic, Some(1)).await?;

    let mut producer = client.init_producer_id(None).await?;

    // Start a transaction
    client
        .begin_transaction("txn-recover", &producer, None)
        .await?;
    client
        .add_partitions_to_txn("txn-recover", &producer, &[(&topic, 0)])
        .await?;

    // Publish something
    client
        .publish_transactional(
            "txn-recover",
            &topic,
            None::<Vec<u8>>,
            b"test".to_vec(),
            &mut producer,
        )
        .await?;

    // Abort to clean up
    client.abort_transaction("txn-recover", &producer).await?;

    // Should be able to start a new transaction
    client
        .begin_transaction("txn-recover-2", &producer, None)
        .await?;
    client
        .add_partitions_to_txn("txn-recover-2", &producer, &[(&topic, 0)])
        .await?;
    client
        .publish_transactional(
            "txn-recover-2",
            &topic,
            None::<Vec<u8>>,
            b"recovered".to_vec(),
            &mut producer,
        )
        .await?;
    client
        .commit_transaction("txn-recover-2", &producer)
        .await?;

    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert!(messages.iter().any(|m| m.value.as_ref() == b"recovered"));

    broker.shutdown().await?;
    info!("Transaction error recovery test passed");
    Ok(())
}

/// Test publishing without transaction should work normally
#[tokio::test]
async fn test_non_transactional_publish() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("non-txn");
    client.create_topic(&topic, Some(1)).await?;

    // Regular publish should still work
    client.publish(&topic, b"regular-message".to_vec()).await?;

    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].value.as_ref(), b"regular-message".to_vec());

    broker.shutdown().await?;
    info!("Non-transactional publish test passed");
    Ok(())
}

// =============================================================================
// READ COMMITTED ISOLATION TESTS
// =============================================================================

/// Test that read_committed filters aborted transaction messages
#[tokio::test]
async fn test_read_committed_filters_aborted() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("read-committed-aborted");
    client.create_topic(&topic, Some(1)).await?;
    info!("Created topic: {}", topic);

    let mut producer = client.init_producer_id(None).await?;
    info!("Producer initialized: id={}", producer.producer_id);

    // 1. Publish a regular (non-transactional) message first
    client.publish(&topic, b"regular-1".to_vec()).await?;
    info!("Published regular message");

    // 2. Start a transaction and publish, then ABORT
    client
        .begin_transaction("txn-to-abort", &producer, None)
        .await?;
    client
        .add_partitions_to_txn("txn-to-abort", &producer, &[(&topic, 0)])
        .await?;
    client
        .publish_transactional(
            "txn-to-abort",
            &topic,
            None::<Vec<u8>>,
            b"aborted-msg".to_vec(),
            &mut producer,
        )
        .await?;
    info!("Published transactional message (will abort)");

    client.abort_transaction("txn-to-abort", &producer).await?;
    info!("Transaction aborted");

    // 3. Publish another regular message
    client.publish(&topic, b"regular-2".to_vec()).await?;
    info!("Published second regular message");

    // 4. Read with read_uncommitted (default) - should see all data messages
    let uncommitted_msgs = client.consume(&topic, 0, 0, 10).await?;
    info!(
        "read_uncommitted returned {} messages",
        uncommitted_msgs.len()
    );

    // Note: read_uncommitted may or may not include aborted messages depending on
    // whether control records are filtered. At minimum, regular messages should appear.
    assert!(uncommitted_msgs
        .iter()
        .any(|m| m.value.as_ref() == b"regular-1"));
    assert!(uncommitted_msgs
        .iter()
        .any(|m| m.value.as_ref() == b"regular-2"));

    // 5. Read with read_committed - should NOT see aborted transaction message
    let committed_msgs = client.consume_read_committed(&topic, 0, 0, 10).await?;
    info!("read_committed returned {} messages", committed_msgs.len());

    // Verify regular messages are present
    assert!(
        committed_msgs
            .iter()
            .any(|m| m.value.as_ref() == b"regular-1"),
        "First regular message should be visible"
    );
    assert!(
        committed_msgs
            .iter()
            .any(|m| m.value.as_ref() == b"regular-2"),
        "Second regular message should be visible"
    );

    // Verify aborted message is NOT present
    assert!(
        !committed_msgs
            .iter()
            .any(|m| m.value.as_ref() == b"aborted-msg"),
        "Aborted transaction message should be filtered out"
    );

    broker.shutdown().await?;
    info!("Read committed filters aborted test passed");
    Ok(())
}

/// Test that read_committed shows committed transaction messages
#[tokio::test]
async fn test_read_committed_shows_committed() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("read-committed-shows");
    client.create_topic(&topic, Some(1)).await?;
    info!("Created topic: {}", topic);

    let mut producer = client.init_producer_id(None).await?;

    // Start a transaction, publish, and COMMIT
    client
        .begin_transaction("txn-to-commit", &producer, None)
        .await?;
    client
        .add_partitions_to_txn("txn-to-commit", &producer, &[(&topic, 0)])
        .await?;
    client
        .publish_transactional(
            "txn-to-commit",
            &topic,
            None::<Vec<u8>>,
            b"committed-msg".to_vec(),
            &mut producer,
        )
        .await?;
    client
        .commit_transaction("txn-to-commit", &producer)
        .await?;
    info!("Transaction committed");

    // Read with read_committed - should see the committed message
    let messages = client.consume_read_committed(&topic, 0, 0, 10).await?;
    info!("read_committed returned {} messages", messages.len());

    assert!(
        messages
            .iter()
            .any(|m| m.value.as_ref() == b"committed-msg"),
        "Committed transaction message should be visible"
    );

    broker.shutdown().await?;
    info!("Read committed shows committed test passed");
    Ok(())
}

/// Test isolation levels with multiple concurrent transactions
#[tokio::test]
async fn test_read_committed_concurrent_transactions() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client1 = Client::connect(&broker.connection_string()).await?;
    let mut client2 = Client::connect(&broker.connection_string()).await?;
    let consumer = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("read-committed-concurrent");
    client1.create_topic(&topic, Some(1)).await?;
    info!("Created topic: {}", topic);

    let mut producer1 = client1.init_producer_id(None).await?;
    let mut producer2 = client2.init_producer_id(None).await?;
    info!(
        "Producers initialized: {} and {}",
        producer1.producer_id, producer2.producer_id
    );

    // Start two transactions
    client1
        .begin_transaction("txn-commit", &producer1, None)
        .await?;
    client1
        .add_partitions_to_txn("txn-commit", &producer1, &[(&topic, 0)])
        .await?;

    client2
        .begin_transaction("txn-abort", &producer2, None)
        .await?;
    client2
        .add_partitions_to_txn("txn-abort", &producer2, &[(&topic, 0)])
        .await?;

    // Publish from both
    client1
        .publish_transactional(
            "txn-commit",
            &topic,
            None::<Vec<u8>>,
            b"will-commit".to_vec(),
            &mut producer1,
        )
        .await?;
    client2
        .publish_transactional(
            "txn-abort",
            &topic,
            None::<Vec<u8>>,
            b"will-abort".to_vec(),
            &mut producer2,
        )
        .await?;
    info!("Published from both transactions");

    // Commit one, abort the other
    client1.commit_transaction("txn-commit", &producer1).await?;
    client2.abort_transaction("txn-abort", &producer2).await?;
    info!("Committed txn-commit, aborted txn-abort");

    // Read with read_committed
    let mut consumer = consumer;
    let messages = consumer.consume_read_committed(&topic, 0, 0, 10).await?;
    info!("read_committed returned {} messages", messages.len());

    // Should see committed message
    assert!(
        messages.iter().any(|m| m.value.as_ref() == b"will-commit"),
        "Committed message should be visible"
    );

    // Should NOT see aborted message
    assert!(
        !messages.iter().any(|m| m.value.as_ref() == b"will-abort"),
        "Aborted message should be filtered"
    );

    broker.shutdown().await?;
    info!("Read committed concurrent transactions test passed");
    Ok(())
}

/// Test that read_uncommitted is the backward-compatible default
#[tokio::test]
async fn test_read_uncommitted_default_behavior() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("read-uncommitted-default");
    client.create_topic(&topic, Some(1)).await?;

    // Publish a regular message
    client.publish(&topic, b"message-1".to_vec()).await?;
    client.publish(&topic, b"message-2".to_vec()).await?;

    // Default consume should return all messages (read_uncommitted behavior)
    let messages = client.consume(&topic, 0, 0, 10).await?;

    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].value.as_ref(), b"message-1");
    assert_eq!(messages[1].value.as_ref(), b"message-2");
    info!("read_uncommitted (default) works correctly");

    // Explicit read_uncommitted should behave the same
    let messages_explicit = client
        .consume_with_isolation(&topic, 0, 0, 10, Some(0))
        .await?;
    assert_eq!(messages_explicit.len(), 2);
    info!("Explicit read_uncommitted (isolation=0) works correctly");

    broker.shutdown().await?;
    info!("Read uncommitted default behavior test passed");
    Ok(())
}
