mod common;

use async_trait::async_trait;
use bytes::Bytes;
use common::{PostgresContainer, PostgresVersion};
use rivven_cdc::common::{
    CdcConnector, CdcError, CdcEvent, CdcSource, EventSink, Result as CdcResult,
};
use rivven_cdc::postgres::{PostgresCdc, PostgresCdcConfig};
use rivven_core::{Config, Message, Partition, TopicManager};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// EventSink adapter for Rivven partitions
#[derive(Debug)]
struct PartitionSink {
    partition: Arc<Partition>,
}

impl PartitionSink {
    fn new(partition: Arc<Partition>) -> Self {
        Self { partition }
    }
}

#[async_trait]
impl EventSink for PartitionSink {
    async fn append(&self, event: &CdcEvent) -> CdcResult<u64> {
        let json = serde_json::to_vec(event).map_err(|e| CdcError::Serialization(e.to_string()))?;
        let message = Message::new(Bytes::from(json));
        self.partition
            .append(message)
            .await
            .map_err(|e| CdcError::Io(std::io::Error::other(e.to_string())))
    }
}

/// Test INSERT events with default PostgreSQL version (16)
#[tokio::test]
#[ignore] // Run with: cargo test -- --ignored
async fn test_postgres_cdc_insert() -> anyhow::Result<()> {
    test_postgres_cdc_insert_impl(PostgresVersion::default()).await
}

/// Test INSERT events across all PostgreSQL versions
#[tokio::test]
#[ignore]
async fn test_postgres_cdc_insert_pg14() -> anyhow::Result<()> {
    test_postgres_cdc_insert_impl(PostgresVersion::V14).await
}

#[tokio::test]
#[ignore]
async fn test_postgres_cdc_insert_pg15() -> anyhow::Result<()> {
    test_postgres_cdc_insert_impl(PostgresVersion::V15).await
}

#[tokio::test]
#[ignore]
async fn test_postgres_cdc_insert_pg17() -> anyhow::Result<()> {
    test_postgres_cdc_insert_impl(PostgresVersion::V17).await
}

async fn test_postgres_cdc_insert_impl(version: PostgresVersion) -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    println!("\n🧪 Test: INSERT events on {}\n", version);

    // Start Postgres container with specific version
    let pg = PostgresContainer::start_version(version).await?;

    // Setup Rivven
    let test_dir = format!("/tmp/rivven_test_{}", uuid::Uuid::new_v4());
    tokio::fs::create_dir_all(&test_dir).await?;

    let rivven_config = Config {
        data_dir: test_dir.clone(),
        ..Default::default()
    };
    let topic_manager = TopicManager::new(rivven_config);
    let topic = topic_manager
        .create_topic("cdc.testdb.public.users".to_string(), None)
        .await?;
    let partition = topic.partition(0)?;

    // Setup CDC connector with PartitionSink
    let connector = Arc::new(CdcConnector::postgres());
    let sink = Arc::new(PartitionSink::new(partition.clone()));
    connector
        .register_sink("cdc.testdb.public.users".to_string(), sink)
        .await;

    // Create connection string
    let conn_str = format!(
        "postgresql://rivven_cdc:test_password@localhost:{}/testdb",
        pg.port
    );

    // Start CDC
    let config = PostgresCdcConfig::builder()
        .connection_string(&conn_str)
        .slot_name("rivven_slot")
        .publication_name("rivven_pub")
        .build()?;
    let mut cdc = PostgresCdc::new(config);

    // Take the event receiver and spawn a task to route events
    let event_rx = cdc
        .take_event_receiver()
        .ok_or_else(|| anyhow::anyhow!("Failed to get event receiver"))?;
    let connector_clone = connector.clone();
    let _event_routing_task = tokio::spawn(async move {
        let mut rx = event_rx;
        while let Some(event) = rx.recv().await {
            if let Err(e) = connector_clone.route_event(&event).await {
                eprintln!("Failed to route event: {:?}", e);
            }
        }
    });

    cdc.start().await?;

    // Give CDC time to connect
    println!("⏳ Waiting for CDC to start...");
    sleep(Duration::from_secs(3)).await;

    // Insert data
    println!("📝 Inserting test data...");
    pg.execute_sql("INSERT INTO users (name, email, age) VALUES ('Dave', 'dave@example.com', 28)")
        .await?;

    // Wait for event to be processed
    sleep(Duration::from_secs(2)).await;

    // Read from partition
    println!("📖 Reading from partition...");
    let messages = partition.read(0, 10).await?;

    // Verify we got events
    assert!(!messages.is_empty(), "Expected CDC events");
    println!("✅ Received {} events", messages.len());

    // Check event payload
    let payload = String::from_utf8_lossy(&messages[0].value);
    println!("📦 Event payload: {}", payload);
    assert!(
        payload.contains("Dave") || payload.contains("dave@example.com"),
        "Expected user data in event"
    );

    // Cleanup
    tokio::fs::remove_dir_all(&test_dir).await?;

    println!("✅ Test passed!\n");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_postgres_cdc_update() -> anyhow::Result<()> {
    println!("\n🧪 Test: UPDATE events\n");

    let pg = PostgresContainer::start().await?;

    let test_dir = format!("/tmp/rivven_test_{}", uuid::Uuid::new_v4());
    tokio::fs::create_dir_all(&test_dir).await?;

    let rivven_config = Config {
        data_dir: test_dir.clone(),
        ..Default::default()
    };
    let topic_manager = TopicManager::new(rivven_config);
    let topic = topic_manager
        .create_topic("cdc.testdb.public.users".to_string(), None)
        .await?;
    let partition = topic.partition(0)?;

    let connector = Arc::new(CdcConnector::postgres());
    let sink = Arc::new(PartitionSink::new(partition.clone()));
    connector
        .register_sink("cdc.testdb.public.users".to_string(), sink)
        .await;

    let conn_str = format!(
        "postgresql://rivven_cdc:test_password@localhost:{}/testdb",
        pg.port
    );

    let config = PostgresCdcConfig::builder()
        .connection_string(&conn_str)
        .slot_name("rivven_slot")
        .publication_name("rivven_pub")
        .build()?;
    let mut cdc = PostgresCdc::new(config);

    // Set up event routing
    let event_rx = cdc
        .take_event_receiver()
        .ok_or_else(|| anyhow::anyhow!("Failed to get event receiver"))?;
    let connector_clone = connector.clone();
    let _event_routing_task = tokio::spawn(async move {
        let mut rx = event_rx;
        while let Some(event) = rx.recv().await {
            if let Err(e) = connector_clone.route_event(&event).await {
                eprintln!("Failed to route event: {:?}", e);
            }
        }
    });

    cdc.start().await?;
    sleep(Duration::from_secs(3)).await;

    // Insert initial data
    println!("📝 Inserting initial data...");
    pg.execute_sql("INSERT INTO users (name, email, age) VALUES ('Eve', 'eve@example.com', 30)")
        .await?;
    sleep(Duration::from_secs(1)).await;

    // Update the row
    println!("📝 Updating data...");
    pg.execute_sql("UPDATE users SET age = 31 WHERE name = 'Eve'")
        .await?;
    sleep(Duration::from_secs(2)).await;

    // Read events
    let messages = partition.read(0, 10).await?;

    // Should have at least 2 events (INSERT + UPDATE)
    assert!(
        messages.len() >= 2,
        "Expected INSERT and UPDATE events, got {}",
        messages.len()
    );
    println!("✅ Received {} events (INSERT + UPDATE)", messages.len());

    // Cleanup
    tokio::fs::remove_dir_all(&test_dir).await?;

    println!("✅ Test passed!\n");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_postgres_cdc_delete() -> anyhow::Result<()> {
    println!("\n🧪 Test: DELETE events\n");

    let pg = PostgresContainer::start().await?;

    let test_dir = format!("/tmp/rivven_test_{}", uuid::Uuid::new_v4());
    tokio::fs::create_dir_all(&test_dir).await?;

    let rivven_config = Config {
        data_dir: test_dir.clone(),
        ..Default::default()
    };
    let topic_manager = TopicManager::new(rivven_config);
    let topic = topic_manager
        .create_topic("cdc.testdb.public.users".to_string(), None)
        .await?;
    let partition = topic.partition(0)?;

    let connector = Arc::new(CdcConnector::postgres());
    let sink = Arc::new(PartitionSink::new(partition.clone()));
    connector
        .register_sink("cdc.testdb.public.users".to_string(), sink)
        .await;

    let conn_str = format!(
        "postgresql://rivven_cdc:test_password@localhost:{}/testdb",
        pg.port
    );

    let config = PostgresCdcConfig::builder()
        .connection_string(&conn_str)
        .slot_name("rivven_slot")
        .publication_name("rivven_pub")
        .build()?;
    let mut cdc = PostgresCdc::new(config);

    // Set up event routing
    let event_rx = cdc
        .take_event_receiver()
        .ok_or_else(|| anyhow::anyhow!("Failed to get event receiver"))?;
    let connector_clone = connector.clone();
    let _event_routing_task = tokio::spawn(async move {
        let mut rx = event_rx;
        while let Some(event) = rx.recv().await {
            if let Err(e) = connector_clone.route_event(&event).await {
                eprintln!("Failed to route event: {:?}", e);
            }
        }
    });

    cdc.start().await?;
    sleep(Duration::from_secs(3)).await;

    // Insert and delete
    println!("📝 Inserting data...");
    pg.execute_sql(
        "INSERT INTO users (name, email, age) VALUES ('Frank', 'frank@example.com', 40)",
    )
    .await?;
    sleep(Duration::from_secs(1)).await;

    println!("🗑️ Deleting data...");
    pg.execute_sql("DELETE FROM users WHERE name = 'Frank'")
        .await?;
    sleep(Duration::from_secs(2)).await;

    // Read events
    let messages = partition.read(0, 10).await?;

    // Should have INSERT + DELETE
    assert!(
        messages.len() >= 2,
        "Expected INSERT and DELETE events, got {}",
        messages.len()
    );
    println!("✅ Received {} events (INSERT + DELETE)", messages.len());

    // Cleanup
    tokio::fs::remove_dir_all(&test_dir).await?;

    println!("✅ Test passed!\n");
    Ok(())
}
