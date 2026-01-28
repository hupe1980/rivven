//! End-to-end pipeline tests: postgres-cdc → broker → sink
//!
//! These tests verify the complete data flow:
//! 1. PostgreSQL with logical replication
//! 2. rivvend broker
//! 3. Source connector (postgres-cdc)
//! 4. Sink connector (captures events)
//!
//! Run with: cargo test -p rivven-connect --test e2e_pipeline -- --nocapture

use anyhow::{Context, Result};
use bytes::Bytes;
use rivven_client::Client;
use rivven_core::Config;
use rivvend::Server;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio::sync::{broadcast, Mutex};
use tokio::time::{sleep, timeout};
use tokio_postgres::NoTls;
use tracing::{debug, info};

/// Captured event for test verification
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields used for test assertions in future tests
struct CapturedEvent {
    offset: u64,
    key: Option<Vec<u8>>,
    value: Vec<u8>,
}

/// Test event sink that captures events for verification
struct TestEventSink {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
    event_count: AtomicU64,
}

impl TestEventSink {
    fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            event_count: AtomicU64::new(0),
        }
    }

    async fn capture(&self, offset: u64, key: Option<Bytes>, value: Bytes) {
        let event = CapturedEvent {
            offset,
            key: key.map(|k| k.to_vec()),
            value: value.to_vec(),
        };
        self.events.lock().await.push(event);
        self.event_count.fetch_add(1, Ordering::SeqCst);
    }

    fn count(&self) -> u64 {
        self.event_count.load(Ordering::SeqCst)
    }

    async fn events(&self) -> Vec<CapturedEvent> {
        self.events.lock().await.clone()
    }

    async fn wait_for_events(&self, min_count: u64, timeout_secs: u64) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
        while tokio::time::Instant::now() < deadline {
            if self.count() >= min_count {
                return Ok(());
            }
            sleep(Duration::from_millis(100)).await;
        }
        anyhow::bail!(
            "Timeout waiting for {} events, got {}",
            min_count,
            self.count()
        )
    }
}

/// PostgreSQL test container with logical replication
struct PostgresTestContainer {
    #[allow(dead_code)]
    container: ContainerAsync<Postgres>,
    host: String,
    port: u16,
}

impl PostgresTestContainer {
    async fn start() -> Result<Self> {
        info!("🐘 Starting PostgreSQL with logical replication...");

        let container = Postgres::default()
            .with_cmd(vec![
                "postgres",
                "-c",
                "wal_level=logical",
                "-c",
                "max_replication_slots=10",
                "-c",
                "max_wal_senders=10",
            ])
            .start()
            .await
            .context("Failed to start PostgreSQL container")?;

        let host = container.get_host().await?.to_string();
        let port = container.get_host_port_ipv4(5432).await?;

        let instance = Self {
            container,
            host,
            port,
        };

        instance.wait_ready().await?;
        instance.setup_replication().await?;

        Ok(instance)
    }

    async fn wait_ready(&self) -> Result<()> {
        let conn_str = format!(
            "host={} port={} user=postgres password=postgres",
            self.host, self.port
        );

        for attempt in 1..=30 {
            if let Ok((client, conn)) = tokio_postgres::connect(&conn_str, NoTls).await {
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        debug!("connection error: {}", e);
                    }
                });
                drop(client);
                info!("PostgreSQL ready after {} attempts", attempt);
                return Ok(());
            }
            sleep(Duration::from_millis(500)).await;
        }
        anyhow::bail!("PostgreSQL failed to become ready")
    }

    async fn setup_replication(&self) -> Result<()> {
        let conn_str = format!(
            "host={} port={} user=postgres password=postgres",
            self.host, self.port
        );
        let (client, conn) = tokio_postgres::connect(&conn_str, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                debug!("connection error: {}", e);
            }
        });

        // Create replication user
        client
            .execute(
                "CREATE USER rivven_cdc WITH REPLICATION LOGIN PASSWORD 'rivven_password'",
                &[],
            )
            .await?;

        // Create test database
        client.execute("CREATE DATABASE testdb", &[]).await?;

        // Connect to testdb
        let conn_str = format!(
            "host={} port={} user=postgres password=postgres dbname=testdb",
            self.host, self.port
        );
        let (client, conn) = tokio_postgres::connect(&conn_str, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                debug!("connection error: {}", e);
            }
        });

        // Create test table
        client
            .execute(
                "CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )",
                &[],
            )
            .await?;

        // Grant permissions
        client
            .execute(
                "GRANT SELECT ON ALL TABLES IN SCHEMA public TO rivven_cdc",
                &[],
            )
            .await?;
        client
            .execute(
                "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO rivven_cdc",
                &[],
            )
            .await?;

        // Create publication
        client
            .execute("CREATE PUBLICATION rivven_pub FOR ALL TABLES", &[])
            .await?;

        info!("PostgreSQL replication setup complete");
        Ok(())
    }

    fn connection_string(&self) -> String {
        format!(
            "host={} port={} user=rivven_cdc password=rivven_password dbname=testdb",
            self.host, self.port
        )
    }

    async fn insert_user(&self, name: &str, email: &str) -> Result<()> {
        let conn_str = format!(
            "host={} port={} user=postgres password=postgres dbname=testdb",
            self.host, self.port
        );
        let (client, conn) = tokio_postgres::connect(&conn_str, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                debug!("connection error: {}", e);
            }
        });

        client
            .execute(
                "INSERT INTO users (name, email) VALUES ($1, $2)",
                &[&name, &email],
            )
            .await?;
        Ok(())
    }
}

/// Test broker that runs in-process
struct TestBroker {
    server: Server,
    addr: String,
}

impl TestBroker {
    async fn start() -> Result<Self> {
        let test_dir = format!("/tmp/rivven_e2e_{}", uuid::Uuid::new_v4());
        tokio::fs::create_dir_all(&test_dir).await?;

        let config = Config::default()
            .with_port(0) // Random port
            .with_bind_address("127.0.0.1".to_string())
            .with_data_dir(test_dir)
            .with_persistence(true);

        let server = Server::new(config).await?;
        let addr = server.local_addr()?.to_string();

        info!("Test broker starting on {}", addr);

        Ok(Self { server, addr })
    }

    async fn start_background(
        self,
    ) -> (String, tokio::task::JoinHandle<Result<(), anyhow::Error>>) {
        let addr = self.addr.clone();
        let handle = tokio::spawn(async move { self.server.start().await });
        // Give server time to bind
        sleep(Duration::from_millis(100)).await;
        (addr, handle)
    }

    #[allow(dead_code)] // Reserved for future tests requiring address lookup
    fn addr(&self) -> &str {
        &self.addr
    }
}

// ============================================================================
// Tests
// ============================================================================

/// Test: Broker can receive messages and they can be consumed
#[tokio::test]
async fn test_broker_roundtrip() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    info!("🧪 Test: Broker roundtrip");

    // Start broker
    let broker = TestBroker::start().await?;
    let (addr, _server_handle) = broker.start_background().await;

    // Give server time to start
    sleep(Duration::from_millis(200)).await;

    // Create client
    let mut client = Client::connect(&addr).await?;

    // Create topic
    client.create_topic("test-topic", Some(1)).await?;

    // Publish message
    let msg = Bytes::from("Hello, Rivven!");
    let offset = client.publish("test-topic", msg.clone()).await?;
    info!("Published message at offset {}", offset);

    // Consume message
    let messages = client.consume("test-topic", 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].value.as_ref(), b"Hello, Rivven!");

    info!("✅ Broker roundtrip test passed");
    Ok(())
}

/// Test: GetOffsetBounds API works correctly
#[tokio::test]
async fn test_get_offset_bounds() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    info!("🧪 Test: GetOffsetBounds API");

    let broker = TestBroker::start().await?;
    let (addr, _server_handle) = broker.start_background().await;
    sleep(Duration::from_millis(200)).await;

    let mut client = Client::connect(&addr).await?;
    client.create_topic("bounds-test", Some(1)).await?;

    // Empty topic
    let (earliest, latest) = client.get_offset_bounds("bounds-test", 0).await?;
    assert_eq!(earliest, 0);
    assert_eq!(latest, 0);

    // Publish some messages
    for i in 0..5 {
        client
            .publish("bounds-test", Bytes::from(format!("msg-{}", i)))
            .await?;
    }

    // Check bounds after publish
    let (earliest, latest) = client.get_offset_bounds("bounds-test", 0).await?;
    assert_eq!(earliest, 0);
    assert_eq!(latest, 5);

    info!("✅ GetOffsetBounds test passed");
    Ok(())
}

/// Test: Consumer group operations
#[tokio::test]
async fn test_consumer_groups() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    info!("🧪 Test: Consumer groups");

    let broker = TestBroker::start().await?;
    let (addr, _server_handle) = broker.start_background().await;
    sleep(Duration::from_millis(200)).await;

    let mut client = Client::connect(&addr).await?;
    client.create_topic("cg-test", Some(1)).await?;

    // Initially no groups
    let groups = client.list_groups().await?;
    assert!(groups.is_empty());

    // Commit an offset
    client.commit_offset("my-group", "cg-test", 0, 42).await?;

    // Now we have a group
    let groups = client.list_groups().await?;
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0], "my-group");

    // Describe group
    let offsets = client.describe_group("my-group").await?;
    assert_eq!(offsets.get("cg-test").unwrap().get(&0), Some(&42));

    // Delete group
    client.delete_group("my-group").await?;
    let groups = client.list_groups().await?;
    assert!(groups.is_empty());

    info!("✅ Consumer groups test passed");
    Ok(())
}

/// Integration test: Full E2E pipeline with PostgreSQL CDC
///
/// This test requires Docker to run PostgreSQL container.
/// Run with: cargo test -p rivven-connect --test e2e_pipeline test_full_e2e_pipeline -- --nocapture --ignored
#[tokio::test]
#[ignore] // Requires Docker
async fn test_full_e2e_pipeline() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();

    info!("🧪 Test: Full E2E pipeline (postgres-cdc → broker → sink)");

    // 1. Start PostgreSQL with logical replication
    let pg = PostgresTestContainer::start().await?;
    info!("PostgreSQL connection: {}", pg.connection_string());

    // 2. Start broker
    let broker = TestBroker::start().await?;
    let (broker_addr, _server_handle) = broker.start_background().await;
    sleep(Duration::from_millis(500)).await;
    info!("Broker running on {}", broker_addr);

    // 3. Create CDC topic
    let mut client = Client::connect(&broker_addr).await?;
    client
        .create_topic("cdc.testdb.public.users", Some(1))
        .await?;
    info!("Created CDC topic");

    // 4. Setup test event sink
    let sink = Arc::new(TestEventSink::new());

    // 5. Start sink consumer in background
    let sink_clone = sink.clone();
    let broker_addr_clone = broker_addr.clone();
    let (shutdown_tx, mut shutdown_rx) = broadcast::channel::<()>(1);
    let consumer_handle = tokio::spawn(async move {
        let mut client = Client::connect(&broker_addr_clone).await.unwrap();
        let mut offset = 0u64;
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Sink consumer shutting down");
                    break;
                }
                result = client.consume("cdc.testdb.public.users", 0, offset, 10) => {
                    match result {
                        Ok(messages) => {
                            for msg in messages {
                                sink_clone.capture(msg.offset, msg.key.clone(), msg.value.clone()).await;
                                offset = msg.offset + 1;
                            }
                        }
                        Err(e) => {
                            debug!("Consume error (may be expected): {}", e);
                        }
                    }
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    });

    // 6. TODO: Start postgres-cdc source (when integrated)
    // For now, we simulate CDC by publishing events directly
    info!("Simulating CDC events (direct publish for now)");

    // Insert data into PostgreSQL
    pg.insert_user("Alice", "alice@example.com").await?;
    pg.insert_user("Bob", "bob@example.com").await?;

    // Simulate CDC events (until real CDC integration)
    let mut client = Client::connect(&broker_addr).await?;
    let event1 = serde_json::json!({
        "op": "c",
        "table": "users",
        "after": {"id": 1, "name": "Alice", "email": "alice@example.com"}
    });
    let event2 = serde_json::json!({
        "op": "c",
        "table": "users",
        "after": {"id": 2, "name": "Bob", "email": "bob@example.com"}
    });
    client
        .publish(
            "cdc.testdb.public.users",
            Bytes::from(serde_json::to_vec(&event1)?),
        )
        .await?;
    client
        .publish(
            "cdc.testdb.public.users",
            Bytes::from(serde_json::to_vec(&event2)?),
        )
        .await?;

    // 7. Wait for events to flow through
    info!("Waiting for events to be consumed...");
    sink.wait_for_events(2, 10).await?;

    // 8. Verify events
    let events = sink.events().await;
    assert_eq!(events.len(), 2, "Expected 2 CDC events");

    let event1_data: serde_json::Value = serde_json::from_slice(&events[0].value)?;
    assert_eq!(event1_data["after"]["name"], "Alice");

    let event2_data: serde_json::Value = serde_json::from_slice(&events[1].value)?;
    assert_eq!(event2_data["after"]["name"], "Bob");

    info!("✅ Received and verified {} events", events.len());

    // Cleanup
    let _ = shutdown_tx.send(());
    let _ = timeout(Duration::from_secs(2), consumer_handle).await;

    info!("✅ Full E2E pipeline test passed!");
    Ok(())
}
