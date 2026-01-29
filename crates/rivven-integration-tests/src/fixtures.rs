//! Test fixtures for integration tests
//!
//! Provides reusable test infrastructure including:
//! - Embedded broker instances
//! - Database containers (PostgreSQL)
//! - Test data generators

use anyhow::Result;
use rivven_core::Config;
use rivvend::Server;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::sleep;
use tracing::info;

// ============================================================================
// TestBroker - Single embedded broker
// ============================================================================

/// An embedded Rivven broker for testing
pub struct TestBroker {
    pub addr: SocketAddr,
    pub config: Config,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl TestBroker {
    /// Start a new test broker on a random available port
    pub async fn start() -> Result<Self> {
        Self::start_with_config(Config::default()).await
    }

    /// Start a test broker with custom configuration
    pub async fn start_with_config(mut config: Config) -> Result<Self> {
        let port = portpicker::pick_unused_port().expect("No available port");
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;

        config.bind_address = "127.0.0.1".to_string();
        config.port = port;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_config = config.clone();

        let handle = tokio::spawn(async move {
            let server = match Server::new(server_config).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Failed to create server: {}", e);
                    return;
                }
            };
            tokio::select! {
                result = server.start() => {
                    if let Err(e) = result {
                        tracing::error!("Server error: {}", e);
                    }
                }
                _ = shutdown_rx => {
                    info!("Test broker shutting down");
                }
            }
        });

        // Wait for server to be ready
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }

        Ok(Self {
            addr,
            config,
            shutdown_tx: Some(shutdown_tx),
            handle: Some(handle),
        })
    }

    /// Get the broker's address as a connection string
    pub fn connection_string(&self) -> String {
        format!("{}:{}", self.addr.ip(), self.addr.port())
    }

    /// Get the broker port
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Gracefully shutdown the broker
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }
        Ok(())
    }
}

impl Drop for TestBroker {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

// ============================================================================
// TestCluster - Multiple broker cluster
// ============================================================================

/// Test cluster with multiple brokers
pub struct TestCluster {
    pub brokers: Vec<TestBroker>,
}

impl TestCluster {
    /// Start a test cluster with the specified number of nodes
    pub async fn start(node_count: usize) -> Result<Self> {
        let mut brokers = Vec::with_capacity(node_count);

        for _i in 0..node_count {
            let config = Config::default();
            let broker = TestBroker::start_with_config(config).await?;
            brokers.push(broker);
        }

        sleep(Duration::from_millis(500)).await;

        Ok(Self { brokers })
    }

    /// Get connection strings for all brokers
    pub fn connection_strings(&self) -> Vec<String> {
        self.brokers.iter().map(|b| b.connection_string()).collect()
    }

    /// Get the first broker's address
    pub fn bootstrap_addr(&self) -> SocketAddr {
        self.brokers[0].addr
    }

    /// Get bootstrap connection string
    pub fn bootstrap_string(&self) -> String {
        self.brokers[0].connection_string()
    }

    /// Shutdown all brokers
    pub async fn shutdown(self) -> Result<()> {
        for broker in self.brokers {
            broker.shutdown().await?;
        }
        Ok(())
    }
}

// ============================================================================
// PostgreSQL Test Container
// ============================================================================

/// PostgreSQL test container with logical replication enabled
pub struct TestPostgres {
    pub container: testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
    pub connection_string: String,
    pub host: String,
    pub port: u16,
}

impl TestPostgres {
    /// Start a PostgreSQL container configured for CDC
    pub async fn start() -> Result<Self> {
        use testcontainers::{runners::AsyncRunner, ImageExt};
        use testcontainers_modules::postgres::Postgres;

        let container = Postgres::default()
            .with_env_var("POSTGRES_DB", "testdb")
            .with_env_var("POSTGRES_USER", "testuser")
            .with_env_var("POSTGRES_PASSWORD", "testpass")
            .with_cmd([
                "-c",
                "wal_level=logical",
                "-c",
                "max_wal_senders=10",
                "-c",
                "max_replication_slots=10",
            ])
            .start()
            .await?;

        // Retry port retrieval to handle testcontainers race condition
        let host = container.get_host().await?.to_string();
        let mut port = None;
        for i in 0..10 {
            match container.get_host_port_ipv4(5432).await {
                Ok(p) => {
                    port = Some(p);
                    break;
                }
                Err(e) => {
                    tracing::debug!(
                        "Waiting for PostgreSQL port exposure (attempt {}): {}",
                        i + 1,
                        e
                    );
                    sleep(Duration::from_millis(100 * (i + 1) as u64)).await;
                }
            }
        }
        let port =
            port.ok_or_else(|| anyhow::anyhow!("PostgreSQL port not exposed after retries"))?;

        let connection_string = format!(
            "host={} port={} user=testuser password=testpass dbname=testdb",
            host, port
        );

        Self::wait_for_postgres(&host, port).await?;

        Ok(Self {
            container,
            connection_string,
            host,
            port,
        })
    }

    async fn wait_for_postgres(host: &str, port: u16) -> Result<()> {
        let conn_str = format!(
            "host={} port={} user=testuser password=testpass dbname=testdb",
            host, port
        );

        for i in 0..30 {
            match tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await {
                Ok((client, connection)) => {
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            tracing::error!("PostgreSQL connection error: {}", e);
                        }
                    });
                    if client.simple_query("SELECT 1").await.is_ok() {
                        info!("PostgreSQL ready after {} attempts", i + 1);
                        return Ok(());
                    }
                }
                Err(e) => {
                    tracing::debug!("Waiting for PostgreSQL (attempt {}): {}", i + 1, e);
                }
            }
            sleep(Duration::from_millis(500)).await;
        }

        anyhow::bail!("PostgreSQL did not become ready in time")
    }

    /// Get a tokio-postgres connection
    pub async fn connect(&self) -> Result<tokio_postgres::Client> {
        let (client, connection) =
            tokio_postgres::connect(&self.connection_string, tokio_postgres::NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("PostgreSQL connection error: {}", e);
            }
        });

        Ok(client)
    }

    /// Get connection URL for CDC configuration
    pub fn cdc_connection_url(&self) -> String {
        format!(
            "postgres://testuser:testpass@{}:{}/testdb",
            self.host, self.port
        )
    }

    /// Setup test schema with sample tables
    pub async fn setup_test_schema(&self) -> Result<()> {
        let client = self.connect().await?;

        client
            .batch_execute(
                r#"
                CREATE SCHEMA IF NOT EXISTS test_schema;
                
                CREATE TABLE IF NOT EXISTS test_schema.users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(100) NOT NULL UNIQUE,
                    email VARCHAR(255) NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                
                CREATE TABLE IF NOT EXISTS test_schema.orders (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES test_schema.users(id),
                    total_amount DECIMAL(10, 2) NOT NULL,
                    status VARCHAR(50) DEFAULT 'pending',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                
                ALTER TABLE test_schema.users REPLICA IDENTITY FULL;
                ALTER TABLE test_schema.orders REPLICA IDENTITY FULL;
                
                DROP PUBLICATION IF EXISTS rivven_pub;
                CREATE PUBLICATION rivven_pub FOR ALL TABLES;
                "#,
            )
            .await?;

        info!("PostgreSQL test schema created");
        Ok(())
    }

    /// Insert test users
    pub async fn insert_test_users(&self, count: usize) -> Result<Vec<i32>> {
        let client = self.connect().await?;
        let mut ids = Vec::with_capacity(count);

        for i in 0..count {
            let row = client
                .query_one(
                    "INSERT INTO test_schema.users (username, email) VALUES ($1, $2) RETURNING id",
                    &[&format!("user_{}", i), &format!("user{}@example.com", i)],
                )
                .await?;
            ids.push(row.get::<_, i32>("id"));
        }

        Ok(ids)
    }

    /// Update a user
    pub async fn update_user(&self, id: i32, new_username: &str) -> Result<()> {
        let client = self.connect().await?;
        client
            .execute(
                "UPDATE test_schema.users SET username = $1, updated_at = NOW() WHERE id = $2",
                &[&new_username, &id],
            )
            .await?;
        Ok(())
    }

    /// Delete a user
    pub async fn delete_user(&self, id: i32) -> Result<()> {
        let client = self.connect().await?;
        client
            .execute("DELETE FROM test_schema.users WHERE id = $1", &[&id])
            .await?;
        Ok(())
    }

    /// Create replication slot for CDC
    pub async fn create_replication_slot(&self, slot_name: &str) -> Result<()> {
        let client = self.connect().await?;
        client
            .execute(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot_name],
            )
            .await?;
        info!("Created replication slot: {}", slot_name);
        Ok(())
    }
}

// ============================================================================
// MySQL Test Container
// ============================================================================

/// MySQL test container with binary log enabled for CDC
pub struct TestMysql {
    pub container: testcontainers::ContainerAsync<testcontainers_modules::mysql::Mysql>,
    pub connection_string: String,
    pub host: String,
    pub port: u16,
}

impl TestMysql {
    /// Start a MySQL container configured for CDC (binary logging enabled)
    pub async fn start() -> Result<Self> {
        use testcontainers::{runners::AsyncRunner, ImageExt};
        use testcontainers_modules::mysql::Mysql;

        let container = Mysql::default()
            .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
            .with_env_var("MYSQL_DATABASE", "testdb")
            .with_env_var("MYSQL_USER", "testuser")
            .with_env_var("MYSQL_PASSWORD", "testpass")
            .with_cmd([
                "--log-bin=mysql-bin",
                "--server-id=1",
                "--binlog-format=ROW",
                "--binlog-row-image=FULL",
                "--gtid-mode=ON",
                "--enforce-gtid-consistency=ON",
            ])
            .start()
            .await?;

        // Retry port retrieval to handle testcontainers race condition
        let host = container.get_host().await?.to_string();
        let mut port = None;
        for i in 0..10 {
            match container.get_host_port_ipv4(3306).await {
                Ok(p) => {
                    port = Some(p);
                    break;
                }
                Err(e) => {
                    tracing::debug!("Waiting for MySQL port exposure (attempt {}): {}", i + 1, e);
                    sleep(Duration::from_millis(100 * (i + 1) as u64)).await;
                }
            }
        }
        let port = port.ok_or_else(|| anyhow::anyhow!("MySQL port not exposed after retries"))?;

        let connection_string = format!("mysql://testuser:testpass@{}:{}/testdb", host, port);

        Self::wait_for_mysql(&host, port).await?;

        // Grant replication privileges to testuser using root connection
        Self::grant_replication_privileges(&host, port).await?;

        Ok(Self {
            container,
            connection_string,
            host,
            port,
        })
    }

    /// Grant replication privileges to testuser
    async fn grant_replication_privileges(host: &str, port: u16) -> Result<()> {
        use mysql_async::prelude::Queryable;

        let root_url = format!("mysql://root:rootpass@{}:{}/testdb", host, port);
        let pool = mysql_async::Pool::new(root_url.as_str());
        let mut conn = pool.get_conn().await?;

        // Grant REPLICATION SLAVE and REPLICATION CLIENT privileges for CDC
        conn.query_drop("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'testuser'@'%'")
            .await?;

        // Keep testuser with caching_sha2_password (MySQL 8 default) for better compatibility
        // with rivven-cdc which supports this auth mechanism
        conn.query_drop("FLUSH PRIVILEGES").await?;

        info!("MySQL replication privileges granted to testuser");
        Ok(())
    }

    async fn wait_for_mysql(host: &str, port: u16) -> Result<()> {
        let url = format!("mysql://testuser:testpass@{}:{}/testdb", host, port);

        for i in 0..30 {
            match mysql_async::Pool::new(url.as_str()).get_conn().await {
                Ok(mut conn) => {
                    use mysql_async::prelude::Queryable;
                    if conn.query_drop("SELECT 1").await.is_ok() {
                        info!("MySQL ready after {} attempts", i + 1);
                        return Ok(());
                    }
                }
                Err(e) => {
                    tracing::debug!("Waiting for MySQL (attempt {}): {}", i + 1, e);
                }
            }
            sleep(Duration::from_millis(500)).await;
        }

        anyhow::bail!("MySQL did not become ready in time")
    }

    /// Get a mysql_async connection pool
    pub fn pool(&self) -> mysql_async::Pool {
        mysql_async::Pool::new(self.connection_string.as_str())
    }

    /// Get connection URL for CDC configuration
    pub fn cdc_connection_url(&self) -> String {
        self.connection_string.clone()
    }

    /// Setup test schema with sample tables
    pub async fn setup_test_schema(&self) -> Result<()> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        conn.query_drop(
            r#"
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(100) NOT NULL UNIQUE,
                email VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
            "#,
        )
        .await?;

        conn.query_drop(
            r#"
            CREATE TABLE IF NOT EXISTS orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                total_amount DECIMAL(10, 2) NOT NULL,
                status VARCHAR(50) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            ) ENGINE=InnoDB
            "#,
        )
        .await?;

        info!("MySQL test schema created");
        Ok(())
    }

    /// Insert test users
    pub async fn insert_test_users(&self, count: usize) -> Result<Vec<u64>> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;
        let mut ids = Vec::with_capacity(count);

        use mysql_async::prelude::Queryable;

        for i in 0..count {
            conn.exec_drop(
                "INSERT INTO users (username, email) VALUES (?, ?)",
                (format!("user_{}", i), format!("user{}@example.com", i)),
            )
            .await?;
            let id: Option<u64> = conn.query_first("SELECT LAST_INSERT_ID()").await?;
            if let Some(id) = id {
                ids.push(id);
            }
        }

        Ok(ids)
    }

    /// Update a user
    pub async fn update_user(&self, id: u64, new_username: &str) -> Result<()> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        conn.exec_drop(
            "UPDATE users SET username = ? WHERE id = ?",
            (new_username, id),
        )
        .await?;
        Ok(())
    }

    /// Delete a user
    pub async fn delete_user(&self, id: u64) -> Result<()> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        conn.exec_drop("DELETE FROM users WHERE id = ?", (id,))
            .await?;
        Ok(())
    }

    /// Get binlog position
    pub async fn get_binlog_position(&self) -> Result<(String, u64)> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        let row: (String, u64, String, String, String) =
            conn.query_first("SHOW MASTER STATUS").await?.unwrap();
        Ok((row.0, row.1))
    }

    /// Get GTID executed set
    pub async fn get_gtid_executed(&self) -> Result<String> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        let gtid: Option<String> = conn.query_first("SELECT @@global.gtid_executed").await?;
        Ok(gtid.unwrap_or_default())
    }
}

// ============================================================================
// MariaDB Test Container
// ============================================================================

/// MariaDB test container with binary log enabled for CDC
pub struct TestMariadb {
    pub container: testcontainers::ContainerAsync<testcontainers_modules::mariadb::Mariadb>,
    pub connection_string: String,
    pub root_connection_string: String,
    pub host: String,
    pub port: u16,
}

impl TestMariadb {
    /// Start a MariaDB container configured for CDC (binary logging enabled)
    pub async fn start() -> Result<Self> {
        use testcontainers::{runners::AsyncRunner, ImageExt};
        use testcontainers_modules::mariadb::Mariadb;

        let container = Mariadb::default()
            .with_env_var("MARIADB_ROOT_PASSWORD", "rootpass")
            .with_env_var("MARIADB_DATABASE", "testdb")
            .with_env_var("MARIADB_USER", "testuser")
            .with_env_var("MARIADB_PASSWORD", "testpass")
            .with_cmd([
                "--log-bin=mariadb-bin",
                "--server-id=1",
                "--binlog-format=ROW",
                "--binlog-row-image=FULL",
            ])
            .start()
            .await?;

        // Retry port retrieval to handle testcontainers race condition
        let host = container.get_host().await?.to_string();
        let mut port = None;
        for i in 0..10 {
            match container.get_host_port_ipv4(3306).await {
                Ok(p) => {
                    port = Some(p);
                    break;
                }
                Err(e) => {
                    tracing::debug!(
                        "Waiting for MariaDB port exposure (attempt {}): {}",
                        i + 1,
                        e
                    );
                    sleep(Duration::from_millis(100 * (i + 1) as u64)).await;
                }
            }
        }
        let port = port.ok_or_else(|| anyhow::anyhow!("MariaDB port not exposed after retries"))?;

        // Use root for setup, then testuser for regular operations
        let root_connection_string = format!("mysql://root:rootpass@{}:{}/testdb", host, port);

        let connection_string = format!("mysql://testuser:testpass@{}:{}/testdb", host, port);

        Self::wait_for_mariadb(&host, port).await?;

        // Grant BINLOG MONITOR, REPLICATION SLAVE, REPLICATION CLIENT privileges to testuser for CDC
        {
            use mysql_async::prelude::Queryable;
            let root_pool = mysql_async::Pool::new(root_connection_string.as_str());
            let mut root_conn = root_pool.get_conn().await?;
            root_conn.query_drop("GRANT BINLOG MONITOR, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'testuser'@'%'").await?;
            root_conn.query_drop("FLUSH PRIVILEGES").await?;
            info!("MariaDB replication privileges granted to testuser");
        }

        Ok(Self {
            container,
            connection_string,
            root_connection_string,
            host,
            port,
        })
    }

    async fn wait_for_mariadb(host: &str, port: u16) -> Result<()> {
        // Wait using root first since testuser might not have all privileges initially
        let url = format!("mysql://root:rootpass@{}:{}/testdb", host, port);

        for i in 0..30 {
            match mysql_async::Pool::new(url.as_str()).get_conn().await {
                Ok(mut conn) => {
                    use mysql_async::prelude::Queryable;
                    if conn.query_drop("SELECT 1").await.is_ok() {
                        info!("MariaDB ready after {} attempts", i + 1);
                        return Ok(());
                    }
                }
                Err(e) => {
                    tracing::debug!("Waiting for MariaDB (attempt {}): {}", i + 1, e);
                }
            }
            sleep(Duration::from_millis(500)).await;
        }

        anyhow::bail!("MariaDB did not become ready in time")
    }

    /// Get a mysql_async connection pool (MariaDB is MySQL compatible)
    pub fn pool(&self) -> mysql_async::Pool {
        mysql_async::Pool::new(self.connection_string.as_str())
    }

    /// Get connection URL for CDC configuration
    pub fn cdc_connection_url(&self) -> String {
        self.connection_string.clone()
    }

    /// Setup test schema with sample tables
    pub async fn setup_test_schema(&self) -> Result<()> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        conn.query_drop(
            r#"
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(100) NOT NULL UNIQUE,
                email VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
            "#,
        )
        .await?;

        conn.query_drop(
            r#"
            CREATE TABLE IF NOT EXISTS orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                total_amount DECIMAL(10, 2) NOT NULL,
                status VARCHAR(50) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            ) ENGINE=InnoDB
            "#,
        )
        .await?;

        // MariaDB-specific: test JSON/Dynamic columns
        conn.query_drop(
            r#"
            CREATE TABLE IF NOT EXISTS events (
                id INT AUTO_INCREMENT PRIMARY KEY,
                event_type VARCHAR(100) NOT NULL,
                payload JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
            "#,
        )
        .await?;

        info!("MariaDB test schema created");
        Ok(())
    }

    /// Insert test users
    pub async fn insert_test_users(&self, count: usize) -> Result<Vec<u64>> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;
        let mut ids = Vec::with_capacity(count);

        use mysql_async::prelude::Queryable;

        for i in 0..count {
            conn.exec_drop(
                "INSERT INTO users (username, email) VALUES (?, ?)",
                (format!("user_{}", i), format!("user{}@example.com", i)),
            )
            .await?;
            let id: Option<u64> = conn.query_first("SELECT LAST_INSERT_ID()").await?;
            if let Some(id) = id {
                ids.push(id);
            }
        }

        Ok(ids)
    }

    /// Update a user
    pub async fn update_user(&self, id: u64, new_username: &str) -> Result<()> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        conn.exec_drop(
            "UPDATE users SET username = ? WHERE id = ?",
            (new_username, id),
        )
        .await?;
        Ok(())
    }

    /// Delete a user
    pub async fn delete_user(&self, id: u64) -> Result<()> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        conn.exec_drop("DELETE FROM users WHERE id = ?", (id,))
            .await?;
        Ok(())
    }

    /// Get binlog position (MariaDB style)
    pub async fn get_binlog_position(&self) -> Result<(String, u64)> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        let row: (String, u64, String, String) =
            conn.query_first("SHOW MASTER STATUS").await?.unwrap();
        Ok((row.0, row.1))
    }

    /// Get GTID current position (MariaDB-specific)
    pub async fn get_gtid_current_pos(&self) -> Result<String> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        let gtid: Option<String> = conn.query_first("SELECT @@global.gtid_current_pos").await?;
        Ok(gtid.unwrap_or_default())
    }

    /// Insert test event with JSON payload (MariaDB JSON support)
    pub async fn insert_event(&self, event_type: &str, payload: serde_json::Value) -> Result<u64> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        conn.exec_drop(
            "INSERT INTO events (event_type, payload) VALUES (?, ?)",
            (event_type, payload.to_string()),
        )
        .await?;
        let id: Option<u64> = conn.query_first("SELECT LAST_INSERT_ID()").await?;
        Ok(id.unwrap_or(0))
    }
}

// ============================================================================
// Test Data Generators
// ============================================================================

pub mod test_data {
    use bytes::Bytes;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct TestEvent {
        pub id: String,
        pub event_type: String,
        pub payload: serde_json::Value,
        pub timestamp: i64,
    }

    impl TestEvent {
        pub fn new(event_type: &str, payload: serde_json::Value) -> Self {
            Self {
                id: Uuid::new_v4().to_string(),
                event_type: event_type.to_string(),
                payload,
                timestamp: chrono::Utc::now().timestamp_millis(),
            }
        }

        pub fn to_bytes(&self) -> Bytes {
            Bytes::from(serde_json::to_vec(self).unwrap())
        }

        pub fn from_bytes(bytes: &[u8]) -> serde_json::Result<Self> {
            serde_json::from_slice(bytes)
        }
    }

    /// Generate a batch of test events
    pub fn generate_events(count: usize, event_type: &str) -> Vec<TestEvent> {
        (0..count)
            .map(|i| {
                TestEvent::new(
                    event_type,
                    serde_json::json!({
                        "index": i,
                        "data": format!("test-data-{}", i),
                    }),
                )
            })
            .collect()
    }

    /// Generate a batch of random messages
    pub fn generate_messages(count: usize) -> Vec<Bytes> {
        (0..count)
            .map(|i| Bytes::from(format!("message-{}", i)))
            .collect()
    }

    /// Generate keyed messages
    pub fn generate_keyed_messages(count: usize) -> Vec<(String, Bytes)> {
        (0..count)
            .map(|i| {
                let key = format!("key-{}", i % 10);
                let value = Bytes::from(format!("value-{}", i));
                (key, value)
            })
            .collect()
    }
}

pub mod assertions {
    use bytes::Bytes;

    /// Assert that messages are received in order
    pub fn assert_message_order(messages: &[Bytes], expected_count: usize) {
        assert_eq!(messages.len(), expected_count);
        for (i, msg) in messages.iter().enumerate() {
            let expected = format!("message-{}", i);
            assert_eq!(msg.as_ref(), expected.as_bytes());
        }
    }

    /// Assert approximate throughput
    pub fn assert_throughput(
        messages_sent: usize,
        duration: std::time::Duration,
        min_msgs_per_sec: f64,
    ) {
        let actual = messages_sent as f64 / duration.as_secs_f64();
        assert!(actual >= min_msgs_per_sec);
    }
}
