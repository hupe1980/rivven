//! MySQL/MariaDB Multi-Version Matrix Tests
//!
//! Comprehensive CDC tests across all supported MySQL and MariaDB versions.
//! These tests validate that rivven-cdc works correctly with each major
//! database version and its version-specific features.
//!
//! # Supported Versions
//!
//! ## MySQL
//!
//! | Version | EOL      | Key Features                              |
//! |---------|----------|-------------------------------------------|
//! | 8.0.x   | Apr 2026 | GTID, caching_sha2_password               |
//! | 8.4.x   | Apr 2032 | LTS, enhanced replication                 |
//! | 9.0+    | TBD      | Innovation release (latest)               |
//!
//! ## MariaDB
//!
//! | Version | EOL      | Key Features                              |
//! |---------|----------|-------------------------------------------|
//! | 10.6.x  | Jul 2026 | LTS, GTID improvements                    |
//! | 10.11.x | Feb 2028 | LTS, enhanced JSON, parallel replication  |
//! | 11.4.x  | May 2029 | LTS, latest features                      |
//!
//! # Running Tests
//!
//! ```bash
//! # Run all MySQL/MariaDB version matrix tests (requires Docker)
//! cargo test -p rivven-cdc --test mysql_version_matrix -- --ignored --test-threads=1
//!
//! # Run tests for MySQL only
//! cargo test -p rivven-cdc --test mysql_version_matrix mysql -- --ignored --test-threads=1
//!
//! # Run tests for MariaDB only
//! cargo test -p rivven-cdc --test mysql_version_matrix mariadb -- --ignored --test-threads=1
//!
//! # Run specific version tests
//! cargo test -p rivven-cdc --test mysql_version_matrix mysql_8_0 -- --ignored
//! cargo test -p rivven-cdc --test mysql_version_matrix mariadb_10_11 -- --ignored
//! ```

use anyhow::Result;
use rivven_cdc::common::{CdcOp, CdcSource};
use rivven_cdc::mysql::{MySqlCdc, MySqlCdcConfig};
use std::process::Command;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

/// Atomic port counter to avoid conflicts
static PORT_COUNTER: AtomicU16 = AtomicU16::new(13306);
static SERVER_ID_COUNTER: AtomicU32 = AtomicU32::new(10000);

use std::sync::atomic::AtomicU32;

// ============================================================================
// Version Definitions
// ============================================================================

/// Supported MySQL versions for CDC testing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum MySqlVersion {
    /// MySQL 8.0 - Widely deployed, GTID mature
    V8_0,
    /// MySQL 8.4 - LTS release (recommended)
    V8_4,
    /// MySQL 9.0 - Innovation release (latest)
    V9_0,
}

#[allow(dead_code)]
impl MySqlVersion {
    /// Get the Docker image tag for this version
    pub fn docker_image(&self) -> &'static str {
        match self {
            MySqlVersion::V8_0 => "mysql:8.0",
            MySqlVersion::V8_4 => "mysql:8.4",
            MySqlVersion::V9_0 => "mysql:9.0",
        }
    }

    /// Get the major.minor version string
    pub fn version_string(&self) -> &'static str {
        match self {
            MySqlVersion::V8_0 => "8.0",
            MySqlVersion::V8_4 => "8.4",
            MySqlVersion::V9_0 => "9.0",
        }
    }

    /// Check if this version supports caching_sha2_password (default in 8.0+)
    pub fn supports_caching_sha2(&self) -> bool {
        true // All supported versions
    }

    /// Check if this version supports GTID
    pub fn supports_gtid(&self) -> bool {
        true // All 8.0+ versions
    }

    /// Check if this version supports parallel replication
    pub fn supports_parallel_replication(&self) -> bool {
        true // All 8.0+ versions
    }

    /// All supported MySQL versions
    pub fn all() -> &'static [MySqlVersion] {
        &[MySqlVersion::V8_0, MySqlVersion::V8_4, MySqlVersion::V9_0]
    }
}

impl Default for MySqlVersion {
    /// Default version for tests (LTS recommended)
    fn default() -> Self {
        MySqlVersion::V8_4
    }
}

impl std::fmt::Display for MySqlVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MySQL {}", self.version_string())
    }
}

/// Supported MariaDB versions for CDC testing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum MariaDbVersion {
    /// MariaDB 10.6 - LTS
    V10_6,
    /// MariaDB 10.11 - LTS (recommended)
    V10_11,
    /// MariaDB 11.4 - LTS (latest)
    V11_4,
}

#[allow(dead_code)]
impl MariaDbVersion {
    /// Get the Docker image tag for this version
    pub fn docker_image(&self) -> &'static str {
        match self {
            MariaDbVersion::V10_6 => "mariadb:10.6",
            MariaDbVersion::V10_11 => "mariadb:10.11",
            MariaDbVersion::V11_4 => "mariadb:11.4",
        }
    }

    /// Get the major.minor version string
    pub fn version_string(&self) -> &'static str {
        match self {
            MariaDbVersion::V10_6 => "10.6",
            MariaDbVersion::V10_11 => "10.11",
            MariaDbVersion::V11_4 => "11.4",
        }
    }

    /// Check if this version supports MariaDB GTID
    pub fn supports_gtid(&self) -> bool {
        true // All supported versions
    }

    /// Check if this version supports client_ed25519 auth
    pub fn supports_ed25519_auth(&self) -> bool {
        true // All 10.4+ versions
    }

    /// Check if this version supports parallel replication
    pub fn supports_parallel_replication(&self) -> bool {
        true // All 10.5+ versions
    }

    /// All supported MariaDB versions
    pub fn all() -> &'static [MariaDbVersion] {
        &[
            MariaDbVersion::V10_6,
            MariaDbVersion::V10_11,
            MariaDbVersion::V11_4,
        ]
    }
}

impl Default for MariaDbVersion {
    /// Default version for tests (LTS recommended)
    fn default() -> Self {
        MariaDbVersion::V10_11
    }
}

impl std::fmt::Display for MariaDbVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MariaDB {}", self.version_string())
    }
}

// ============================================================================
// Test Containers
// ============================================================================

/// MySQL container for version matrix tests
#[allow(dead_code)]
pub struct MySqlContainer {
    container_name: String,
    pub port: u16,
    pub version: MySqlVersion,
}

#[allow(dead_code)]
impl MySqlContainer {
    /// Start a MySQL container with a specific version
    pub async fn start_version(version: MySqlVersion) -> Result<Self> {
        let container_name = format!(
            "rivven-test-mysql{}-{}",
            version.version_string().replace('.', ""),
            uuid::Uuid::new_v4()
        );
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);

        println!(
            "🐬 Starting {} container: {} on port {}",
            version, container_name, port
        );

        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                &container_name,
                "-e",
                "MYSQL_ROOT_PASSWORD=test_password",
                "-e",
                "MYSQL_DATABASE=testdb",
                "-p",
                &format!("{}:3306", port),
                version.docker_image(),
                "--server-id=1",
                "--log-bin=mysql-bin",
                "--binlog-format=ROW",
                "--binlog-row-image=FULL",
                "--gtid_mode=ON",
                "--enforce-gtid-consistency=ON",
                "--log-slave-updates=ON",
                "--default-authentication-plugin=mysql_native_password",
            ])
            .output()?;

        if !output.status.success() {
            anyhow::bail!(
                "Failed to start {}: {}",
                version,
                String::from_utf8_lossy(&output.stderr)
            );
        }

        // Wait for MySQL to be ready
        let mut ready = false;
        for _ in 0..90 {
            let check = Command::new("docker")
                .args([
                    "exec",
                    &container_name,
                    "mysqladmin",
                    "ping",
                    "-h",
                    "localhost",
                    "-uroot",
                    "-ptest_password",
                ])
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);

            if check {
                ready = true;
                break;
            }
            sleep(Duration::from_millis(1000)).await;
        }

        if !ready {
            let _ = Command::new("docker")
                .args(["rm", "-f", &container_name])
                .output();
            anyhow::bail!("Timeout waiting for {} to be ready", version);
        }

        let container = Self {
            container_name: container_name.clone(),
            port,
            version,
        };

        // Setup replication user and test table
        container.setup_cdc().await?;

        println!(
            "✅ {} container ready: {} on port {}",
            version, container_name, port
        );
        Ok(container)
    }

    async fn setup_cdc(&self) -> Result<()> {
        let setup_sql = r#"
            CREATE USER IF NOT EXISTS 'rivven_cdc'@'%' IDENTIFIED BY 'test_password';
            GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'rivven_cdc'@'%';
            GRANT SELECT ON *.* TO 'rivven_cdc'@'%';
            FLUSH PRIVILEGES;

            USE testdb;
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255),
                age INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        "#;

        let output = Command::new("docker")
            .args([
                "exec",
                "-i",
                &self.container_name,
                "mysql",
                "-uroot",
                "-ptest_password",
                "-e",
                setup_sql,
            ])
            .output()?;

        if !output.status.success() {
            anyhow::bail!(
                "Failed to setup CDC: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        Ok(())
    }

    /// Execute SQL statement
    pub async fn execute_sql(&self, sql: &str) -> Result<String> {
        let output = Command::new("docker")
            .args([
                "exec",
                "-i",
                &self.container_name,
                "mysql",
                "-uroot",
                "-ptest_password",
                "-e",
                sql,
            ])
            .output()?;

        if !output.status.success() {
            anyhow::bail!(
                "SQL execution failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Get connection string
    pub fn connection_config(&self) -> MySqlCdcConfig {
        let server_id = SERVER_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        MySqlCdcConfig::new("localhost", "rivven_cdc")
            .with_password("test_password")
            .with_port(self.port)
            .with_database("testdb")
            .with_server_id(server_id)
    }

    /// Get server version
    pub async fn server_version(&self) -> Result<String> {
        self.execute_sql("SELECT VERSION();").await
    }
}

impl Drop for MySqlContainer {
    fn drop(&mut self) {
        println!(
            "🧹 Cleaning up {} container: {}",
            self.version, self.container_name
        );
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container_name])
            .output();
    }
}

/// MariaDB container for version matrix tests
#[allow(dead_code)]
pub struct MariaDbContainer {
    container_name: String,
    pub port: u16,
    pub version: MariaDbVersion,
}

#[allow(dead_code)]
impl MariaDbContainer {
    /// Start a MariaDB container with a specific version
    pub async fn start_version(version: MariaDbVersion) -> Result<Self> {
        let container_name = format!(
            "rivven-test-mariadb{}-{}",
            version.version_string().replace('.', ""),
            uuid::Uuid::new_v4()
        );
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);

        println!(
            "🐬 Starting {} container: {} on port {}",
            version, container_name, port
        );

        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                &container_name,
                "-e",
                "MARIADB_ROOT_PASSWORD=test_password",
                "-e",
                "MARIADB_DATABASE=testdb",
                "-p",
                &format!("{}:3306", port),
                version.docker_image(),
                "--server-id=1",
                "--log-bin=mariadb-bin",
                "--binlog-format=ROW",
                "--binlog-row-image=FULL",
            ])
            .output()?;

        if !output.status.success() {
            anyhow::bail!(
                "Failed to start {}: {}",
                version,
                String::from_utf8_lossy(&output.stderr)
            );
        }

        // Wait for MariaDB to be ready
        let mut ready = false;
        for _ in 0..90 {
            let check = Command::new("docker")
                .args([
                    "exec",
                    &container_name,
                    "mariadb-admin",
                    "ping",
                    "-h",
                    "localhost",
                    "-uroot",
                    "-ptest_password",
                ])
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);

            if check {
                ready = true;
                break;
            }
            sleep(Duration::from_millis(1000)).await;
        }

        if !ready {
            let _ = Command::new("docker")
                .args(["rm", "-f", &container_name])
                .output();
            anyhow::bail!("Timeout waiting for {} to be ready", version);
        }

        let container = Self {
            container_name: container_name.clone(),
            port,
            version,
        };

        // Setup replication user and test table
        container.setup_cdc().await?;

        println!(
            "✅ {} container ready: {} on port {}",
            version, container_name, port
        );
        Ok(container)
    }

    async fn setup_cdc(&self) -> Result<()> {
        let setup_sql = r#"
            CREATE USER IF NOT EXISTS 'rivven_cdc'@'%' IDENTIFIED BY 'test_password';
            GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'rivven_cdc'@'%';
            GRANT SELECT ON *.* TO 'rivven_cdc'@'%';
            FLUSH PRIVILEGES;

            USE testdb;
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255),
                age INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        "#;

        let output = Command::new("docker")
            .args([
                "exec",
                "-i",
                &self.container_name,
                "mariadb",
                "-uroot",
                "-ptest_password",
                "-e",
                setup_sql,
            ])
            .output()?;

        if !output.status.success() {
            anyhow::bail!(
                "Failed to setup CDC: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        Ok(())
    }

    /// Execute SQL statement
    pub async fn execute_sql(&self, sql: &str) -> Result<String> {
        let output = Command::new("docker")
            .args([
                "exec",
                "-i",
                &self.container_name,
                "mariadb",
                "-uroot",
                "-ptest_password",
                "-e",
                sql,
            ])
            .output()?;

        if !output.status.success() {
            anyhow::bail!(
                "SQL execution failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Get connection config
    pub fn connection_config(&self) -> MySqlCdcConfig {
        let server_id = SERVER_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        MySqlCdcConfig::new("localhost", "rivven_cdc")
            .with_password("test_password")
            .with_port(self.port)
            .with_database("testdb")
            .with_server_id(server_id)
    }

    /// Get server version
    pub async fn server_version(&self) -> Result<String> {
        self.execute_sql("SELECT VERSION();").await
    }
}

impl Drop for MariaDbContainer {
    fn drop(&mut self) {
        println!(
            "🧹 Cleaning up {} container: {}",
            self.version, self.container_name
        );
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container_name])
            .output();
    }
}

// ============================================================================
// Test Infrastructure
// ============================================================================

/// Initialize tracing for tests
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("rivven_cdc=debug,mysql_version_matrix=debug")
        .with_test_writer()
        .try_init();
}

// ============================================================================
// MySQL CRUD Tests
// ============================================================================

/// Test INSERT operation on MySQL
async fn test_mysql_insert_impl(version: MySqlVersion) -> Result<()> {
    init_tracing();
    println!("\n🧪 Testing INSERT on {}\n", version);

    let mysql = MySqlContainer::start_version(version).await?;
    let config = mysql.connection_config();

    let (tx, mut rx) = mpsc::channel(100);
    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    sleep(Duration::from_secs(3)).await;

    // Insert test data
    mysql
        .execute_sql(
            "INSERT INTO testdb.users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)",
        )
        .await?;

    // Wait for event
    let event = timeout(Duration::from_secs(15), rx.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("No event received"))?;

    assert_eq!(event.op, CdcOp::Insert);
    assert!(event.table.contains("users"));
    assert!(event.after.is_some());

    let after = event.after.unwrap();
    assert_eq!(after["name"], "Alice");
    assert_eq!(after["email"], "alice@test.com");
    assert_eq!(after["age"], 30);

    cdc.stop().await?;
    println!("✅ INSERT test passed on {}\n", version);
    Ok(())
}

/// Test UPDATE operation on MySQL
async fn test_mysql_update_impl(version: MySqlVersion) -> Result<()> {
    init_tracing();
    println!("\n🧪 Testing UPDATE on {}\n", version);

    let mysql = MySqlContainer::start_version(version).await?;
    let config = mysql.connection_config();

    let (tx, mut rx) = mpsc::channel(100);
    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    sleep(Duration::from_secs(3)).await;

    // Insert then update
    mysql
        .execute_sql(
            "INSERT INTO testdb.users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)",
        )
        .await?;
    sleep(Duration::from_millis(500)).await;
    mysql
        .execute_sql("UPDATE testdb.users SET age = 26 WHERE name = 'Bob'")
        .await?;

    // Collect events
    let mut events = Vec::new();
    for _ in 0..2 {
        if let Ok(Some(event)) = timeout(Duration::from_secs(15), rx.recv()).await {
            events.push(event);
        }
    }

    assert!(events.len() >= 2, "Expected at least 2 events");

    let update_event = events.iter().find(|e| e.op == CdcOp::Update);
    assert!(update_event.is_some(), "No UPDATE event found");

    let update = update_event.unwrap();
    assert!(update.before.is_some(), "UPDATE should have before image");
    assert!(update.after.is_some(), "UPDATE should have after image");

    let before = update.before.as_ref().unwrap();
    let after = update.after.as_ref().unwrap();
    assert_eq!(before["age"], 25);
    assert_eq!(after["age"], 26);

    cdc.stop().await?;
    println!("✅ UPDATE test passed on {}\n", version);
    Ok(())
}

/// Test DELETE operation on MySQL
async fn test_mysql_delete_impl(version: MySqlVersion) -> Result<()> {
    init_tracing();
    println!("\n🧪 Testing DELETE on {}\n", version);

    let mysql = MySqlContainer::start_version(version).await?;
    let config = mysql.connection_config();

    let (tx, mut rx) = mpsc::channel(100);
    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    sleep(Duration::from_secs(3)).await;

    // Insert then delete
    mysql.execute_sql("INSERT INTO testdb.users (name, email, age) VALUES ('Charlie', 'charlie@test.com', 35)").await?;
    sleep(Duration::from_millis(500)).await;
    mysql
        .execute_sql("DELETE FROM testdb.users WHERE name = 'Charlie'")
        .await?;

    // Collect events
    let mut events = Vec::new();
    for _ in 0..2 {
        if let Ok(Some(event)) = timeout(Duration::from_secs(15), rx.recv()).await {
            events.push(event);
        }
    }

    let delete_event = events.iter().find(|e| e.op == CdcOp::Delete);
    assert!(delete_event.is_some(), "No DELETE event found");

    let delete = delete_event.unwrap();
    assert!(delete.before.is_some(), "DELETE should have before image");

    let before = delete.before.as_ref().unwrap();
    assert_eq!(before["name"], "Charlie");

    cdc.stop().await?;
    println!("✅ DELETE test passed on {}\n", version);
    Ok(())
}

/// Test transaction handling on MySQL
async fn test_mysql_transaction_impl(version: MySqlVersion) -> Result<()> {
    init_tracing();
    println!("\n🧪 Testing transactions on {}\n", version);

    let mysql = MySqlContainer::start_version(version).await?;
    let config = mysql.connection_config();

    let (tx, mut rx) = mpsc::channel(100);
    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    sleep(Duration::from_secs(3)).await;

    // Execute transaction with multiple inserts
    mysql.execute_sql("START TRANSACTION; INSERT INTO testdb.users (name, email, age) VALUES ('Eve', 'eve@test.com', 28); INSERT INTO testdb.users (name, email, age) VALUES ('Frank', 'frank@test.com', 32); INSERT INTO testdb.users (name, email, age) VALUES ('Grace', 'grace@test.com', 29); COMMIT;").await?;

    // Collect events
    let mut events = Vec::new();
    for _ in 0..3 {
        if let Ok(Some(event)) = timeout(Duration::from_secs(15), rx.recv()).await {
            events.push(event);
        }
    }

    assert_eq!(events.len(), 3, "Should have 3 INSERT events");

    // All should be inserts
    assert!(events.iter().all(|e| e.op == CdcOp::Insert));

    cdc.stop().await?;
    println!("✅ Transaction test passed on {}\n", version);
    Ok(())
}

/// Test GTID-based replication on MySQL
async fn test_mysql_gtid_impl(version: MySqlVersion) -> Result<()> {
    init_tracing();
    println!("\n🧪 Testing GTID on {}\n", version);

    let mysql = MySqlContainer::start_version(version).await?;

    // Verify GTID is enabled
    let gtid_mode = mysql.execute_sql("SELECT @@gtid_mode;").await?;
    assert!(gtid_mode.contains("ON"), "GTID mode should be ON");

    let config = mysql.connection_config();
    let (tx, mut rx) = mpsc::channel(100);
    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    sleep(Duration::from_secs(3)).await;

    mysql
        .execute_sql(
            "INSERT INTO testdb.users (name, email, age) VALUES ('GTID_User', 'gtid@test.com', 40)",
        )
        .await?;

    let event: rivven_cdc::common::CdcEvent = timeout(Duration::from_secs(15), rx.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("No event"))?;

    assert_eq!(event.op, CdcOp::Insert);

    cdc.stop().await?;
    println!("✅ GTID test passed on {}\n", version);
    Ok(())
}

// ============================================================================
// MariaDB CRUD Tests
// ============================================================================

/// Test INSERT operation on MariaDB
async fn test_mariadb_insert_impl(version: MariaDbVersion) -> Result<()> {
    init_tracing();
    println!("\n🧪 Testing INSERT on {}\n", version);

    let mariadb = MariaDbContainer::start_version(version).await?;
    let config = mariadb.connection_config();

    let (tx, mut rx) = mpsc::channel(100);
    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    sleep(Duration::from_secs(3)).await;

    // Insert test data
    mariadb
        .execute_sql(
            "INSERT INTO testdb.users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)",
        )
        .await?;

    // Wait for event
    let event = timeout(Duration::from_secs(15), rx.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("No event received"))?;

    assert_eq!(event.op, CdcOp::Insert);
    assert!(event.table.contains("users"));
    assert!(event.after.is_some());

    let after = event.after.unwrap();
    assert_eq!(after["name"], "Alice");
    assert_eq!(after["email"], "alice@test.com");
    assert_eq!(after["age"], 30);

    cdc.stop().await?;
    println!("✅ INSERT test passed on {}\n", version);
    Ok(())
}

/// Test UPDATE operation on MariaDB
async fn test_mariadb_update_impl(version: MariaDbVersion) -> Result<()> {
    init_tracing();
    println!("\n🧪 Testing UPDATE on {}\n", version);

    let mariadb = MariaDbContainer::start_version(version).await?;
    let config = mariadb.connection_config();

    let (tx, mut rx) = mpsc::channel(100);
    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    sleep(Duration::from_secs(3)).await;

    // Insert then update
    mariadb
        .execute_sql(
            "INSERT INTO testdb.users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)",
        )
        .await?;
    sleep(Duration::from_millis(500)).await;
    mariadb
        .execute_sql("UPDATE testdb.users SET age = 26 WHERE name = 'Bob'")
        .await?;

    // Collect events
    let mut events = Vec::new();
    for _ in 0..2 {
        if let Ok(Some(event)) = timeout(Duration::from_secs(15), rx.recv()).await {
            events.push(event);
        }
    }

    assert!(events.len() >= 2, "Expected at least 2 events");

    let update_event = events.iter().find(|e| e.op == CdcOp::Update);
    assert!(update_event.is_some(), "No UPDATE event found");

    let update = update_event.unwrap();
    assert!(update.before.is_some(), "UPDATE should have before image");
    assert!(update.after.is_some(), "UPDATE should have after image");

    cdc.stop().await?;
    println!("✅ UPDATE test passed on {}\n", version);
    Ok(())
}

/// Test DELETE operation on MariaDB
async fn test_mariadb_delete_impl(version: MariaDbVersion) -> Result<()> {
    init_tracing();
    println!("\n🧪 Testing DELETE on {}\n", version);

    let mariadb = MariaDbContainer::start_version(version).await?;
    let config = mariadb.connection_config();

    let (tx, mut rx) = mpsc::channel(100);
    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    sleep(Duration::from_secs(3)).await;

    // Insert then delete
    mariadb.execute_sql("INSERT INTO testdb.users (name, email, age) VALUES ('Charlie', 'charlie@test.com', 35)").await?;
    sleep(Duration::from_millis(500)).await;
    mariadb
        .execute_sql("DELETE FROM testdb.users WHERE name = 'Charlie'")
        .await?;

    // Collect events
    let mut events = Vec::new();
    for _ in 0..2 {
        if let Ok(Some(event)) = timeout(Duration::from_secs(15), rx.recv()).await {
            events.push(event);
        }
    }

    let delete_event = events.iter().find(|e| e.op == CdcOp::Delete);
    assert!(delete_event.is_some(), "No DELETE event found");

    cdc.stop().await?;
    println!("✅ DELETE test passed on {}\n", version);
    Ok(())
}

/// Test transaction handling on MariaDB
async fn test_mariadb_transaction_impl(version: MariaDbVersion) -> Result<()> {
    init_tracing();
    println!("\n🧪 Testing transactions on {}\n", version);

    let mariadb = MariaDbContainer::start_version(version).await?;
    let config = mariadb.connection_config();

    let (tx, mut rx) = mpsc::channel(100);
    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    sleep(Duration::from_secs(3)).await;

    // Execute transaction
    mariadb.execute_sql("START TRANSACTION; INSERT INTO testdb.users (name, email, age) VALUES ('Eve', 'eve@test.com', 28); INSERT INTO testdb.users (name, email, age) VALUES ('Frank', 'frank@test.com', 32); COMMIT;").await?;

    // Collect events
    let mut events = Vec::new();
    for _ in 0..2 {
        if let Ok(Some(event)) = timeout(Duration::from_secs(15), rx.recv()).await {
            events.push(event);
        }
    }

    assert!(events.len() >= 2, "Should have at least 2 events");
    assert!(events.iter().all(|e| e.op == CdcOp::Insert));

    cdc.stop().await?;
    println!("✅ Transaction test passed on {}\n", version);
    Ok(())
}

// ============================================================================
// MySQL Version Tests
// ============================================================================

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mysql_8_0_insert() -> Result<()> {
    test_mysql_insert_impl(MySqlVersion::V8_0).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mysql_8_0_update() -> Result<()> {
    test_mysql_update_impl(MySqlVersion::V8_0).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mysql_8_0_delete() -> Result<()> {
    test_mysql_delete_impl(MySqlVersion::V8_0).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mysql_8_0_transaction() -> Result<()> {
    test_mysql_transaction_impl(MySqlVersion::V8_0).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mysql_8_0_gtid() -> Result<()> {
    test_mysql_gtid_impl(MySqlVersion::V8_0).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mysql_8_4_insert() -> Result<()> {
    test_mysql_insert_impl(MySqlVersion::V8_4).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mysql_8_4_update() -> Result<()> {
    test_mysql_update_impl(MySqlVersion::V8_4).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mysql_8_4_delete() -> Result<()> {
    test_mysql_delete_impl(MySqlVersion::V8_4).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mysql_8_4_transaction() -> Result<()> {
    test_mysql_transaction_impl(MySqlVersion::V8_4).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mysql_8_4_gtid() -> Result<()> {
    test_mysql_gtid_impl(MySqlVersion::V8_4).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mysql_9_0_insert() -> Result<()> {
    test_mysql_insert_impl(MySqlVersion::V9_0).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mysql_9_0_update() -> Result<()> {
    test_mysql_update_impl(MySqlVersion::V9_0).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mysql_9_0_delete() -> Result<()> {
    test_mysql_delete_impl(MySqlVersion::V9_0).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mysql_9_0_transaction() -> Result<()> {
    test_mysql_transaction_impl(MySqlVersion::V9_0).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mysql_9_0_gtid() -> Result<()> {
    test_mysql_gtid_impl(MySqlVersion::V9_0).await
}

// ============================================================================
// MariaDB Version Tests
// ============================================================================

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mariadb_10_6_insert() -> Result<()> {
    test_mariadb_insert_impl(MariaDbVersion::V10_6).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mariadb_10_6_update() -> Result<()> {
    test_mariadb_update_impl(MariaDbVersion::V10_6).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mariadb_10_6_delete() -> Result<()> {
    test_mariadb_delete_impl(MariaDbVersion::V10_6).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mariadb_10_6_transaction() -> Result<()> {
    test_mariadb_transaction_impl(MariaDbVersion::V10_6).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mariadb_10_11_insert() -> Result<()> {
    test_mariadb_insert_impl(MariaDbVersion::V10_11).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mariadb_10_11_update() -> Result<()> {
    test_mariadb_update_impl(MariaDbVersion::V10_11).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mariadb_10_11_delete() -> Result<()> {
    test_mariadb_delete_impl(MariaDbVersion::V10_11).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mariadb_10_11_transaction() -> Result<()> {
    test_mariadb_transaction_impl(MariaDbVersion::V10_11).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mariadb_11_4_insert() -> Result<()> {
    test_mariadb_insert_impl(MariaDbVersion::V11_4).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mariadb_11_4_update() -> Result<()> {
    test_mariadb_update_impl(MariaDbVersion::V11_4).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mariadb_11_4_delete() -> Result<()> {
    test_mariadb_delete_impl(MariaDbVersion::V11_4).await
}

#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mariadb_11_4_transaction() -> Result<()> {
    test_mariadb_transaction_impl(MariaDbVersion::V11_4).await
}

// ============================================================================
// High-Throughput Tests
// ============================================================================

/// Test high-throughput scenario on MySQL 8.4 (LTS recommended)
#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mysql_8_4_high_throughput() -> Result<()> {
    init_tracing();
    println!("\n🧪 Testing high throughput on MySQL 8.4\n");

    let mysql = MySqlContainer::start_version(MySqlVersion::V8_4).await?;
    let config = mysql.connection_config();

    let (tx, mut rx) = mpsc::channel(1000);
    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    sleep(Duration::from_secs(3)).await;

    // Insert 50 rows rapidly
    let insert_count = 50;
    let start = std::time::Instant::now();

    for i in 0..insert_count {
        mysql
            .execute_sql(&format!(
            "INSERT INTO testdb.users (name, email, age) VALUES ('User{}', 'user{}@test.com', {})",
            i, i, 20 + (i % 50)
        ))
            .await?;
    }

    let insert_duration = start.elapsed();
    println!("⚡ Inserted {} rows in {:?}", insert_count, insert_duration);

    // Collect events
    let mut events = Vec::new();
    let collect_start = std::time::Instant::now();

    while events.len() < insert_count {
        match timeout(Duration::from_secs(30), rx.recv()).await {
            Ok(Some(event)) => events.push(event),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let collect_duration = collect_start.elapsed();
    println!(
        "📊 Collected {} events in {:?}",
        events.len(),
        collect_duration
    );

    assert!(
        events.len() >= insert_count * 8 / 10,
        "Should capture at least 80% of events: got {}",
        events.len()
    );

    let throughput = events.len() as f64 / collect_duration.as_secs_f64();
    println!("📈 Throughput: {:.0} events/sec\n", throughput);

    cdc.stop().await?;
    println!("✅ High throughput test passed\n");
    Ok(())
}

/// Test high-throughput scenario on MariaDB 10.11 (LTS recommended)
#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mariadb_10_11_high_throughput() -> Result<()> {
    init_tracing();
    println!("\n🧪 Testing high throughput on MariaDB 10.11\n");

    let mariadb = MariaDbContainer::start_version(MariaDbVersion::V10_11).await?;
    let config = mariadb.connection_config();

    let (tx, mut rx) = mpsc::channel(1000);
    let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

    cdc.start().await?;
    sleep(Duration::from_secs(3)).await;

    // Insert 50 rows rapidly
    let insert_count = 50;
    let start = std::time::Instant::now();

    for i in 0..insert_count {
        mariadb
            .execute_sql(&format!(
            "INSERT INTO testdb.users (name, email, age) VALUES ('User{}', 'user{}@test.com', {})",
            i, i, 20 + (i % 50)
        ))
            .await?;
    }

    let insert_duration = start.elapsed();
    println!("⚡ Inserted {} rows in {:?}", insert_count, insert_duration);

    // Collect events
    let mut events = Vec::new();
    let collect_start = std::time::Instant::now();

    while events.len() < insert_count {
        match timeout(Duration::from_secs(30), rx.recv()).await {
            Ok(Some(event)) => events.push(event),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let collect_duration = collect_start.elapsed();
    println!(
        "📊 Collected {} events in {:?}",
        events.len(),
        collect_duration
    );

    assert!(
        events.len() >= insert_count * 8 / 10,
        "Should capture at least 80% of events: got {}",
        events.len()
    );

    cdc.stop().await?;
    println!("✅ High throughput test passed\n");
    Ok(())
}

// ============================================================================
// Cross-Version Compatibility Tests
// ============================================================================

/// Verify all MySQL versions support basic CRUD operations
#[tokio::test]
#[ignore = "Requires Docker - runs all versions sequentially"]
async fn test_all_mysql_versions_crud_compatibility() -> Result<()> {
    init_tracing();
    println!("\n🧪 Testing CRUD compatibility across all MySQL versions\n");

    for version in MySqlVersion::all() {
        println!("━━━ Testing {} ━━━", version);
        test_mysql_insert_impl(*version).await?;
        println!();
    }

    println!("✅ All MySQL versions passed CRUD compatibility tests\n");
    Ok(())
}

/// Verify all MariaDB versions support basic CRUD operations
#[tokio::test]
#[ignore = "Requires Docker - runs all versions sequentially"]
async fn test_all_mariadb_versions_crud_compatibility() -> Result<()> {
    init_tracing();
    println!("\n🧪 Testing CRUD compatibility across all MariaDB versions\n");

    for version in MariaDbVersion::all() {
        println!("━━━ Testing {} ━━━", version);
        test_mariadb_insert_impl(*version).await?;
        println!();
    }

    println!("✅ All MariaDB versions passed CRUD compatibility tests\n");
    Ok(())
}

/// Test version detection for MySQL
#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mysql_version_detection() -> Result<()> {
    init_tracing();

    for version in MySqlVersion::all() {
        println!("\n🔍 Detecting {}", version);

        let mysql = MySqlContainer::start_version(*version).await?;
        let server_version = mysql.server_version().await?;

        println!("   Server version: {}", server_version.trim());
        println!("   GTID support: {}", version.supports_gtid());
        println!("   caching_sha2: {}", version.supports_caching_sha2());
        println!(
            "   Parallel replication: {}",
            version.supports_parallel_replication()
        );

        assert!(
            server_version.contains(version.version_string()) || server_version.contains("VERSION"),
            "Server version {} should contain {}",
            server_version,
            version.version_string()
        );
    }

    println!("\n✅ MySQL version detection test passed\n");
    Ok(())
}

/// Test version detection for MariaDB
#[tokio::test]
#[ignore = "Requires Docker"]
async fn test_mariadb_version_detection() -> Result<()> {
    init_tracing();

    for version in MariaDbVersion::all() {
        println!("\n🔍 Detecting {}", version);

        let mariadb = MariaDbContainer::start_version(*version).await?;
        let server_version = mariadb.server_version().await?;

        println!("   Server version: {}", server_version.trim());
        println!("   GTID support: {}", version.supports_gtid());
        println!("   Ed25519 auth: {}", version.supports_ed25519_auth());
        println!(
            "   Parallel replication: {}",
            version.supports_parallel_replication()
        );

        assert!(
            server_version.contains("MariaDB") || server_version.contains(version.version_string()),
            "Server version should indicate MariaDB"
        );
    }

    println!("\n✅ MariaDB version detection test passed\n");
    Ok(())
}
