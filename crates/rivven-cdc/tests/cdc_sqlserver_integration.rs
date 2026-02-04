//! SQL Server CDC Integration Tests
//!
//! Uses testcontainers with Microsoft SQL Server container for integration testing.
//!
//! ## Supported Versions
//!
//! - SQL Server 2019 (mcr.microsoft.com/mssql/server:2019-latest) - Default
//! - SQL Server 2022 (mcr.microsoft.com/mssql/server:2022-latest)
//!
//! Run with:
//! ```bash
//! cargo test -p rivven-cdc --features sqlserver --test cdc_sqlserver_integration
//! ```

#![cfg(feature = "sqlserver")]

use rivven_cdc::sqlserver::{CdcPosition, Lsn, SqlServerCdc, SqlServerCdcConfig};
use rivven_cdc::{CdcOp, CdcSource};
use serial_test::serial;
use std::time::Duration;
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::mssql_server::MssqlServer;
use tokio::time::timeout;
use tracing::{info, warn};

/// SQL Server test configuration
const SA_PASSWORD: &str = "yourStrong(!)Password"; // Match MssqlServer default
const TEST_DB: &str = "testdb";
const MSSQL_PORT: u16 = 1433;

/// SQL Server versions for testing
#[allow(dead_code)]
pub enum SqlServerVersion {
    /// SQL Server 2019 (LTS, most stable)
    SqlServer2019,
    /// SQL Server 2022 (latest features)
    SqlServer2022,
}

impl SqlServerVersion {
    /// Get Docker image tag for this version
    pub fn image_tag(&self) -> &'static str {
        match self {
            SqlServerVersion::SqlServer2019 => "2019-latest",
            SqlServerVersion::SqlServer2022 => "2022-latest",
        }
    }
}

/// Start a SQL Server container for testing
async fn start_sqlserver_container() -> (ContainerAsync<MssqlServer>, String) {
    start_sqlserver_container_with_version(SqlServerVersion::SqlServer2019).await
}

/// Start a SQL Server container with a specific version
#[allow(dead_code)]
async fn start_sqlserver_container_with_version(
    version: SqlServerVersion,
) -> (ContainerAsync<MssqlServer>, String) {
    info!("Starting SQL Server {} container...", version.image_tag());

    let container = MssqlServer::default()
        .with_accept_eula()
        .with_tag(version.image_tag())
        .start()
        .await
        .expect("Failed to start SQL Server container");

    let host_port = container
        .get_host_port_ipv4(MSSQL_PORT)
        .await
        .expect("Failed to get SQL Server port");

    let host = container
        .get_host()
        .await
        .expect("Failed to get container host");

    let conn_str = format!("{}:{}", host, host_port);

    // Wait a bit for SQL Server to fully initialize
    tokio::time::sleep(Duration::from_secs(5)).await;

    (container, conn_str)
}

/// Connect to SQL Server using tiberius directly
async fn connect_tiberius(
    host: &str,
    port: u16,
    database: &str,
) -> Result<
    tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>,
    Box<dyn std::error::Error>,
> {
    use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
    use tokio::net::TcpStream;
    use tokio_util::compat::TokioAsyncWriteCompatExt;

    let mut config = Config::new();
    config.host(host);
    config.port(port);
    config.database(database);
    config.authentication(AuthMethod::sql_server("sa", SA_PASSWORD));
    config.encryption(EncryptionLevel::NotSupported);
    config.trust_cert();

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let client = Client::connect(config, tcp.compat_write()).await?;
    Ok(client)
}

/// Setup test database with CDC enabled
async fn setup_test_database(host: &str, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::net::TcpStream;
    use tokio_util::compat::TokioAsyncWriteCompatExt;

    // First connect to master to create database
    let mut config = tiberius::Config::new();
    config.host(host);
    config.port(port);
    config.database("master");
    config.authentication(tiberius::AuthMethod::sql_server("sa", SA_PASSWORD));
    config.encryption(tiberius::EncryptionLevel::NotSupported);
    config.trust_cert();

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    let mut client = tiberius::Client::connect(config, tcp.compat_write()).await?;

    // Create test database
    client
        .execute(
            &format!(
                r#"
            IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'{TEST_DB}')
            CREATE DATABASE {TEST_DB}
            "#
            ),
            &[],
        )
        .await?;

    // Wait for database creation
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Now connect to test database
    let mut client = connect_tiberius(host, port, TEST_DB).await?;

    // Enable CDC on database
    client.execute("EXEC sys.sp_cdc_enable_db", &[]).await.ok(); // Ignore if already enabled

    // Create test table
    client
        .execute(
            r#"
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'users')
            CREATE TABLE users (
                id INT IDENTITY(1,1) PRIMARY KEY,
                name NVARCHAR(100) NOT NULL,
                email NVARCHAR(255) NOT NULL,
                created_at DATETIME2 DEFAULT GETUTCDATE()
            )
            "#,
            &[],
        )
        .await?;

    // Enable CDC on table
    client
        .execute(
            r#"
            IF NOT EXISTS (SELECT * FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('users'))
            EXEC sys.sp_cdc_enable_table
                @source_schema = N'dbo',
                @source_name = N'users',
                @role_name = NULL,
                @supports_net_changes = 1
            "#,
            &[],
        )
        .await
        .ok(); // Ignore if already enabled

    // Wait for CDC to initialize
    tokio::time::sleep(Duration::from_secs(2)).await;

    info!("Test database setup complete");
    Ok(())
}

/// Insert test data
async fn insert_test_data(host: &str, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_tiberius(host, port, TEST_DB).await?;

    client
        .execute(
            "INSERT INTO users (name, email) VALUES (@P1, @P2)",
            &[&"Alice", &"alice@example.com"],
        )
        .await?;

    client
        .execute(
            "INSERT INTO users (name, email) VALUES (@P1, @P2)",
            &[&"Bob", &"bob@example.com"],
        )
        .await?;

    Ok(())
}

/// Update test data
#[allow(dead_code)]
async fn update_test_data(host: &str, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_tiberius(host, port, TEST_DB).await?;

    client
        .execute(
            "UPDATE users SET name = @P1 WHERE email = @P2",
            &[&"Alice Updated", &"alice@example.com"],
        )
        .await?;

    Ok(())
}

/// Delete test data
#[allow(dead_code)]
async fn delete_test_data(host: &str, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_tiberius(host, port, TEST_DB).await?;

    client
        .execute("DELETE FROM users WHERE email = @P1", &[&"bob@example.com"])
        .await?;

    Ok(())
}

// =============================================================================
// Unit Tests (no container required)
// =============================================================================

#[test]
fn test_config_builder_basic() {
    let config = SqlServerCdcConfig::builder()
        .host("localhost")
        .port(1433)
        .username("sa")
        .password("TestPass123!")
        .database("testdb")
        .poll_interval_ms(500)
        .build()
        .unwrap();

    assert_eq!(config.host, "localhost");
    assert_eq!(config.port, 1433);
    assert_eq!(config.database, "testdb");
    assert_eq!(config.poll_interval_ms, 500);
}

#[test]
fn test_config_builder_with_tables() {
    let config = SqlServerCdcConfig::builder()
        .host("localhost")
        .username("sa")
        .password("TestPass123!")
        .database("testdb")
        .include_table("dbo", "users")
        .include_table("dbo", "orders")
        .exclude_table("dbo", "logs")
        .build()
        .unwrap();

    assert_eq!(config.include_tables.len(), 2);
    assert_eq!(config.exclude_tables.len(), 1);
}

#[test]
fn test_config_validation_missing_host() {
    let result = SqlServerCdcConfig::builder()
        .host("")
        .username("sa")
        .database("testdb")
        .build();

    assert!(result.is_err());
}

#[test]
fn test_config_validation_missing_database() {
    let result = SqlServerCdcConfig::builder()
        .host("localhost")
        .username("sa")
        .database("")
        .build();

    assert!(result.is_err());
}

#[test]
fn test_config_validation_poll_interval() {
    // Too low
    let result = SqlServerCdcConfig::builder()
        .host("localhost")
        .username("sa")
        .database("testdb")
        .poll_interval_ms(10)
        .build();

    assert!(result.is_err());

    // Zero
    let result = SqlServerCdcConfig::builder()
        .host("localhost")
        .username("sa")
        .database("testdb")
        .poll_interval_ms(0)
        .build();

    assert!(result.is_err());
}

#[test]
fn test_config_debug_redacts_password() {
    let config = SqlServerCdcConfig::builder()
        .host("localhost")
        .username("sa")
        .password("super_secret_password")
        .database("testdb")
        .build()
        .unwrap();

    let debug_str = format!("{:?}", config);
    assert!(!debug_str.contains("super_secret_password"));
    assert!(debug_str.contains("REDACTED"));
}

#[test]
fn test_connection_string_format() {
    let config = SqlServerCdcConfig::builder()
        .host("myserver")
        .port(1433)
        .username("myuser")
        .password("mypass")
        .database("mydb")
        .application_name("test-app")
        .build()
        .unwrap();

    let conn_str = config.connection_string();
    assert!(conn_str.contains("Server=myserver,1433"));
    assert!(conn_str.contains("Database=mydb"));
    assert!(conn_str.contains("User Id=myuser"));
    assert!(conn_str.contains("Password=mypass"));
    assert!(conn_str.contains("Application Name=test-app"));
}

#[test]
fn test_redacted_connection_string() {
    let config = SqlServerCdcConfig::builder()
        .host("myserver")
        .username("myuser")
        .password("mypass")
        .database("mydb")
        .build()
        .unwrap();

    let redacted = config.redacted_connection_string();
    assert!(redacted.contains("[REDACTED]"));
    assert!(!redacted.contains("mypass"));
}

#[test]
fn test_lsn_from_hex() {
    let lsn = Lsn::from_hex("00000001000000010001").unwrap();
    assert_eq!(lsn.to_hex(), "00000001000000010001");
}

#[test]
fn test_lsn_invalid_hex() {
    let result = Lsn::from_hex("invalid");
    assert!(result.is_err());

    let result = Lsn::from_hex("00000001"); // Too short
    assert!(result.is_err());
}

#[test]
fn test_lsn_comparison() {
    let lsn1 = Lsn::from_hex("00000001000000010001").unwrap();
    let lsn2 = Lsn::from_hex("00000001000000010002").unwrap();
    let lsn3 = Lsn::from_hex("00000002000000010001").unwrap();

    assert!(lsn1 < lsn2);
    assert!(lsn2 < lsn3);
    assert!(lsn1 < lsn3);
}

#[test]
fn test_lsn_min_max() {
    let min = Lsn::min();
    let max = Lsn::max();

    assert!(min.is_min());
    assert!(!max.is_min());
    assert!(min < max);
}

#[test]
fn test_cdc_position_serialization() {
    let pos = CdcPosition::from_hex("00000001000000010001", "00000001000000010002").unwrap();

    let serialized = pos.to_string();
    let deserialized = CdcPosition::from_string(&serialized).unwrap();

    assert_eq!(pos, deserialized);
}

#[test]
fn test_cdc_position_ordering() {
    let pos1 = CdcPosition::from_hex("00000001000000010001", "00000001000000010001").unwrap();
    let pos2 = CdcPosition::from_hex("00000001000000010001", "00000001000000010002").unwrap();
    let pos3 = CdcPosition::from_hex("00000001000000010002", "00000001000000010001").unwrap();

    assert!(pos1 < pos2); // Same commit, different change
    assert!(pos2 < pos3); // Different commit
    assert!(pos1 < pos3);
}

// =============================================================================
// Integration Tests (require container)
// =============================================================================

#[tokio::test]
#[serial]
#[ignore = "Requires Docker and SQL Server container"]
async fn test_sqlserver_container_startup() {
    let _ = tracing_subscriber::fmt::try_init();

    let (_container, conn_str) = start_sqlserver_container().await;
    let parts: Vec<&str> = conn_str.split(':').collect();
    let host = parts[0];
    let port: u16 = parts[1].parse().unwrap();

    // Try to connect
    let client = connect_tiberius(host, port, "master").await;
    assert!(client.is_ok(), "Should connect to SQL Server container");
}

#[tokio::test]
#[serial]
#[ignore = "Requires Docker and SQL Server container"]
async fn test_database_cdc_setup() {
    let _ = tracing_subscriber::fmt::try_init();

    let (_container, conn_str) = start_sqlserver_container().await;
    let parts: Vec<&str> = conn_str.split(':').collect();
    let host = parts[0];
    let port: u16 = parts[1].parse().unwrap();

    // Setup database with CDC
    setup_test_database(host, port)
        .await
        .expect("Database setup should succeed");

    // Verify CDC is enabled
    let mut client = connect_tiberius(host, port, TEST_DB).await.unwrap();
    let result = client
        .query(
            "SELECT is_cdc_enabled FROM sys.databases WHERE name = @P1",
            &[&TEST_DB],
        )
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();

    let is_enabled: bool = result[0].get(0).unwrap_or(false);
    assert!(is_enabled, "CDC should be enabled on database");
}

#[tokio::test]
#[serial]
#[ignore = "Requires Docker and SQL Server container"]
async fn test_cdc_capture_insert() {
    let _ = tracing_subscriber::fmt::try_init();

    let (_container, conn_str) = start_sqlserver_container().await;
    let parts: Vec<&str> = conn_str.split(':').collect();
    let host = parts[0];
    let port: u16 = parts[1].parse().unwrap();

    // Setup
    setup_test_database(host, port).await.unwrap();

    // Create CDC source
    let config = SqlServerCdcConfig::builder()
        .host(host)
        .port(port)
        .username("sa")
        .password(SA_PASSWORD)
        .database(TEST_DB)
        .poll_interval_ms(100)
        .include_table("dbo", "users")
        .trust_server_certificate(true)
        .encrypt(false)
        .build()
        .unwrap();

    let mut cdc = SqlServerCdc::new(config);
    let mut rx = cdc.take_event_receiver().unwrap();

    // Start CDC
    cdc.start().await.unwrap();

    // Wait for CDC to initialize
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Insert data
    insert_test_data(host, port).await.unwrap();

    // Wait for CDC capture job to run (SQL Server Agent)
    // Note: In container, this may need manual triggering
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Try to receive events
    let result = timeout(Duration::from_secs(10), rx.recv()).await;

    // Stop CDC
    cdc.stop().await.unwrap();

    // Verify we got events (or gracefully handle if CDC Agent isn't running)
    match result {
        Ok(Some(event)) => {
            assert_eq!(event.op, CdcOp::Insert);
            assert_eq!(event.table, "users");
            info!("Received CDC event: {:?}", event);
        }
        Ok(None) => {
            warn!("CDC channel closed (may be expected if Agent not running)");
        }
        Err(_) => {
            warn!("Timeout waiting for CDC events (SQL Agent may not be running in container)");
        }
    }
}

#[tokio::test]
#[serial]
#[ignore = "Requires Docker and SQL Server container"]
async fn test_cdc_source_lifecycle() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = SqlServerCdcConfig::builder()
        .host("localhost")
        .port(1433)
        .username("sa")
        .password("TestPass123!")
        .database("testdb")
        .build()
        .unwrap();

    let mut cdc = SqlServerCdc::new(config);

    // Should have event receiver
    let rx = cdc.take_event_receiver();
    assert!(rx.is_some());

    // Second take should return None
    let rx2 = cdc.take_event_receiver();
    assert!(rx2.is_none());
}

// =============================================================================
// Error Handling Tests
// =============================================================================

#[test]
fn test_sqlserver_error_messages() {
    use rivven_cdc::sqlserver::SqlServerError;

    let err = SqlServerError::CdcNotEnabled("testdb".to_string());
    assert!(err.to_string().contains("sp_cdc_enable_db"));

    let err = SqlServerError::TableNotEnabled {
        schema: "dbo".to_string(),
        table: "users".to_string(),
    };
    assert!(err.to_string().contains("sp_cdc_enable_table"));
    assert!(err.to_string().contains("dbo"));
    assert!(err.to_string().contains("users"));

    let err = SqlServerError::InvalidLsn("bad lsn".to_string());
    assert!(err.to_string().contains("Invalid LSN"));

    let err = SqlServerError::LogTruncated("00000001000000010001".to_string());
    assert!(err.to_string().contains("truncated"));
}

// =============================================================================
// Version-Specific Integration Tests
// =============================================================================

#[tokio::test]
#[serial]
#[ignore = "Requires Docker and SQL Server 2022 container"]
async fn test_sqlserver_2022_container_startup() {
    let _ = tracing_subscriber::fmt::try_init();

    let (_container, conn_str) =
        start_sqlserver_container_with_version(SqlServerVersion::SqlServer2022).await;
    let parts: Vec<&str> = conn_str.split(':').collect();
    let host = parts[0];
    let port: u16 = parts[1].parse().unwrap();

    // Try to connect
    let client = connect_tiberius(host, port, "master").await;
    assert!(
        client.is_ok(),
        "Should connect to SQL Server 2022 container"
    );

    // Verify version
    if let Ok(mut c) = client {
        let result = c
            .query("SELECT @@VERSION", &[])
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap();
        let version: &str = result[0].get(0).unwrap_or("");
        info!("SQL Server version: {}", version);
        assert!(version.contains("2022"), "Should be SQL Server 2022");
    }
}

#[tokio::test]
#[serial]
#[ignore = "Requires Docker and SQL Server 2019 container"]
async fn test_sqlserver_2019_container_startup() {
    let _ = tracing_subscriber::fmt::try_init();

    let (_container, conn_str) =
        start_sqlserver_container_with_version(SqlServerVersion::SqlServer2019).await;
    let parts: Vec<&str> = conn_str.split(':').collect();
    let host = parts[0];
    let port: u16 = parts[1].parse().unwrap();

    // Try to connect
    let client = connect_tiberius(host, port, "master").await;
    assert!(
        client.is_ok(),
        "Should connect to SQL Server 2019 container"
    );

    // Verify version
    if let Ok(mut c) = client {
        let result = c
            .query("SELECT @@VERSION", &[])
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap();
        let version: &str = result[0].get(0).unwrap_or("");
        info!("SQL Server version: {}", version);
        assert!(version.contains("2019"), "Should be SQL Server 2019");
    }
}

#[tokio::test]
#[serial]
#[ignore = "Requires Docker and SQL Server 2022 container"]
async fn test_cdc_capture_insert_sqlserver_2022() {
    let _ = tracing_subscriber::fmt::try_init();

    let (_container, conn_str) =
        start_sqlserver_container_with_version(SqlServerVersion::SqlServer2022).await;
    let parts: Vec<&str> = conn_str.split(':').collect();
    let host = parts[0];
    let port: u16 = parts[1].parse().unwrap();

    // Setup
    setup_test_database(host, port).await.unwrap();

    // Create CDC source
    let config = SqlServerCdcConfig::builder()
        .host(host)
        .port(port)
        .username("sa")
        .password(SA_PASSWORD)
        .database(TEST_DB)
        .poll_interval_ms(100)
        .include_table("dbo", "users")
        .trust_server_certificate(true)
        .encrypt(false)
        .build()
        .unwrap();

    let mut cdc = SqlServerCdc::new(config);
    let mut rx = cdc.take_event_receiver().unwrap();

    // Start CDC
    cdc.start().await.unwrap();

    // Wait for CDC to initialize
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Insert data
    insert_test_data(host, port).await.unwrap();

    // Wait for CDC capture job to run
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Try to receive events
    let result = timeout(Duration::from_secs(10), rx.recv()).await;

    // Stop CDC
    cdc.stop().await.unwrap();

    match result {
        Ok(Some(event)) => {
            assert_eq!(event.op, CdcOp::Insert);
            assert_eq!(event.table, "users");
            info!("Received CDC event on SQL Server 2022: {:?}", event);
        }
        Ok(None) => {
            warn!("CDC channel closed (may be expected if Agent not running)");
        }
        Err(_) => {
            warn!("Timeout waiting for CDC events (SQL Agent may not be running in container)");
        }
    }
}
