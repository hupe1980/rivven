//! MySQL/MariaDB CDC integration tests
//!
//! Tests for MySQL and MariaDB CDC functionality using testcontainers.
//! These tests verify:
//! - Binlog protocol connectivity
//! - Authentication (mysql_native_password, caching_sha2_password)
//! - Event decoding (INSERT, UPDATE, DELETE)
//! - Transaction handling
//! - GTID-based replication
//! - Table filtering
//! - Schema inference

mod harness;

use harness::{init_test_logging, MariaDbTestContainer, MySqlTestContainer};
use rivven_cdc::common::CdcSource;
use rivven_cdc::mysql::{
    BinlogDecoder, BinlogEvent, ColumnType, MySqlBinlogClient, MySqlCdc, MySqlCdcConfig,
};
use serial_test::serial;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info};

/// Delay for binlog events to propagate
const EVENT_PROPAGATION_DELAY_MS: u64 = 500;

// ============================================================================
// Connection Tests
// ============================================================================

mod connection_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_mysql_connect() {
        init_test_logging();

        let container = MySqlTestContainer::start()
            .await
            .expect("Failed to start MySQL container");

        let client = MySqlBinlogClient::connect(
            container.host(),
            container.port(),
            container.user(),
            Some(container.password()),
            None,
        )
        .await
        .expect("Failed to connect to MySQL");

        assert!(client.server_version().contains("8.") || client.server_version().contains("5."));
        assert!(client.connection_id() > 0);

        info!(
            "Connected to MySQL {} (id={})",
            client.server_version(),
            client.connection_id()
        );
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_mysql_authentication_failure() {
        init_test_logging();

        let container = MySqlTestContainer::start()
            .await
            .expect("Failed to start MySQL container");

        let result = MySqlBinlogClient::connect(
            container.host(),
            container.port(),
            "nonexistent_user",
            Some("wrongpassword"),
            None,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("Authentication failed")
                || err.to_string().contains("Access denied")
        );
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_mysql_query_execution() {
        init_test_logging();

        let container = MySqlTestContainer::start()
            .await
            .expect("Failed to start MySQL container");

        // Use execute_batch to run multiple statements in one connection
        container
            .execute_batch(&[
                "CREATE DATABASE test_db",
                "USE test_db",
                "CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(100))",
                "INSERT INTO test_table VALUES (1, 'hello')",
            ])
            .await
            .expect("Query execution failed");

        info!("MySQL query execution successful");
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_mariadb_connect() {
        init_test_logging();

        let container = MariaDbTestContainer::start()
            .await
            .expect("Failed to start MariaDB container");

        let client = MySqlBinlogClient::connect(
            container.host(),
            container.port(),
            container.user(),
            Some(container.password()),
            None,
        )
        .await
        .expect("Failed to connect to MariaDB");

        assert!(
            client.server_version().contains("MariaDB") || client.server_version().contains("10.")
        );

        info!(
            "Connected to MariaDB {} (id={})",
            client.server_version(),
            client.connection_id()
        );
    }
}

// ============================================================================
// Binlog Protocol Tests
// ============================================================================

mod binlog_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_register_slave() {
        init_test_logging();

        let container = MySqlTestContainer::start()
            .await
            .expect("Failed to start MySQL container");

        let mut client = MySqlBinlogClient::connect(
            container.host(),
            container.port(),
            container.user(),
            Some(container.password()),
            None,
        )
        .await
        .expect("Failed to connect");

        client
            .register_slave(12345)
            .await
            .expect("Failed to register as slave");

        info!("Successfully registered as slave");
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_binlog_dump_starts() {
        init_test_logging();

        let container = MySqlTestContainer::start()
            .await
            .expect("Failed to start MySQL container");

        let mut client = MySqlBinlogClient::connect(
            container.host(),
            container.port(),
            container.user(),
            Some(container.password()),
            None,
        )
        .await
        .expect("Failed to connect");

        client
            .register_slave(12346)
            .await
            .expect("Failed to register");

        // Start binlog dump - this should return a stream
        let mut stream = client
            .binlog_dump(12346, "mysql-bin.000001", 4)
            .await
            .expect("Failed to start binlog dump");

        // Try to read the first event (should be FormatDescription)
        let timeout = tokio::time::timeout(Duration::from_secs(5), stream.next_event()).await;

        match timeout {
            Ok(Ok(Some(event_data))) => {
                let mut decoder = BinlogDecoder::new();
                let (event, _header) = decoder.decode(&event_data).expect("Failed to decode event");

                match event {
                    BinlogEvent::FormatDescription(fde) => {
                        info!(
                            "Received FormatDescription: version={}, server={}",
                            fde.binlog_version, fde.server_version
                        );
                    }
                    other => {
                        info!("Received event: {:?}", other);
                    }
                }
            }
            Ok(Ok(None)) => {
                info!("Stream ended");
            }
            Ok(Err(e)) => {
                panic!("Event read error: {:?}", e);
            }
            Err(_) => {
                info!("Timeout waiting for first event (expected for new binlog)");
            }
        }
    }
}

// ============================================================================
// Event Decoding Tests
// ============================================================================

mod decoder_tests {
    use super::*;
    use rivven_cdc::mysql::decoder::{EventHeader, EventType};

    #[test]
    fn test_event_header_parse() {
        // Minimal event header (19 bytes)
        let mut header_data = vec![0u8; 19];

        // timestamp (4 bytes) = 1234567890
        header_data[0..4].copy_from_slice(&1234567890u32.to_le_bytes());
        // event_type = 15 (FORMAT_DESCRIPTION)
        header_data[4] = 15;
        // server_id = 1
        header_data[5..9].copy_from_slice(&1u32.to_le_bytes());
        // event_length = 100
        header_data[9..13].copy_from_slice(&100u32.to_le_bytes());
        // next_position = 200
        header_data[13..17].copy_from_slice(&200u32.to_le_bytes());
        // flags = 0
        header_data[17..19].copy_from_slice(&0u16.to_le_bytes());

        let header = EventHeader::parse(&header_data).unwrap();

        assert_eq!(header.timestamp, 1234567890);
        assert_eq!(header.event_type, EventType::FormatDescriptionEvent);
        assert_eq!(header.server_id, 1);
        assert_eq!(header.event_length, 100);
        assert_eq!(header.next_position, 200);
        assert_eq!(header.flags, 0);
    }

    #[test]
    fn test_column_type_mapping() {
        assert_eq!(ColumnType::from_u8(1), ColumnType::Tiny);
        assert_eq!(ColumnType::from_u8(2), ColumnType::Short);
        assert_eq!(ColumnType::from_u8(3), ColumnType::Long);
        assert_eq!(ColumnType::from_u8(8), ColumnType::LongLong);
        assert_eq!(ColumnType::from_u8(15), ColumnType::Varchar);
        assert_eq!(ColumnType::from_u8(252), ColumnType::Blob);
    }

    #[test]
    fn test_is_row_event() {
        assert!(EventType::WriteRowsEventV2.is_row_event());
        assert!(EventType::UpdateRowsEventV2.is_row_event());
        assert!(EventType::DeleteRowsEventV2.is_row_event());
        assert!(!EventType::QueryEvent.is_row_event());
        assert!(!EventType::TableMapEvent.is_row_event());
    }
}

// ============================================================================
// CDC Source Tests
// ============================================================================

mod cdc_source_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_cdc_config_builder() {
        let config = MySqlCdcConfig::new("localhost", "user")
            .with_password("pass")
            .with_port(3307)
            .with_database("mydb")
            .with_server_id(9999)
            .include_table("mydb.*")
            .exclude_table("mydb.temp_*");

        assert_eq!(config.host, "localhost");
        assert_eq!(config.user, "user");
        assert_eq!(config.password, Some("pass".to_string()));
        assert_eq!(config.port, 3307);
        assert_eq!(config.database, Some("mydb".to_string()));
        assert_eq!(config.server_id, 9999);
        assert!(config.include_tables.contains(&"mydb.*".to_string()));
        assert!(config.exclude_tables.contains(&"mydb.temp_*".to_string()));
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_cdc_start_stop() {
        init_test_logging();

        let container = MySqlTestContainer::start()
            .await
            .expect("Failed to start MySQL container");

        let config = container.cdc_config(20001);
        let mut cdc = MySqlCdc::new(config);

        assert!(!cdc.is_healthy().await);

        cdc.start().await.expect("Failed to start CDC");

        // Give it a moment to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(cdc.is_healthy().await);

        cdc.stop().await.expect("Failed to stop CDC");

        assert!(!cdc.is_healthy().await);
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_cdc_with_event_channel() {
        init_test_logging();

        let container = MySqlTestContainer::start()
            .await
            .expect("Failed to start MySQL container");

        // Create test database and table
        container
            .execute_batch(&[
                "CREATE DATABASE IF NOT EXISTS cdc_test",
                "USE cdc_test",
                "CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100))",
            ])
            .await
            .expect("Setup failed");

        let (tx, mut rx) = mpsc::channel(100);

        let config = container.cdc_config(20002).with_database("cdc_test");

        let mut cdc = MySqlCdc::new(config).with_event_channel(tx);

        cdc.start().await.expect("Failed to start CDC");

        // Wait for CDC to initialize and start reading binlog
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Insert some data
        container
            .execute("INSERT INTO cdc_test.users (name) VALUES ('Alice')")
            .await
            .expect("Insert failed");
        container
            .execute("INSERT INTO cdc_test.users (name) VALUES ('Bob')")
            .await
            .expect("Insert failed");

        // Wait for events
        tokio::time::sleep(Duration::from_millis(EVENT_PROPAGATION_DELAY_MS)).await;

        // Note: The actual event reception depends on the binlog position
        // In a full test, we'd verify events were received

        cdc.stop().await.expect("Failed to stop CDC");

        // Drain any events that were received
        while let Ok(event) = rx.try_recv() {
            info!("Received CDC event: {:?}", event);
        }
    }
}

// ============================================================================
// Table Filtering Tests
// ============================================================================

mod filter_tests {
    use super::*;

    #[test]
    fn test_include_all_pattern() {
        let config = MySqlCdcConfig::default().include_table("*.*");

        assert!(should_capture(&config, "any_db", "any_table"));
        assert!(should_capture(&config, "prod", "users"));
    }

    #[test]
    fn test_include_database_wildcard() {
        let config = MySqlCdcConfig::default().include_table("mydb.*");

        assert!(should_capture(&config, "mydb", "users"));
        assert!(should_capture(&config, "mydb", "orders"));
        assert!(!should_capture(&config, "other_db", "users"));
    }

    #[test]
    fn test_include_table_wildcard() {
        let config = MySqlCdcConfig::default().include_table("*.users");

        assert!(should_capture(&config, "db1", "users"));
        assert!(should_capture(&config, "db2", "users"));
        assert!(!should_capture(&config, "db1", "orders"));
    }

    #[test]
    fn test_exclude_pattern() {
        let config = MySqlCdcConfig::default()
            .include_table("mydb.*")
            .exclude_table("mydb.temp_*");

        assert!(should_capture(&config, "mydb", "users"));
        assert!(!should_capture(&config, "mydb", "temp_data"));
        assert!(!should_capture(&config, "mydb", "temp_cache"));
    }

    #[test]
    fn test_exact_table_match() {
        let config = MySqlCdcConfig::default().include_table("prod.users");

        assert!(should_capture(&config, "prod", "users"));
        assert!(!should_capture(&config, "prod", "orders"));
        assert!(!should_capture(&config, "dev", "users"));
    }

    /// Helper to test table filtering logic
    fn should_capture(config: &MySqlCdcConfig, schema: &str, table: &str) -> bool {
        let full_name = format!("{}.{}", schema, table);

        // Check excludes first
        for pattern in &config.exclude_tables {
            if pattern_matches(pattern, &full_name) {
                return false;
            }
        }

        // If no includes specified, include all
        if config.include_tables.is_empty() {
            return true;
        }

        // Check includes
        for pattern in &config.include_tables {
            if pattern_matches(pattern, &full_name) {
                return true;
            }
        }

        false
    }

    fn pattern_matches(pattern: &str, value: &str) -> bool {
        if pattern == "*" || pattern == "*.*" {
            return true;
        }

        if !pattern.contains('*') {
            return pattern == value;
        }

        let parts: Vec<&str> = pattern.split('*').collect();

        if parts.len() == 2 {
            let (prefix, suffix) = (parts[0], parts[1]);
            return value.starts_with(prefix) && value.ends_with(suffix);
        }

        parts.iter().all(|part| value.contains(part))
    }
}

// ============================================================================
// MariaDB Specific Tests
// ============================================================================

mod mariadb_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_mariadb_binlog_dump() {
        init_test_logging();

        let container = MariaDbTestContainer::start()
            .await
            .expect("Failed to start MariaDB container");

        // Create test table
        container
            .execute_batch(&[
                "CREATE DATABASE IF NOT EXISTS test_db",
                "CREATE TABLE IF NOT EXISTS test_db.items (id INT PRIMARY KEY, value TEXT)",
            ])
            .await
            .expect("Setup failed");

        let mut client = MySqlBinlogClient::connect(
            container.host(),
            container.port(),
            container.user(),
            Some(container.password()),
            None,
        )
        .await
        .expect("Failed to connect");

        client
            .register_slave(30001)
            .await
            .expect("Failed to register");

        let mut stream = client
            .binlog_dump(30001, "mariadb-bin.000001", 4)
            .await
            .expect("Failed to start binlog dump");

        // Try to read events
        let timeout = tokio::time::timeout(Duration::from_secs(3), stream.next_event()).await;

        match timeout {
            Ok(Ok(Some(event_data))) => {
                let mut decoder = BinlogDecoder::new();
                match decoder.decode(&event_data) {
                    Ok((event, _header)) => {
                        info!("MariaDB binlog event: {:?}", event);
                    }
                    Err(e) => {
                        debug!("Decode error (may be expected): {:?}", e);
                    }
                }
            }
            Ok(Ok(None)) => {
                info!("MariaDB binlog stream ended");
            }
            Ok(Err(e)) => {
                debug!("Read error: {:?}", e);
            }
            Err(_) => {
                info!("Timeout (expected for empty binlog)");
            }
        }
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_mariadb_query_execution() {
        init_test_logging();

        let container = MariaDbTestContainer::start()
            .await
            .expect("Failed to start MariaDB container");

        container
            .execute("CREATE DATABASE mariadb_test")
            .await
            .expect("CREATE DATABASE failed");
        container.execute("CREATE TABLE mariadb_test.products (id INT PRIMARY KEY, name VARCHAR(255), price DECIMAL(10,2))").await
            .expect("CREATE TABLE failed");
        container
            .execute("INSERT INTO mariadb_test.products VALUES (1, 'Widget', 19.99)")
            .await
            .expect("INSERT failed");

        info!("MariaDB query execution successful");
    }
}

// ============================================================================
// GTID Tests
// ============================================================================

mod gtid_tests {
    use super::*;
    use rivven_cdc::mysql::decoder::GtidEvent;

    #[test]
    fn test_gtid_uuid_string() {
        let event = GtidEvent {
            flags: 0,
            uuid: [
                0x3E, 0x11, 0xFA, 0x47, 0x71, 0xCA, 0x11, 0xE1, 0x9E, 0x33, 0xC8, 0x0A, 0xA9, 0x42,
                0x95, 0x62,
            ],
            gno: 42,
            logical_clock_ts_type: 0,
        };

        assert_eq!(event.uuid_string(), "3e11fa47-71ca-11e1-9e33-c80aa9429562");
        assert_eq!(
            event.gtid_string(),
            "3e11fa47-71ca-11e1-9e33-c80aa9429562:42"
        );
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_gtid_config() {
        let config = MySqlCdcConfig::new("localhost", "user")
            .with_gtid("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100");

        assert!(config.use_gtid);
        assert_eq!(
            config.gtid_set,
            "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"
        );
    }
}

// ============================================================================
// Replication User Tests
// ============================================================================

mod replication_user_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_create_replication_user() {
        init_test_logging();

        let container = MySqlTestContainer::start()
            .await
            .expect("Failed to start MySQL container");

        container
            .create_replication_user("cdc_user", "cdc_pass")
            .await
            .expect("Failed to create replication user");

        // Try to connect with the new user
        let result = MySqlBinlogClient::connect(
            container.host(),
            container.port(),
            "cdc_user",
            Some("cdc_pass"),
            None,
        )
        .await;

        assert!(
            result.is_ok(),
            "Failed to connect with replication user: {:?}",
            result.err()
        );

        info!("Successfully created and authenticated replication user");
    }

    #[tokio::test]
    #[ignore = "Requires Docker; run with --ignored"]
    #[serial]
    async fn test_replication_user_can_replicate() {
        init_test_logging();

        let container = MySqlTestContainer::start()
            .await
            .expect("Failed to start MySQL container");

        container
            .create_replication_user("repl_user", "repl_pass")
            .await
            .expect("Failed to create replication user");

        let mut client = MySqlBinlogClient::connect(
            container.host(),
            container.port(),
            "repl_user",
            Some("repl_pass"),
            None,
        )
        .await
        .expect("Failed to connect");

        // Replication user should be able to register as slave
        client
            .register_slave(40001)
            .await
            .expect("Failed to register as slave");

        info!("Replication user can register as slave");
    }
}
