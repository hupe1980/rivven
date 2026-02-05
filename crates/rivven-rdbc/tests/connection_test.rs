//! Unit tests for rivven-rdbc connection module

use rivven_rdbc::connection::{ConnectionConfig, IsolationLevel};

#[test]
fn test_connection_config_default() {
    let config = ConnectionConfig::default();

    assert!(config.url.is_empty());
    assert!(config.connect_timeout_ms > 0);
    assert!(config.query_timeout_ms > 0);
    assert!(config.statement_cache_size > 0);
}

#[test]
fn test_connection_config_new() {
    let config = ConnectionConfig::new("postgresql://localhost/testdb");

    assert_eq!(config.url, "postgresql://localhost/testdb");
    assert!(config.connect_timeout_ms > 0);
    assert!(config.query_timeout_ms > 0);
}

#[test]
fn test_connection_config_with_timeouts() {
    let config = ConnectionConfig::new("postgresql://localhost/testdb")
        .with_connect_timeout(5000)
        .with_query_timeout(60000);

    assert_eq!(config.connect_timeout_ms, 5000);
    assert_eq!(config.query_timeout_ms, 60000);
}

#[test]
fn test_connection_config_with_app_name() {
    let config =
        ConnectionConfig::new("postgresql://localhost/testdb").with_application_name("my-app");

    assert_eq!(config.application_name, Some("my-app".to_string()));
}

#[test]
fn test_connection_config_with_statement_cache() {
    let config =
        ConnectionConfig::new("postgresql://localhost/testdb").with_statement_cache_size(500);

    assert_eq!(config.statement_cache_size, 500);
}

#[test]
fn test_connection_config_with_property() {
    let config = ConnectionConfig::new("postgresql://localhost/testdb")
        .with_property("sslmode", "require")
        .with_property("connect_timeout", "10");

    assert_eq!(
        config.properties.get("sslmode"),
        Some(&"require".to_string())
    );
    assert_eq!(
        config.properties.get("connect_timeout"),
        Some(&"10".to_string())
    );
}

#[test]
fn test_isolation_level_to_sql() {
    assert_eq!(IsolationLevel::ReadUncommitted.to_sql(), "READ UNCOMMITTED");
    assert_eq!(IsolationLevel::ReadCommitted.to_sql(), "READ COMMITTED");
    assert_eq!(IsolationLevel::RepeatableRead.to_sql(), "REPEATABLE READ");
    assert_eq!(IsolationLevel::Serializable.to_sql(), "SERIALIZABLE");
    assert_eq!(IsolationLevel::Snapshot.to_sql(), "SNAPSHOT");
}

#[test]
fn test_isolation_level_display() {
    assert_eq!(
        format!("{}", IsolationLevel::ReadUncommitted),
        "READ UNCOMMITTED"
    );
    assert_eq!(
        format!("{}", IsolationLevel::ReadCommitted),
        "READ COMMITTED"
    );
    assert_eq!(
        format!("{}", IsolationLevel::RepeatableRead),
        "REPEATABLE READ"
    );
    assert_eq!(format!("{}", IsolationLevel::Serializable), "SERIALIZABLE");
    assert_eq!(format!("{}", IsolationLevel::Snapshot), "SNAPSHOT");
}

#[test]
fn test_isolation_level_equality() {
    let level1 = IsolationLevel::ReadCommitted;
    let level2 = IsolationLevel::ReadCommitted;
    let level3 = IsolationLevel::Serializable;

    assert_eq!(level1, level2);
    assert_ne!(level1, level3);
}

#[test]
fn test_isolation_level_clone() {
    let level = IsolationLevel::Serializable;
    let cloned = level;

    assert_eq!(level, cloned);
}

#[test]
fn test_isolation_level_debug() {
    let debug = format!("{:?}", IsolationLevel::ReadCommitted);
    assert!(debug.contains("ReadCommitted"));
}
