//! RDBC Connector Integration Tests
//!
//! Tests for RDBC source and sink connectors.

#![cfg(feature = "rdbc")]

// Tests for RdbcSourceConfig
#[cfg(test)]
mod source_config_tests {
    /// Helper to create a minimal valid config
    fn minimal_config() -> serde_json::Value {
        serde_json::json!({
            "connection_url": "postgres://user:pass@localhost/db",
            "table": "users"
        })
    }

    #[test]
    fn test_deserialize_minimal_config() {
        let config: Result<rivven_connect::connectors::rdbc::RdbcSourceConfig, _> =
            serde_json::from_value(minimal_config());
        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.table, "users");
    }

    #[test]
    fn test_deserialize_full_config() {
        let json = serde_json::json!({
            "connection_url": "postgres://user:pass@localhost/db",
            "schema": "public",
            "table": "users",
            "mode": "incrementing",
            "incrementing_column": "id",
            "poll_interval_ms": 1000,
            "batch_size": 500,
            "topic": "user-changes"
        });

        let config: rivven_connect::connectors::rdbc::RdbcSourceConfig =
            serde_json::from_value(json).unwrap();

        assert_eq!(config.schema, Some("public".to_string()));
        assert_eq!(config.incrementing_column, Some("id".to_string()));
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.poll_interval_ms, 1000);
        assert_eq!(config.topic, Some("user-changes".to_string()));
    }

    #[test]
    fn test_query_mode_variants() {
        // Test Bulk mode
        let json = serde_json::json!({
            "connection_url": "postgres://localhost/db",
            "table": "users",
            "mode": "bulk"
        });
        let config: rivven_connect::connectors::rdbc::RdbcSourceConfig =
            serde_json::from_value(json).unwrap();
        assert!(matches!(
            config.mode,
            rivven_connect::connectors::rdbc::RdbcQueryMode::Bulk
        ));

        // Test Incrementing mode
        let json = serde_json::json!({
            "connection_url": "mysql://localhost/db",
            "table": "events",
            "mode": "incrementing",
            "incrementing_column": "event_id"
        });
        let config: rivven_connect::connectors::rdbc::RdbcSourceConfig =
            serde_json::from_value(json).unwrap();
        assert!(matches!(
            config.mode,
            rivven_connect::connectors::rdbc::RdbcQueryMode::Incrementing
        ));

        // Test Timestamp mode
        let json = serde_json::json!({
            "connection_url": "sqlserver://localhost/db",
            "table": "logs",
            "mode": "timestamp",
            "timestamp_column": "created_at"
        });
        let config: rivven_connect::connectors::rdbc::RdbcSourceConfig =
            serde_json::from_value(json).unwrap();
        assert!(matches!(
            config.mode,
            rivven_connect::connectors::rdbc::RdbcQueryMode::Timestamp
        ));
        assert_eq!(config.timestamp_column, Some("created_at".to_string()));

        // Test TimestampIncrementing mode
        let json = serde_json::json!({
            "connection_url": "postgres://localhost/db",
            "table": "orders",
            "mode": "timestamp_incrementing",
            "timestamp_column": "updated_at",
            "incrementing_column": "id"
        });
        let config: rivven_connect::connectors::rdbc::RdbcSourceConfig =
            serde_json::from_value(json).unwrap();
        assert!(matches!(
            config.mode,
            rivven_connect::connectors::rdbc::RdbcQueryMode::TimestampIncrementing
        ));
    }
}

// Tests for RdbcSinkConfig
#[cfg(test)]
mod sink_config_tests {
    #[test]
    fn test_deserialize_minimal_sink_config() {
        let json = serde_json::json!({
            "connection_url": "postgres://user:pass@localhost/db",
            "table": "users"
        });

        let config: rivven_connect::connectors::rdbc::RdbcSinkConfig =
            serde_json::from_value(json).unwrap();
        assert_eq!(config.table, "users");
    }

    #[test]
    fn test_deserialize_full_sink_config() {
        let json = serde_json::json!({
            "connection_url": "postgres://user:pass@localhost/db",
            "schema": "public",
            "table": "users",
            "write_mode": "upsert",
            "pk_columns": ["id"],
            "delete_enabled": true,
            "batch_size": 1000,
            "batch_timeout_ms": 5000
        });

        let config: rivven_connect::connectors::rdbc::RdbcSinkConfig =
            serde_json::from_value(json).unwrap();

        assert_eq!(config.schema, Some("public".to_string()));
        assert_eq!(config.pk_columns, Some(vec!["id".to_string()]));
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.batch_timeout_ms, 5000);
        assert!(config.delete_enabled);
    }

    #[test]
    fn test_write_mode_variants() {
        let modes = [
            (
                "insert",
                rivven_connect::connectors::rdbc::RdbcWriteMode::Insert,
            ),
            (
                "update",
                rivven_connect::connectors::rdbc::RdbcWriteMode::Update,
            ),
            (
                "upsert",
                rivven_connect::connectors::rdbc::RdbcWriteMode::Upsert,
            ),
        ];

        for (mode_str, expected) in modes {
            let json = serde_json::json!({
                "connection_url": "postgres://localhost/db",
                "table": "users",
                "write_mode": mode_str
            });
            let config: rivven_connect::connectors::rdbc::RdbcSinkConfig =
                serde_json::from_value(json).unwrap();
            assert_eq!(config.write_mode, expected);
        }
    }

    #[test]
    fn test_default_values() {
        let json = serde_json::json!({
            "connection_url": "postgres://localhost/db",
            "table": "users"
        });
        let config: rivven_connect::connectors::rdbc::RdbcSinkConfig =
            serde_json::from_value(json).unwrap();

        // Default write mode is Insert
        assert_eq!(
            config.write_mode,
            rivven_connect::connectors::rdbc::RdbcWriteMode::Insert
        );
        // Default batch size is 1000
        assert_eq!(config.batch_size, 1000);
        // Default batch timeout is 5000
        assert_eq!(config.batch_timeout_ms, 5000);
        // Delete enabled by default
        assert!(config.delete_enabled);
        // Transactional disabled by default
        assert!(!config.transactional);
        // Default pool size is 4
        assert_eq!(config.pool_size, 4);
    }

    #[test]
    fn test_transactional_and_pool_config() {
        let json = serde_json::json!({
            "connection_url": "postgres://localhost/db",
            "table": "users",
            "transactional": true,
            "pool_size": 8
        });
        let config: rivven_connect::connectors::rdbc::RdbcSinkConfig =
            serde_json::from_value(json).unwrap();

        assert!(config.transactional);
        assert_eq!(config.pool_size, 8);
    }
}

// Tests for connector spec
#[cfg(test)]
mod spec_tests {
    use rivven_connect::traits::sink::Sink;
    use rivven_connect::traits::source::Source;

    #[test]
    fn test_rdbc_source_spec() {
        let spec = rivven_connect::connectors::rdbc::RdbcSource::spec();
        assert_eq!(spec.connector_type, "rdbc-source");
        // Version matches package version
        assert!(!spec.version.is_empty());
        assert!(spec.supports_incremental);
        assert!(spec.description.is_some());
    }

    #[test]
    fn test_rdbc_sink_spec() {
        let spec = rivven_connect::connectors::rdbc::RdbcSink::spec();
        assert_eq!(spec.connector_type, "rdbc-sink");
        // Version matches package version
        assert!(!spec.version.is_empty());
        assert!(spec.description.is_some());
    }
}

// Tests for factory registration
#[cfg(test)]
mod registry_tests {
    use rivven_connect::connectors::{create_sink_registry, create_source_registry};

    #[test]
    fn test_rdbc_source_registered() {
        let registry = create_source_registry();
        let sources = registry.list();
        assert!(sources.iter().any(|(name, _)| *name == "rdbc-source"));
    }

    #[test]
    fn test_rdbc_sink_registered() {
        let registry = create_sink_registry();
        let sinks = registry.list();
        assert!(sinks.iter().any(|(name, _)| *name == "rdbc-sink"));
    }
}

// Tests for connector inventory
#[cfg(test)]
mod inventory_tests {
    use rivven_connect::connectors::{create_connector_inventory, ConnectorCategory};

    #[test]
    fn test_rdbc_source_in_inventory() {
        let inventory = create_connector_inventory();

        let metadata = inventory.get_source_metadata("rdbc-source");
        assert!(metadata.is_some());

        let metadata = metadata.unwrap();
        assert_eq!(metadata.title, "RDBC Source");
        assert_eq!(metadata.category, ConnectorCategory::DatabaseQuery);
    }

    #[test]
    fn test_rdbc_sink_in_inventory() {
        let inventory = create_connector_inventory();

        let metadata = inventory.get_sink_metadata("rdbc-sink");
        assert!(metadata.is_some());

        let metadata = metadata.unwrap();
        assert_eq!(metadata.title, "RDBC Sink");
        assert_eq!(metadata.category, ConnectorCategory::DatabaseQuery);
    }

    #[test]
    fn test_rdbc_search() {
        let inventory = create_connector_inventory();

        // Search for "rdbc"
        let results = inventory.search("rdbc");
        assert!(results.iter().any(|m| m.name == "rdbc-source"));
        assert!(results.iter().any(|m| m.name == "rdbc-sink"));
    }

    #[test]
    fn test_database_category() {
        let inventory = create_connector_inventory();

        let db_connectors = inventory.by_category(ConnectorCategory::DatabaseQuery);
        assert!(!db_connectors.is_empty());
        assert!(db_connectors.iter().any(|m| m.name == "rdbc-source"));
        assert!(db_connectors.iter().any(|m| m.name == "rdbc-sink"));
    }
}
