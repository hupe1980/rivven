//! # CDC Connector
//!
//! Database-agnostic connector for routing CDC events to topics.
//!
//! ## Design Philosophy
//!
//! The CDC connector is intentionally simple and format-agnostic:
//! - It captures database changes and routes them as `CdcEvent` objects
//! - Serialization format (Avro, Protobuf, JSON) is decided by the consumer (rivven-connect)
//! - Schema registry integration is handled by rivven-connect, not here
//!
//! This separation of concerns allows:
//! - Using CDC events without any schema registry
//! - Choosing serialization format at the connector level, not CDC level
//! - Simpler CDC library with fewer dependencies
//!
//! ## Architecture
//!
//! ```text
//! rivven-cdc                          rivven-connect
//! ┌──────────────────┐                ┌──────────────────────┐
//! │ Database         │                │ CDC Source Connector │
//! │    ↓             │                │    ↓                 │
//! │ CdcSource        │ ──CdcEvent──►  │ Schema Inference     │
//! │    ↓             │                │    ↓                 │
//! │ CdcConnector     │                │ Schema Registry      │
//! │    ↓             │                │    ↓                 │
//! │ Topic Routing    │                │ Serialization        │
//! └──────────────────┘                └──────────────────────┘
//! ```

use crate::common::{CdcError, CdcEvent, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Trait for receiving CDC events.
///
/// This trait allows CDC to work standalone without requiring the full broker.
/// Implementations can write to Rivven partitions, Kafka, files, etc.
#[async_trait::async_trait]
pub trait EventSink: Send + Sync + std::fmt::Debug {
    /// Append an event to the sink, returning the offset
    async fn append(&self, event: &CdcEvent) -> Result<u64>;
}

/// CDC Connector that routes database events to topics.
///
/// This connector is database-agnostic and format-agnostic. It simply
/// routes CDC events to registered event sinks (topics/partitions).
///
/// # Example
///
/// ```ignore
/// use rivven_cdc::common::CdcConnector;
///
/// let connector = CdcConnector::new("postgres");
///
/// // Register event sinks for topics
/// connector.register_sink("cdc.mydb.public.users", sink).await;
///
/// // Route events
/// connector.route_event(&event).await?;
/// ```
pub struct CdcConnector {
    /// Topic -> EventSink mapping
    sinks: Arc<RwLock<HashMap<String, Arc<dyn EventSink>>>>,

    /// Source type identifier (postgres, mysql, mariadb)
    source_type: String,

    /// Database name (for topic naming)
    database: Option<String>,
}

impl CdcConnector {
    /// Create a new CDC connector.
    ///
    /// # Arguments
    ///
    /// * `source_type` - The database type (postgres, mysql, mariadb)
    pub fn new(source_type: impl Into<String>) -> Self {
        Self {
            sinks: Arc::new(RwLock::new(HashMap::new())),
            source_type: source_type.into(),
            database: None,
        }
    }

    /// Create a new CDC connector for PostgreSQL.
    pub fn postgres() -> Self {
        Self::new("postgres")
    }

    /// Create a new CDC connector for MySQL.
    pub fn mysql() -> Self {
        Self::new("mysql")
    }

    /// Create a new CDC connector for MariaDB.
    pub fn mariadb() -> Self {
        Self::new("mariadb")
    }

    /// Set the database name for topic naming.
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Get the source type.
    pub fn source_type(&self) -> &str {
        &self.source_type
    }

    /// Get the database name.
    pub fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    /// Register an event sink for a topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic name (e.g., "cdc.mydb.public.users")
    /// * `sink` - The event sink to route events to
    pub async fn register_sink(&self, topic: impl Into<String>, sink: Arc<dyn EventSink>) {
        let topic = topic.into();
        let mut sinks = self.sinks.write().await;
        sinks.insert(topic.clone(), sink);
        info!("Registered CDC sink for topic: {}", topic);
    }

    /// Unregister an event sink.
    pub async fn unregister_sink(&self, topic: &str) -> Option<Arc<dyn EventSink>> {
        let mut sinks = self.sinks.write().await;
        let result = sinks.remove(topic);
        if result.is_some() {
            info!("Unregistered CDC sink for topic: {}", topic);
        }
        result
    }

    /// Get registered topic names.
    pub async fn topics(&self) -> Vec<String> {
        let sinks = self.sinks.read().await;
        sinks.keys().cloned().collect()
    }

    /// Check if a topic has a registered sink.
    pub async fn has_sink(&self, topic: &str) -> bool {
        let sinks = self.sinks.read().await;
        sinks.contains_key(topic)
    }

    /// Generate the topic name for a CDC event.
    ///
    /// Topic naming convention: `cdc.{database}.{schema}.{table}`
    pub fn topic_name(&self, event: &CdcEvent) -> String {
        format!("cdc.{}.{}.{}", event.database, event.schema, event.table)
    }

    /// Route a CDC event to the appropriate topic.
    ///
    /// Topic naming convention: `cdc.{database}.{schema}.{table}`
    pub async fn route_event(&self, event: &CdcEvent) -> Result<u64> {
        let topic_name = self.topic_name(event);

        // Get sink
        let sinks = self.sinks.read().await;
        let sink = sinks.get(&topic_name).ok_or_else(|| {
            CdcError::Topic(format!("No sink registered for topic: {}", topic_name))
        })?;

        // Append to sink
        let offset = sink.append(event).await?;

        debug!(
            "Routed {:?} event for {}.{} to topic {} at offset {}",
            event.op, event.schema, event.table, topic_name, offset
        );

        Ok(offset)
    }

    /// Process a batch of CDC events.
    ///
    /// Returns the number of successfully routed events.
    pub async fn process_events(&self, events: &[CdcEvent]) -> Result<usize> {
        let mut success_count = 0;

        for event in events {
            match self.route_event(event).await {
                Ok(_) => success_count += 1,
                Err(e) => {
                    warn!("Failed to route event: {}", e);
                }
            }
        }

        Ok(success_count)
    }

    /// Process events with a callback for each event.
    pub async fn process_events_with_callback<F>(
        &self,
        events: &[CdcEvent],
        mut callback: F,
    ) -> Result<usize>
    where
        F: FnMut(&CdcEvent, std::result::Result<u64, &CdcError>),
    {
        let mut success_count = 0;

        for event in events {
            match self.route_event(event).await {
                Ok(offset) => {
                    callback(event, Ok(offset));
                    success_count += 1;
                }
                Err(e) => {
                    callback(event, Err(&e));
                    warn!("Failed to route event: {}", e);
                }
            }
        }

        Ok(success_count)
    }
}

impl Default for CdcConnector {
    fn default() -> Self {
        Self::new("unknown")
    }
}

impl std::fmt::Debug for CdcConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CdcConnector")
            .field("source_type", &self.source_type)
            .field("database", &self.database)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::CdcOp;

    /// Mock event sink for testing
    #[derive(Debug)]
    struct MockSink {
        offset: std::sync::atomic::AtomicU64,
    }

    impl MockSink {
        fn new() -> Self {
            Self {
                offset: std::sync::atomic::AtomicU64::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl EventSink for MockSink {
        async fn append(&self, _event: &CdcEvent) -> Result<u64> {
            let offset = self
                .offset
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(offset)
        }
    }

    #[test]
    fn test_topic_naming() {
        let connector = CdcConnector::postgres();
        let event = CdcEvent {
            source_type: "postgres".into(),
            database: "mydb".into(),
            schema: "public".into(),
            table: "users".into(),
            op: CdcOp::Insert,
            before: None,
            after: Some(serde_json::json!({"id": 1, "name": "Alice"})),
            timestamp: 0,
            transaction: None,
        };

        assert_eq!(connector.topic_name(&event), "cdc.mydb.public.users");
    }

    #[test]
    fn test_connector_creation() {
        let connector = CdcConnector::new("postgres");
        assert_eq!(connector.source_type(), "postgres");
    }

    #[test]
    fn test_connector_helpers() {
        assert_eq!(CdcConnector::postgres().source_type(), "postgres");
        assert_eq!(CdcConnector::mysql().source_type(), "mysql");
        assert_eq!(CdcConnector::mariadb().source_type(), "mariadb");
    }

    #[test]
    fn test_connector_with_database() {
        let connector = CdcConnector::postgres().with_database("mydb");
        assert_eq!(connector.database(), Some("mydb"));
    }

    #[tokio::test]
    async fn test_sink_registration() {
        let connector = CdcConnector::postgres();

        // Initially no sinks
        assert!(connector.topics().await.is_empty());

        // Register a sink
        let sink = Arc::new(MockSink::new());
        connector.register_sink("cdc.mydb.public.users", sink).await;

        // Verify sink is registered
        assert!(connector.has_sink("cdc.mydb.public.users").await);
        assert_eq!(connector.topics().await.len(), 1);

        // Unregister
        connector.unregister_sink("cdc.mydb.public.users").await;
        assert!(!connector.has_sink("cdc.mydb.public.users").await);
    }

    #[tokio::test]
    async fn test_route_event() {
        let connector = CdcConnector::postgres();
        let sink = Arc::new(MockSink::new());
        connector.register_sink("cdc.mydb.public.users", sink).await;

        let event = CdcEvent {
            source_type: "postgres".into(),
            database: "mydb".into(),
            schema: "public".into(),
            table: "users".into(),
            op: CdcOp::Insert,
            before: None,
            after: Some(serde_json::json!({"id": 1, "name": "Alice"})),
            timestamp: 0,
            transaction: None,
        };

        // Route event
        let offset = connector.route_event(&event).await.unwrap();
        assert_eq!(offset, 0);

        // Route another event
        let offset = connector.route_event(&event).await.unwrap();
        assert_eq!(offset, 1);
    }

    #[tokio::test]
    async fn test_route_event_no_sink() {
        let connector = CdcConnector::postgres();

        let event = CdcEvent {
            source_type: "postgres".into(),
            database: "mydb".into(),
            schema: "public".into(),
            table: "users".into(),
            op: CdcOp::Insert,
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            timestamp: 0,
            transaction: None,
        };

        // Should fail - no sink registered
        let result = connector.route_event(&event).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_process_events() {
        let connector = CdcConnector::postgres();
        let sink = Arc::new(MockSink::new());
        connector.register_sink("cdc.mydb.public.users", sink).await;

        let events = vec![
            CdcEvent {
                source_type: "postgres".into(),
                database: "mydb".into(),
                schema: "public".into(),
                table: "users".into(),
                op: CdcOp::Insert,
                before: None,
                after: Some(serde_json::json!({"id": 1})),
                timestamp: 0,
                transaction: None,
            },
            CdcEvent {
                source_type: "postgres".into(),
                database: "mydb".into(),
                schema: "public".into(),
                table: "users".into(),
                op: CdcOp::Insert,
                before: None,
                after: Some(serde_json::json!({"id": 2})),
                timestamp: 0,
                transaction: None,
            },
        ];

        let count = connector.process_events(&events).await.unwrap();
        assert_eq!(count, 2);
    }
}
