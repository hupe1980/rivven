//! # CDC Connector
//!
//! Database-agnostic connector for routing CDC events to Rivven topics.
//!
//! The connector handles:
//! - Schema caching and management
//! - Topic/partition routing
//! - Integration with schema registry
//! - Batch event processing
//!
//! ## Architecture
//!
//! ```text
//! CDC Source → CdcConnector → Schema Registry (optional)
//!                    ↓
//!              Topic/Partition
//! ```

use crate::common::{CdcEvent, CdcError, Result};
use rivven_core::{Partition, schema_registry::SchemaRegistry};
use apache_avro::Schema as AvroSchema;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Schema generator function type for database-specific Avro schema generation
pub type SchemaGenerator = Box<
    dyn Fn(&str, &str, &[(String, i32, String)]) -> Result<AvroSchema> + Send + Sync,
>;

/// CDC Connector that routes database events to Rivven topics.
///
/// This connector is database-agnostic - it uses a pluggable schema generator
/// to support different database types (PostgreSQL, MySQL, MariaDB).
///
/// # Example
///
/// ```ignore
/// use rivven_cdc::common::CdcConnector;
///
/// let connector = CdcConnector::new()
///     .with_schema_generator(|schema, table, columns| {
///         // Generate Avro schema from table metadata
///         generate_avro_schema(schema, table, columns)
///     });
///
/// // Register a topic
/// connector.register_topic("cdc.mydb.public.users".into(), partition).await;
///
/// // Route events
/// connector.route_event(&event).await?;
/// ```
pub struct CdcConnector {
    /// Schema cache: (namespace.table) -> Avro Schema
    schema_cache: Arc<RwLock<HashMap<String, AvroSchema>>>,

    /// Topic/Partition mapping
    partitions: Arc<RwLock<HashMap<String, Arc<Partition>>>>,

    /// Schema registry reference
    schema_registry: Option<Arc<dyn SchemaRegistry>>,

    /// Database-specific schema generator
    schema_generator: Option<SchemaGenerator>,

    /// Source type identifier (postgres, mysql, mariadb)
    source_type: String,
}

impl CdcConnector {
    /// Create a new CDC connector.
    pub fn new() -> Self {
        Self {
            schema_cache: Arc::new(RwLock::new(HashMap::new())),
            partitions: Arc::new(RwLock::new(HashMap::new())),
            schema_registry: None,
            schema_generator: None,
            source_type: "unknown".into(),
        }
    }

    /// Create a new CDC connector for PostgreSQL.
    #[cfg(feature = "postgres")]
    pub fn for_postgres() -> Self {
        use crate::postgres::PostgresTypeMapper;

        Self::new()
            .with_source_type("postgres")
            .with_schema_generator(Box::new(|schema, table, columns| {
                PostgresTypeMapper::generate_avro_schema(schema, table, columns)
                    .map_err(|e| CdcError::Schema(e.to_string()))
            }))
    }

    /// Create a new CDC connector for MySQL.
    #[cfg(feature = "mysql")]
    pub fn for_mysql() -> Self {
        use crate::mysql::MySqlTypeMapper;

        Self::new()
            .with_source_type("mysql")
            .with_schema_generator(Box::new(|schema, table, columns| {
                MySqlTypeMapper::generate_avro_schema(schema, table, columns)
                    .map_err(|e| CdcError::Schema(e.to_string()))
            }))
    }

    /// Create a new CDC connector for MariaDB.
    #[cfg(feature = "mariadb")]
    pub fn for_mariadb() -> Self {
        // MariaDB uses MySQL-compatible binlog format
        Self::for_mysql().with_source_type("mariadb")
    }

    /// Set the source type identifier.
    pub fn with_source_type(mut self, source_type: &str) -> Self {
        self.source_type = source_type.into();
        self
    }

    /// Set the schema registry for schema management.
    pub fn with_schema_registry(mut self, registry: Arc<dyn SchemaRegistry>) -> Self {
        self.schema_registry = Some(registry);
        self
    }

    /// Set the schema generator function.
    pub fn with_schema_generator(mut self, generator: SchemaGenerator) -> Self {
        self.schema_generator = Some(generator);
        self
    }

    /// Get the source type.
    pub fn source_type(&self) -> &str {
        &self.source_type
    }

    /// Register a topic partition for CDC events.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic name (e.g., "cdc.mydb.public.users")
    /// * `partition` - The partition to route events to
    pub async fn register_topic(&self, topic: String, partition: Arc<Partition>) {
        let mut partitions = self.partitions.write().await;
        partitions.insert(topic.clone(), partition);
        info!("Registered CDC topic: {}", topic);
    }

    /// Unregister a topic.
    pub async fn unregister_topic(&self, topic: &str) -> Option<Arc<Partition>> {
        let mut partitions = self.partitions.write().await;
        let result = partitions.remove(topic);
        if result.is_some() {
            info!("Unregistered CDC topic: {}", topic);
        }
        result
    }

    /// Get registered topic names.
    pub async fn topics(&self) -> Vec<String> {
        let partitions = self.partitions.read().await;
        partitions.keys().cloned().collect()
    }

    /// Get or create Avro schema for a table.
    ///
    /// This method:
    /// 1. Checks the local cache for an existing schema
    /// 2. If not found, generates a new schema using the schema generator
    /// 3. Optionally registers the schema with the schema registry
    /// 4. Caches the schema for future use
    ///
    /// # Arguments
    ///
    /// * `database` - Database name
    /// * `schema` - Schema/namespace name
    /// * `table` - Table name
    /// * `columns` - Column metadata: (name, type_oid, type_name)
    pub async fn get_or_infer_schema(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        columns: &[(String, i32, String)],
    ) -> Result<AvroSchema> {
        let cache_key = format!("{}.{}", schema, table);

        // Check cache first
        {
            let cache = self.schema_cache.read().await;
            if let Some(avro_schema) = cache.get(&cache_key) {
                return Ok(avro_schema.clone());
            }
        }

        // Generate new schema
        info!(
            "Inferring Avro schema for {}.{}.{} ({})",
            database, schema, table, self.source_type
        );

        let avro_schema = match &self.schema_generator {
            Some(generator) => generator(schema, table, columns)?,
            None => {
                return Err(CdcError::Schema(
                    "No schema generator configured".into(),
                ));
            }
        };

        // Register with schema registry if available
        if let Some(registry) = &self.schema_registry {
            let subject = format!("cdc.{}.{}.{}", database, schema, table);
            let schema_str = avro_schema.canonical_form();

            match registry.register(&subject, &schema_str).await {
                Ok(schema_id) => {
                    info!("Registered schema {} with ID {}", subject, schema_id);
                }
                Err(e) => {
                    warn!("Failed to register schema {}: {}", subject, e);
                }
            }
        }

        // Cache it
        let mut cache = self.schema_cache.write().await;
        cache.insert(cache_key, avro_schema.clone());

        Ok(avro_schema)
    }

    /// Check if a schema is cached for a table.
    pub async fn has_schema(&self, schema: &str, table: &str) -> bool {
        let cache_key = format!("{}.{}", schema, table);
        let cache = self.schema_cache.read().await;
        cache.contains_key(&cache_key)
    }

    /// Clear the schema cache.
    pub async fn clear_schema_cache(&self) {
        let mut cache = self.schema_cache.write().await;
        cache.clear();
        info!("Cleared schema cache");
    }

    /// Route a CDC event to the appropriate topic.
    ///
    /// Topic naming convention: `cdc.{database}.{schema}.{table}`
    pub async fn route_event(&self, event: &CdcEvent) -> Result<u64> {
        // Topic naming: cdc.{database}.{schema}.{table}
        let topic_name = format!("cdc.{}.{}.{}", event.database, event.schema, event.table);

        // Get partition
        let partitions = self.partitions.read().await;
        let partition = partitions.get(&topic_name).ok_or_else(|| {
            CdcError::Topic(format!("No partition registered for topic: {}", topic_name))
        })?;

        // Convert to Message
        let message = event
            .to_message()
            .map_err(|e| CdcError::Serialization(e.to_string()))?;

        // Append to partition
        let offset = partition
            .append(message)
            .await
            .map_err(|e| CdcError::Io(std::io::Error::other(e.to_string())))?;

        debug!(
            "Routed {:?} event for {}.{} to topic {} at offset {}",
            event.op, event.schema, event.table, topic_name, offset
        );

        Ok(offset)
    }

    /// Process a batch of CDC events.
    ///
    /// Returns the number of successfully routed events.
    pub async fn process_events(&self, events: Vec<CdcEvent>) -> Result<usize> {
        let mut success_count = 0;

        for event in events {
            match self.route_event(&event).await {
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
        events: Vec<CdcEvent>,
        mut callback: F,
    ) -> Result<usize>
    where
        F: FnMut(&CdcEvent, std::result::Result<u64, &CdcError>),
    {
        let mut success_count = 0;

        for event in events {
            match self.route_event(&event).await {
                Ok(offset) => {
                    callback(&event, Ok(offset));
                    success_count += 1;
                }
                Err(e) => {
                    callback(&event, Err(&e));
                    warn!("Failed to route event: {}", e);
                }
            }
        }

        Ok(success_count)
    }
}

impl Default for CdcConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for CdcConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CdcConnector")
            .field("source_type", &self.source_type)
            .field("has_schema_registry", &self.schema_registry.is_some())
            .field("has_schema_generator", &self.schema_generator.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::CdcOp;

    #[test]
    fn test_topic_naming() {
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

        let topic_name = format!("cdc.{}.{}.{}", event.database, event.schema, event.table);
        assert_eq!(topic_name, "cdc.mydb.public.users");
    }

    #[test]
    fn test_connector_creation() {
        let connector = CdcConnector::new();
        assert_eq!(connector.source_type(), "unknown");
    }

    #[test]
    fn test_connector_with_source_type() {
        let connector = CdcConnector::new().with_source_type("postgres");
        assert_eq!(connector.source_type(), "postgres");
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn test_postgres_connector() {
        let connector = CdcConnector::for_postgres();
        assert_eq!(connector.source_type(), "postgres");
        assert!(connector.schema_generator.is_some());
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn test_mysql_connector() {
        let connector = CdcConnector::for_mysql();
        assert_eq!(connector.source_type(), "mysql");
        assert!(connector.schema_generator.is_some());
    }

    #[tokio::test]
    async fn test_schema_cache_operations() {
        let connector = CdcConnector::new();
        
        assert!(!connector.has_schema("public", "users").await);
        
        // Clear should work even when empty
        connector.clear_schema_cache().await;
    }

    #[tokio::test]
    async fn test_topic_registration() {
        let connector = CdcConnector::new();
        
        // Initially no topics
        assert!(connector.topics().await.is_empty());
    }
}
