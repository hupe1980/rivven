//! PostgreSQL Snapshot Source
//!
//! Implementation of `SnapshotSource` trait for PostgreSQL databases.
//!
//! # Features
//!
//! - Efficient keyset pagination (no OFFSET)
//! - Automatic column type inference
//! - WAL LSN watermark for consistency
//! - Row count estimation via pg_class statistics
//! - Table and primary key discovery
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_cdc::postgres::PostgresSnapshotSource;
//! use rivven_cdc::common::{SnapshotCoordinator, SnapshotConfig, MemoryProgressStore};
//!
//! // Create snapshot source
//! let source = PostgresSnapshotSource::connect("postgresql://localhost/mydb").await?;
//!
//! // Get watermark before snapshot
//! let watermark = source.get_watermark().await?;
//! println!("Starting at WAL position: {}", watermark);
//!
//! // Use with coordinator
//! let config = SnapshotConfig::default();
//! let progress = MemoryProgressStore::new();
//! let coordinator = SnapshotCoordinator::new(config, source, progress);
//! ```
//!
//! # Table Discovery
//!
//! ```rust,ignore
//! use rivven_cdc::postgres::{discover_tables_with_keys, discover_primary_key};
//!
//! // Discover all tables in schemas
//! let tables = discover_tables_with_keys(&client, &["public"]).await?;
//! for (schema, table, key_column) in tables {
//!     println!("{}.{} (key: {})", schema, table, key_column);
//! }
//!
//! // Get primary key for a specific table
//! if let Some(pk) = discover_primary_key(&client, "public", "users").await? {
//!     println!("Primary key: {}", pk);
//! }
//! ```

use crate::common::{CdcError, CdcEvent, CdcOp, Result, SnapshotBatch, SnapshotSource};
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, warn};

// ============================================================================
// PostgreSQL Snapshot Source
// ============================================================================

/// PostgreSQL implementation of `SnapshotSource`.
///
/// Uses keyset pagination (WHERE key > last_key ORDER BY key LIMIT batch_size)
/// for efficient, memory-safe batch fetching.
///
/// # Features
///
/// - Efficient keyset pagination (no OFFSET)
/// - Automatic column type inference
/// - WAL LSN watermark for consistency
/// - Row count estimation via pg_class statistics
///
/// # Example
///
/// ```rust,ignore
/// use rivven_cdc::postgres::PostgresSnapshotSource;
///
/// let source = PostgresSnapshotSource::connect("postgresql://localhost/mydb").await?;
/// let watermark = source.get_watermark().await?;
/// println!("Starting at WAL position: {}", watermark);
/// ```
pub struct PostgresSnapshotSource {
    /// Connection pool or single connection
    client: Arc<tokio_postgres::Client>,
    /// Database name for events
    database: String,
}

impl PostgresSnapshotSource {
    /// Create a new PostgreSQL snapshot source from an existing client.
    pub fn new(client: Arc<tokio_postgres::Client>, database: impl Into<String>) -> Self {
        Self {
            client,
            database: database.into(),
        }
    }

    /// Create from connection string.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let source = PostgresSnapshotSource::connect(
    ///     "postgresql://user:pass@localhost:5432/mydb"
    /// ).await?;
    /// ```
    pub async fn connect(conn_str: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|e| {
                CdcError::connection_refused(format!("PostgreSQL connection failed: {}", e))
            })?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("PostgreSQL connection error: {}", e);
            }
        });

        // Extract database name from connection string
        let database = Self::extract_database_name(conn_str);

        Ok(Self {
            client: Arc::new(client),
            database,
        })
    }

    /// Create from existing connection with custom database name.
    pub fn from_client(client: tokio_postgres::Client, database: impl Into<String>) -> Self {
        Self {
            client: Arc::new(client),
            database: database.into(),
        }
    }

    /// Get a reference to the underlying client.
    pub fn client(&self) -> &tokio_postgres::Client {
        &self.client
    }

    /// Get the database name.
    pub fn database(&self) -> &str {
        &self.database
    }

    fn extract_database_name(conn_str: &str) -> String {
        // Try key=value format first
        if let Some(db) = conn_str
            .split_whitespace()
            .find(|s| s.starts_with("dbname="))
            .and_then(|s| s.strip_prefix("dbname="))
        {
            return db.to_string();
        }

        // Try URL format: postgresql://host/database?params
        if let Some(path) = conn_str.split("://").nth(1) {
            if let Some(db) = path.split('/').nth(1) {
                if let Some(db) = db.split('?').next() {
                    if !db.is_empty() {
                        return db.to_string();
                    }
                }
            }
        }

        "postgres".to_string()
    }

    /// Convert a PostgreSQL row to a CdcEvent.
    fn row_to_event(
        &self,
        row: &tokio_postgres::Row,
        schema: &str,
        table: &str,
        columns: &[String],
    ) -> CdcEvent {
        let mut data = serde_json::Map::new();

        for (i, col_name) in columns.iter().enumerate() {
            let value = Self::extract_column_value(row, i);
            data.insert(col_name.clone(), value);
        }

        CdcEvent {
            source_type: "postgres".to_string(),
            database: self.database.clone(),
            schema: schema.to_string(),
            table: table.to_string(),
            op: CdcOp::Snapshot,
            before: None,
            after: Some(serde_json::Value::Object(data)),
            timestamp: chrono::Utc::now().timestamp(),
            transaction: None,
        }
    }

    /// Extract a column value from a row, handling various types.
    fn extract_column_value(row: &tokio_postgres::Row, idx: usize) -> serde_json::Value {
        // Try common types in order of likelihood
        if let Ok(v) = row.try_get::<_, Option<i64>>(idx) {
            return v
                .map(|n| serde_json::Value::Number(n.into()))
                .unwrap_or(serde_json::Value::Null);
        }
        if let Ok(v) = row.try_get::<_, Option<i32>>(idx) {
            return v
                .map(|n| serde_json::Value::Number(n.into()))
                .unwrap_or(serde_json::Value::Null);
        }
        if let Ok(v) = row.try_get::<_, Option<i16>>(idx) {
            return v
                .map(|n| serde_json::Value::Number(n.into()))
                .unwrap_or(serde_json::Value::Null);
        }
        if let Ok(v) = row.try_get::<_, Option<f64>>(idx) {
            return v
                .and_then(serde_json::Number::from_f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null);
        }
        if let Ok(v) = row.try_get::<_, Option<f32>>(idx) {
            return v
                .and_then(|n| serde_json::Number::from_f64(n as f64))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null);
        }
        if let Ok(v) = row.try_get::<_, Option<bool>>(idx) {
            return v
                .map(serde_json::Value::Bool)
                .unwrap_or(serde_json::Value::Null);
        }
        if let Ok(v) = row.try_get::<_, Option<String>>(idx) {
            return v
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null);
        }
        if let Ok(v) = row.try_get::<_, Option<chrono::NaiveDateTime>>(idx) {
            return v
                .map(|dt| serde_json::Value::String(dt.to_string()))
                .unwrap_or(serde_json::Value::Null);
        }
        if let Ok(v) = row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx) {
            return v
                .map(|dt| serde_json::Value::String(dt.to_rfc3339()))
                .unwrap_or(serde_json::Value::Null);
        }
        if let Ok(v) = row.try_get::<_, Option<Vec<u8>>>(idx) {
            return v
                .map(|bytes| serde_json::Value::String(hex::encode(bytes)))
                .unwrap_or(serde_json::Value::Null);
        }

        serde_json::Value::Null
    }

    /// Get column names for a table.
    async fn get_columns(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        let query = r#"
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
        "#;

        let rows = self
            .client
            .query(query, &[&schema, &table])
            .await
            .map_err(|e| CdcError::replication(format!("Failed to get columns: {}", e)))?;

        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    /// Extract key value from a row for pagination.
    fn extract_key_value(row: &tokio_postgres::Row, key_idx: usize) -> Option<String> {
        if let Ok(v) = row.try_get::<_, String>(key_idx) {
            return Some(v);
        }
        if let Ok(v) = row.try_get::<_, i64>(key_idx) {
            return Some(v.to_string());
        }
        if let Ok(v) = row.try_get::<_, i32>(key_idx) {
            return Some(v.to_string());
        }
        if let Ok(v) = row.try_get::<_, i16>(key_idx) {
            return Some(v.to_string());
        }
        None
    }
}

#[async_trait]
impl SnapshotSource for PostgresSnapshotSource {
    async fn get_watermark(&self) -> Result<String> {
        let row = self
            .client
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await
            .map_err(|e| CdcError::replication(format!("Failed to get WAL LSN: {}", e)))?;

        let lsn: String = row.get(0);
        debug!("PostgreSQL watermark (WAL LSN): {}", lsn);
        Ok(lsn)
    }

    async fn estimate_row_count(&self, schema: &str, table: &str) -> Result<Option<u64>> {
        // Use pg_class statistics for fast estimate (updated by ANALYZE)
        let query = r#"
            SELECT GREATEST(reltuples::bigint, 0) 
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND c.relname = $2
        "#;

        match self.client.query_one(query, &[&schema, &table]).await {
            Ok(row) => {
                let count: i64 = row.get(0);
                debug!("Estimated row count for {}.{}: {}", schema, table, count);
                Ok(if count >= 0 { Some(count as u64) } else { None })
            }
            Err(e) => {
                warn!(
                    "Failed to estimate row count for {}.{}: {}",
                    schema, table, e
                );
                Ok(None)
            }
        }
    }

    async fn fetch_batch(
        &self,
        schema: &str,
        table: &str,
        key_column: &str,
        last_key: Option<&str>,
        batch_size: usize,
    ) -> Result<SnapshotBatch> {
        // Get column names
        let columns = self.get_columns(schema, table).await?;

        // Build SELECT query with keyset pagination (efficient, no OFFSET)
        let rows = if let Some(last) = last_key {
            let q = format!(
                r#"SELECT * FROM "{}"."{}" WHERE "{}" > $1 ORDER BY "{}" LIMIT {}"#,
                schema, table, key_column, key_column, batch_size
            );
            self.client
                .query(&q, &[&last])
                .await
                .map_err(|e| CdcError::replication(format!("Snapshot query failed: {}", e)))?
        } else {
            let q = format!(
                r#"SELECT * FROM "{}"."{}" ORDER BY "{}" LIMIT {}"#,
                schema, table, key_column, batch_size
            );
            self.client
                .query(&q, &[])
                .await
                .map_err(|e| CdcError::replication(format!("Snapshot query failed: {}", e)))?
        };

        // Convert rows to events
        let events: Vec<CdcEvent> = rows
            .iter()
            .map(|row| self.row_to_event(row, schema, table, &columns))
            .collect();

        // Get last key for pagination
        let last_key = if let Some(last_row) = rows.last() {
            columns
                .iter()
                .position(|c| c == key_column)
                .and_then(|idx| Self::extract_key_value(last_row, idx))
        } else {
            None
        };

        let is_last = rows.len() < batch_size;

        debug!(
            "Fetched {} rows from {}.{} (last_key: {:?}, is_last: {})",
            events.len(),
            schema,
            table,
            last_key,
            is_last
        );

        Ok(SnapshotBatch {
            events,
            sequence: 0,
            is_last,
            last_key,
        })
    }
}

// ============================================================================
// Table Discovery Functions
// ============================================================================

/// Discover the primary key column for a PostgreSQL table.
///
/// # Returns
///
/// - `Some(column_name)` if a primary key exists
/// - `None` if no primary key (consider using `ctid` as fallback)
///
/// # Example
///
/// ```rust,ignore
/// let pk = discover_primary_key(&client, "public", "users").await?;
/// let key_column = pk.unwrap_or_else(|| "ctid".to_string());
/// ```
pub async fn discover_primary_key(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
) -> Result<Option<String>> {
    let query = r#"
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2
        AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum)
        LIMIT 1
    "#;

    match client.query_opt(query, &[&schema, &table]).await {
        Ok(Some(row)) => {
            let pk: String = row.get(0);
            debug!("Primary key for {}.{}: {}", schema, table, pk);
            Ok(Some(pk))
        }
        Ok(None) => {
            debug!("No primary key for {}.{}", schema, table);
            Ok(None)
        }
        Err(e) => {
            warn!(
                "Failed to discover primary key for {}.{}: {}",
                schema, table, e
            );
            Ok(None)
        }
    }
}

/// Discover all tables in given schemas with their primary keys.
///
/// Returns tuples of (schema, table, key_column). If a table has no primary key,
/// `ctid` is used as a fallback (PostgreSQL's internal row identifier).
///
/// # Example
///
/// ```rust,ignore
/// let tables = discover_tables_with_keys(&client, &["public", "app"]).await?;
/// for (schema, table, key) in tables {
///     println!("Found {}.{} with key column: {}", schema, table, key);
/// }
/// ```
pub async fn discover_tables_with_keys(
    client: &tokio_postgres::Client,
    schemas: &[impl AsRef<str>],
) -> Result<Vec<(String, String, String)>> {
    let mut results = Vec::new();

    for schema in schemas {
        let schema = schema.as_ref();

        // Get all base tables (not views) in the schema
        let query = r#"
            SELECT t.table_name,
                   COALESCE(
                       (SELECT a.attname 
                        FROM pg_index i
                        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                        JOIN pg_class c ON c.oid = i.indrelid
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = t.table_schema AND c.relname = t.table_name
                        AND i.indisprimary
                        LIMIT 1),
                       'ctid'
                   ) as key_column
            FROM information_schema.tables t
            WHERE t.table_schema = $1
            AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name
        "#;

        let rows = client
            .query(query, &[&schema])
            .await
            .map_err(|e| CdcError::replication(format!("Failed to discover tables: {}", e)))?;

        for row in rows {
            let table: String = row.get(0);
            let key: String = row.get(1);
            results.push((schema.to_string(), table, key));
        }
    }

    debug!("Discovered {} tables", results.len());
    Ok(results)
}

/// Get table statistics including row count estimate and table size.
pub async fn get_table_statistics(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
) -> Result<TableStatistics> {
    let query = r#"
        SELECT 
            GREATEST(c.reltuples::bigint, 0) as row_estimate,
            pg_table_size((n.nspname || '.' || c.relname)::regclass) as table_size,
            pg_indexes_size((n.nspname || '.' || c.relname)::regclass) as index_size
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2
    "#;

    match client.query_one(query, &[&schema, &table]).await {
        Ok(row) => Ok(TableStatistics {
            row_estimate: row.get::<_, i64>(0) as u64,
            table_size_bytes: row.get::<_, i64>(1) as u64,
            index_size_bytes: row.get::<_, i64>(2) as u64,
        }),
        Err(e) => Err(CdcError::replication(format!(
            "Failed to get table statistics: {}",
            e
        ))),
    }
}

/// Table statistics for capacity planning.
#[derive(Debug, Clone)]
pub struct TableStatistics {
    /// Estimated row count (from pg_class.reltuples)
    pub row_estimate: u64,
    /// Table data size in bytes
    pub table_size_bytes: u64,
    /// Index size in bytes
    pub index_size_bytes: u64,
}

impl TableStatistics {
    /// Total size (table + indexes)
    pub fn total_size_bytes(&self) -> u64 {
        self.table_size_bytes + self.index_size_bytes
    }

    /// Human-readable table size
    pub fn table_size_human(&self) -> String {
        Self::format_bytes(self.table_size_bytes)
    }

    fn format_bytes(bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;

        if bytes >= GB {
            format!("{:.2} GB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.2} MB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.2} KB", bytes as f64 / KB as f64)
        } else {
            format!("{} B", bytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_database_name() {
        // URL format
        assert_eq!(
            PostgresSnapshotSource::extract_database_name("postgresql://localhost/mydb"),
            "mydb"
        );
        assert_eq!(
            PostgresSnapshotSource::extract_database_name(
                "postgresql://user:pass@host:5432/testdb?sslmode=require"
            ),
            "testdb"
        );

        // Key=value format
        assert_eq!(
            PostgresSnapshotSource::extract_database_name("host=localhost dbname=mydb user=test"),
            "mydb"
        );

        // Default fallback
        assert_eq!(
            PostgresSnapshotSource::extract_database_name("host=localhost user=test"),
            "postgres"
        );
    }

    #[test]
    fn test_table_statistics_formatting() {
        let stats = TableStatistics {
            row_estimate: 1_000_000,
            table_size_bytes: 1024 * 1024 * 512, // 512 MB
            index_size_bytes: 1024 * 1024 * 64,  // 64 MB
        };

        assert_eq!(stats.table_size_human(), "512.00 MB");
        assert_eq!(stats.total_size_bytes(), 1024 * 1024 * 576);
    }
}
