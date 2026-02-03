//! MySQL Snapshot Source
//!
//! Implementation of `SnapshotSource` trait for MySQL/MariaDB databases.
//!
//! # Features
//!
//! - Efficient keyset pagination (no OFFSET)
//! - GTID watermark for consistency tracking
//! - Row count estimation via INFORMATION_SCHEMA
//! - Automatic column type inference
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_cdc::mysql::MySqlSnapshotSource;
//! use rivven_cdc::common::{SnapshotCoordinator, SnapshotConfig, MemoryProgressStore};
//!
//! // Create snapshot source
//! let source = MySqlSnapshotSource::connect("mysql://localhost/mydb").await?;
//!
//! // Get watermark (GTID set or binlog position)
//! let watermark = source.get_watermark().await?;
//! println!("Starting at position: {}", watermark);
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
//! use rivven_cdc::mysql::{discover_tables_with_keys, discover_primary_key};
//!
//! // Discover all tables in a database
//! let tables = discover_tables_with_keys(&pool, "mydb").await?;
//! for (database, table, key_column) in tables {
//!     println!("{}.{} (key: {})", database, table, key_column);
//! }
//! ```

use crate::common::{CdcError, CdcEvent, CdcOp, Result, SnapshotBatch, SnapshotSource};
use async_trait::async_trait;
use mysql_async::prelude::*;
use mysql_async::{Pool, Row, Value};
use std::sync::Arc;
use tracing::{debug, info, warn};

// ============================================================================
// MySQL Snapshot Source
// ============================================================================

/// MySQL/MariaDB implementation of `SnapshotSource`.
///
/// Uses keyset pagination for efficient batch fetching.
///
/// # Features
///
/// - Keyset pagination (WHERE key > last_key)
/// - GTID-based watermarks for consistency
/// - Automatic column type inference
/// - Row count estimation via INFORMATION_SCHEMA
///
/// # Example
///
/// ```rust,ignore
/// use rivven_cdc::mysql::MySqlSnapshotSource;
///
/// let source = MySqlSnapshotSource::connect("mysql://localhost/mydb").await?;
/// let watermark = source.get_watermark().await?;
/// ```
pub struct MySqlSnapshotSource {
    /// Connection pool
    pool: Arc<Pool>,
    /// Database name for events
    database: String,
}

impl MySqlSnapshotSource {
    /// Create a new MySQL snapshot source from an existing pool.
    pub fn new(pool: Arc<Pool>, database: impl Into<String>) -> Self {
        Self {
            pool,
            database: database.into(),
        }
    }

    /// Create from a connection string.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let source = MySqlSnapshotSource::connect(
    ///     "mysql://user:pass@localhost:3306/mydb"
    /// ).await?;
    /// ```
    pub async fn connect(conn_str: &str) -> Result<Self> {
        let database = Self::extract_database_name(conn_str);
        let pool = Pool::new(conn_str);

        // Test connection
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| CdcError::connection_refused(format!("MySQL connection failed: {}", e)))?;

        // Verify we can query
        let _: Option<Row> = conn
            .query_first("SELECT 1")
            .await
            .map_err(|e| CdcError::connection_refused(format!("MySQL query failed: {}", e)))?;

        info!("MySQL snapshot source connected to database: {}", database);

        Ok(Self {
            pool: Arc::new(pool),
            database,
        })
    }

    /// Create from existing pool with custom database name.
    pub fn from_pool(pool: Pool, database: impl Into<String>) -> Self {
        Self {
            pool: Arc::new(pool),
            database: database.into(),
        }
    }

    /// Get a reference to the underlying pool.
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Get the database name.
    pub fn database(&self) -> &str {
        &self.database
    }

    fn extract_database_name(conn_str: &str) -> String {
        // Handle mysql:// URL format
        if let Some(without_scheme) = conn_str.strip_prefix("mysql://") {
            // Find the path part after host:port
            if let Some(at_pos) = without_scheme.rfind('@') {
                let host_part = &without_scheme[at_pos + 1..];
                if let Some(slash_pos) = host_part.find('/') {
                    let db = &host_part[slash_pos + 1..];
                    // Remove query params
                    let db = db.split('?').next().unwrap_or(db);
                    if !db.is_empty() {
                        return db.to_string();
                    }
                }
            } else if let Some(slash_pos) = without_scheme.find('/') {
                let db = &without_scheme[slash_pos + 1..];
                let db = db.split('?').next().unwrap_or(db);
                if !db.is_empty() {
                    return db.to_string();
                }
            }
        }
        "mysql".to_string()
    }

    /// Convert a MySQL row to a CdcEvent.
    fn row_to_event(&self, row: Row, schema: &str, table: &str, columns: &[String]) -> CdcEvent {
        let mut data = serde_json::Map::new();

        for (i, col_name) in columns.iter().enumerate() {
            let value = Self::extract_column_value(&row, i);
            data.insert(col_name.clone(), value);
        }

        CdcEvent {
            source_type: "mysql".to_string(),
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
    fn extract_column_value(row: &Row, idx: usize) -> serde_json::Value {
        match row.get_opt::<Value, _>(idx) {
            Some(Ok(Value::NULL)) | None => serde_json::Value::Null,
            Some(Ok(Value::Int(n))) => serde_json::Value::Number(n.into()),
            Some(Ok(Value::UInt(n))) => serde_json::Value::Number(n.into()),
            Some(Ok(Value::Float(n))) => serde_json::Number::from_f64(n as f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Some(Ok(Value::Double(n))) => serde_json::Number::from_f64(n)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Some(Ok(Value::Bytes(bytes))) => {
                // Try to interpret as UTF-8 string first
                match String::from_utf8(bytes.clone()) {
                    Ok(s) => serde_json::Value::String(s),
                    Err(_) => serde_json::Value::String(hex::encode(bytes)),
                }
            }
            Some(Ok(Value::Date(year, month, day, hour, min, sec, micro))) => {
                let dt = format!(
                    "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}",
                    year, month, day, hour, min, sec, micro
                );
                serde_json::Value::String(dt)
            }
            Some(Ok(Value::Time(neg, days, hours, mins, secs, micro))) => {
                let sign = if neg { "-" } else { "" };
                let total_hours = days * 24 + hours as u32;
                let time = format!(
                    "{}{:02}:{:02}:{:02}.{:06}",
                    sign, total_hours, mins, secs, micro
                );
                serde_json::Value::String(time)
            }
            Some(Err(_)) => serde_json::Value::Null,
        }
    }

    /// Get column names for a table.
    async fn get_columns(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| CdcError::mysql(format!("Failed to get connection: {}", e)))?;

        let query = r"
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        ";

        let columns: Vec<Row> = conn
            .exec(query, (schema, table))
            .await
            .map_err(|e| CdcError::replication(format!("Failed to get columns: {}", e)))?;

        Ok(columns
            .into_iter()
            .map(|r| r.get(0).unwrap_or_default())
            .collect())
    }

    /// Extract key value from a row for pagination.
    fn extract_key_value(row: &Row, key_idx: usize) -> Option<String> {
        match row.get_opt::<Value, _>(key_idx) {
            Some(Ok(Value::Int(n))) => Some(n.to_string()),
            Some(Ok(Value::UInt(n))) => Some(n.to_string()),
            Some(Ok(Value::Bytes(bytes))) => String::from_utf8(bytes).ok(),
            _ => None,
        }
    }
}

#[async_trait]
impl SnapshotSource for MySqlSnapshotSource {
    async fn get_watermark(&self) -> Result<String> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| CdcError::mysql(format!("Failed to get connection: {}", e)))?;

        // Try GTID first (MySQL 5.6+)
        let gtid_result: Option<String> = conn
            .query_first("SELECT @@gtid_executed")
            .await
            .ok()
            .flatten();

        if let Some(gtid) = gtid_result {
            if !gtid.is_empty() {
                debug!("MySQL watermark (GTID): {}", gtid);
                return Ok(format!("gtid:{}", gtid));
            }
        }

        // Fall back to binlog position
        let rows: Vec<Row> = conn
            .query("SHOW MASTER STATUS")
            .await
            .map_err(|e| CdcError::replication(format!("Failed to get binlog position: {}", e)))?;

        if let Some(row) = rows.into_iter().next() {
            let file: String = row.get(0).unwrap_or_default();
            let pos: u64 = row.get(1).unwrap_or(4);
            let watermark = format!("binlog:{}:{}", file, pos);
            debug!("MySQL watermark (binlog): {}", watermark);
            return Ok(watermark);
        }

        // Fallback for replicas or minimal permissions
        warn!("Could not get MySQL watermark - using timestamp fallback");
        Ok(format!(
            "timestamp:{}",
            chrono::Utc::now().timestamp_millis()
        ))
    }

    async fn estimate_row_count(&self, schema: &str, table: &str) -> Result<Option<u64>> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| CdcError::mysql(format!("Failed to get connection: {}", e)))?;

        let query = r"
            SELECT TABLE_ROWS 
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ";

        let count: Option<u64> = conn
            .exec_first(query, (schema, table))
            .await
            .map_err(|e| CdcError::replication(format!("Failed to estimate row count: {}", e)))?;

        debug!("Estimated row count for {}.{}: {:?}", schema, table, count);

        Ok(count)
    }

    async fn fetch_batch(
        &self,
        schema: &str,
        table: &str,
        key_column: &str,
        last_key: Option<&str>,
        batch_size: usize,
    ) -> Result<SnapshotBatch> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| CdcError::mysql(format!("Failed to get connection: {}", e)))?;

        // Get column names
        let columns = self.get_columns(schema, table).await?;
        let key_idx = columns.iter().position(|c| c == key_column).unwrap_or(0);

        // Build SELECT query with keyset pagination (efficient, no OFFSET)
        let rows: Vec<Row> = if let Some(last) = last_key {
            let query = format!(
                "SELECT * FROM `{}`.`{}` WHERE `{}` > ? ORDER BY `{}` LIMIT {}",
                schema, table, key_column, key_column, batch_size
            );
            conn.exec(&query, (last,)).await
        } else {
            let query = format!(
                "SELECT * FROM `{}`.`{}` ORDER BY `{}` LIMIT {}",
                schema, table, key_column, batch_size
            );
            conn.query(&query).await
        }
        .map_err(|e| CdcError::replication(format!("Failed to fetch batch: {}", e)))?;

        let is_last = rows.len() < batch_size;
        let last_key_value = rows
            .last()
            .and_then(|row| Self::extract_key_value(row, key_idx));

        // Convert rows to events
        let events: Vec<CdcEvent> = rows
            .into_iter()
            .map(|row| self.row_to_event(row, schema, table, &columns))
            .collect();

        debug!(
            "Fetched {} rows from {}.{} (is_last: {})",
            events.len(),
            schema,
            table,
            is_last
        );

        Ok(SnapshotBatch {
            events,
            sequence: 0,
            is_last,
            last_key: last_key_value,
        })
    }
}

// ============================================================================
// Table Discovery Functions
// ============================================================================

/// Discover the primary key column for a MySQL table.
///
/// # Returns
///
/// - `Some(column_name)` if a primary key exists
/// - `None` if no primary key
///
/// # Example
///
/// ```rust,ignore
/// let pk = discover_primary_key(&pool, "mydb", "users").await?;
/// ```
pub async fn discover_primary_key(
    pool: &Pool,
    database: &str,
    table: &str,
) -> Result<Option<String>> {
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| CdcError::mysql(format!("Failed to get connection: {}", e)))?;

    let query = r"
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_KEY = 'PRI'
        ORDER BY ORDINAL_POSITION 
        LIMIT 1
    ";

    let pk: Option<String> = conn
        .exec_first(query, (database, table))
        .await
        .map_err(|e| CdcError::replication(format!("Failed to discover primary key: {}", e)))?;

    Ok(pk)
}

/// Discover all tables in a database with their primary keys.
///
/// # Returns
///
/// Vector of tuples: (database, table, primary_key_column)
///
/// # Example
///
/// ```rust,ignore
/// let tables = discover_tables_with_keys(&pool, "mydb").await?;
/// for (db, table, pk) in tables {
///     println!("{}.{} (pk: {})", db, table, pk);
/// }
/// ```
pub async fn discover_tables_with_keys(
    pool: &Pool,
    database: &str,
) -> Result<Vec<(String, String, String)>> {
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| CdcError::mysql(format!("Failed to get connection: {}", e)))?;

    let query = r"
        SELECT 
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            COALESCE(c.COLUMN_NAME, '') as PK_COLUMN
        FROM INFORMATION_SCHEMA.TABLES t
        LEFT JOIN INFORMATION_SCHEMA.COLUMNS c ON 
            t.TABLE_SCHEMA = c.TABLE_SCHEMA 
            AND t.TABLE_NAME = c.TABLE_NAME 
            AND c.COLUMN_KEY = 'PRI'
        WHERE t.TABLE_SCHEMA = ? AND t.TABLE_TYPE = 'BASE TABLE'
        GROUP BY t.TABLE_SCHEMA, t.TABLE_NAME
        ORDER BY t.TABLE_NAME
    ";

    let rows: Vec<(String, String, String)> = conn
        .exec(query, (database,))
        .await
        .map_err(|e| CdcError::replication(format!("Failed to discover tables: {}", e)))?;

    Ok(rows)
}

/// MySQL table statistics.
#[derive(Debug, Clone)]
pub struct MySqlTableStatistics {
    /// Estimated row count (from INFORMATION_SCHEMA.TABLES)
    pub row_estimate: u64,
    /// Table data length in bytes
    pub data_length: u64,
    /// Index length in bytes
    pub index_length: u64,
}

impl MySqlTableStatistics {
    /// Total size (data + indexes)
    pub fn total_size_bytes(&self) -> u64 {
        self.data_length + self.index_length
    }
}

/// Get table statistics from MySQL.
///
/// # Example
///
/// ```rust,ignore
/// let stats = get_table_statistics(&pool, "mydb", "users").await?;
/// println!("Rows: {}, Size: {} bytes", stats.row_estimate, stats.total_size_bytes());
/// ```
pub async fn get_table_statistics(
    pool: &Pool,
    database: &str,
    table: &str,
) -> Result<MySqlTableStatistics> {
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| CdcError::mysql(format!("Failed to get connection: {}", e)))?;

    let query = r"
        SELECT TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ";

    let result: Option<(u64, u64, u64)> = conn
        .exec_first(query, (database, table))
        .await
        .map_err(|e| CdcError::replication(format!("Failed to get table statistics: {}", e)))?;

    match result {
        Some((rows, data, index)) => Ok(MySqlTableStatistics {
            row_estimate: rows,
            data_length: data,
            index_length: index,
        }),
        None => Ok(MySqlTableStatistics {
            row_estimate: 0,
            data_length: 0,
            index_length: 0,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_database_name() {
        // Full URL with auth
        assert_eq!(
            MySqlSnapshotSource::extract_database_name("mysql://user:pass@localhost:3307/mydb"),
            "mydb"
        );

        // URL without auth
        assert_eq!(
            MySqlSnapshotSource::extract_database_name("mysql://localhost/testdb"),
            "testdb"
        );

        // URL with query params
        assert_eq!(
            MySqlSnapshotSource::extract_database_name("mysql://localhost/db?param=value"),
            "db"
        );

        // No database
        assert_eq!(
            MySqlSnapshotSource::extract_database_name("mysql://localhost"),
            "mysql"
        );

        // Non-URL format
        assert_eq!(
            MySqlSnapshotSource::extract_database_name("somedb"),
            "mysql"
        );
    }

    #[test]
    fn test_mysql_table_statistics() {
        let stats = MySqlTableStatistics {
            row_estimate: 1_000_000,
            data_length: 1024 * 1024 * 512,
            index_length: 1024 * 1024 * 64,
        };

        assert_eq!(stats.total_size_bytes(), 1024 * 1024 * 576);
    }
}
