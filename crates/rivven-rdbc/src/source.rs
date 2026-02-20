//! Table source for reading data from databases
//!
//! Provides:
//! - TableSource: Query-based table polling
//! - Incremental (timestamp/incrementing) modes
//! - Bulk mode for full table scans
//! - Change tracking support

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::dialect::SqlDialect;
use crate::error::Result;
use crate::types::{TableMetadata, Value};

/// Query mode for table source
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum QueryMode {
    /// Full table scan (no tracking)
    #[default]
    Bulk,
    /// Incrementing column (auto-increment ID)
    Incrementing {
        /// Column name for tracking
        column: String,
    },
    /// Timestamp column (updated_at, etc)
    Timestamp {
        /// Column name for tracking
        column: String,
        /// Delay to handle out-of-order writes (e.g., 5s)
        delay: Option<Duration>,
    },
    /// Both incrementing and timestamp
    TimestampIncrementing {
        /// Incrementing column
        incrementing_column: String,
        /// Timestamp column
        timestamp_column: String,
        /// Delay
        delay: Option<Duration>,
    },
    /// Custom query with parameter placeholders
    Custom {
        /// SQL query with offset parameter
        query: String,
    },
}

impl QueryMode {
    /// Create incrementing mode
    pub fn incrementing(column: impl Into<String>) -> Self {
        Self::Incrementing {
            column: column.into(),
        }
    }

    /// Create timestamp mode
    pub fn timestamp(column: impl Into<String>) -> Self {
        Self::Timestamp {
            column: column.into(),
            delay: None,
        }
    }

    /// Create timestamp mode with delay
    pub fn timestamp_with_delay(column: impl Into<String>, delay: Duration) -> Self {
        Self::Timestamp {
            column: column.into(),
            delay: Some(delay),
        }
    }

    /// Create timestamp+incrementing mode
    pub fn timestamp_incrementing(
        timestamp_column: impl Into<String>,
        incrementing_column: impl Into<String>,
    ) -> Self {
        Self::TimestampIncrementing {
            incrementing_column: incrementing_column.into(),
            timestamp_column: timestamp_column.into(),
            delay: None,
        }
    }

    /// Check if this mode is incremental
    pub fn is_incremental(&self) -> bool {
        !matches!(self, Self::Bulk)
    }
}

/// Source offset for tracking position
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SourceOffset {
    /// Incrementing value (if using incrementing mode)
    pub incrementing: Option<Value>,
    /// Timestamp value (if using timestamp mode)
    pub timestamp: Option<Value>,
    /// Custom offset data
    pub custom: HashMap<String, Value>,
}

impl SourceOffset {
    /// Create a new offset with incrementing value
    pub fn with_incrementing(value: impl Into<Value>) -> Self {
        Self {
            incrementing: Some(value.into()),
            ..Default::default()
        }
    }

    /// Create a new offset with timestamp value
    pub fn with_timestamp(value: impl Into<Value>) -> Self {
        Self {
            timestamp: Some(value.into()),
            ..Default::default()
        }
    }

    /// Update incrementing value
    pub fn set_incrementing(&mut self, value: impl Into<Value>) {
        self.incrementing = Some(value.into());
    }

    /// Update timestamp value
    pub fn set_timestamp(&mut self, value: impl Into<Value>) {
        self.timestamp = Some(value.into());
    }

    /// Check if offset has any values
    pub fn is_empty(&self) -> bool {
        self.incrementing.is_none() && self.timestamp.is_none() && self.custom.is_empty()
    }
}

/// Table source configuration
#[derive(Debug, Clone)]
pub struct TableSourceConfig {
    /// Schema name
    pub schema: Option<String>,
    /// Table name
    pub table: String,
    /// Query mode
    pub mode: QueryMode,
    /// Columns to select (None = all)
    pub columns: Option<Vec<String>>,
    /// Additional WHERE clause
    pub where_clause: Option<String>,
    /// Batch size for fetching
    pub batch_size: u32,
    /// Poll interval
    pub poll_interval: Duration,
    /// Topic to write to (default: table name)
    pub topic: Option<String>,
}

impl TableSourceConfig {
    /// Create a new bulk source config
    pub fn bulk(table: impl Into<String>) -> Self {
        Self {
            schema: None,
            table: table.into(),
            mode: QueryMode::Bulk,
            columns: None,
            where_clause: None,
            batch_size: 1000,
            poll_interval: Duration::from_secs(5),
            topic: None,
        }
    }

    /// Create an incrementing source config
    pub fn incrementing(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            schema: None,
            table: table.into(),
            mode: QueryMode::incrementing(column),
            columns: None,
            where_clause: None,
            batch_size: 1000,
            poll_interval: Duration::from_secs(1),
            topic: None,
        }
    }

    /// Create a timestamp source config
    pub fn timestamp(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            schema: None,
            table: table.into(),
            mode: QueryMode::timestamp(column),
            columns: None,
            where_clause: None,
            batch_size: 1000,
            poll_interval: Duration::from_secs(1),
            topic: None,
        }
    }

    /// Set schema
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set columns
    pub fn with_columns(mut self, columns: Vec<String>) -> Self {
        self.columns = Some(columns);
        self
    }

    /// Set where clause
    pub fn with_where(mut self, clause: impl Into<String>) -> Self {
        self.where_clause = Some(clause.into());
        self
    }

    /// Set batch size
    pub fn with_batch_size(mut self, size: u32) -> Self {
        self.batch_size = size;
        self
    }

    /// Set poll interval
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Set topic
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// Get the topic name (defaults to table name)
    pub fn topic_name(&self) -> &str {
        self.topic.as_deref().unwrap_or(&self.table)
    }
}

/// Source record produced by table source
#[derive(Debug, Clone)]
pub struct SourceRecord {
    /// Schema
    pub schema: Option<String>,
    /// Table name
    pub table: String,
    /// Record key (primary key values)
    pub key: Vec<Value>,
    /// Record values (all columns)
    pub values: HashMap<String, Value>,
    /// Current offset
    pub offset: SourceOffset,
    /// Partition key (for routing)
    pub partition_key: Option<String>,
}

impl SourceRecord {
    /// Create a new source record
    pub fn new(
        schema: Option<String>,
        table: impl Into<String>,
        key: Vec<Value>,
        values: HashMap<String, Value>,
        offset: SourceOffset,
    ) -> Self {
        Self {
            schema,
            table: table.into(),
            key,
            values,
            offset,
            partition_key: None,
        }
    }

    /// Set partition key
    pub fn with_partition_key(mut self, key: impl Into<String>) -> Self {
        self.partition_key = Some(key.into());
        self
    }
}

/// Poll result from table source
#[derive(Debug)]
pub struct PollResult {
    /// Records fetched
    pub records: Vec<SourceRecord>,
    /// New offset after this poll
    pub offset: SourceOffset,
    /// Whether there are more records available
    pub has_more: bool,
}

impl PollResult {
    /// Create an empty result
    pub fn empty(offset: SourceOffset) -> Self {
        Self {
            records: vec![],
            offset,
            has_more: false,
        }
    }

    /// Check if result has records
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Get record count
    pub fn len(&self) -> usize {
        self.records.len()
    }
}

/// Source statistics
#[derive(Debug, Clone, Default)]
pub struct SourceStats {
    /// Total records polled
    pub records_polled: u64,
    /// Total polls performed
    pub polls: u64,
    /// Empty polls (no new data)
    pub empty_polls: u64,
    /// Total poll duration (milliseconds)
    pub total_poll_time_ms: u64,
    /// Average records per poll
    pub avg_records_per_poll: f64,
}

/// Atomic source statistics
#[derive(Debug, Default)]
#[allow(missing_docs)]
pub struct AtomicSourceStats {
    pub records_polled: AtomicU64,
    pub polls: AtomicU64,
    pub empty_polls: AtomicU64,
    pub total_poll_time_ms: AtomicU64,
}

impl AtomicSourceStats {
    /// Record a poll
    pub fn record_poll(&self, records: u64, duration_ms: u64) {
        self.records_polled.fetch_add(records, Ordering::Relaxed);
        self.polls.fetch_add(1, Ordering::Relaxed);
        self.total_poll_time_ms
            .fetch_add(duration_ms, Ordering::Relaxed);
        if records == 0 {
            self.empty_polls.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get a snapshot
    pub fn snapshot(&self) -> SourceStats {
        let records = self.records_polled.load(Ordering::Relaxed);
        let polls = self.polls.load(Ordering::Relaxed);
        let avg = if polls > 0 {
            records as f64 / polls as f64
        } else {
            0.0
        };

        SourceStats {
            records_polled: records,
            polls,
            empty_polls: self.empty_polls.load(Ordering::Relaxed),
            total_poll_time_ms: self.total_poll_time_ms.load(Ordering::Relaxed),
            avg_records_per_poll: avg,
        }
    }
}

/// Table source trait for polling data from databases
#[async_trait]
pub trait TableSource: Send + Sync {
    /// Poll for new records starting from the given offset
    async fn poll(&self, offset: &SourceOffset) -> Result<PollResult>;

    /// Get the table metadata
    async fn table_metadata(&self) -> Result<TableMetadata>;

    /// Get the current source configuration
    fn config(&self) -> &TableSourceConfig;

    /// Get source statistics
    fn stats(&self) -> SourceStats;
}

/// Query builder for table source
pub struct SourceQueryBuilder<'a> {
    config: &'a TableSourceConfig,
    dialect: &'a dyn SqlDialect,
}

impl<'a> SourceQueryBuilder<'a> {
    /// Create a new query builder
    pub fn new(config: &'a TableSourceConfig, dialect: &'a dyn SqlDialect) -> Self {
        Self { config, dialect }
    }

    /// Build the poll query
    pub fn build_poll_query(&self, offset: &SourceOffset) -> (String, Vec<Value>) {
        let columns = self
            .config
            .columns
            .as_ref()
            .map(|c| c.iter().map(String::as_str).collect::<Vec<_>>())
            .unwrap_or_default();

        let cols_str: Vec<_> = if columns.is_empty() {
            vec!["*".to_string()]
        } else {
            columns
                .iter()
                .map(|c| self.dialect.quote_identifier(c))
                .collect()
        };

        let table = match &self.config.schema {
            Some(s) => format!(
                "{}.{}",
                self.dialect.quote_identifier(s),
                self.dialect.quote_identifier(&self.config.table)
            ),
            None => self.dialect.quote_identifier(&self.config.table),
        };

        let mut conditions = Vec::new();
        let mut params = Vec::new();

        // Add mode-specific conditions
        match &self.config.mode {
            QueryMode::Bulk => {
                // No offset tracking for bulk
            }
            QueryMode::Incrementing { column } => {
                if let Some(ref val) = offset.incrementing {
                    conditions.push(format!(
                        "{} > {}",
                        self.dialect.quote_identifier(column),
                        self.dialect.placeholder(params.len() + 1)
                    ));
                    params.push(val.clone());
                }
            }
            QueryMode::Timestamp { column, delay } => {
                if let Some(ref val) = offset.timestamp {
                    conditions.push(format!(
                        "{} > {}",
                        self.dialect.quote_identifier(column),
                        self.dialect.placeholder(params.len() + 1)
                    ));
                    params.push(val.clone());
                }
                // Add delay condition if specified
                if let Some(_delay) = delay {
                    // use dialect-specific timestamp subtraction
                    conditions.push(self.dialect.timestamp_subtract_seconds(
                        &self.dialect.quote_identifier(column),
                        _delay.as_secs(),
                    ));
                }
            }
            QueryMode::TimestampIncrementing {
                incrementing_column,
                timestamp_column,
                delay: _,
            } => {
                if let (Some(ref ts), Some(ref inc)) = (&offset.timestamp, &offset.incrementing) {
                    conditions.push(format!(
                        "({} > {} OR ({} = {} AND {} > {}))",
                        self.dialect.quote_identifier(timestamp_column),
                        self.dialect.placeholder(params.len() + 1),
                        self.dialect.quote_identifier(timestamp_column),
                        self.dialect.placeholder(params.len() + 2),
                        self.dialect.quote_identifier(incrementing_column),
                        self.dialect.placeholder(params.len() + 3)
                    ));
                    params.push(ts.clone());
                    params.push(ts.clone());
                    params.push(inc.clone());
                }
            }
            QueryMode::Custom { query } => {
                // Custom query is used directly
                return (query.clone(), params);
            }
        }

        // Add user-specified where clause
        if let Some(ref clause) = self.config.where_clause {
            conditions.push(format!("({})", clause));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };

        // Build ORDER BY
        let order_by = match &self.config.mode {
            QueryMode::Bulk => String::new(),
            QueryMode::Incrementing { column } => {
                format!(" ORDER BY {} ASC", self.dialect.quote_identifier(column))
            }
            QueryMode::Timestamp { column, .. } => {
                format!(" ORDER BY {} ASC", self.dialect.quote_identifier(column))
            }
            QueryMode::TimestampIncrementing {
                incrementing_column,
                timestamp_column,
                ..
            } => {
                format!(
                    " ORDER BY {} ASC, {} ASC",
                    self.dialect.quote_identifier(timestamp_column),
                    self.dialect.quote_identifier(incrementing_column)
                )
            }
            QueryMode::Custom { .. } => String::new(),
        };

        let limit = self
            .dialect
            .limit_offset_sql(Some(u64::from(self.config.batch_size)), None);

        let sql = format!(
            "SELECT {} FROM {}{}{}{}",
            cols_str.join(", "),
            table,
            where_clause,
            order_by,
            limit
        );

        (sql, params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::PostgresDialect;

    #[test]
    fn test_query_mode() {
        assert!(!QueryMode::Bulk.is_incremental());
        assert!(QueryMode::incrementing("id").is_incremental());
        assert!(QueryMode::timestamp("updated_at").is_incremental());
    }

    #[test]
    fn test_source_offset() {
        let mut offset = SourceOffset::with_incrementing(100_i64);
        assert!(!offset.is_empty());
        assert_eq!(offset.incrementing, Some(Value::Int64(100)));

        offset.set_timestamp("2024-01-01T00:00:00Z");
        assert!(offset.timestamp.is_some());
    }

    #[test]
    fn test_table_source_config() {
        let config = TableSourceConfig::incrementing("users", "id")
            .with_schema("public")
            .with_batch_size(500)
            .with_poll_interval(Duration::from_secs(2))
            .with_topic("user-changes");

        assert_eq!(config.table, "users");
        assert_eq!(config.schema, Some("public".into()));
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.topic_name(), "user-changes");

        if let QueryMode::Incrementing { column } = &config.mode {
            assert_eq!(column, "id");
        } else {
            panic!("Expected Incrementing mode");
        }
    }

    #[test]
    fn test_source_record() {
        let mut values = HashMap::new();
        values.insert("id".into(), Value::Int32(1));
        values.insert("name".into(), Value::String("test".into()));

        let record = SourceRecord::new(
            Some("public".into()),
            "users",
            vec![Value::Int32(1)],
            values,
            SourceOffset::with_incrementing(1_i64),
        )
        .with_partition_key("user-1");

        assert_eq!(record.table, "users");
        assert_eq!(record.partition_key, Some("user-1".into()));
    }

    #[test]
    fn test_poll_result() {
        let result = PollResult::empty(SourceOffset::default());
        assert!(result.is_empty());
        assert_eq!(result.len(), 0);
        assert!(!result.has_more);
    }

    #[test]
    fn test_atomic_source_stats() {
        let stats = AtomicSourceStats::default();

        stats.record_poll(100, 50);
        stats.record_poll(0, 10); // empty poll
        stats.record_poll(50, 30);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.records_polled, 150);
        assert_eq!(snapshot.polls, 3);
        assert_eq!(snapshot.empty_polls, 1);
        assert_eq!(snapshot.total_poll_time_ms, 90);
        assert!((snapshot.avg_records_per_poll - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_source_query_builder_bulk() {
        let config = TableSourceConfig::bulk("users").with_schema("public");
        let dialect = PostgresDialect;
        let builder = SourceQueryBuilder::new(&config, &dialect);

        let (sql, params) = builder.build_poll_query(&SourceOffset::default());

        assert!(sql.contains("SELECT *"));
        assert!(sql.contains("\"public\".\"users\""));
        assert!(sql.contains("LIMIT 1000"));
        assert!(params.is_empty());
    }

    #[test]
    fn test_source_query_builder_incrementing() {
        let config = TableSourceConfig::incrementing("users", "id");
        let dialect = PostgresDialect;
        let builder = SourceQueryBuilder::new(&config, &dialect);

        let offset = SourceOffset::with_incrementing(100_i64);
        let (sql, params) = builder.build_poll_query(&offset);

        assert!(sql.contains("WHERE"));
        assert!(sql.contains("\"id\" > $1"));
        assert!(sql.contains("ORDER BY \"id\" ASC"));
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_source_query_builder_timestamp() {
        let config =
            TableSourceConfig::timestamp("events", "created_at").with_where("status = 'active'");
        let dialect = PostgresDialect;
        let builder = SourceQueryBuilder::new(&config, &dialect);

        let offset = SourceOffset::with_timestamp("2024-01-01T00:00:00Z");
        let (sql, params) = builder.build_poll_query(&offset);

        assert!(sql.contains("\"created_at\" > $1"));
        assert!(sql.contains("(status = 'active')"));
        assert!(sql.contains("ORDER BY \"created_at\" ASC"));
        assert_eq!(params.len(), 1);
    }
}
