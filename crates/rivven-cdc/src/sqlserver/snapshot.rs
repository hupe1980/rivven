//! SQL Server snapshot support for initial data sync
//!
//! Provides initial table snapshots before transitioning to CDC streaming.

use super::protocol::SqlServerClient;
use super::source::{Lsn, SqlServerCdcConfig};
use crate::common::{CdcError, CdcEvent, CdcOp, Result, SnapshotMode, TransactionMetadata};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Snapshot progress for a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSnapshotProgress {
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Total rows in table (at snapshot start)
    pub total_rows: u64,
    /// Rows processed so far
    pub processed_rows: u64,
    /// Last processed primary key (for resume)
    pub last_key: Option<String>,
    /// Whether snapshot is complete
    pub complete: bool,
}

/// Overall snapshot state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotState {
    /// Database name
    pub database: String,
    /// LSN at snapshot start (watermark for CDC handoff)
    pub snapshot_lsn: String,
    /// Per-table progress
    pub tables: HashMap<String, TableSnapshotProgress>,
    /// Snapshot mode used
    pub mode: String,
    /// Start timestamp
    pub started_at: i64,
    /// Completion timestamp
    pub completed_at: Option<i64>,
}

impl SnapshotState {
    /// Create new snapshot state
    pub fn new(database: &str, snapshot_lsn: &Lsn, mode: SnapshotMode) -> Self {
        Self {
            database: database.to_string(),
            snapshot_lsn: snapshot_lsn.to_hex(),
            tables: HashMap::new(),
            mode: format!("{:?}", mode),
            started_at: chrono::Utc::now().timestamp(),
            completed_at: None,
        }
    }

    /// Check if all tables are complete
    pub fn is_complete(&self) -> bool {
        self.tables.values().all(|t| t.complete)
    }

    /// Add a table to track
    pub fn add_table(&mut self, schema: &str, table: &str, total_rows: u64) {
        let key = format!("{}.{}", schema, table);
        self.tables.insert(
            key,
            TableSnapshotProgress {
                schema: schema.to_string(),
                table: table.to_string(),
                total_rows,
                processed_rows: 0,
                last_key: None,
                complete: false,
            },
        );
    }

    /// Update table progress
    pub fn update_progress(
        &mut self,
        schema: &str,
        table: &str,
        processed: u64,
        last_key: Option<String>,
    ) {
        let key = format!("{}.{}", schema, table);
        if let Some(progress) = self.tables.get_mut(&key) {
            progress.processed_rows = processed;
            progress.last_key = last_key;
            if processed >= progress.total_rows {
                progress.complete = true;
            }
        }
    }

    /// Mark table as complete
    pub fn mark_complete(&mut self, schema: &str, table: &str) {
        let key = format!("{}.{}", schema, table);
        if let Some(progress) = self.tables.get_mut(&key) {
            progress.complete = true;
        }
    }

    /// Save state to file
    pub async fn save(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| CdcError::config(format!("Failed to serialize snapshot state: {}", e)))?;
        tokio::fs::write(path, json).await.map_err(|e| {
            CdcError::config(format!(
                "Failed to write snapshot state to {:?}: {}",
                path, e
            ))
        })?;
        Ok(())
    }

    /// Load state from file
    pub async fn load(path: &Path) -> Result<Self> {
        let json = tokio::fs::read_to_string(path).await.map_err(|e| {
            CdcError::config(format!(
                "Failed to read snapshot state from {:?}: {}",
                path, e
            ))
        })?;
        serde_json::from_str(&json)
            .map_err(|e| CdcError::config(format!("Failed to parse snapshot state: {}", e)))
    }
}

/// SQL Server snapshot executor
pub struct SqlServerSnapshotExecutor {
    config: SqlServerCdcConfig,
    batch_size: u32,
    event_tx: mpsc::Sender<CdcEvent>,
}

impl SqlServerSnapshotExecutor {
    /// Create a new snapshot executor
    pub fn new(
        config: SqlServerCdcConfig,
        batch_size: u32,
        event_tx: mpsc::Sender<CdcEvent>,
    ) -> Self {
        Self {
            config,
            batch_size,
            event_tx,
        }
    }

    /// Execute snapshot for specified tables
    ///
    /// Returns the LSN at which the snapshot was taken (watermark for CDC handoff)
    pub async fn execute(
        &self,
        tables: &[(String, String)], // (schema, table) pairs
        progress_dir: Option<&Path>,
    ) -> Result<Lsn> {
        info!("Starting SQL Server snapshot for {} tables", tables.len());

        // Connect to SQL Server
        let mut client = SqlServerClient::connect(&self.config).await?;

        // Get current max LSN as watermark
        let snapshot_lsn = client.get_max_lsn().await?;
        info!("Snapshot watermark LSN: {}", snapshot_lsn);

        // Initialize or load progress state
        let state_path = progress_dir.map(|p| p.join("sqlserver_snapshot_state.json"));
        let mut state = if let Some(ref path) = state_path {
            if path.exists() {
                match SnapshotState::load(path).await {
                    Ok(s) => {
                        info!("Resuming snapshot from saved state");
                        s
                    }
                    Err(e) => {
                        warn!("Failed to load snapshot state, starting fresh: {}", e);
                        SnapshotState::new(
                            &self.config.database,
                            &snapshot_lsn,
                            SnapshotMode::Initial,
                        )
                    }
                }
            } else {
                SnapshotState::new(&self.config.database, &snapshot_lsn, SnapshotMode::Initial)
            }
        } else {
            SnapshotState::new(&self.config.database, &snapshot_lsn, SnapshotMode::Initial)
        };

        // Get row counts and initialize progress tracking
        for (schema, table) in tables {
            let key = format!("{}.{}", schema, table);
            if !state.tables.contains_key(&key) {
                let count = client.get_table_row_count(schema, table).await?;
                state.add_table(schema, table, count);
                debug!("Table {}.{} has {} rows", schema, table, count);
            }
        }

        // Snapshot each table
        for (schema, table) in tables {
            let key = format!("{}.{}", schema, table);

            // Skip if already complete
            if state.tables.get(&key).map(|t| t.complete).unwrap_or(false) {
                info!("Skipping {}.{} (already complete)", schema, table);
                continue;
            }

            info!("Snapshotting table {}.{}", schema, table);

            // Get columns for this table from capture instance
            let instances = client.discover_capture_instances().await?;
            let columns: Vec<String> = instances
                .iter()
                .find(|i| i.source_schema == *schema && i.source_table == *table)
                .map(|i| i.columns.clone())
                .unwrap_or_default();

            // Get primary key columns for deterministic pagination ordering
            let pk_columns = client.get_pk_columns_by_name(schema, table).await?;

            // Get starting offset from progress
            let start_offset = state
                .tables
                .get(&key)
                .map(|t| t.processed_rows)
                .unwrap_or(0);

            // Paginate through table
            let mut offset = start_offset;
            loop {
                let rows = client
                    .snapshot_table(
                        schema,
                        table,
                        &columns,
                        &pk_columns,
                        self.batch_size,
                        offset,
                    )
                    .await?;

                if rows.is_empty() {
                    break;
                }

                let row_count = rows.len() as u64;

                // Emit snapshot events
                for row in rows {
                    let event = CdcEvent {
                        source_type: "sqlserver".to_string(),
                        database: self.config.database.clone(),
                        schema: schema.clone(),
                        table: table.clone(),
                        op: CdcOp::Snapshot, // Snapshot read
                        before: None,
                        after: Some(Value::Object(row)),
                        timestamp: chrono::Utc::now().timestamp(),
                        transaction: Some(TransactionMetadata {
                            id: format!("snapshot:{}", snapshot_lsn.to_hex()),
                            lsn: snapshot_lsn.to_hex(),
                            sequence: offset as u64,
                            total_events: state.tables.get(&key).map(|t| t.total_rows).unwrap_or(0),
                            commit_ts: Some(chrono::Utc::now().timestamp()),
                            is_last: false,
                        }),
                    };

                    if self.event_tx.send(event).await.is_err() {
                        warn!("Event receiver dropped during snapshot");
                        return Err(CdcError::config("Event receiver dropped"));
                    }
                }

                offset += row_count;

                // Update progress
                state.update_progress(schema, table, offset, None);

                // Save progress periodically
                if let Some(ref path) = state_path {
                    if offset % (self.batch_size as u64 * 10) == 0 {
                        state.save(path).await?;
                    }
                }

                debug!(
                    "Snapshotted {}.{}: {}/{} rows",
                    schema,
                    table,
                    offset,
                    state.tables.get(&key).map(|t| t.total_rows).unwrap_or(0)
                );
            }

            state.mark_complete(schema, table);
            info!("Completed snapshot of {}.{}", schema, table);

            // Save final progress
            if let Some(ref path) = state_path {
                state.save(path).await?;
            }
        }

        // Mark overall snapshot complete
        state.completed_at = Some(chrono::Utc::now().timestamp());
        if let Some(ref path) = state_path {
            state.save(path).await?;
        }

        info!(
            "Snapshot complete. Watermark LSN: {} for CDC handoff",
            snapshot_lsn
        );

        Ok(snapshot_lsn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_state_serialization() {
        let lsn = Lsn::from_hex("00000001000000010001").unwrap();
        let mut state = SnapshotState::new("testdb", &lsn, SnapshotMode::Initial);
        state.add_table("dbo", "users", 1000);
        state.update_progress("dbo", "users", 500, Some("id:500".to_string()));

        let json = serde_json::to_string(&state).unwrap();
        let restored: SnapshotState = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.database, "testdb");
        assert_eq!(restored.tables.len(), 1);
        assert_eq!(restored.tables["dbo.users"].processed_rows, 500);
    }

    #[test]
    fn test_snapshot_state_completion() {
        let lsn = Lsn::from_hex("00000001000000010001").unwrap();
        let mut state = SnapshotState::new("testdb", &lsn, SnapshotMode::Initial);
        state.add_table("dbo", "users", 100);
        state.add_table("dbo", "orders", 200);

        assert!(!state.is_complete());

        state.mark_complete("dbo", "users");
        assert!(!state.is_complete());

        state.mark_complete("dbo", "orders");
        assert!(state.is_complete());
    }

    #[test]
    fn test_progress_auto_complete() {
        let lsn = Lsn::from_hex("00000001000000010001").unwrap();
        let mut state = SnapshotState::new("testdb", &lsn, SnapshotMode::Initial);
        state.add_table("dbo", "users", 100);

        // Update to total rows should auto-complete
        state.update_progress("dbo", "users", 100, None);
        assert!(state.tables["dbo.users"].complete);
    }
}
