//! # Progress Store Implementations
//!
//! File-based implementations of `ProgressStore` trait for snapshot resumability.
//!
//! ## Offset Storage Philosophy
//!
//! **Key principle**: Offsets/progress are stored in the DESTINATION system,
//! never in the source database. This is because:
//!
//! 1. Source databases might be read-only replicas
//! 2. We don't want to pollute source schemas with CDC metadata
//! 3. The broker/destination is the natural consumer offset location
//!
//! ## Available Stores
//!
//! | Store | Use Case |
//! |-------|----------|
//! | `MemoryProgressStore` | Testing, ephemeral snapshots |
//! | `FileProgressStore` | Single-node production |
//! | Broker-based (via topics) | Distributed/HA deployments |
//!
//! For distributed deployments, store progress in Rivven topics (similar to
//! Kafka Connect's `connect-offsets` topic). See `rivven-connect` for
//! broker-based offset storage via consumer groups.
//!
//! ## File Progress Store
//!
//! ```rust,ignore
//! use rivven_cdc::common::FileProgressStore;
//!
//! let store = FileProgressStore::new("/var/lib/rivven/snapshot-progress").await?;
//!
//! // Progress is automatically persisted to:
//! // /var/lib/rivven/snapshot-progress/schema.table.json
//! ```
//!
//! ## Resumability
//!
//! All progress stores support resuming interrupted snapshots:
//!
//! ```rust,ignore
//! // On restart, check for existing progress
//! if let Some(progress) = store.load("public", "users").await? {
//!     println!("Resuming from row {}", progress.rows_processed);
//!     // Use progress.last_key to continue from where we left off
//! }
//! ```

use crate::common::{CdcError, ProgressStore, Result, SnapshotProgress};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs;
use tokio::sync::RwLock;
use tracing::{debug, warn};

// ============================================================================
// File-based Progress Store
// ============================================================================

/// File-based progress store for snapshot resumability.
///
/// Persists progress to JSON files in a directory:
/// ```text
/// progress_dir/
///   schema.table.json
/// ```
///
/// # Features
///
/// - Automatic directory creation
/// - Atomic writes (via temp file + rename)
/// - In-memory cache for fast reads
/// - Automatic loading of existing progress on startup
///
/// # Example
///
/// ```rust,ignore
/// use rivven_cdc::common::FileProgressStore;
///
/// let store = FileProgressStore::new("/var/lib/rivven/progress").await?;
///
/// // Save progress
/// store.save(&progress).await?;
///
/// // Load progress
/// if let Some(p) = store.load("public", "users").await? {
///     println!("Rows processed: {}", p.rows_processed);
/// }
/// ```
pub struct FileProgressStore {
    dir: PathBuf,
    cache: RwLock<HashMap<String, SnapshotProgress>>,
}

impl FileProgressStore {
    /// Create a new file progress store.
    ///
    /// Creates the directory if it doesn't exist and loads any existing progress files.
    pub async fn new(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();

        fs::create_dir_all(&dir)
            .await
            .map_err(|e| CdcError::config(format!("Failed to create progress directory: {}", e)))?;

        let store = Self {
            dir,
            cache: RwLock::new(HashMap::new()),
        };

        // Load existing progress files
        store.load_all().await?;

        Ok(store)
    }

    /// Get the directory path.
    pub fn dir(&self) -> &PathBuf {
        &self.dir
    }

    fn file_path(&self, schema: &str, table: &str) -> PathBuf {
        self.dir.join(format!("{}.{}.json", schema, table))
    }

    fn key(schema: &str, table: &str) -> String {
        format!("{}.{}", schema, table)
    }

    async fn load_all(&self) -> Result<()> {
        let mut entries = match fs::read_dir(&self.dir).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(CdcError::config(format!(
                    "Failed to read progress directory: {}",
                    e
                )))
            }
        };

        let mut cache = self.cache.write().await;
        let mut loaded = 0;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| CdcError::config(format!("Failed to read directory entry: {}", e)))?
        {
            let path = entry.path();

            // Only process .json files
            if path.extension().is_some_and(|ext| ext == "json") {
                match fs::read_to_string(&path).await {
                    Ok(content) => match serde_json::from_str::<SnapshotProgress>(&content) {
                        Ok(progress) => {
                            let key = Self::key(&progress.schema, &progress.table);
                            cache.insert(key, progress);
                            loaded += 1;
                        }
                        Err(e) => {
                            warn!("Failed to parse progress file {:?}: {}", path, e);
                        }
                    },
                    Err(e) => {
                        warn!("Failed to read progress file {:?}: {}", path, e);
                    }
                }
            }
        }

        if loaded > 0 {
            debug!(
                "Loaded {} snapshot progress files from {:?}",
                loaded, self.dir
            );
        }

        Ok(())
    }

    /// Clear all progress (useful for forced re-snapshots).
    pub async fn clear_all(&self) -> Result<()> {
        let mut cache = self.cache.write().await;

        for key in cache.keys().cloned().collect::<Vec<_>>() {
            let parts: Vec<&str> = key.split('.').collect();
            if parts.len() >= 2 {
                let path = self.file_path(parts[0], &parts[1..].join("."));
                if path.exists() {
                    fs::remove_file(&path).await.map_err(|e| {
                        CdcError::config(format!("Failed to delete progress file: {}", e))
                    })?;
                }
            }
        }

        cache.clear();
        debug!("Cleared all snapshot progress");
        Ok(())
    }
}

#[async_trait]
impl ProgressStore for FileProgressStore {
    async fn save(&self, progress: &SnapshotProgress) -> Result<()> {
        let key = Self::key(&progress.schema, &progress.table);
        let path = self.file_path(&progress.schema, &progress.table);

        // Serialize to JSON (pretty for debugging)
        let content = serde_json::to_string_pretty(progress)
            .map_err(|e| CdcError::config(format!("Failed to serialize progress: {}", e)))?;

        // Write to temp file first, then rename (atomic on most filesystems)
        let temp_path = path.with_extension("json.tmp");
        fs::write(&temp_path, &content)
            .await
            .map_err(|e| CdcError::config(format!("Failed to write progress file: {}", e)))?;

        fs::rename(&temp_path, &path)
            .await
            .map_err(|e| CdcError::config(format!("Failed to rename progress file: {}", e)))?;

        // Update cache
        self.cache.write().await.insert(key, progress.clone());

        debug!(
            "Saved snapshot progress for {}.{}: {} rows",
            progress.schema, progress.table, progress.rows_processed
        );

        Ok(())
    }

    async fn load(&self, schema: &str, table: &str) -> Result<Option<SnapshotProgress>> {
        let key = Self::key(schema, table);
        Ok(self.cache.read().await.get(&key).cloned())
    }

    async fn delete(&self, schema: &str, table: &str) -> Result<()> {
        let key = Self::key(schema, table);
        let path = self.file_path(schema, table);

        // Remove file if exists
        if path.exists() {
            fs::remove_file(&path)
                .await
                .map_err(|e| CdcError::config(format!("Failed to delete progress file: {}", e)))?;
        }

        // Update cache
        self.cache.write().await.remove(&key);

        debug!("Deleted snapshot progress for {}.{}", schema, table);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<SnapshotProgress>> {
        Ok(self.cache.read().await.values().cloned().collect())
    }
}

// ============================================================================
// PostgreSQL Progress Store (Metadata Database)
// ============================================================================

/// PostgreSQL-backed progress store for a **dedicated metadata database**.
///
/// # ⚠️ Important: Do NOT Use the Source Database
///
/// Progress should be stored in the DESTINATION system (broker topics) or a
/// dedicated metadata database - NEVER in the source database being captured.
/// Reasons:
///
/// 1. Source databases might be read-only replicas
/// 2. Avoid polluting source schemas with CDC metadata
/// 3. Separation of concerns: CDC metadata belongs with CDC infrastructure
///
/// # Recommended Alternatives
///
/// - **Single-node**: Use `FileProgressStore`
/// - **Distributed**: Store progress in Rivven topics via consumer groups
///   (similar to Kafka Connect's `connect-offsets` topic)
///
/// # When to Use This Store
///
/// Only use `PostgresProgressStore` with a **dedicated metadata PostgreSQL**
/// instance that is separate from your source databases. This is useful for:
///
/// - Centralized CDC metadata management
/// - Organizations with existing PostgreSQL infrastructure for metadata
///
/// # Table Schema
///
/// ```sql
/// CREATE TABLE IF NOT EXISTS _rivven_snapshot_progress (
///     schema_name TEXT NOT NULL,
///     table_name TEXT NOT NULL,
///     state TEXT NOT NULL,
///     rows_processed BIGINT NOT NULL,
///     total_rows BIGINT,
///     last_key TEXT,
///     watermark TEXT,
///     started_at TIMESTAMPTZ,
///     completed_at TIMESTAMPTZ,
///     PRIMARY KEY (schema_name, table_name)
/// );
/// ```
#[cfg(feature = "postgres")]
pub struct PostgresProgressStore {
    client: std::sync::Arc<tokio_postgres::Client>,
    table_name: String,
}

#[cfg(feature = "postgres")]
impl PostgresProgressStore {
    /// Create a new PostgreSQL progress store.
    pub async fn new(
        client: std::sync::Arc<tokio_postgres::Client>,
        table_name: Option<&str>,
    ) -> Result<Self> {
        let table_name = table_name
            .unwrap_or("_rivven_snapshot_progress")
            .to_string();

        let store = Self { client, table_name };
        store.ensure_table().await?;

        Ok(store)
    }

    async fn ensure_table(&self) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                state TEXT NOT NULL,
                rows_processed BIGINT NOT NULL DEFAULT 0,
                total_rows BIGINT,
                last_key TEXT,
                watermark TEXT,
                started_at BIGINT,
                completed_at BIGINT,
                error TEXT,
                PRIMARY KEY (schema_name, table_name)
            )
            "#,
            self.table_name
        );

        self.client
            .execute(&query, &[])
            .await
            .map_err(|e| CdcError::config(format!("Failed to create progress table: {}", e)))?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl ProgressStore for PostgresProgressStore {
    async fn save(&self, progress: &SnapshotProgress) -> Result<()> {
        use crate::common::SnapshotState;

        let state_str = match &progress.state {
            SnapshotState::Pending => "pending",
            SnapshotState::InProgress => "in_progress",
            SnapshotState::Completed => "completed",
            SnapshotState::Failed => "failed",
            SnapshotState::Paused => "paused",
        };

        let query = format!(
            r#"
            INSERT INTO {} (schema_name, table_name, state, rows_processed, total_rows, last_key, watermark, started_at, completed_at, error)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (schema_name, table_name) 
            DO UPDATE SET 
                state = EXCLUDED.state,
                rows_processed = EXCLUDED.rows_processed,
                total_rows = EXCLUDED.total_rows,
                last_key = EXCLUDED.last_key,
                watermark = EXCLUDED.watermark,
                started_at = EXCLUDED.started_at,
                completed_at = EXCLUDED.completed_at,
                error = EXCLUDED.error
            "#,
            self.table_name
        );

        self.client
            .execute(
                &query,
                &[
                    &progress.schema,
                    &progress.table,
                    &state_str,
                    &(progress.rows_processed as i64),
                    &progress.total_rows.map(|r| r as i64),
                    &progress.last_key,
                    &progress.watermark,
                    &progress.started_at,
                    &progress.completed_at,
                    &progress.error,
                ],
            )
            .await
            .map_err(|e| CdcError::config(format!("Failed to save progress: {}", e)))?;

        Ok(())
    }

    async fn load(&self, schema: &str, table: &str) -> Result<Option<SnapshotProgress>> {
        use crate::common::SnapshotState;

        let query = format!(
            r#"
            SELECT state, rows_processed, total_rows, last_key, watermark, started_at, completed_at, error
            FROM {}
            WHERE schema_name = $1 AND table_name = $2
            "#,
            self.table_name
        );

        match self.client.query_opt(&query, &[&schema, &table]).await {
            Ok(Some(row)) => {
                let state_str: String = row.get(0);
                let state = match state_str.as_str() {
                    "pending" => SnapshotState::Pending,
                    "in_progress" => SnapshotState::InProgress,
                    "completed" => SnapshotState::Completed,
                    "failed" => SnapshotState::Failed,
                    "paused" => SnapshotState::Paused,
                    _ => SnapshotState::Pending,
                };

                Ok(Some(SnapshotProgress {
                    schema: schema.to_string(),
                    table: table.to_string(),
                    state,
                    rows_processed: row.get::<_, i64>(1) as u64,
                    total_rows: row.get::<_, Option<i64>>(2).map(|v| v as u64),
                    last_key: row.get(3),
                    watermark: row.get(4),
                    started_at: row.get(5),
                    completed_at: row.get(6),
                    error: row.get(7),
                }))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(CdcError::config(format!("Failed to load progress: {}", e))),
        }
    }

    async fn delete(&self, schema: &str, table: &str) -> Result<()> {
        let query = format!(
            "DELETE FROM {} WHERE schema_name = $1 AND table_name = $2",
            self.table_name
        );

        self.client
            .execute(&query, &[&schema, &table])
            .await
            .map_err(|e| CdcError::config(format!("Failed to delete progress: {}", e)))?;

        Ok(())
    }

    async fn list(&self) -> Result<Vec<SnapshotProgress>> {
        use crate::common::SnapshotState;

        let query = format!(
            r#"
            SELECT schema_name, table_name, state, rows_processed, total_rows, last_key, watermark, started_at, completed_at, error
            FROM {}
            "#,
            self.table_name
        );

        let rows = self
            .client
            .query(&query, &[])
            .await
            .map_err(|e| CdcError::config(format!("Failed to list progress: {}", e)))?;

        let progress = rows
            .iter()
            .map(|row| {
                let state_str: String = row.get(2);
                let state = match state_str.as_str() {
                    "pending" => SnapshotState::Pending,
                    "in_progress" => SnapshotState::InProgress,
                    "completed" => SnapshotState::Completed,
                    "failed" => SnapshotState::Failed,
                    "paused" => SnapshotState::Paused,
                    _ => SnapshotState::Pending,
                };

                SnapshotProgress {
                    schema: row.get(0),
                    table: row.get(1),
                    state,
                    rows_processed: row.get::<_, i64>(3) as u64,
                    total_rows: row.get::<_, Option<i64>>(4).map(|v| v as u64),
                    last_key: row.get(5),
                    watermark: row.get(6),
                    started_at: row.get(7),
                    completed_at: row.get(8),
                    error: row.get(9),
                }
            })
            .collect();

        Ok(progress)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::SnapshotState;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_file_progress_store_save_load() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileProgressStore::new(temp_dir.path()).await.unwrap();

        let progress = SnapshotProgress {
            schema: "public".to_string(),
            table: "users".to_string(),
            state: SnapshotState::InProgress,
            rows_processed: 1000,
            total_rows: Some(5000),
            last_key: Some("1000".to_string()),
            watermark: Some("0/1234".to_string()),
            started_at: Some(chrono::Utc::now().timestamp()),
            completed_at: None,
            error: None,
        };

        // Save
        store.save(&progress).await.unwrap();

        // Load
        let loaded = store.load("public", "users").await.unwrap();
        assert!(loaded.is_some());

        let loaded = loaded.unwrap();
        assert_eq!(loaded.rows_processed, 1000);
        assert_eq!(loaded.last_key, Some("1000".to_string()));
    }

    #[tokio::test]
    async fn test_file_progress_store_delete() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileProgressStore::new(temp_dir.path()).await.unwrap();

        let progress = SnapshotProgress {
            schema: "public".to_string(),
            table: "orders".to_string(),
            state: SnapshotState::Completed,
            rows_processed: 5000,
            total_rows: Some(5000),
            last_key: None,
            watermark: None,
            started_at: None,
            completed_at: Some(chrono::Utc::now().timestamp()),
            error: None,
        };

        store.save(&progress).await.unwrap();
        assert!(store.load("public", "orders").await.unwrap().is_some());

        store.delete("public", "orders").await.unwrap();
        assert!(store.load("public", "orders").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_file_progress_store_list() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileProgressStore::new(temp_dir.path()).await.unwrap();

        let progress1 = SnapshotProgress {
            schema: "public".to_string(),
            table: "users".to_string(),
            state: SnapshotState::Completed,
            rows_processed: 1000,
            total_rows: Some(1000),
            last_key: None,
            watermark: None,
            started_at: None,
            completed_at: None,
            error: None,
        };

        let progress2 = SnapshotProgress {
            schema: "public".to_string(),
            table: "orders".to_string(),
            state: SnapshotState::InProgress,
            rows_processed: 500,
            total_rows: Some(2000),
            last_key: Some("500".to_string()),
            watermark: None,
            started_at: None,
            completed_at: None,
            error: None,
        };

        store.save(&progress1).await.unwrap();
        store.save(&progress2).await.unwrap();

        let all = store.list().await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn test_file_progress_store_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create store and save progress
        {
            let store = FileProgressStore::new(&path).await.unwrap();
            let progress = SnapshotProgress {
                schema: "public".to_string(),
                table: "persistent".to_string(),
                state: SnapshotState::InProgress,
                rows_processed: 999,
                total_rows: Some(9999),
                last_key: Some("999".to_string()),
                watermark: None,
                started_at: None,
                completed_at: None,
                error: None,
            };
            store.save(&progress).await.unwrap();
        }

        // Create new store and verify progress was loaded
        {
            let store = FileProgressStore::new(&path).await.unwrap();
            let loaded = store.load("public", "persistent").await.unwrap();
            assert!(loaded.is_some());
            assert_eq!(loaded.unwrap().rows_processed, 999);
        }
    }
}
