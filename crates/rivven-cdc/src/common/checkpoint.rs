//! # CDC Checkpointing
//!
//! Persistent offset tracking for resumable CDC operations.
//!
//! ## Features
//!
//! - **Durable Storage**: Persist checkpoints to disk
//! - **Atomic Updates**: Safe checkpoint updates with fsync
//! - **Multi-Source**: Track offsets for multiple tables/sources
//! - **Recovery**: Automatic resume from last checkpoint
//!
//! ## Checkpoint Formats
//!
//! ### PostgreSQL
//! - LSN (Log Sequence Number): `0/1234ABCD`
//!
//! ### MySQL/MariaDB
//! - Binlog position: `mysql-bin.000003:12345`
//! - GTID: `3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5`
//!
//! ## Usage
//!
//! ```ignore
//! use rivven_cdc::common::Checkpoint;
//!
//! // Create checkpoint store
//! let store = CheckpointStore::new("/var/rivven/checkpoints").await?;
//!
//! // Save checkpoint
//! store.save("mydb.public.users", Checkpoint::postgres_lsn(0x1234ABCD)).await?;
//!
//! // Load checkpoint on restart
//! if let Some(cp) = store.load("mydb.public.users").await? {
//!     cdc.resume_from(cp);
//! }
//! ```

use crate::common::{CdcError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// CDC checkpoint representing a position in the replication stream.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Checkpoint {
    /// Source type (postgres, mysql, mariadb)
    pub source_type: String,
    /// Position type
    pub position_type: PositionType,
    /// Position value (LSN, binlog position, or GTID)
    pub position: String,
    /// Timestamp when checkpoint was created
    pub timestamp: u64,
    /// Optional transaction ID
    pub transaction_id: Option<String>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Type of position tracking.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum PositionType {
    /// PostgreSQL Log Sequence Number
    PostgresLsn,
    /// MySQL binlog file + position
    MysqlBinlog,
    /// MySQL/MariaDB Global Transaction ID
    Gtid,
    /// Custom position type
    Custom,
}

impl Checkpoint {
    /// Create a PostgreSQL LSN checkpoint.
    pub fn postgres_lsn(lsn: u64) -> Self {
        Self {
            source_type: "postgres".to_string(),
            position_type: PositionType::PostgresLsn,
            position: format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFFFFFF),
            timestamp: current_timestamp(),
            transaction_id: None,
            metadata: HashMap::new(),
        }
    }

    /// Create a PostgreSQL LSN checkpoint from string.
    pub fn postgres_lsn_str(lsn: &str) -> Self {
        Self {
            source_type: "postgres".to_string(),
            position_type: PositionType::PostgresLsn,
            position: lsn.to_string(),
            timestamp: current_timestamp(),
            transaction_id: None,
            metadata: HashMap::new(),
        }
    }

    /// Create a MySQL binlog position checkpoint.
    pub fn mysql_binlog(file: &str, position: u64) -> Self {
        Self {
            source_type: "mysql".to_string(),
            position_type: PositionType::MysqlBinlog,
            position: format!("{}:{}", file, position),
            timestamp: current_timestamp(),
            transaction_id: None,
            metadata: HashMap::new(),
        }
    }

    /// Create a GTID checkpoint.
    pub fn gtid(source_type: &str, gtid: &str) -> Self {
        Self {
            source_type: source_type.to_string(),
            position_type: PositionType::Gtid,
            position: gtid.to_string(),
            timestamp: current_timestamp(),
            transaction_id: None,
            metadata: HashMap::new(),
        }
    }

    /// Set transaction ID.
    pub fn with_transaction_id(mut self, txn_id: &str) -> Self {
        self.transaction_id = Some(txn_id.to_string());
        self
    }

    /// Add metadata.
    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    /// Parse PostgreSQL LSN to u64.
    pub fn parse_postgres_lsn(&self) -> Option<u64> {
        if self.position_type != PositionType::PostgresLsn {
            return None;
        }
        parse_postgres_lsn(&self.position)
    }

    /// Parse MySQL binlog position.
    pub fn parse_mysql_binlog(&self) -> Option<(String, u64)> {
        if self.position_type != PositionType::MysqlBinlog {
            return None;
        }
        let parts: Vec<&str> = self.position.splitn(2, ':').collect();
        if parts.len() != 2 {
            return None;
        }
        let pos: u64 = parts[1].parse().ok()?;
        Some((parts[0].to_string(), pos))
    }

    /// Get age of checkpoint in seconds.
    pub fn age_secs(&self) -> u64 {
        current_timestamp().saturating_sub(self.timestamp)
    }
}

/// Parse PostgreSQL LSN string to u64.
fn parse_postgres_lsn(lsn: &str) -> Option<u64> {
    let parts: Vec<&str> = lsn.split('/').collect();
    if parts.len() != 2 {
        return None;
    }
    let high = u64::from_str_radix(parts[0], 16).ok()?;
    let low = u64::from_str_radix(parts[1], 16).ok()?;
    Some((high << 32) | low)
}

/// Get current Unix timestamp in seconds.
fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Persistent checkpoint storage.
///
/// Stores checkpoints as JSON files with atomic writes.
pub struct CheckpointStore {
    /// Base directory for checkpoint files
    base_dir: PathBuf,
    /// In-memory cache
    cache: RwLock<HashMap<String, Checkpoint>>,
    /// Whether to fsync after writes
    fsync: bool,
}

impl CheckpointStore {
    /// Create a new checkpoint store.
    pub async fn new(base_dir: impl AsRef<Path>) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        fs::create_dir_all(&base_dir).await.map_err(CdcError::Io)?;

        let store = Self {
            base_dir,
            cache: RwLock::new(HashMap::new()),
            fsync: true,
        };

        // Load existing checkpoints
        store.load_all().await?;

        Ok(store)
    }

    /// Create checkpoint store with custom options.
    pub async fn with_options(base_dir: impl AsRef<Path>, fsync: bool) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        fs::create_dir_all(&base_dir).await.map_err(CdcError::Io)?;

        let store = Self {
            base_dir,
            cache: RwLock::new(HashMap::new()),
            fsync,
        };

        store.load_all().await?;

        Ok(store)
    }

    /// Save a checkpoint.
    pub async fn save(&self, key: &str, checkpoint: Checkpoint) -> Result<()> {
        // Validate key
        if key.is_empty() || key.contains('/') || key.contains('\\') {
            return Err(CdcError::config("Invalid checkpoint key"));
        }

        // Write to file atomically
        let file_path = self.file_path(key);
        let temp_path = file_path.with_extension("tmp");

        let json = serde_json::to_string_pretty(&checkpoint)
            .map_err(|e| CdcError::Serialization(e.to_string()))?;

        // Write to temp file
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)
            .await
            .map_err(CdcError::Io)?;

        file.write_all(json.as_bytes()).await.map_err(CdcError::Io)?;

        if self.fsync {
            file.sync_all().await.map_err(CdcError::Io)?;
        }

        // Atomic rename
        fs::rename(&temp_path, &file_path)
            .await
            .map_err(CdcError::Io)?;

        // Update cache
        {
            let mut cache = self.cache.write().await;
            cache.insert(key.to_string(), checkpoint.clone());
        }

        debug!("Saved checkpoint for {}: {}", key, checkpoint.position);
        Ok(())
    }

    /// Load a checkpoint.
    pub async fn load(&self, key: &str) -> Result<Option<Checkpoint>> {
        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some(cp) = cache.get(key) {
                return Ok(Some(cp.clone()));
            }
        }

        // Load from file
        let file_path = self.file_path(key);
        if !file_path.exists() {
            return Ok(None);
        }

        let mut file = File::open(&file_path).await.map_err(CdcError::Io)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .await
            .map_err(CdcError::Io)?;

        let checkpoint: Checkpoint = serde_json::from_str(&contents)
            .map_err(|e| CdcError::Serialization(e.to_string()))?;

        // Update cache
        {
            let mut cache = self.cache.write().await;
            cache.insert(key.to_string(), checkpoint.clone());
        }

        Ok(Some(checkpoint))
    }

    /// Delete a checkpoint.
    pub async fn delete(&self, key: &str) -> Result<()> {
        let file_path = self.file_path(key);

        if file_path.exists() {
            fs::remove_file(&file_path).await.map_err(CdcError::Io)?;
        }

        {
            let mut cache = self.cache.write().await;
            cache.remove(key);
        }

        info!("Deleted checkpoint for {}", key);
        Ok(())
    }

    /// List all checkpoint keys.
    pub async fn list(&self) -> Result<Vec<String>> {
        let cache = self.cache.read().await;
        Ok(cache.keys().cloned().collect())
    }

    /// Get all checkpoints.
    pub async fn all(&self) -> Result<HashMap<String, Checkpoint>> {
        let cache = self.cache.read().await;
        Ok(cache.clone())
    }

    /// Load all checkpoints from disk.
    async fn load_all(&self) -> Result<()> {
        let mut entries = fs::read_dir(&self.base_dir).await.map_err(CdcError::Io)?;

        let mut loaded = 0;
        while let Some(entry) = entries.next_entry().await.map_err(CdcError::Io)? {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "json") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    match self.load(stem).await {
                        Ok(Some(_)) => loaded += 1,
                        Ok(None) => {}
                        Err(e) => {
                            warn!("Failed to load checkpoint {}: {}", stem, e);
                        }
                    }
                }
            }
        }

        if loaded > 0 {
            info!("Loaded {} checkpoints from {}", loaded, self.base_dir.display());
        }

        Ok(())
    }

    /// Get file path for a checkpoint key.
    fn file_path(&self, key: &str) -> PathBuf {
        self.base_dir.join(format!("{}.json", key))
    }
}

/// In-memory checkpoint tracker (for testing or when persistence isn't needed).
#[derive(Debug, Default)]
pub struct MemoryCheckpointStore {
    checkpoints: RwLock<HashMap<String, Checkpoint>>,
}

impl MemoryCheckpointStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn save(&self, key: &str, checkpoint: Checkpoint) -> Result<()> {
        let mut checkpoints = self.checkpoints.write().await;
        checkpoints.insert(key.to_string(), checkpoint);
        Ok(())
    }

    pub async fn load(&self, key: &str) -> Result<Option<Checkpoint>> {
        let checkpoints = self.checkpoints.read().await;
        Ok(checkpoints.get(key).cloned())
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let mut checkpoints = self.checkpoints.write().await;
        checkpoints.remove(key);
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<String>> {
        let checkpoints = self.checkpoints.read().await;
        Ok(checkpoints.keys().cloned().collect())
    }
}

/// Trait for checkpoint storage backends.
#[async_trait::async_trait]
pub trait CheckpointBackend: Send + Sync {
    async fn save(&self, key: &str, checkpoint: Checkpoint) -> Result<()>;
    async fn load(&self, key: &str) -> Result<Option<Checkpoint>>;
    async fn delete(&self, key: &str) -> Result<()>;
    async fn list(&self) -> Result<Vec<String>>;
}

#[async_trait::async_trait]
impl CheckpointBackend for CheckpointStore {
    async fn save(&self, key: &str, checkpoint: Checkpoint) -> Result<()> {
        CheckpointStore::save(self, key, checkpoint).await
    }

    async fn load(&self, key: &str) -> Result<Option<Checkpoint>> {
        CheckpointStore::load(self, key).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        CheckpointStore::delete(self, key).await
    }

    async fn list(&self) -> Result<Vec<String>> {
        CheckpointStore::list(self).await
    }
}

#[async_trait::async_trait]
impl CheckpointBackend for MemoryCheckpointStore {
    async fn save(&self, key: &str, checkpoint: Checkpoint) -> Result<()> {
        MemoryCheckpointStore::save(self, key, checkpoint).await
    }

    async fn load(&self, key: &str) -> Result<Option<Checkpoint>> {
        MemoryCheckpointStore::load(self, key).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        MemoryCheckpointStore::delete(self, key).await
    }

    async fn list(&self) -> Result<Vec<String>> {
        MemoryCheckpointStore::list(self).await
    }
}

/// Shared checkpoint backend.
pub type SharedCheckpointBackend = Arc<dyn CheckpointBackend>;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_postgres_lsn_checkpoint() {
        let cp = Checkpoint::postgres_lsn(0x0000000112345678);
        assert_eq!(cp.source_type, "postgres");
        assert_eq!(cp.position_type, PositionType::PostgresLsn);
        assert_eq!(cp.position, "1/12345678");
        assert_eq!(cp.parse_postgres_lsn(), Some(0x0000000112345678));
    }

    #[test]
    fn test_mysql_binlog_checkpoint() {
        let cp = Checkpoint::mysql_binlog("mysql-bin.000003", 12345);
        assert_eq!(cp.source_type, "mysql");
        assert_eq!(cp.position_type, PositionType::MysqlBinlog);
        assert_eq!(cp.position, "mysql-bin.000003:12345");
        assert_eq!(
            cp.parse_mysql_binlog(),
            Some(("mysql-bin.000003".to_string(), 12345))
        );
    }

    #[test]
    fn test_gtid_checkpoint() {
        let cp = Checkpoint::gtid("mysql", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5");
        assert_eq!(cp.position_type, PositionType::Gtid);
        assert!(cp.position.contains("3E11FA47"));
    }

    #[test]
    fn test_checkpoint_metadata() {
        let cp = Checkpoint::postgres_lsn(0x1234)
            .with_transaction_id("txn-123")
            .with_metadata("table", "users");

        assert_eq!(cp.transaction_id, Some("txn-123".to_string()));
        assert_eq!(cp.metadata.get("table"), Some(&"users".to_string()));
    }

    #[test]
    fn test_parse_postgres_lsn() {
        assert_eq!(parse_postgres_lsn("0/12345678"), Some(0x12345678));
        assert_eq!(parse_postgres_lsn("1/0"), Some(0x100000000));
        assert_eq!(parse_postgres_lsn("invalid"), None);
    }

    #[tokio::test]
    async fn test_memory_checkpoint_store() {
        let store = MemoryCheckpointStore::new();

        let cp = Checkpoint::postgres_lsn(0x1234);
        store.save("test-key", cp.clone()).await.unwrap();

        let loaded = store.load("test-key").await.unwrap();
        assert_eq!(loaded, Some(cp));

        assert_eq!(store.list().await.unwrap(), vec!["test-key"]);

        store.delete("test-key").await.unwrap();
        assert_eq!(store.load("test-key").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_persistent_checkpoint_store() {
        let dir = tempdir().unwrap();
        let store = CheckpointStore::new(dir.path()).await.unwrap();

        let cp = Checkpoint::mysql_binlog("mysql-bin.000001", 1234);
        store.save("mysql-test", cp.clone()).await.unwrap();

        // Create new store (simulates restart)
        let store2 = CheckpointStore::new(dir.path()).await.unwrap();
        let loaded = store2.load("mysql-test").await.unwrap();

        assert_eq!(loaded, Some(cp));
    }

    #[tokio::test]
    async fn test_checkpoint_store_invalid_key() {
        let dir = tempdir().unwrap();
        let store = CheckpointStore::new(dir.path()).await.unwrap();

        let cp = Checkpoint::postgres_lsn(0x1234);

        // Invalid keys should fail
        assert!(store.save("", cp.clone()).await.is_err());
        assert!(store.save("foo/bar", cp.clone()).await.is_err());
        assert!(store.save("foo\\bar", cp).await.is_err());
    }

    #[tokio::test]
    async fn test_checkpoint_age() {
        let cp = Checkpoint::postgres_lsn(0x1234);

        // Should be very recent
        assert!(cp.age_secs() < 2);

        // Simulate old checkpoint
        let mut old_cp = cp;
        old_cp.timestamp -= 3600; // 1 hour ago
        assert!(old_cp.age_secs() >= 3599);
    }

    #[tokio::test]
    async fn test_checkpoint_backend_trait() {
        let store: SharedCheckpointBackend = Arc::new(MemoryCheckpointStore::new());

        let cp = Checkpoint::postgres_lsn(0x5678);
        store.save("trait-test", cp.clone()).await.unwrap();

        let loaded = store.load("trait-test").await.unwrap();
        assert_eq!(loaded, Some(cp));
    }
}
