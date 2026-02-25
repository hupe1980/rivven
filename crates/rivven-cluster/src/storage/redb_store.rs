//! redb-based Raft log storage
//!
//! Pure Rust implementation using redb - zero C dependencies, compiles
//! cleanly for all targets including musl.

// Suppress clippy warning for large error types from openraft crate
#![allow(clippy::result_large_err)]

use crate::error::{ClusterError, Result};
use openraft::storage::{LogState, RaftLogReader, RaftLogStorage};
use openraft::{StorageError, StorageIOError};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::raft::{NodeId, RaftEntry, RaftLogId, RaftVote, TypeConfig};

/// Table for Raft log entries (key: u64 index, value: serialized entry)
const LOGS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_logs");

/// Table for Raft state (key: string, value: serialized data)
const STATE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_state");

/// State keys
const KEY_VOTE: &str = "vote";
const KEY_LAST_PURGED: &str = "last_purged";
const KEY_COMMITTED: &str = "committed";

/// redb-backed Raft log storage
///
/// This is a pure Rust implementation that:
/// - Has zero C/C++ dependencies
/// - Compiles for any target (including musl)
/// - Provides ACID transactions
/// - Uses B-tree for efficient ordered access
///
/// # Performance Characteristics
///
/// - **Write**: O(log n) per entry, batched for throughput
/// - **Read**: O(log n) point lookup, O(k) range scan for k entries
/// - **Space**: ~1.5x raw data size (B-tree overhead)
///
/// For Raft workloads (append-heavy with sequential reads), this provides
/// excellent performance with the benefit of pure Rust simplicity.
pub struct RedbLogStore {
    /// redb database instance
    db: Arc<Database>,
    /// Cached vote (also persisted)
    vote: RwLock<Option<RaftVote>>,
    /// Last purged log ID
    last_purged: RwLock<Option<RaftLogId>>,
    /// Committed log ID
    committed: RwLock<Option<RaftLogId>>,
    /// Monotonic write-version counter.
    ///
    /// Incremented on every mutation (save_vote, save_committed, append, truncate,
    /// purge). Log readers snapshot this value at clone time and can detect
    /// staleness by comparing against the shared counter.
    write_version: Arc<AtomicU64>,
    /// The write-version observed when this instance was cloned (for readers).
    /// `None` on the primary store (always authoritative).
    snapshot_version: Option<u64>,
}

impl RedbLogStore {
    /// Create new redb log storage at the given path
    ///
    /// Creates the directory if it doesn't exist.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        // Create directory if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| ClusterError::RaftStorage(format!("Failed to create dir: {}", e)))?;
        }

        // Open redb database
        let db = Database::create(path)
            .map_err(|e| ClusterError::RaftStorage(format!("Failed to open redb: {}", e)))?;

        let db = Arc::new(db);

        // Initialize tables
        {
            let write_txn = db
                .begin_write()
                .map_err(|e| ClusterError::RaftStorage(e.to_string()))?;
            {
                // Create tables if they don't exist
                let _ = write_txn.open_table(LOGS_TABLE);
                let _ = write_txn.open_table(STATE_TABLE);
            }
            write_txn
                .commit()
                .map_err(|e| ClusterError::RaftStorage(e.to_string()))?;
        }

        // Load persisted state
        let vote = Self::load_state_static(&db, KEY_VOTE);
        let last_purged = Self::load_state_static(&db, KEY_LAST_PURGED);
        let committed = Self::load_state_static(&db, KEY_COMMITTED);

        info!(
            ?vote,
            ?last_purged,
            ?committed,
            "Opened redb Raft log storage"
        );

        Ok(Self {
            db,
            vote: RwLock::new(vote),
            last_purged: RwLock::new(last_purged),
            committed: RwLock::new(committed),
            write_version: Arc::new(AtomicU64::new(0)),
            snapshot_version: None,
        })
    }

    /// Load state from database
    fn load_state_static<T: for<'de> Deserialize<'de>>(db: &Database, key: &str) -> Option<T> {
        let read_txn = db.begin_read().ok()?;
        let table = read_txn.open_table(STATE_TABLE).ok()?;
        let value = table.get(key).ok()??;
        postcard::from_bytes(value.value()).ok()
    }

    /// Save state to database (single key convenience wrapper)
    fn save_state<T: Serialize>(
        &self,
        key: &str,
        value: &T,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let bytes = postcard::to_allocvec(value).map_err(|e| StorageError::IO {
            source: StorageIOError::write_logs(openraft::AnyError::new(&e)),
        })?;
        self.save_states(&[(key, &bytes)])
    }

    /// Save multiple state entries in a single write transaction.
    ///
    /// Consolidates multiple key-value writes into one ACID transaction,
    /// avoiding the overhead and inconsistency risk of separate transactions.
    fn save_states(
        &self,
        entries: &[(&str, &[u8])],
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let write_txn = self.db.begin_write().map_err(|e| StorageError::IO {
            source: StorageIOError::write_logs(openraft::AnyError::new(&e)),
        })?;
        {
            let mut table = write_txn
                .open_table(STATE_TABLE)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::write_logs(openraft::AnyError::new(&e)),
                })?;
            for (key, bytes) in entries {
                table.insert(*key, *bytes).map_err(|e| StorageError::IO {
                    source: StorageIOError::write_logs(openraft::AnyError::new(&e)),
                })?;
            }
        }
        write_txn.commit().map_err(|e| StorageError::IO {
            source: StorageIOError::write_logs(openraft::AnyError::new(&e)),
        })?;
        Ok(())
    }

    /// Get the last log entry
    fn last_log(&self) -> std::result::Result<Option<RaftEntry>, StorageError<NodeId>> {
        let read_txn = self.db.begin_read().map_err(|e| StorageError::IO {
            source: StorageIOError::read_logs(openraft::AnyError::new(&e)),
        })?;
        let table = read_txn
            .open_table(LOGS_TABLE)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read_logs(openraft::AnyError::new(&e)),
            })?;

        // Get the last entry using reverse iteration
        let mut iter = table.iter().map_err(|e| StorageError::IO {
            source: StorageIOError::read_logs(openraft::AnyError::new(&e)),
        })?;

        if let Some(result) = iter.next_back() {
            let (_, value) = result.map_err(|e| StorageError::IO {
                source: StorageIOError::read_logs(openraft::AnyError::new(&e)),
            })?;
            let entry: RaftEntry =
                postcard::from_bytes(value.value()).map_err(|e| StorageError::IO {
                    source: StorageIOError::read_logs(openraft::AnyError::new(&e)),
                })?;
            return Ok(Some(entry));
        }
        Ok(None)
    }

    /// Get log entry by index
    #[allow(dead_code)]
    fn get_log(&self, index: u64) -> std::result::Result<Option<RaftEntry>, StorageError<NodeId>> {
        let read_txn = self.db.begin_read().map_err(|e| StorageError::IO {
            source: StorageIOError::read_logs(openraft::AnyError::new(&e)),
        })?;
        let table = read_txn
            .open_table(LOGS_TABLE)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read_logs(openraft::AnyError::new(&e)),
            })?;

        match table.get(index) {
            Ok(Some(value)) => {
                let entry: RaftEntry =
                    postcard::from_bytes(value.value()).map_err(|e| StorageError::IO {
                        source: StorageIOError::read_logs(openraft::AnyError::new(&e)),
                    })?;
                Ok(Some(entry))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::IO {
                source: StorageIOError::read_logs(openraft::AnyError::new(&e)),
            }),
        }
    }

    /// Append a batch of log entries (single transaction for efficiency)
    fn append_logs(&self, entries: &[RaftEntry]) -> std::result::Result<(), StorageError<NodeId>> {
        if entries.is_empty() {
            return Ok(());
        }

        let write_txn = self.db.begin_write().map_err(|e| StorageError::IO {
            source: StorageIOError::write_logs(openraft::AnyError::new(&e)),
        })?;
        {
            let mut table = write_txn
                .open_table(LOGS_TABLE)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::write_logs(openraft::AnyError::new(&e)),
                })?;

            for entry in entries {
                let value = postcard::to_allocvec(entry).map_err(|e| StorageError::IO {
                    source: StorageIOError::write_logs(openraft::AnyError::new(&e)),
                })?;
                table
                    .insert(entry.log_id.index, value.as_slice())
                    .map_err(|e| StorageError::IO {
                        source: StorageIOError::write_logs(openraft::AnyError::new(&e)),
                    })?;
            }
        }
        write_txn.commit().map_err(|e| StorageError::IO {
            source: StorageIOError::write_logs(openraft::AnyError::new(&e)),
        })?;
        Ok(())
    }

    /// Bump the write-version counter to invalidate reader caches.
    fn bump_version(&self) {
        self.write_version.fetch_add(1, Ordering::Release);
    }

    /// Check whether this instance's cached metadata is still current.
    ///
    /// Returns `true` when the shared write-version counter has advanced
    /// past the version observed at clone-time, indicating that the
    /// parent store has been mutated (vote, committed, purge, etc.).
    /// The primary store (snapshot_version == None) is always authoritative.
    pub fn is_cache_stale(&self) -> bool {
        match self.snapshot_version {
            Some(v) => self.write_version.load(Ordering::Acquire) != v,
            None => false,
        }
    }

    /// Reload cached metadata (vote, last_purged, committed) from the
    /// database if the cache is stale. No-op on the primary store.
    pub async fn refresh_cache_if_stale(&self) {
        if !self.is_cache_stale() {
            return;
        }
        let vote: Option<RaftVote> = Self::load_state_static(&self.db, KEY_VOTE);
        let last_purged: Option<RaftLogId> = Self::load_state_static(&self.db, KEY_LAST_PURGED);
        let committed: Option<RaftLogId> = Self::load_state_static(&self.db, KEY_COMMITTED);
        *self.vote.write().await = vote;
        *self.last_purged.write().await = last_purged;
        *self.committed.write().await = committed;
        debug!("Refreshed stale reader cache from DB");
    }

    /// Delete log entries in range [start, end)
    ///
    /// Uses redb's `retain_in()` for efficient batch deletion instead
    /// of per-key `remove()` calls. This performs a single B-tree
    /// traversal over the range.
    fn delete_logs_range(
        &self,
        start: u64,
        end: u64,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let write_txn = self.db.begin_write().map_err(|e| StorageError::IO {
            source: StorageIOError::write_logs(openraft::AnyError::new(&e)),
        })?;
        {
            let mut table = write_txn
                .open_table(LOGS_TABLE)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::write_logs(openraft::AnyError::new(&e)),
                })?;

            // Batch-remove all entries in [start, end) with a single B-tree pass
            table
                .retain_in(start..end, |_, _| false)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::write_logs(openraft::AnyError::new(&e)),
                })?;
        }
        write_txn.commit().map_err(|e| StorageError::IO {
            source: StorageIOError::write_logs(openraft::AnyError::new(&e)),
        })?;
        Ok(())
    }
}

// Implement RaftLogReader for RedbLogStore
impl RaftLogReader<TypeConfig> for RedbLogStore {
    /// Use a single read transaction with range iteration instead
    /// of opening a separate transaction per entry via `get_log()`.
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<RaftEntry>, StorageError<NodeId>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&n) => n,
            std::ops::Bound::Excluded(&n) => n + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&n) => n + 1,
            std::ops::Bound::Excluded(&n) => n,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        // Single read transaction for the entire range scan
        let read_txn = self.db.begin_read().map_err(|e| StorageError::IO {
            source: StorageIOError::read_logs(openraft::AnyError::new(&e)),
        })?;
        let table = read_txn
            .open_table(LOGS_TABLE)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read_logs(openraft::AnyError::new(&e)),
            })?;

        let mut entries = Vec::new();
        let iter = table.range(start..end).map_err(|e| StorageError::IO {
            source: StorageIOError::read_logs(openraft::AnyError::new(&e)),
        })?;

        for item in iter {
            let (_, value) = item.map_err(|e| StorageError::IO {
                source: StorageIOError::read_logs(openraft::AnyError::new(&e)),
            })?;
            let entry: RaftEntry =
                postcard::from_bytes(value.value()).map_err(|e| StorageError::IO {
                    source: StorageIOError::read_logs(openraft::AnyError::new(&e)),
                })?;
            entries.push(entry);
        }

        Ok(entries)
    }
}

// Implement RaftLogStorage for RedbLogStore
impl RaftLogStorage<TypeConfig> for RedbLogStore {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> std::result::Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last_purged = *self.last_purged.read().await;
        let last_log = self.last_log()?;

        let last_log_id = last_log.map(|e| e.log_id).or(last_purged);

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        // Return a new instance sharing the same DB and write-version counter.
        //
        // The reader records the current write-version at
        // clone time. Consumers can call `is_cache_stale()` to detect when
        // the parent store has been mutated, and `refresh_cache_if_stale()`
        // to reload cached metadata from DB. The shared `Arc<AtomicU64>`
        // counter is incremented on every mutation (save_vote, purge, etc.)
        // providing a lightweight invalidation signal without locking.
        let current_version = self.write_version.load(Ordering::Acquire);
        Self {
            db: self.db.clone(),
            vote: RwLock::new(*self.vote.read().await),
            last_purged: RwLock::new(*self.last_purged.read().await),
            committed: RwLock::new(*self.committed.read().await),
            write_version: self.write_version.clone(),
            snapshot_version: Some(current_version),
        }
    }

    async fn save_vote(
        &mut self,
        vote: &RaftVote,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        self.save_state(KEY_VOTE, vote)?;
        *self.vote.write().await = Some(*vote);
        self.bump_version();
        debug!(?vote, "Saved vote");
        Ok(())
    }

    async fn read_vote(&mut self) -> std::result::Result<Option<RaftVote>, StorageError<NodeId>> {
        Ok(*self.vote.read().await)
    }

    async fn save_committed(
        &mut self,
        committed: Option<RaftLogId>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        if let Some(ref c) = committed {
            self.save_state(KEY_COMMITTED, c)?;
        }
        *self.committed.write().await = committed;
        self.bump_version();
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> std::result::Result<Option<RaftLogId>, StorageError<NodeId>> {
        Ok(*self.committed.read().await)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::LogFlushed<TypeConfig>,
    ) -> std::result::Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = RaftEntry> + Send,
        I::IntoIter: Send,
    {
        // Collect entries for batch write
        let entries: Vec<_> = entries.into_iter().collect();
        self.append_logs(&entries)?;
        self.bump_version();

        // Callback after successful write
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: RaftLogId,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        // Delete all logs after log_id.index
        let start = log_id.index + 1;
        let log_state = RaftLogStorage::get_log_state(self).await?;
        if let Some(last) = log_state.last_log_id {
            self.delete_logs_range(start, last.index + 1)?;
        }
        self.bump_version();
        debug!(?log_id, "Truncated logs");
        Ok(())
    }

    async fn purge(&mut self, log_id: RaftLogId) -> std::result::Result<(), StorageError<NodeId>> {
        // Delete logs up to and including log_id.index
        let current_purged = *self.last_purged.read().await;
        let start = current_purged.map(|l| l.index + 1).unwrap_or(0);

        self.delete_logs_range(start, log_id.index + 1)?;

        // Update and persist last_purged
        self.save_state(KEY_LAST_PURGED, &log_id)?;
        *self.last_purged.write().await = Some(log_id);
        self.bump_version();
        debug!(?log_id, "Purged logs");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{RaftEntry, RaftLogId, RaftVote};
    use openraft::{Entry, EntryPayload, LogId, Vote};
    use tempfile::TempDir;

    fn create_entry(index: u64, term: u64) -> RaftEntry {
        Entry {
            log_id: LogId {
                leader_id: openraft::LeaderId::new(term, 1),
                index,
            },
            payload: EntryPayload::Blank,
        }
    }

    #[tokio::test]
    async fn test_redb_store_basic() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("raft.redb");

        let mut store = RedbLogStore::new(&path).unwrap();

        // Initially empty
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());

        // Append entries directly using internal method
        let entries = vec![create_entry(1, 1), create_entry(2, 1), create_entry(3, 1)];
        store.append_logs(&entries).unwrap();

        // Check state
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 3);

        // Read entries back
        let read_entries = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(read_entries.len(), 3);
    }

    #[tokio::test]
    async fn test_redb_store_vote() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("raft.redb");

        let mut store = RedbLogStore::new(&path).unwrap();

        // No vote initially
        let vote = store.read_vote().await.unwrap();
        assert!(vote.is_none());

        // Save vote
        let test_vote: RaftVote = Vote::new(1, 1);
        store.save_vote(&test_vote).await.unwrap();

        // Read back
        let vote = store.read_vote().await.unwrap();
        assert_eq!(vote, Some(test_vote));

        // Persistence test - reopen
        drop(store);
        let mut store = RedbLogStore::new(&path).unwrap();
        let vote = store.read_vote().await.unwrap();
        assert_eq!(vote, Some(test_vote));
    }

    #[tokio::test]
    async fn test_redb_store_truncate() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("raft.redb");

        let mut store = RedbLogStore::new(&path).unwrap();

        // Append entries
        let entries = vec![
            create_entry(1, 1),
            create_entry(2, 1),
            create_entry(3, 1),
            create_entry(4, 1),
            create_entry(5, 1),
        ];
        store.append_logs(&entries).unwrap();

        // Truncate at index 3
        let log_id: RaftLogId = LogId {
            leader_id: openraft::LeaderId::new(1, 1),
            index: 3,
        };
        store.truncate(log_id).await.unwrap();

        // Should have entries 1-3
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 3);
    }

    #[tokio::test]
    async fn test_redb_store_purge() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("raft.redb");

        let mut store = RedbLogStore::new(&path).unwrap();

        // Append entries
        let entries = vec![
            create_entry(1, 1),
            create_entry(2, 1),
            create_entry(3, 1),
            create_entry(4, 1),
            create_entry(5, 1),
        ];
        store.append_logs(&entries).unwrap();

        // Purge up to index 3
        let log_id: RaftLogId = LogId {
            leader_id: openraft::LeaderId::new(1, 1),
            index: 3,
        };
        store.purge(log_id).await.unwrap();

        // Check last_purged
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 3);

        // Entries 1-3 should be gone
        let entries = store.try_get_log_entries(1..=3).await.unwrap();
        assert!(entries.is_empty());

        // Entries 4-5 should still exist
        let entries = store.try_get_log_entries(4..=5).await.unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn test_redb_store_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("raft.redb");

        // Write some data
        {
            let mut store = RedbLogStore::new(&path).unwrap();
            let entries = vec![create_entry(1, 1), create_entry(2, 1)];
            store.append_logs(&entries).unwrap();

            let vote: RaftVote = Vote::new(2, 1);
            store.save_vote(&vote).await.unwrap();
        }

        // Reopen and verify
        {
            let mut store = RedbLogStore::new(&path).unwrap();

            // Check entries persisted
            let state = store.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.unwrap().index, 2);

            // Check vote persisted
            let vote = store.read_vote().await.unwrap();
            assert_eq!(vote.unwrap().leader_id().term, 2);
        }
    }
}
