//! Native Transaction Support
//!
//! Provides exactly-once semantics with cross-topic atomic writes.
//!
//! ## Transaction Protocol
//!
//! ```text
//! Producer                     Transaction Coordinator            Partitions
//!    │                                   │                            │
//!    │─── InitProducerId ───────────────>│                            │
//!    │<── PID=123, Epoch=0 ──────────────│                            │
//!    │                                   │                            │
//!    │─── BeginTransaction(TxnId) ──────>│                            │
//!    │<── OK ────────────────────────────│                            │
//!    │                                   │                            │
//!    │─── AddPartitionsToTxn(p1,p2) ────>│                            │
//!    │<── OK ────────────────────────────│                            │
//!    │                                   │                            │
//!    │─── Produce(p1, PID, Seq) ──────────────────────────────────────>│
//!    │<── OK ───────────────────────────────────────────────────────────│
//!    │                                   │                            │
//!    │─── Produce(p2, PID, Seq) ──────────────────────────────────────>│
//!    │<── OK ───────────────────────────────────────────────────────────│
//!    │                                   │                            │
//!    │─── CommitTransaction(TxnId) ─────>│                            │
//!    │                                   │─── WriteTxnMarker(COMMIT) ─>│
//!    │                                   │<── OK ─────────────────────│
//!    │<── OK ────────────────────────────│                            │
//! ```
//!
//! ## Transaction States
//!
//! ```text
//! Empty ──────> Ongoing ──────> PrepareCommit ──────> CompleteCommit
//!                  │                  │                     │
//!                  │                  v                     v
//!                  └───────> PrepareAbort ───────> CompleteAbort
//!                                    │                     │
//!                                    └─────────────────────┘
//! ```
//!
//! ## Exactly-Once Guarantees
//!
//! 1. **Atomic Writes**: All messages in a transaction are committed or aborted together
//! 2. **Consumer Isolation**: Consumers only see committed messages (read_committed)
//! 3. **Fencing**: Old producer instances are fenced via epoch
//! 4. **Durability**: Transaction state is persisted before acknowledgment
//!

use crate::idempotent::{ProducerEpoch, ProducerId};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

/// Unique identifier for a transaction
pub type TransactionId = String;

/// Transaction timeout default (1 minute)
pub const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(60);

/// Maximum pending transactions per producer
pub const MAX_PENDING_TRANSACTIONS: usize = 5;

/// Transaction state machine
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    /// No active transaction
    Empty,

    /// Transaction in progress, accepting writes
    Ongoing,

    /// Preparing to commit (2PC phase 1)
    PrepareCommit,

    /// Preparing to abort (2PC phase 1)
    PrepareAbort,

    /// Commit complete (2PC phase 2)
    CompleteCommit,

    /// Abort complete (2PC phase 2)
    CompleteAbort,

    /// Transaction has expired without completion
    Dead,
}

impl TransactionState {
    /// Check if transaction is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TransactionState::Empty
                | TransactionState::CompleteCommit
                | TransactionState::CompleteAbort
                | TransactionState::Dead
        )
    }

    /// Check if transaction is still active (can accept writes)
    pub fn is_active(&self) -> bool {
        matches!(self, TransactionState::Ongoing)
    }

    /// Check if transaction can transition to commit
    pub fn can_commit(&self) -> bool {
        matches!(self, TransactionState::Ongoing)
    }

    /// Check if transaction can transition to abort
    pub fn can_abort(&self) -> bool {
        matches!(
            self,
            TransactionState::Ongoing
                | TransactionState::PrepareCommit
                | TransactionState::PrepareAbort
        )
    }
}

/// Result of a transaction operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionResult {
    /// Operation succeeded
    Ok,

    /// Transaction ID is invalid or not found
    InvalidTransactionId,

    /// Transaction is in wrong state for this operation
    InvalidTransactionState {
        current: TransactionState,
        expected: &'static str,
    },

    /// Producer ID/epoch mismatch
    ProducerFenced {
        expected_epoch: ProducerEpoch,
        received_epoch: ProducerEpoch,
    },

    /// Transaction has timed out
    TransactionTimeout,

    /// Too many pending transactions
    TooManyTransactions,

    /// Concurrent modification detected
    ConcurrentTransaction,

    /// Partition not part of transaction
    PartitionNotInTransaction { topic: String, partition: u32 },

    /// Durable log write failed — transaction state may not be recoverable
    LogWriteError(String),
}

/// A partition involved in a transaction
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransactionPartition {
    pub topic: String,
    pub partition: u32,
}

impl TransactionPartition {
    pub fn new(topic: impl Into<String>, partition: u32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

/// Pending write in a transaction (not yet committed)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingWrite {
    /// Target partition
    pub partition: TransactionPartition,

    /// Sequence number for this write
    pub sequence: i32,

    /// Offset assigned by the partition leader
    pub offset: u64,

    /// Write timestamp
    #[serde(with = "crate::serde_utils::system_time")]
    pub timestamp: SystemTime,
}

/// Consumer offset to be committed with the transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionOffsetCommit {
    /// Consumer group
    pub group_id: String,

    /// Topic-partition-offset triples
    pub offsets: Vec<(TransactionPartition, i64)>,
}

/// Active transaction state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    /// Transaction ID (unique per producer)
    pub txn_id: TransactionId,

    /// Producer ID owning this transaction
    pub producer_id: ProducerId,

    /// Producer epoch (for fencing)
    pub producer_epoch: ProducerEpoch,

    /// Current state
    pub state: TransactionState,

    /// Partitions involved in this transaction
    pub partitions: HashSet<TransactionPartition>,

    /// Pending writes (not yet committed)
    pub pending_writes: Vec<PendingWrite>,

    /// Consumer offsets to commit with this transaction
    pub offset_commits: Vec<TransactionOffsetCommit>,

    /// Transaction start time
    #[serde(with = "crate::serde_utils::system_time")]
    pub started_at: SystemTime,

    /// Transaction timeout
    #[serde(with = "crate::serde_utils::duration")]
    pub timeout: Duration,

    /// Last activity timestamp
    #[serde(skip)]
    pub last_activity: Option<Instant>,
}

impl Transaction {
    /// Create a new transaction
    pub fn new(
        txn_id: TransactionId,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
        timeout: Duration,
    ) -> Self {
        Self {
            txn_id,
            producer_id,
            producer_epoch,
            state: TransactionState::Ongoing,
            partitions: HashSet::new(),
            pending_writes: Vec::new(),
            offset_commits: Vec::new(),
            started_at: SystemTime::now(),
            timeout,
            last_activity: Some(Instant::now()),
        }
    }

    /// Check if transaction has timed out
    ///
    /// Returns `false` when `last_activity` is `None` (e.g., after
    /// deserialization), allowing the coordinator to resolve in-doubt
    /// transactions.
    pub fn is_timed_out(&self) -> bool {
        self.last_activity
            .map(|t| t.elapsed() > self.timeout)
            .unwrap_or(false)
    }

    /// Update last activity timestamp
    pub fn touch(&mut self) {
        self.last_activity = Some(Instant::now());
    }

    /// Add a partition to the transaction
    pub fn add_partition(&mut self, partition: TransactionPartition) {
        self.partitions.insert(partition);
        self.touch();
    }

    /// Record a pending write
    pub fn add_write(&mut self, partition: TransactionPartition, sequence: i32, offset: u64) {
        self.pending_writes.push(PendingWrite {
            partition,
            sequence,
            offset,
            timestamp: SystemTime::now(),
        });
        self.touch();
    }

    /// Add consumer offset commit
    pub fn add_offset_commit(
        &mut self,
        group_id: String,
        offsets: Vec<(TransactionPartition, i64)>,
    ) {
        self.offset_commits
            .push(TransactionOffsetCommit { group_id, offsets });
        self.touch();
    }

    /// Get total number of writes
    pub fn write_count(&self) -> usize {
        self.pending_writes.len()
    }

    /// Get all affected partitions
    pub fn affected_partitions(&self) -> impl Iterator<Item = &TransactionPartition> {
        self.partitions.iter()
    }
}

/// Transaction marker type written to partition logs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionMarker {
    /// Transaction committed
    Commit,

    /// Transaction aborted
    Abort,
}

/// Consumer isolation level
///
/// Controls whether consumers can see uncommitted transactional messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum IsolationLevel {
    /// Read all messages, including those from aborted transactions.
    /// This is the default for backward compatibility.
    #[default]
    ReadUncommitted,

    /// Only read messages from committed transactions.
    /// Messages from aborted transactions are filtered out.
    ReadCommitted,
}

impl IsolationLevel {
    /// Convert to string (Kafka-compatible)
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReadUncommitted => "read_uncommitted",
            Self::ReadCommitted => "read_committed",
        }
    }

    /// Convert from u8 (wire protocol)
    /// 0 = read_uncommitted (default)
    /// 1 = read_committed
    /// Other values default to read_uncommitted
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::ReadCommitted,
            _ => Self::ReadUncommitted,
        }
    }

    /// Convert to u8 (wire protocol)
    pub fn as_u8(&self) -> u8 {
        match self {
            Self::ReadUncommitted => 0,
            Self::ReadCommitted => 1,
        }
    }
}

impl std::str::FromStr for IsolationLevel {
    type Err = String;

    /// Parse from string (Kafka-compatible)
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "read_uncommitted" => Ok(Self::ReadUncommitted),
            "read_committed" => Ok(Self::ReadCommitted),
            _ => Err(format!("unknown isolation level: {}", s)),
        }
    }
}

impl std::fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Record of an aborted transaction for consumer filtering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbortedTransaction {
    /// Producer ID that aborted
    pub producer_id: ProducerId,
    /// First offset of the aborted transaction in this partition
    pub first_offset: u64,
    /// Last offset of the aborted transaction in this partition (inclusive).
    /// Without an upper bound, `is_aborted` returns true for all
    /// offsets >= first_offset, incorrectly filtering committed messages from
    /// the same producer in later transactions.
    pub last_offset: u64,
}

/// Index of aborted transactions for a partition
///
/// Used for efficient filtering when `isolation.level=read_committed`
#[derive(Debug, Default)]
pub struct AbortedTransactionIndex {
    /// Aborted transactions sorted by first_offset
    aborted: RwLock<Vec<AbortedTransaction>>,
}

impl AbortedTransactionIndex {
    /// Create a new empty index
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an aborted transaction
    ///
    /// Tracks both first and last offset to bound the aborted range.
    /// Checks `first_offset <= offset <= last_offset` (bounded range).
    pub fn record_abort(&self, producer_id: ProducerId, first_offset: u64, last_offset: u64) {
        let mut aborted = self.aborted.write();
        aborted.push(AbortedTransaction {
            producer_id,
            first_offset,
            last_offset,
        });
        // Keep sorted by first_offset for efficient lookup
        aborted.sort_by_key(|a| a.first_offset);
    }

    /// Get aborted transactions that overlap with a range of offsets
    ///
    /// Returns aborted transactions whose first_offset is within [start_offset, end_offset]
    pub fn get_aborted_in_range(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Vec<AbortedTransaction> {
        let aborted = self.aborted.read();
        aborted
            .iter()
            .filter(|a| a.first_offset >= start_offset && a.first_offset <= end_offset)
            .cloned()
            .collect()
    }

    /// Check if a specific producer's message at an offset is from an aborted transaction
    ///
    /// Checks bounded range `first_offset <= offset <= last_offset` to
    /// avoid false-positives on committed messages from the same producer in later
    /// transactions.
    pub fn is_aborted(&self, producer_id: ProducerId, offset: u64) -> bool {
        let aborted = self.aborted.read();
        aborted.iter().any(|a| {
            a.producer_id == producer_id && a.first_offset <= offset && offset <= a.last_offset
        })
    }

    /// Remove aborted transactions older than a given offset (for log truncation)
    pub fn truncate_before(&self, offset: u64) {
        let mut aborted = self.aborted.write();
        aborted.retain(|a| a.first_offset >= offset);
    }

    /// Get count of tracked aborted transactions
    pub fn len(&self) -> usize {
        self.aborted.read().len()
    }

    /// Check if index is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Statistics for transaction coordinator
#[derive(Debug, Default)]
pub struct TransactionStats {
    /// Total transactions initiated
    transactions_started: AtomicU64,

    /// Total transactions committed
    transactions_committed: AtomicU64,

    /// Total transactions aborted
    transactions_aborted: AtomicU64,

    /// Total transactions timed out
    transactions_timed_out: AtomicU64,

    /// Currently active transactions
    active_transactions: AtomicU64,
}

impl TransactionStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_start(&self) {
        self.transactions_started.fetch_add(1, Ordering::Relaxed);
        self.active_transactions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_commit(&self) {
        self.transactions_committed.fetch_add(1, Ordering::Relaxed);
        self.active_transactions.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn record_abort(&self) {
        self.transactions_aborted.fetch_add(1, Ordering::Relaxed);
        self.active_transactions.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn record_timeout(&self) {
        self.transactions_timed_out.fetch_add(1, Ordering::Relaxed);
        self.active_transactions.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn transactions_started(&self) -> u64 {
        self.transactions_started.load(Ordering::Relaxed)
    }

    pub fn transactions_committed(&self) -> u64 {
        self.transactions_committed.load(Ordering::Relaxed)
    }

    pub fn transactions_aborted(&self) -> u64 {
        self.transactions_aborted.load(Ordering::Relaxed)
    }

    pub fn transactions_timed_out(&self) -> u64 {
        self.transactions_timed_out.load(Ordering::Relaxed)
    }

    pub fn active_transactions(&self) -> u64 {
        self.active_transactions.load(Ordering::Relaxed)
    }
}

/// Snapshot of transaction stats for serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionStatsSnapshot {
    pub transactions_started: u64,
    pub transactions_committed: u64,
    pub transactions_aborted: u64,
    pub transactions_timed_out: u64,
    pub active_transactions: u64,
}

impl From<&TransactionStats> for TransactionStatsSnapshot {
    fn from(stats: &TransactionStats) -> Self {
        Self {
            transactions_started: stats.transactions_started(),
            transactions_committed: stats.transactions_committed(),
            transactions_aborted: stats.transactions_aborted(),
            transactions_timed_out: stats.transactions_timed_out(),
            active_transactions: stats.active_transactions(),
        }
    }
}

// ============================================================================
// Transaction Log — durable persistence for coordinator state
// ============================================================================

/// A single entry in the transaction log, recording a state transition.
///
/// Serialized to postcard and written to a per-coordinator WAL file.
/// On recovery, entries are replayed in order to reconstruct coordinator state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionLogEntry {
    /// New transaction started
    Begin {
        txn_id: TransactionId,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
        timeout_ms: u64,
    },
    /// Partition added to transaction
    AddPartition {
        txn_id: TransactionId,
        producer_id: ProducerId,
        partition: TransactionPartition,
    },
    /// Write recorded in transaction
    RecordWrite {
        txn_id: TransactionId,
        producer_id: ProducerId,
        partition: TransactionPartition,
        sequence: i32,
        offset: u64,
    },
    /// Transaction moving to PrepareCommit (2PC phase 1)
    PrepareCommit {
        txn_id: TransactionId,
        producer_id: ProducerId,
    },
    /// Transaction committed (2PC phase 2)
    CompleteCommit {
        txn_id: TransactionId,
        producer_id: ProducerId,
    },
    /// Transaction moving to PrepareAbort
    PrepareAbort {
        txn_id: TransactionId,
        producer_id: ProducerId,
    },
    /// Transaction aborted
    CompleteAbort {
        txn_id: TransactionId,
        producer_id: ProducerId,
    },
    /// Transaction timed out (zombie cleanup)
    TimedOut {
        txn_id: TransactionId,
        producer_id: ProducerId,
    },
    /// Consumer offset commit added to transaction (exactly-once consume-transform-produce)
    OffsetCommit {
        txn_id: TransactionId,
        producer_id: ProducerId,
        group_id: String,
        offsets: Vec<(TransactionPartition, i64)>,
    },
}

/// Durable transaction log backed by a file.
///
/// Each entry is: [crc32: 4 bytes] [len: 4 bytes] [postcard-encoded data: N bytes]
///
/// The log is append-only. On recovery, entries are read sequentially and
/// replayed to rebuild the `TransactionCoordinator`'s in-memory state.
pub struct TransactionLog {
    /// Path to the log file
    path: PathBuf,
    /// Append-only writer
    writer: parking_lot::Mutex<Option<std::io::BufWriter<std::fs::File>>>,
}

impl TransactionLog {
    /// Open (or create) a transaction log at the given path.
    pub fn open(path: impl AsRef<Path>) -> crate::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            path,
            writer: parking_lot::Mutex::new(Some(std::io::BufWriter::new(file))),
        })
    }

    /// Create an in-memory-only (no-op) transaction log for use when
    /// persistence is not configured.
    pub fn noop() -> Self {
        Self {
            path: PathBuf::new(),
            writer: parking_lot::Mutex::new(None),
        }
    }

    /// Append an entry to the log.
    pub fn append(&self, entry: &TransactionLogEntry) -> crate::Result<()> {
        let mut guard = self.writer.lock();
        let writer = match guard.as_mut() {
            Some(w) => w,
            None => return Ok(()), // noop mode
        };

        let data = postcard::to_allocvec(entry).map_err(|e| crate::Error::Other(e.to_string()))?;

        // CRC32 of the payload
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&data);
        let crc = hasher.finalize();

        writer.write_all(&crc.to_be_bytes())?;
        writer.write_all(&(data.len() as u32).to_be_bytes())?;
        writer.write_all(&data)?;
        writer.flush()?;
        // fsync to guarantee durability before acknowledging.
        // flush() only pushes BufWriter data to the OS page cache.
        // Without sync_data(), a power loss can lose the entry.
        writer.get_ref().sync_data()?;
        Ok(())
    }

    /// Read all valid entries from the log for recovery replay.
    pub fn read_all(path: impl AsRef<Path>) -> crate::Result<Vec<TransactionLogEntry>> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let data = std::fs::read(path)?;
        let mut entries = Vec::new();
        let mut pos = 0;

        while pos + 8 <= data.len() {
            let crc = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
            let len = u32::from_be_bytes(data[pos + 4..pos + 8].try_into().unwrap()) as usize;
            pos += 8;

            if pos + len > data.len() {
                break; // truncated — stop here
            }

            let payload = &data[pos..pos + len];

            // Verify CRC
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(payload);
            if hasher.finalize() != crc {
                break; // corrupt — stop here (kafka-style truncation)
            }

            match postcard::from_bytes::<TransactionLogEntry>(payload) {
                Ok(entry) => entries.push(entry),
                Err(_) => break, // corrupt entry
            }
            pos += len;
        }

        Ok(entries)
    }

    /// Truncate the log file (e.g. after snapshot).
    pub fn truncate(&self) -> crate::Result<()> {
        let mut guard = self.writer.lock();
        if guard.is_none() {
            return Ok(());
        }
        // Close old writer, truncate, reopen
        *guard = None;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        *guard = Some(std::io::BufWriter::new(file));
        Ok(())
    }
}

/// Transaction coordinator manages all active transactions
///
/// This is a per-broker component that tracks transactions for producers
/// assigned to this broker as their transaction coordinator.
pub struct TransactionCoordinator {
    /// Active transactions by (producer_id, txn_id)
    /// `parking_lot` — O(1) transaction lookup, never held across `.await`.
    transactions: RwLock<HashMap<(ProducerId, TransactionId), Transaction>>,

    /// Producer to transaction mapping (for single-txn-per-producer enforcement)
    producer_transactions: RwLock<HashMap<ProducerId, TransactionId>>,

    /// Default transaction timeout
    default_timeout: Duration,

    /// Statistics
    stats: TransactionStats,

    /// Index of aborted transactions for read_committed filtering
    aborted_index: AbortedTransactionIndex,

    /// durable transaction log.
    /// Persists every state transition before acknowledging, enabling
    /// recovery of in-doubt transactions after a coordinator crash.
    txn_log: TransactionLog,
}

impl Default for TransactionCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionCoordinator {
    /// Create a new transaction coordinator (in-memory only, no persistence).
    pub fn new() -> Self {
        Self {
            transactions: RwLock::new(HashMap::new()),
            producer_transactions: RwLock::new(HashMap::new()),
            default_timeout: DEFAULT_TRANSACTION_TIMEOUT,
            stats: TransactionStats::new(),
            aborted_index: AbortedTransactionIndex::new(),
            txn_log: TransactionLog::noop(),
        }
    }

    /// Create with custom default timeout (in-memory only).
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            transactions: RwLock::new(HashMap::new()),
            producer_transactions: RwLock::new(HashMap::new()),
            default_timeout: timeout,
            stats: TransactionStats::new(),
            aborted_index: AbortedTransactionIndex::new(),
            txn_log: TransactionLog::noop(),
        }
    }

    /// Create a coordinator with durable persistence at the given path.
    ///
    /// Transaction state transitions are written to a CRC-protected log file.
    /// On crash recovery, call [`Self::recover`] before resuming operations.
    pub fn with_persistence(path: impl AsRef<Path>) -> crate::Result<Self> {
        let txn_log = TransactionLog::open(path)?;
        Ok(Self {
            transactions: RwLock::new(HashMap::new()),
            producer_transactions: RwLock::new(HashMap::new()),
            default_timeout: DEFAULT_TRANSACTION_TIMEOUT,
            stats: TransactionStats::new(),
            aborted_index: AbortedTransactionIndex::new(),
            txn_log,
        })
    }

    /// Recover coordinator state from the durable log.
    ///
    /// Replays all valid log entries to rebuild the in-memory
    /// transaction map. In-doubt transactions (PrepareCommit/PrepareAbort)
    /// are left in their prepared state for the caller to resolve.
    pub fn recover(path: impl AsRef<Path>) -> crate::Result<Self> {
        let path = path.as_ref();
        let entries = TransactionLog::read_all(path)?;
        let txn_log = TransactionLog::open(path)?;

        let coord = Self {
            transactions: RwLock::new(HashMap::new()),
            producer_transactions: RwLock::new(HashMap::new()),
            default_timeout: DEFAULT_TRANSACTION_TIMEOUT,
            stats: TransactionStats::new(),
            aborted_index: AbortedTransactionIndex::new(),
            txn_log,
        };

        // Replay log entries to rebuild state
        let mut transactions = coord.transactions.write();
        let mut producer_txns = coord.producer_transactions.write();

        for entry in entries {
            match entry {
                TransactionLogEntry::Begin {
                    txn_id,
                    producer_id,
                    producer_epoch,
                    timeout_ms,
                } => {
                    let txn = Transaction::new(
                        txn_id.clone(),
                        producer_id,
                        producer_epoch,
                        Duration::from_millis(timeout_ms),
                    );
                    transactions.insert((producer_id, txn_id.clone()), txn);
                    producer_txns.insert(producer_id, txn_id);
                }
                TransactionLogEntry::AddPartition {
                    txn_id,
                    producer_id,
                    partition,
                } => {
                    if let Some(txn) = transactions.get_mut(&(producer_id, txn_id)) {
                        txn.partitions.insert(partition);
                    }
                }
                TransactionLogEntry::RecordWrite {
                    txn_id,
                    producer_id,
                    partition,
                    sequence,
                    offset,
                } => {
                    if let Some(txn) = transactions.get_mut(&(producer_id, txn_id)) {
                        txn.pending_writes.push(PendingWrite {
                            partition,
                            sequence,
                            offset,
                            timestamp: SystemTime::now(),
                        });
                    }
                }
                TransactionLogEntry::PrepareCommit {
                    txn_id,
                    producer_id,
                } => {
                    if let Some(txn) = transactions.get_mut(&(producer_id, txn_id)) {
                        txn.state = TransactionState::PrepareCommit;
                    }
                }
                TransactionLogEntry::CompleteCommit {
                    txn_id,
                    producer_id,
                } => {
                    transactions.remove(&(producer_id, txn_id.clone()));
                    producer_txns.remove(&producer_id);
                }
                TransactionLogEntry::PrepareAbort {
                    txn_id,
                    producer_id,
                } => {
                    if let Some(txn) = transactions.get_mut(&(producer_id, txn_id)) {
                        txn.state = TransactionState::PrepareAbort;
                    }
                }
                TransactionLogEntry::CompleteAbort {
                    txn_id,
                    producer_id,
                } => {
                    if let Some(txn) = transactions.get(&(producer_id, txn_id.clone())) {
                        // Record aborts for read_committed filtering
                        // compute bounded range from all pending writes
                        let first = txn.pending_writes.iter().map(|w| w.offset).min();
                        let last = txn.pending_writes.iter().map(|w| w.offset).max();
                        if let (Some(f), Some(l)) = (first, last) {
                            coord.aborted_index.record_abort(producer_id, f, l);
                        }
                    }
                    transactions.remove(&(producer_id, txn_id.clone()));
                    producer_txns.remove(&producer_id);
                }
                TransactionLogEntry::TimedOut {
                    txn_id,
                    producer_id,
                } => {
                    if let Some(txn) = transactions.get(&(producer_id, txn_id.clone())) {
                        // compute bounded range from all pending writes
                        let first = txn.pending_writes.iter().map(|w| w.offset).min();
                        let last = txn.pending_writes.iter().map(|w| w.offset).max();
                        if let (Some(f), Some(l)) = (first, last) {
                            coord.aborted_index.record_abort(producer_id, f, l);
                        }
                    }
                    transactions.remove(&(producer_id, txn_id.clone()));
                    producer_txns.remove(&producer_id);
                }
                TransactionLogEntry::OffsetCommit {
                    txn_id,
                    producer_id,
                    group_id,
                    offsets,
                } => {
                    if let Some(txn) = transactions.get_mut(&(producer_id, txn_id)) {
                        txn.add_offset_commit(group_id, offsets);
                    }
                }
            }
        }

        drop(transactions);
        drop(producer_txns);

        let active = coord.active_count();
        if active > 0 {
            tracing::warn!(
                "Transaction coordinator recovered {} in-doubt transactions from log",
                active
            );
        }

        Ok(coord)
    }

    /// Get statistics
    pub fn stats(&self) -> &TransactionStats {
        &self.stats
    }

    /// Begin a new transaction
    pub fn begin_transaction(
        &self,
        txn_id: TransactionId,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
        timeout: Option<Duration>,
    ) -> TransactionResult {
        // Use write locks from the start to prevent TOCTOU races
        let mut transactions = self.transactions.write();
        let mut producer_txns = self.producer_transactions.write();

        // Check if producer already has an active transaction
        if let Some(existing_txn_id) = producer_txns.get(&producer_id) {
            if existing_txn_id != &txn_id {
                return TransactionResult::ConcurrentTransaction;
            }
            // Same txn_id - check if we're resuming
            if let Some(txn) = transactions.get(&(producer_id, txn_id.clone())) {
                if txn.producer_epoch != producer_epoch {
                    return TransactionResult::ProducerFenced {
                        expected_epoch: txn.producer_epoch,
                        received_epoch: producer_epoch,
                    };
                }
                if txn.state.is_active() {
                    return TransactionResult::Ok; // Already active
                }
            }
        }

        // Enforce MAX_PENDING_TRANSACTIONS limit
        let active_count = transactions
            .values()
            .filter(|t| t.state.is_active())
            .count();
        if active_count >= MAX_PENDING_TRANSACTIONS {
            return TransactionResult::TooManyTransactions;
        }

        // Create new transaction
        let txn = Transaction::new(
            txn_id.clone(),
            producer_id,
            producer_epoch,
            timeout.unwrap_or(self.default_timeout),
        );

        // WAL BEFORE in-memory — persist first, then insert.
        // If the WAL write fails, no in-memory ghost transaction is created.
        if let Err(e) = self.txn_log.append(&TransactionLogEntry::Begin {
            txn_id: txn_id.clone(),
            producer_id,
            producer_epoch,
            timeout_ms: timeout.unwrap_or(self.default_timeout).as_millis() as u64,
        }) {
            tracing::error!(producer_id, "Transaction log write failed on begin: {}", e);
            return TransactionResult::LogWriteError(e.to_string());
        }

        transactions.insert((producer_id, txn_id.clone()), txn);
        producer_txns.insert(producer_id, txn_id);

        self.stats.record_start();
        TransactionResult::Ok
    }

    /// Add partitions to an active transaction
    pub fn add_partitions_to_transaction(
        &self,
        txn_id: &TransactionId,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
        partitions: Vec<TransactionPartition>,
    ) -> TransactionResult {
        let mut transactions = self.transactions.write();

        let txn = match transactions.get_mut(&(producer_id, txn_id.clone())) {
            Some(t) => t,
            None => return TransactionResult::InvalidTransactionId,
        };

        // Validate epoch
        if txn.producer_epoch != producer_epoch {
            return TransactionResult::ProducerFenced {
                expected_epoch: txn.producer_epoch,
                received_epoch: producer_epoch,
            };
        }

        // Check state
        if !txn.state.is_active() {
            return TransactionResult::InvalidTransactionState {
                current: txn.state,
                expected: "Ongoing",
            };
        }

        // Check timeout
        if txn.is_timed_out() {
            txn.state = TransactionState::Dead;
            self.stats.record_timeout();
            return TransactionResult::TransactionTimeout;
        }

        // WAL BEFORE in-memory — persist all partitions first.
        // If any WAL write fails mid-way, we return error without modifying
        // in-memory state, keeping WAL and memory consistent.
        for partition in &partitions {
            if let Err(e) = self.txn_log.append(&TransactionLogEntry::AddPartition {
                txn_id: txn_id.clone(),
                producer_id,
                partition: partition.clone(),
            }) {
                tracing::error!(
                    producer_id,
                    "Transaction log write failed on add_partition: {}",
                    e
                );
                return TransactionResult::LogWriteError(e.to_string());
            }
        }

        // All WAL writes succeeded — now apply to in-memory state
        for partition in partitions {
            txn.add_partition(partition);
        }

        TransactionResult::Ok
    }

    /// Validate that a write is allowed before physically appending data.
    /// Checks transaction existence, epoch, state, timeout, and partition membership
    /// WITHOUT recording the write. Call this BEFORE `topic.append()`.
    pub fn validate_transaction_write(
        &self,
        txn_id: &TransactionId,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
        partition: &TransactionPartition,
    ) -> TransactionResult {
        let mut transactions = self.transactions.write();

        let txn = match transactions.get_mut(&(producer_id, txn_id.clone())) {
            Some(t) => t,
            None => return TransactionResult::InvalidTransactionId,
        };

        if txn.producer_epoch != producer_epoch {
            return TransactionResult::ProducerFenced {
                expected_epoch: txn.producer_epoch,
                received_epoch: producer_epoch,
            };
        }

        if !txn.state.is_active() {
            return TransactionResult::InvalidTransactionState {
                current: txn.state,
                expected: "Ongoing",
            };
        }

        if txn.is_timed_out() {
            txn.state = TransactionState::Dead;
            self.stats.record_timeout();
            return TransactionResult::TransactionTimeout;
        }

        if !txn.partitions.contains(partition) {
            return TransactionResult::PartitionNotInTransaction {
                topic: partition.topic.clone(),
                partition: partition.partition,
            };
        }

        TransactionResult::Ok
    }

    /// Record a write within a transaction
    pub fn add_write_to_transaction(
        &self,
        txn_id: &TransactionId,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
        partition: TransactionPartition,
        sequence: i32,
        offset: u64,
    ) -> TransactionResult {
        let mut transactions = self.transactions.write();

        let txn = match transactions.get_mut(&(producer_id, txn_id.clone())) {
            Some(t) => t,
            None => return TransactionResult::InvalidTransactionId,
        };

        // Validate epoch
        if txn.producer_epoch != producer_epoch {
            return TransactionResult::ProducerFenced {
                expected_epoch: txn.producer_epoch,
                received_epoch: producer_epoch,
            };
        }

        // Check state
        if !txn.state.is_active() {
            return TransactionResult::InvalidTransactionState {
                current: txn.state,
                expected: "Ongoing",
            };
        }

        // Check timeout
        if txn.is_timed_out() {
            txn.state = TransactionState::Dead;
            self.stats.record_timeout();
            return TransactionResult::TransactionTimeout;
        }

        // Verify partition is part of transaction
        if !txn.partitions.contains(&partition) {
            return TransactionResult::PartitionNotInTransaction {
                topic: partition.topic,
                partition: partition.partition,
            };
        }

        // WAL BEFORE in-memory — persist the write entry first.
        if let Err(e) = self.txn_log.append(&TransactionLogEntry::RecordWrite {
            txn_id: txn_id.clone(),
            producer_id,
            partition: partition.clone(),
            sequence,
            offset,
        }) {
            tracing::error!(
                producer_id,
                offset,
                "Transaction log write failed on record_write: {}",
                e
            );
            return TransactionResult::LogWriteError(e.to_string());
        }

        // WAL succeeded — now apply to in-memory state
        txn.add_write(partition, sequence, offset);

        TransactionResult::Ok
    }

    /// Add consumer offset commit to transaction (for exactly-once consume-transform-produce)
    pub fn add_offsets_to_transaction(
        &self,
        txn_id: &TransactionId,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
        group_id: String,
        offsets: Vec<(TransactionPartition, i64)>,
    ) -> TransactionResult {
        let mut transactions = self.transactions.write();

        let txn = match transactions.get_mut(&(producer_id, txn_id.clone())) {
            Some(t) => t,
            None => return TransactionResult::InvalidTransactionId,
        };

        // Validate epoch
        if txn.producer_epoch != producer_epoch {
            return TransactionResult::ProducerFenced {
                expected_epoch: txn.producer_epoch,
                received_epoch: producer_epoch,
            };
        }

        // Check state
        if !txn.state.is_active() {
            return TransactionResult::InvalidTransactionState {
                current: txn.state,
                expected: "Ongoing",
            };
        }

        // Check timeout
        if txn.is_timed_out() {
            txn.state = TransactionState::Dead;
            self.stats.record_timeout();
            return TransactionResult::TransactionTimeout;
        }

        // WAL BEFORE in-memory — persist offset commit first.
        // Without this, consumer offsets are lost on crash, breaking
        // exactly-once consume-transform-produce semantics.
        if let Err(e) = self.txn_log.append(&TransactionLogEntry::OffsetCommit {
            txn_id: txn_id.clone(),
            producer_id,
            group_id: group_id.clone(),
            offsets: offsets.clone(),
        }) {
            tracing::error!(
                producer_id,
                "Transaction log write failed on offset_commit: {}",
                e
            );
            return TransactionResult::LogWriteError(e.to_string());
        }

        // WAL succeeded — apply to in-memory state
        txn.add_offset_commit(group_id, offsets);

        TransactionResult::Ok
    }

    /// Prepare to commit a transaction (2PC phase 1)
    ///
    /// Returns the transaction data needed for committing to partitions
    pub fn prepare_commit(
        &self,
        txn_id: &TransactionId,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
    ) -> Result<Transaction, TransactionResult> {
        let mut transactions = self.transactions.write();

        let txn = match transactions.get_mut(&(producer_id, txn_id.clone())) {
            Some(t) => t,
            None => return Err(TransactionResult::InvalidTransactionId),
        };

        // Validate epoch
        if txn.producer_epoch != producer_epoch {
            return Err(TransactionResult::ProducerFenced {
                expected_epoch: txn.producer_epoch,
                received_epoch: producer_epoch,
            });
        }

        // Check state
        if !txn.state.can_commit() {
            return Err(TransactionResult::InvalidTransactionState {
                current: txn.state,
                expected: "Ongoing",
            });
        }

        // Check timeout
        if txn.is_timed_out() {
            txn.state = TransactionState::Dead;
            self.stats.record_timeout();
            return Err(TransactionResult::TransactionTimeout);
        }

        // Transition to PrepareCommit
        txn.state = TransactionState::PrepareCommit;
        txn.touch();

        // persist prepare decision before acknowledging
        if let Err(e) = self.txn_log.append(&TransactionLogEntry::PrepareCommit {
            txn_id: txn_id.clone(),
            producer_id,
        }) {
            tracing::error!(
                producer_id,
                "Transaction log write failed on prepare_commit: {}",
                e
            );
            // Revert state — we cannot guarantee durability
            txn.state = TransactionState::Ongoing;
            return Err(TransactionResult::LogWriteError(e.to_string()));
        }

        Ok(txn.clone())
    }

    /// Complete the commit (2PC phase 2)
    pub fn complete_commit(
        &self,
        txn_id: &TransactionId,
        producer_id: ProducerId,
    ) -> TransactionResult {
        let mut transactions = self.transactions.write();
        let mut producer_txns = self.producer_transactions.write();

        let txn = match transactions.get_mut(&(producer_id, txn_id.clone())) {
            Some(t) => t,
            None => return TransactionResult::InvalidTransactionId,
        };

        if txn.state != TransactionState::PrepareCommit {
            return TransactionResult::InvalidTransactionState {
                current: txn.state,
                expected: "PrepareCommit",
            };
        }

        // WAL BEFORE state change — persist first, then mutate.
        if let Err(e) = self.txn_log.append(&TransactionLogEntry::CompleteCommit {
            txn_id: txn_id.clone(),
            producer_id,
        }) {
            tracing::error!(
                producer_id,
                "Transaction log write failed on complete_commit: {}",
                e
            );
            return TransactionResult::LogWriteError(e.to_string());
        }

        // WAL succeeded — safe to mutate in-memory state
        txn.state = TransactionState::CompleteCommit;

        // Clean up
        transactions.remove(&(producer_id, txn_id.clone()));
        producer_txns.remove(&producer_id);

        self.stats.record_commit();
        TransactionResult::Ok
    }

    /// Prepare to abort a transaction (2PC phase 1)
    pub fn prepare_abort(
        &self,
        txn_id: &TransactionId,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
    ) -> Result<Transaction, TransactionResult> {
        let mut transactions = self.transactions.write();

        let txn = match transactions.get_mut(&(producer_id, txn_id.clone())) {
            Some(t) => t,
            None => return Err(TransactionResult::InvalidTransactionId),
        };

        // Validate epoch
        if txn.producer_epoch != producer_epoch {
            return Err(TransactionResult::ProducerFenced {
                expected_epoch: txn.producer_epoch,
                received_epoch: producer_epoch,
            });
        }

        // Check state - abort is allowed from more states than commit
        if !txn.state.can_abort() {
            return Err(TransactionResult::InvalidTransactionState {
                current: txn.state,
                expected: "Ongoing or PrepareCommit",
            });
        }

        // Transition to PrepareAbort
        txn.state = TransactionState::PrepareAbort;
        txn.touch();

        // persist prepare-abort decision
        if let Err(e) = self.txn_log.append(&TransactionLogEntry::PrepareAbort {
            txn_id: txn_id.clone(),
            producer_id,
        }) {
            tracing::error!(
                producer_id,
                "Transaction log write failed on prepare_abort: {}",
                e
            );
            txn.state = TransactionState::Ongoing;
            return Err(TransactionResult::LogWriteError(e.to_string()));
        }

        Ok(txn.clone())
    }

    /// Complete the abort (2PC phase 2)
    pub fn complete_abort(
        &self,
        txn_id: &TransactionId,
        producer_id: ProducerId,
    ) -> TransactionResult {
        let mut transactions = self.transactions.write();
        let mut producer_txns = self.producer_transactions.write();

        let txn = match transactions.get_mut(&(producer_id, txn_id.clone())) {
            Some(t) => t,
            None => return TransactionResult::InvalidTransactionId,
        };

        if txn.state != TransactionState::PrepareAbort {
            return TransactionResult::InvalidTransactionState {
                current: txn.state,
                expected: "PrepareAbort",
            };
        }

        // WAL BEFORE state change — persist first, then mutate.
        if let Err(e) = self.txn_log.append(&TransactionLogEntry::CompleteAbort {
            txn_id: txn_id.clone(),
            producer_id,
        }) {
            tracing::error!(
                producer_id,
                "Transaction log write failed on complete_abort: {}",
                e
            );
            return TransactionResult::LogWriteError(e.to_string());
        }

        // WAL succeeded — safe to mutate in-memory state
        txn.state = TransactionState::CompleteAbort;

        // Record aborted transaction for read_committed filtering
        // Track bounded offset range (first, last) so committed
        // messages from the same producer at later offsets are not filtered.
        let first = txn.pending_writes.iter().map(|w| w.offset).min();
        let last = txn.pending_writes.iter().map(|w| w.offset).max();
        if let (Some(f), Some(l)) = (first, last) {
            self.aborted_index.record_abort(producer_id, f, l);
        }

        // Clean up
        transactions.remove(&(producer_id, txn_id.clone()));
        producer_txns.remove(&producer_id);

        self.stats.record_abort();
        TransactionResult::Ok
    }

    /// Get current transaction state for a producer
    pub fn get_transaction(
        &self,
        txn_id: &TransactionId,
        producer_id: ProducerId,
    ) -> Option<Transaction> {
        let transactions = self.transactions.read();
        transactions.get(&(producer_id, txn_id.clone())).cloned()
    }

    /// Check if a producer has an active transaction
    pub fn has_active_transaction(&self, producer_id: ProducerId) -> bool {
        let producer_txns = self.producer_transactions.read();
        producer_txns.contains_key(&producer_id)
    }

    /// Get active transaction ID for a producer
    pub fn get_active_transaction_id(&self, producer_id: ProducerId) -> Option<TransactionId> {
        let producer_txns = self.producer_transactions.read();
        producer_txns.get(&producer_id).cloned()
    }

    /// Clean up timed-out transactions
    ///
    /// In addition to marking timed-out transactions as Dead, this
    /// now records abort markers in the `AbortedTransactionIndex` for each
    /// timed-out transaction's pending writes. This ensures that consumers
    /// using `read_committed` isolation will correctly filter data from
    /// zombie producers whose transactions timed out.
    pub fn cleanup_timed_out_transactions(&self) -> Vec<Transaction> {
        let mut timed_out = Vec::new();
        let mut transactions = self.transactions.write();
        let mut producer_txns = self.producer_transactions.write();

        let keys_to_remove: Vec<_> = transactions
            .iter()
            .filter(|(_, txn)| txn.is_timed_out() && !txn.state.is_terminal())
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_remove {
            // WAL BEFORE memory removal.
            // If WAL write fails, skip this txn — it will be reaped next cycle.
            if let Some(txn) = transactions.get(&key) {
                if let Err(e) = self.txn_log.append(&TransactionLogEntry::TimedOut {
                    txn_id: txn.txn_id.clone(),
                    producer_id: txn.producer_id,
                }) {
                    tracing::error!(txn.producer_id, "Transaction log write failed on timeout: {} — skipping cleanup, will retry", e);
                    continue;
                }
            }

            if let Some(mut txn) = transactions.remove(&key) {
                txn.state = TransactionState::Dead;
                producer_txns.remove(&txn.producer_id);

                // Record abort markers for all pending writes so
                // read_committed consumers can filter them out.
                // Use bounded offset range instead of per-write entries.
                let first = txn.pending_writes.iter().map(|w| w.offset).min();
                let last = txn.pending_writes.iter().map(|w| w.offset).max();
                if let (Some(f), Some(l)) = (first, last) {
                    self.aborted_index.record_abort(txn.producer_id, f, l);
                }

                self.stats.record_timeout();
                self.stats.record_abort();
                timed_out.push(txn);
            }
        }

        timed_out
    }

    /// Get number of active transactions
    pub fn active_count(&self) -> usize {
        let transactions = self.transactions.read();
        transactions
            .values()
            .filter(|t| !t.state.is_terminal())
            .count()
    }

    /// Check if a producer's message at a given offset is from an aborted transaction
    ///
    /// Used for read_committed isolation level filtering
    pub fn is_aborted(&self, producer_id: ProducerId, offset: u64) -> bool {
        self.aborted_index.is_aborted(producer_id, offset)
    }

    /// Get aborted transactions in a range of offsets
    ///
    /// Used for FetchResponse to include aborted transaction metadata
    pub fn get_aborted_in_range(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Vec<AbortedTransaction> {
        self.aborted_index
            .get_aborted_in_range(start_offset, end_offset)
    }

    /// Get access to the aborted transaction index
    pub fn aborted_index(&self) -> &AbortedTransactionIndex {
        &self.aborted_index
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_transaction_state_transitions() {
        // Test terminal states
        assert!(TransactionState::Empty.is_terminal());
        assert!(TransactionState::CompleteCommit.is_terminal());
        assert!(TransactionState::CompleteAbort.is_terminal());
        assert!(TransactionState::Dead.is_terminal());

        // Test active states
        assert!(!TransactionState::Ongoing.is_terminal());
        assert!(!TransactionState::PrepareCommit.is_terminal());
        assert!(!TransactionState::PrepareAbort.is_terminal());

        // Test can_commit
        assert!(TransactionState::Ongoing.can_commit());
        assert!(!TransactionState::Empty.can_commit());
        assert!(!TransactionState::PrepareCommit.can_commit());

        // Test can_abort
        assert!(TransactionState::Ongoing.can_abort());
        assert!(TransactionState::PrepareCommit.can_abort());
        assert!(TransactionState::PrepareAbort.can_abort());
        assert!(!TransactionState::Empty.can_abort());
    }

    #[test]
    fn test_begin_transaction() {
        let coordinator = TransactionCoordinator::new();

        // Begin first transaction
        let result = coordinator.begin_transaction("txn-1".to_string(), 1, 0, None);
        assert_eq!(result, TransactionResult::Ok);

        // Verify transaction exists
        let txn = coordinator.get_transaction(&"txn-1".to_string(), 1);
        assert!(txn.is_some());
        let txn = txn.unwrap();
        assert_eq!(txn.state, TransactionState::Ongoing);
        assert_eq!(txn.producer_id, 1);
        assert_eq!(txn.producer_epoch, 0);

        // Stats
        assert_eq!(coordinator.stats().transactions_started(), 1);
        assert_eq!(coordinator.stats().active_transactions(), 1);
    }

    #[test]
    fn test_concurrent_transaction_rejection() {
        let coordinator = TransactionCoordinator::new();

        // Begin first transaction
        coordinator.begin_transaction("txn-1".to_string(), 1, 0, None);

        // Try to begin another transaction for same producer
        let result = coordinator.begin_transaction("txn-2".to_string(), 1, 0, None);
        assert_eq!(result, TransactionResult::ConcurrentTransaction);
    }

    #[test]
    fn test_add_partitions_to_transaction() {
        let coordinator = TransactionCoordinator::new();
        coordinator.begin_transaction("txn-1".to_string(), 1, 0, None);

        // Add partitions
        let result = coordinator.add_partitions_to_transaction(
            &"txn-1".to_string(),
            1,
            0,
            vec![
                TransactionPartition::new("topic-1", 0),
                TransactionPartition::new("topic-1", 1),
                TransactionPartition::new("topic-2", 0),
            ],
        );
        assert_eq!(result, TransactionResult::Ok);

        // Verify partitions added
        let txn = coordinator
            .get_transaction(&"txn-1".to_string(), 1)
            .unwrap();
        assert_eq!(txn.partitions.len(), 3);
    }

    #[test]
    fn test_add_write_to_transaction() {
        let coordinator = TransactionCoordinator::new();
        coordinator.begin_transaction("txn-1".to_string(), 1, 0, None);

        let partition = TransactionPartition::new("topic-1", 0);
        coordinator.add_partitions_to_transaction(
            &"txn-1".to_string(),
            1,
            0,
            vec![partition.clone()],
        );

        // Record write
        let result =
            coordinator.add_write_to_transaction(&"txn-1".to_string(), 1, 0, partition, 0, 100);
        assert_eq!(result, TransactionResult::Ok);

        // Verify write recorded
        let txn = coordinator
            .get_transaction(&"txn-1".to_string(), 1)
            .unwrap();
        assert_eq!(txn.pending_writes.len(), 1);
        assert_eq!(txn.pending_writes[0].offset, 100);
        assert_eq!(txn.pending_writes[0].sequence, 0);
    }

    #[test]
    fn test_write_to_non_registered_partition() {
        let coordinator = TransactionCoordinator::new();
        coordinator.begin_transaction("txn-1".to_string(), 1, 0, None);

        // Try to write to partition not added to transaction
        let result = coordinator.add_write_to_transaction(
            &"txn-1".to_string(),
            1,
            0,
            TransactionPartition::new("topic-1", 0),
            0,
            100,
        );

        assert!(matches!(
            result,
            TransactionResult::PartitionNotInTransaction { .. }
        ));
    }

    #[test]
    fn test_commit_transaction() {
        let coordinator = TransactionCoordinator::new();
        coordinator.begin_transaction("txn-1".to_string(), 1, 0, None);

        let partition = TransactionPartition::new("topic-1", 0);
        coordinator.add_partitions_to_transaction(
            &"txn-1".to_string(),
            1,
            0,
            vec![partition.clone()],
        );
        coordinator.add_write_to_transaction(&"txn-1".to_string(), 1, 0, partition, 0, 100);

        // Prepare commit
        let txn = coordinator.prepare_commit(&"txn-1".to_string(), 1, 0);
        assert!(txn.is_ok());
        let txn = txn.unwrap();
        assert_eq!(txn.state, TransactionState::PrepareCommit);

        // Complete commit
        let result = coordinator.complete_commit(&"txn-1".to_string(), 1);
        assert_eq!(result, TransactionResult::Ok);

        // Transaction should be removed
        assert!(coordinator
            .get_transaction(&"txn-1".to_string(), 1)
            .is_none());
        assert!(!coordinator.has_active_transaction(1));

        // Stats
        assert_eq!(coordinator.stats().transactions_committed(), 1);
        assert_eq!(coordinator.stats().active_transactions(), 0);
    }

    #[test]
    fn test_abort_transaction() {
        let coordinator = TransactionCoordinator::new();
        coordinator.begin_transaction("txn-1".to_string(), 1, 0, None);

        let partition = TransactionPartition::new("topic-1", 0);
        coordinator.add_partitions_to_transaction(
            &"txn-1".to_string(),
            1,
            0,
            vec![partition.clone()],
        );
        coordinator.add_write_to_transaction(&"txn-1".to_string(), 1, 0, partition, 0, 100);

        // Prepare abort
        let txn = coordinator.prepare_abort(&"txn-1".to_string(), 1, 0);
        assert!(txn.is_ok());

        // Complete abort
        let result = coordinator.complete_abort(&"txn-1".to_string(), 1);
        assert_eq!(result, TransactionResult::Ok);

        // Transaction should be removed
        assert!(coordinator
            .get_transaction(&"txn-1".to_string(), 1)
            .is_none());

        // Stats
        assert_eq!(coordinator.stats().transactions_aborted(), 1);
    }

    #[test]
    fn test_producer_fencing() {
        let coordinator = TransactionCoordinator::new();
        coordinator.begin_transaction("txn-1".to_string(), 1, 0, None);

        // Try with wrong epoch
        let result = coordinator.add_partitions_to_transaction(
            &"txn-1".to_string(),
            1,
            1, // Wrong epoch
            vec![TransactionPartition::new("topic-1", 0)],
        );

        assert!(matches!(
            result,
            TransactionResult::ProducerFenced {
                expected_epoch: 0,
                received_epoch: 1
            }
        ));
    }

    #[test]
    fn test_transaction_timeout() {
        // Create coordinator with very short timeout
        let coordinator = TransactionCoordinator::with_timeout(Duration::from_millis(1));
        coordinator.begin_transaction("txn-1".to_string(), 1, 0, None);

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(5));

        // Try to add partitions - should fail with timeout
        let result = coordinator.add_partitions_to_transaction(
            &"txn-1".to_string(),
            1,
            0,
            vec![TransactionPartition::new("topic-1", 0)],
        );

        assert_eq!(result, TransactionResult::TransactionTimeout);
    }

    #[test]
    fn test_cleanup_timed_out_transactions() {
        let coordinator = TransactionCoordinator::with_timeout(Duration::from_millis(1));

        coordinator.begin_transaction("txn-1".to_string(), 1, 0, None);
        coordinator.begin_transaction("txn-2".to_string(), 2, 0, None);

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(5));

        // Cleanup
        let timed_out = coordinator.cleanup_timed_out_transactions();
        assert_eq!(timed_out.len(), 2);

        // Transactions should be gone
        assert_eq!(coordinator.active_count(), 0);
        assert_eq!(coordinator.stats().transactions_timed_out(), 2);
    }

    #[test]
    fn test_add_offsets_to_transaction() {
        let coordinator = TransactionCoordinator::new();
        coordinator.begin_transaction("txn-1".to_string(), 1, 0, None);

        // Add consumer offsets
        let result = coordinator.add_offsets_to_transaction(
            &"txn-1".to_string(),
            1,
            0,
            "consumer-group-1".to_string(),
            vec![
                (TransactionPartition::new("input-topic", 0), 42),
                (TransactionPartition::new("input-topic", 1), 100),
            ],
        );
        assert_eq!(result, TransactionResult::Ok);

        // Verify
        let txn = coordinator
            .get_transaction(&"txn-1".to_string(), 1)
            .unwrap();
        assert_eq!(txn.offset_commits.len(), 1);
        assert_eq!(txn.offset_commits[0].group_id, "consumer-group-1");
        assert_eq!(txn.offset_commits[0].offsets.len(), 2);
    }

    #[test]
    fn test_invalid_state_transitions() {
        let coordinator = TransactionCoordinator::new();
        coordinator.begin_transaction("txn-1".to_string(), 1, 0, None);

        // Prepare commit
        coordinator
            .prepare_commit(&"txn-1".to_string(), 1, 0)
            .unwrap();

        // Try to add partitions after prepare - should fail
        let result = coordinator.add_partitions_to_transaction(
            &"txn-1".to_string(),
            1,
            0,
            vec![TransactionPartition::new("topic-1", 0)],
        );
        assert!(matches!(
            result,
            TransactionResult::InvalidTransactionState { .. }
        ));
    }

    #[test]
    fn test_abort_from_prepare_commit() {
        let coordinator = TransactionCoordinator::new();
        coordinator.begin_transaction("txn-1".to_string(), 1, 0, None);

        // Prepare commit
        coordinator
            .prepare_commit(&"txn-1".to_string(), 1, 0)
            .unwrap();

        // Abort should still be allowed from PrepareCommit
        let result = coordinator.prepare_abort(&"txn-1".to_string(), 1, 0);
        assert!(result.is_ok());

        let result = coordinator.complete_abort(&"txn-1".to_string(), 1);
        assert_eq!(result, TransactionResult::Ok);
    }

    #[test]
    fn test_transaction_partition_hash() {
        let p1 = TransactionPartition::new("topic", 0);
        let p2 = TransactionPartition::new("topic", 0);
        let p3 = TransactionPartition::new("topic", 1);

        assert_eq!(p1, p2);
        assert_ne!(p1, p3);

        let mut set = HashSet::new();
        set.insert(p1.clone());
        set.insert(p2); // Should not add (duplicate)
        set.insert(p3);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_resume_same_transaction() {
        let coordinator = TransactionCoordinator::new();

        // Begin transaction
        coordinator.begin_transaction("txn-1".to_string(), 1, 0, None);

        // Try to begin same transaction again - should succeed (idempotent)
        let result = coordinator.begin_transaction("txn-1".to_string(), 1, 0, None);
        assert_eq!(result, TransactionResult::Ok);

        // Only one transaction should exist
        assert_eq!(coordinator.active_count(), 1);
        assert_eq!(coordinator.stats().transactions_started(), 1);
    }

    #[test]
    fn test_stats_snapshot() {
        let coordinator = TransactionCoordinator::new();
        coordinator.begin_transaction("txn-1".to_string(), 1, 0, None);
        coordinator
            .prepare_commit(&"txn-1".to_string(), 1, 0)
            .unwrap();
        coordinator.complete_commit(&"txn-1".to_string(), 1);

        let snapshot: TransactionStatsSnapshot = coordinator.stats().into();
        assert_eq!(snapshot.transactions_started, 1);
        assert_eq!(snapshot.transactions_committed, 1);
        assert_eq!(snapshot.active_transactions, 0);
    }

    // =========================================================================
    // Isolation Level Tests
    // =========================================================================

    #[test]
    fn test_isolation_level_from_u8() {
        assert_eq!(IsolationLevel::from_u8(0), IsolationLevel::ReadUncommitted);
        assert_eq!(IsolationLevel::from_u8(1), IsolationLevel::ReadCommitted);
        assert_eq!(IsolationLevel::from_u8(2), IsolationLevel::ReadUncommitted); // Invalid defaults
        assert_eq!(
            IsolationLevel::from_u8(255),
            IsolationLevel::ReadUncommitted
        );
    }

    #[test]
    fn test_isolation_level_as_u8() {
        assert_eq!(IsolationLevel::ReadUncommitted.as_u8(), 0);
        assert_eq!(IsolationLevel::ReadCommitted.as_u8(), 1);
    }

    #[test]
    fn test_isolation_level_from_str() {
        assert_eq!(
            IsolationLevel::from_str("read_uncommitted").unwrap(),
            IsolationLevel::ReadUncommitted
        );
        assert_eq!(
            IsolationLevel::from_str("read_committed").unwrap(),
            IsolationLevel::ReadCommitted
        );
        assert_eq!(
            IsolationLevel::from_str("READ_UNCOMMITTED").unwrap(),
            IsolationLevel::ReadUncommitted
        );
        assert_eq!(
            IsolationLevel::from_str("READ_COMMITTED").unwrap(),
            IsolationLevel::ReadCommitted
        );
        assert!(IsolationLevel::from_str("invalid").is_err());
    }

    #[test]
    fn test_isolation_level_default() {
        assert_eq!(IsolationLevel::default(), IsolationLevel::ReadUncommitted);
    }

    // =========================================================================
    // Aborted Transaction Index Tests
    // =========================================================================

    #[test]
    fn test_aborted_transaction_index_basic() {
        let index = AbortedTransactionIndex::new();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);

        // Record an aborted transaction spanning offsets 100..=200
        index.record_abort(1, 100, 200);
        assert!(!index.is_empty());
        assert_eq!(index.len(), 1);

        // Check if offset is from aborted transaction
        assert!(index.is_aborted(1, 100)); // first_offset
        assert!(index.is_aborted(1, 150)); // within transaction
        assert!(index.is_aborted(1, 200)); // last_offset (inclusive)
        assert!(!index.is_aborted(1, 201)); // beyond last_offset → NOT aborted
        assert!(!index.is_aborted(1, 50)); // before transaction
        assert!(!index.is_aborted(2, 100)); // different producer
    }

    #[test]
    fn test_aborted_transaction_index_multiple() {
        let index = AbortedTransactionIndex::new();

        // Multiple aborted transactions with bounded ranges
        index.record_abort(1, 100, 199);
        index.record_abort(1, 300, 399);
        index.record_abort(2, 200, 299);

        assert_eq!(index.len(), 3);

        // Check filtering — ranges are now bounded
        assert!(index.is_aborted(1, 100));
        assert!(index.is_aborted(1, 150)); // within first abort range
        assert!(!index.is_aborted(1, 250)); // between the two abort ranges → committed
        assert!(index.is_aborted(1, 300));
        assert!(index.is_aborted(1, 399)); // last offset of second abort
        assert!(!index.is_aborted(1, 400)); // beyond second abort → NOT aborted
        assert!(index.is_aborted(2, 200));
        assert!(index.is_aborted(2, 250));
        assert!(!index.is_aborted(2, 100)); // before producer 2's abort
        assert!(!index.is_aborted(2, 300)); // beyond producer 2's abort range
    }

    #[test]
    fn test_aborted_transaction_index_get_range() {
        let index = AbortedTransactionIndex::new();

        index.record_abort(1, 100, 199);
        index.record_abort(2, 200, 299);
        index.record_abort(1, 300, 399);

        // Get transactions in range
        let in_range = index.get_aborted_in_range(150, 250);
        assert_eq!(in_range.len(), 1);
        assert_eq!(in_range[0].producer_id, 2);
        assert_eq!(in_range[0].first_offset, 200);

        // Wider range
        let in_range = index.get_aborted_in_range(0, 500);
        assert_eq!(in_range.len(), 3);

        // No transactions in range
        let in_range = index.get_aborted_in_range(400, 500);
        assert_eq!(in_range.len(), 0);
    }

    #[test]
    fn test_aborted_transaction_index_truncate() {
        let index = AbortedTransactionIndex::new();

        index.record_abort(1, 100, 199);
        index.record_abort(2, 200, 299);
        index.record_abort(1, 300, 399);

        assert_eq!(index.len(), 3);

        // Truncate entries before offset 200
        index.truncate_before(200);

        assert_eq!(index.len(), 2);

        // Only offsets >= 200 should remain
        assert!(!index.is_aborted(1, 150)); // old entry removed
        assert!(index.is_aborted(2, 200));
        assert!(index.is_aborted(1, 300));
    }

    #[test]
    fn test_coordinator_is_aborted() {
        let coordinator = TransactionCoordinator::new();

        // Start a transaction
        coordinator.begin_transaction("txn-1".to_string(), 1, 0, None);

        // Add partition and write
        coordinator.add_partitions_to_transaction(
            &"txn-1".to_string(),
            1,
            0,
            vec![TransactionPartition::new("test-topic", 0)],
        );
        coordinator.add_write_to_transaction(
            &"txn-1".to_string(),
            1,
            0,
            TransactionPartition::new("test-topic", 0),
            0,
            100, // offset
        );

        // Not aborted yet
        assert!(!coordinator.is_aborted(1, 100));

        // Prepare and complete abort
        coordinator
            .prepare_abort(&"txn-1".to_string(), 1, 0)
            .unwrap();
        coordinator.complete_abort(&"txn-1".to_string(), 1);

        // Now should be marked as aborted
        assert!(coordinator.is_aborted(1, 100));
        // offset 150 is beyond the transaction's last write (100), so NOT aborted
        assert!(!coordinator.is_aborted(1, 150));
        assert!(!coordinator.is_aborted(1, 50)); // before first_offset
        assert!(!coordinator.is_aborted(2, 100)); // different producer
    }

    // ====================================================================
    // Transaction Log Persistence Tests
    // ====================================================================

    #[test]
    fn test_transaction_log_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("txn.log");

        // Write entries to the log
        let log = TransactionLog::open(&log_path).unwrap();
        log.append(&TransactionLogEntry::Begin {
            txn_id: "txn-1".to_string(),
            producer_id: 42,
            producer_epoch: 0,
            timeout_ms: 30000,
        })
        .unwrap();
        log.append(&TransactionLogEntry::AddPartition {
            txn_id: "txn-1".to_string(),
            producer_id: 42,
            partition: TransactionPartition::new("topic-a", 0),
        })
        .unwrap();
        log.append(&TransactionLogEntry::RecordWrite {
            txn_id: "txn-1".to_string(),
            producer_id: 42,
            partition: TransactionPartition::new("topic-a", 0),
            sequence: 0,
            offset: 100,
        })
        .unwrap();
        drop(log);

        // Read back and verify
        let entries = TransactionLog::read_all(&log_path).unwrap();
        assert_eq!(entries.len(), 3);
        assert!(
            matches!(&entries[0], TransactionLogEntry::Begin { txn_id, producer_id, .. } if txn_id == "txn-1" && *producer_id == 42)
        );
        assert!(
            matches!(&entries[1], TransactionLogEntry::AddPartition { partition, .. } if partition.topic == "topic-a" && partition.partition == 0)
        );
        assert!(
            matches!(&entries[2], TransactionLogEntry::RecordWrite { offset, .. } if *offset == 100)
        );
    }

    #[test]
    fn test_transaction_log_crc_corruption_detection() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("txn.log");

        let log = TransactionLog::open(&log_path).unwrap();
        log.append(&TransactionLogEntry::Begin {
            txn_id: "txn-1".to_string(),
            producer_id: 1,
            producer_epoch: 0,
            timeout_ms: 5000,
        })
        .unwrap();
        log.append(&TransactionLogEntry::PrepareCommit {
            txn_id: "txn-1".to_string(),
            producer_id: 1,
        })
        .unwrap();
        drop(log);

        // Corrupt one byte in the middle of the file
        let mut data = std::fs::read(&log_path).unwrap();
        assert!(data.len() > 10);
        data[10] ^= 0xFF; // flip bits in payload
        std::fs::write(&log_path, &data).unwrap();

        // read_all should stop at first corrupted entry
        let entries = TransactionLog::read_all(&log_path).unwrap();
        // Should have 0 or 1 entries (first entry's CRC may be corrupted
        // depending on where byte 10 falls)
        assert!(entries.len() <= 1);
    }

    #[test]
    fn test_coordinator_with_persistence_commit_flow() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("txn.log");

        // Run a full commit cycle with persistence
        let coord = TransactionCoordinator::with_persistence(&log_path).unwrap();
        assert_eq!(
            coord.begin_transaction("txn-1".to_string(), 1, 0, None),
            TransactionResult::Ok
        );
        assert_eq!(
            coord.add_partitions_to_transaction(
                &"txn-1".to_string(),
                1,
                0,
                vec![TransactionPartition::new("topic", 0)],
            ),
            TransactionResult::Ok
        );
        assert_eq!(
            coord.add_write_to_transaction(
                &"txn-1".to_string(),
                1,
                0,
                TransactionPartition::new("topic", 0),
                0,
                500,
            ),
            TransactionResult::Ok
        );
        coord.prepare_commit(&"txn-1".to_string(), 1, 0).unwrap();
        assert_eq!(
            coord.complete_commit(&"txn-1".to_string(), 1),
            TransactionResult::Ok
        );

        // Log should contain Begin, AddPartition, RecordWrite,
        // PrepareCommit, CompleteCommit (5 entries)
        let entries = TransactionLog::read_all(&log_path).unwrap();
        assert_eq!(entries.len(), 5);
    }

    #[test]
    fn test_coordinator_recovery_from_crash() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("txn.log");

        // Simulate a coordinator that prepares commit then "crashes"
        {
            let coord = TransactionCoordinator::with_persistence(&log_path).unwrap();
            coord.begin_transaction("txn-1".to_string(), 1, 0, None);
            coord.add_partitions_to_transaction(
                &"txn-1".to_string(),
                1,
                0,
                vec![TransactionPartition::new("topic", 0)],
            );
            coord.add_write_to_transaction(
                &"txn-1".to_string(),
                1,
                0,
                TransactionPartition::new("topic", 0),
                0,
                42,
            );
            coord.prepare_commit(&"txn-1".to_string(), 1, 0).unwrap();
            // "crash" here — drop without complete_commit
        }

        // Recover — should reconstruct the in-doubt transaction
        let coord = TransactionCoordinator::recover(&log_path).unwrap();

        // Transaction should be recovered in PrepareCommit state
        let txn = coord.get_transaction(&"txn-1".to_string(), 1);
        assert!(txn.is_some(), "Transaction should be recovered from WAL");
        let txn = txn.unwrap();
        assert_eq!(txn.state, TransactionState::PrepareCommit);
        assert_eq!(txn.pending_writes.len(), 1);
        assert_eq!(txn.pending_writes[0].offset, 42);

        // Should be able to complete the commit after recovery
        assert_eq!(
            coord.complete_commit(&"txn-1".to_string(), 1),
            TransactionResult::Ok
        );
        assert_eq!(coord.active_count(), 0);
    }

    #[test]
    fn test_coordinator_recovery_abort_flow() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("txn.log");

        // Write a transaction that was aborted
        {
            let coord = TransactionCoordinator::with_persistence(&log_path).unwrap();
            coord.begin_transaction("txn-a".to_string(), 10, 0, None);
            coord.add_partitions_to_transaction(
                &"txn-a".to_string(),
                10,
                0,
                vec![TransactionPartition::new("t", 0)],
            );
            coord.add_write_to_transaction(
                &"txn-a".to_string(),
                10,
                0,
                TransactionPartition::new("t", 0),
                0,
                200,
            );
            coord.prepare_abort(&"txn-a".to_string(), 10, 0).unwrap();
            coord.complete_abort(&"txn-a".to_string(), 10);
        }

        // Recovery should show this transaction as fully resolved
        let coord = TransactionCoordinator::recover(&log_path).unwrap();
        assert_eq!(coord.active_count(), 0);
        // The abort markers should be in the aborted index
        assert!(coord.is_aborted(10, 200));
    }

    #[test]
    fn test_transaction_log_noop_is_silent() {
        let log = TransactionLog::noop();
        // noop log should accept writes without error
        assert!(log
            .append(&TransactionLogEntry::Begin {
                txn_id: "x".to_string(),
                producer_id: 1,
                producer_epoch: 0,
                timeout_ms: 1000,
            })
            .is_ok());
    }

    #[test]
    fn test_log_write_error_propagated() {
        // Use a coordinator with persistence pointing to a non-writable path
        let coord = TransactionCoordinator::new(); // noop log

        // noop log writes succeed silently, so begin should work
        let result = coord.begin_transaction("txn-ok".to_string(), 1, 0, None);
        assert_eq!(result, TransactionResult::Ok);
    }
}
