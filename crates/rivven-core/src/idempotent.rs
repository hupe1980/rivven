//! Idempotent Producer Support (KIP-98)
//!
//! Provides exactly-once semantics for producers without full transactions.
//!
//! ## How It Works
//!
//! 1. **Producer ID (PID)**: Each producer is assigned a unique 64-bit ID
//! 2. **Epoch**: Increments on producer restart (fencing old instances)
//! 3. **Sequence Numbers**: Per-partition sequence tracking for deduplication
//!
//! ## Protocol
//!
//! ```text
//! Producer                           Broker
//!    │                                  │
//!    │─── InitProducerId ──────────────>│  (Request PID)
//!    │<── PID=123, Epoch=0 ─────────────│
//!    │                                  │
//!    │─── Produce(PID=123,Seq=0) ──────>│  (First message)
//!    │<── Success(offset=0) ────────────│
//!    │                                  │
//!    │─── Produce(PID=123,Seq=0) ──────>│  (Retry - duplicate!)
//!    │<── DuplicateSequence(offset=0) ──│  (Returns cached offset)
//!    │                                  │
//!    │─── Produce(PID=123,Seq=1) ──────>│  (Next message)
//!    │<── Success(offset=1) ────────────│
//! ```
//!
//! ## Fencing with Epochs
//!
//! ```text
//! Producer A (PID=123, Epoch=0)   starts producing
//! Producer A crashes
//! Producer A restarts            → InitProducerId → PID=123, Epoch=1
//! Old Producer A instance        → Produce(Epoch=0) → FENCED (rejected)
//! New Producer A instance        → Produce(Epoch=1) → Success
//! ```
//!
//! ## Sequence Validation
//!
//! - Expected sequence: last_sequence + 1
//! - If sequence == expected: Accept, update last_sequence
//! - If sequence < expected: Duplicate, return cached offset
//! - If sequence > expected: OutOfOrderSequence error
//!

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, SystemTime};

/// Producer ID type (unique identifier for each producer instance)
pub type ProducerId = u64;

/// Producer epoch (increments on restart, used for fencing)
pub type ProducerEpoch = u16;

/// Sequence number (per-partition, per-producer)
pub type SequenceNumber = i32;

/// Special value indicating no sequence number
pub const NO_SEQUENCE: SequenceNumber = -1;

/// Result of sequence validation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceResult {
    /// Sequence is valid, message should be appended
    Valid,

    /// Duplicate message detected, return cached offset
    Duplicate { cached_offset: u64 },

    /// Sequence number is out of order (gap in sequence)
    OutOfOrder {
        expected: SequenceNumber,
        received: SequenceNumber,
    },

    /// Producer epoch is stale (fenced by newer instance)
    Fenced {
        current_epoch: ProducerEpoch,
        received_epoch: ProducerEpoch,
    },

    /// Unknown producer ID
    UnknownProducer,
}

/// Producer state for a single partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionProducerState {
    /// Last accepted sequence number
    pub last_sequence: SequenceNumber,

    /// Offset of the last accepted message (for duplicate detection)
    pub last_offset: u64,

    /// Timestamp of last activity
    #[serde(with = "crate::serde_utils::system_time")]
    pub last_activity: SystemTime,
}

impl PartitionProducerState {
    /// Create new state with initial sequence
    pub fn new(sequence: SequenceNumber, offset: u64) -> Self {
        Self {
            last_sequence: sequence,
            last_offset: offset,
            last_activity: SystemTime::now(),
        }
    }

    /// Update state after accepting a message
    pub fn update(&mut self, sequence: SequenceNumber, offset: u64) {
        self.last_sequence = sequence;
        self.last_offset = offset;
        self.last_activity = SystemTime::now();
    }
}

/// Producer metadata (stored per producer ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerMetadata {
    /// Producer ID
    pub producer_id: ProducerId,

    /// Current epoch (increments on restart)
    pub epoch: ProducerEpoch,

    /// Per-partition sequence state
    /// Maps: partition_id → PartitionProducerState
    pub partitions: HashMap<u32, PartitionProducerState>,

    /// When this producer was first registered
    #[serde(with = "crate::serde_utils::system_time")]
    pub created_at: SystemTime,

    /// Last activity across all partitions
    #[serde(with = "crate::serde_utils::system_time")]
    pub last_activity: SystemTime,
}

impl ProducerMetadata {
    /// Create new producer metadata
    pub fn new(producer_id: ProducerId, epoch: ProducerEpoch) -> Self {
        let now = SystemTime::now();
        Self {
            producer_id,
            epoch,
            partitions: HashMap::new(),
            created_at: now,
            last_activity: now,
        }
    }

    /// Bump epoch (on producer restart)
    pub fn bump_epoch(&mut self) -> ProducerEpoch {
        self.epoch = self.epoch.wrapping_add(1);
        self.last_activity = SystemTime::now();
        // Clear partition state on epoch bump (new producer instance)
        self.partitions.clear();
        self.epoch
    }

    /// Validate and update sequence for a partition
    pub fn validate_sequence(
        &mut self,
        partition: u32,
        epoch: ProducerEpoch,
        sequence: SequenceNumber,
        offset: u64,
    ) -> SequenceResult {
        // Check epoch first (fencing)
        if epoch < self.epoch {
            return SequenceResult::Fenced {
                current_epoch: self.epoch,
                received_epoch: epoch,
            };
        }

        // If epoch is higher, this is a new producer instance
        if epoch > self.epoch {
            self.epoch = epoch;
            self.partitions.clear();
        }

        self.last_activity = SystemTime::now();

        // Get or create partition state
        if let Some(state) = self.partitions.get_mut(&partition) {
            let expected = state.last_sequence.wrapping_add(1);

            if sequence == expected {
                // Valid sequence
                state.update(sequence, offset);
                SequenceResult::Valid
            } else if sequence <= state.last_sequence {
                // Duplicate
                SequenceResult::Duplicate {
                    cached_offset: state.last_offset,
                }
            } else {
                // Out of order (gap)
                SequenceResult::OutOfOrder {
                    expected,
                    received: sequence,
                }
            }
        } else {
            // First message for this partition
            if sequence == 0 {
                self.partitions
                    .insert(partition, PartitionProducerState::new(sequence, offset));
                SequenceResult::Valid
            } else {
                // First message should have sequence 0
                SequenceResult::OutOfOrder {
                    expected: 0,
                    received: sequence,
                }
            }
        }
    }

    /// Check if producer is idle (no activity for duration)
    pub fn is_idle(&self, timeout: Duration) -> bool {
        self.last_activity
            .elapsed()
            .map(|d| d > timeout)
            .unwrap_or(false)
    }
}

/// Idempotent producer state manager
///
/// Thread-safe manager for all producer state in a broker.
#[derive(Debug)]
pub struct IdempotentProducerManager {
    /// Next producer ID to assign
    next_producer_id: AtomicU64,

    /// Producer state: producer_id → ProducerMetadata
    producers: RwLock<HashMap<ProducerId, ProducerMetadata>>,

    /// Idle timeout for producer state cleanup
    idle_timeout: Duration,
}

impl Default for IdempotentProducerManager {
    fn default() -> Self {
        Self::new()
    }
}

impl IdempotentProducerManager {
    /// Create a new manager
    pub fn new() -> Self {
        Self {
            next_producer_id: AtomicU64::new(1), // Start from 1 (0 is reserved)
            producers: RwLock::new(HashMap::new()),
            idle_timeout: Duration::from_secs(7 * 24 * 60 * 60), // 7 days default
        }
    }

    /// Create with custom idle timeout
    pub fn with_idle_timeout(idle_timeout: Duration) -> Self {
        Self {
            next_producer_id: AtomicU64::new(1),
            producers: RwLock::new(HashMap::new()),
            idle_timeout,
        }
    }

    /// Initialize a new producer, returning (producer_id, epoch)
    ///
    /// If the producer_id is provided (reconnecting producer), bumps the epoch.
    /// If None, generates a new producer_id with epoch 0.
    pub fn init_producer(
        &self,
        existing_producer_id: Option<ProducerId>,
    ) -> (ProducerId, ProducerEpoch) {
        let mut producers = self
            .producers
            .write()
            .expect("producer registry lock poisoned");

        if let Some(pid) = existing_producer_id {
            // Reconnecting producer - bump epoch
            if let Some(metadata) = producers.get_mut(&pid) {
                let new_epoch = metadata.bump_epoch();
                return (pid, new_epoch);
            }
            // Producer ID not found, create new entry with epoch 0
            let metadata = ProducerMetadata::new(pid, 0);
            producers.insert(pid, metadata);
            (pid, 0)
        } else {
            // New producer
            let pid = self.next_producer_id.fetch_add(1, Ordering::SeqCst);
            let metadata = ProducerMetadata::new(pid, 0);
            producers.insert(pid, metadata);
            (pid, 0)
        }
    }

    /// Validate a produce request
    ///
    /// Returns the validation result and optionally updates internal state.
    pub fn validate_produce(
        &self,
        producer_id: ProducerId,
        epoch: ProducerEpoch,
        partition: u32,
        sequence: SequenceNumber,
        offset: u64,
    ) -> SequenceResult {
        let mut producers = self
            .producers
            .write()
            .expect("producer registry lock poisoned");

        if let Some(metadata) = producers.get_mut(&producer_id) {
            metadata.validate_sequence(partition, epoch, sequence, offset)
        } else {
            SequenceResult::UnknownProducer
        }
    }

    /// Record a successful produce (after append succeeds)
    ///
    /// This is called after the message is actually appended to update state.
    pub fn record_produce(
        &self,
        producer_id: ProducerId,
        epoch: ProducerEpoch,
        partition: u32,
        sequence: SequenceNumber,
        offset: u64,
    ) {
        let mut producers = self
            .producers
            .write()
            .expect("producer registry lock poisoned");

        if let Some(metadata) = producers.get_mut(&producer_id) {
            if metadata.epoch == epoch {
                if let Some(state) = metadata.partitions.get_mut(&partition) {
                    state.update(sequence, offset);
                } else {
                    metadata
                        .partitions
                        .insert(partition, PartitionProducerState::new(sequence, offset));
                }
                metadata.last_activity = SystemTime::now();
            }
        }
    }

    /// Get producer metadata (for debugging/monitoring)
    pub fn get_producer(&self, producer_id: ProducerId) -> Option<ProducerMetadata> {
        self.producers
            .read()
            .expect("producer registry lock poisoned")
            .get(&producer_id)
            .cloned()
    }

    /// Check if a producer exists
    pub fn has_producer(&self, producer_id: ProducerId) -> bool {
        self.producers
            .read()
            .expect("producer registry lock poisoned")
            .contains_key(&producer_id)
    }

    /// Get number of active producers
    pub fn producer_count(&self) -> usize {
        self.producers
            .read()
            .expect("producer registry lock poisoned")
            .len()
    }

    /// Cleanup idle producers
    ///
    /// Returns the number of producers removed.
    pub fn cleanup_idle_producers(&self) -> usize {
        let mut producers = self
            .producers
            .write()
            .expect("producer registry lock poisoned");
        let before = producers.len();
        producers.retain(|_, metadata| !metadata.is_idle(self.idle_timeout));
        before - producers.len()
    }

    /// Get statistics
    pub fn stats(&self) -> IdempotentProducerStats {
        let producers = self
            .producers
            .read()
            .expect("producer registry lock poisoned");
        let mut total_partitions = 0;
        let mut oldest_activity = SystemTime::now();

        for metadata in producers.values() {
            total_partitions += metadata.partitions.len();
            if metadata.last_activity < oldest_activity {
                oldest_activity = metadata.last_activity;
            }
        }

        IdempotentProducerStats {
            active_producers: producers.len(),
            total_partition_states: total_partitions,
            oldest_activity,
            next_producer_id: self.next_producer_id.load(Ordering::Relaxed),
        }
    }
}

/// Statistics for idempotent producer manager
#[derive(Debug, Clone)]
pub struct IdempotentProducerStats {
    /// Number of active producers
    pub active_producers: usize,

    /// Total partition states across all producers
    pub total_partition_states: usize,

    /// Oldest activity timestamp
    pub oldest_activity: SystemTime,

    /// Next producer ID to be assigned
    pub next_producer_id: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_new_producer() {
        let manager = IdempotentProducerManager::new();

        let (pid1, epoch1) = manager.init_producer(None);
        assert_eq!(pid1, 1);
        assert_eq!(epoch1, 0);

        let (pid2, epoch2) = manager.init_producer(None);
        assert_eq!(pid2, 2);
        assert_eq!(epoch2, 0);
    }

    #[test]
    fn test_reconnecting_producer_bumps_epoch() {
        let manager = IdempotentProducerManager::new();

        let (pid, epoch0) = manager.init_producer(None);
        assert_eq!(epoch0, 0);

        // Reconnect
        let (pid2, epoch1) = manager.init_producer(Some(pid));
        assert_eq!(pid2, pid);
        assert_eq!(epoch1, 1);

        // Reconnect again
        let (pid3, epoch2) = manager.init_producer(Some(pid));
        assert_eq!(pid3, pid);
        assert_eq!(epoch2, 2);
    }

    #[test]
    fn test_valid_sequence() {
        let manager = IdempotentProducerManager::new();
        let (pid, epoch) = manager.init_producer(None);

        // First message (seq=0)
        let result = manager.validate_produce(pid, epoch, 0, 0, 100);
        assert_eq!(result, SequenceResult::Valid);

        // Second message (seq=1)
        let result = manager.validate_produce(pid, epoch, 0, 1, 101);
        assert_eq!(result, SequenceResult::Valid);

        // Third message (seq=2)
        let result = manager.validate_produce(pid, epoch, 0, 2, 102);
        assert_eq!(result, SequenceResult::Valid);
    }

    #[test]
    fn test_duplicate_detection() {
        let manager = IdempotentProducerManager::new();
        let (pid, epoch) = manager.init_producer(None);

        // First message
        let result = manager.validate_produce(pid, epoch, 0, 0, 100);
        assert_eq!(result, SequenceResult::Valid);

        // Retry same sequence (duplicate) - returns last known offset for this sequence
        // Note: In Kafka, duplicates of the last sequence return the last offset
        let result = manager.validate_produce(pid, epoch, 0, 0, 999);
        assert_eq!(result, SequenceResult::Duplicate { cached_offset: 100 });

        // Next message
        let result = manager.validate_produce(pid, epoch, 0, 1, 101);
        assert_eq!(result, SequenceResult::Valid);

        // Retry of seq=1 returns the offset for seq=1
        let result = manager.validate_produce(pid, epoch, 0, 1, 999);
        assert_eq!(result, SequenceResult::Duplicate { cached_offset: 101 });

        // Retry of seq=0 is now also a duplicate (older than last sequence)
        // Kafka treats any sequence <= last_sequence as duplicate
        let result = manager.validate_produce(pid, epoch, 0, 0, 999);
        assert_eq!(result, SequenceResult::Duplicate { cached_offset: 101 });
    }

    #[test]
    fn test_out_of_order_sequence() {
        let manager = IdempotentProducerManager::new();
        let (pid, epoch) = manager.init_producer(None);

        // First message
        let result = manager.validate_produce(pid, epoch, 0, 0, 100);
        assert_eq!(result, SequenceResult::Valid);

        // Skip seq=1, send seq=2 (out of order)
        let result = manager.validate_produce(pid, epoch, 0, 2, 101);
        assert_eq!(
            result,
            SequenceResult::OutOfOrder {
                expected: 1,
                received: 2
            }
        );
    }

    #[test]
    fn test_first_message_must_be_zero() {
        let manager = IdempotentProducerManager::new();
        let (pid, epoch) = manager.init_producer(None);

        // First message should be seq=0
        let result = manager.validate_produce(pid, epoch, 0, 5, 100);
        assert_eq!(
            result,
            SequenceResult::OutOfOrder {
                expected: 0,
                received: 5
            }
        );
    }

    #[test]
    fn test_producer_fencing() {
        let manager = IdempotentProducerManager::new();
        let (pid, epoch0) = manager.init_producer(None);

        // Send with epoch 0
        let result = manager.validate_produce(pid, epoch0, 0, 0, 100);
        assert_eq!(result, SequenceResult::Valid);

        // Reconnect - bumps epoch to 1
        let (_, epoch1) = manager.init_producer(Some(pid));
        assert_eq!(epoch1, 1);

        // Old producer instance tries to send with epoch 0 (fenced)
        let result = manager.validate_produce(pid, epoch0, 0, 1, 101);
        assert_eq!(
            result,
            SequenceResult::Fenced {
                current_epoch: 1,
                received_epoch: 0
            }
        );

        // New instance with epoch 1 works (starts fresh at seq=0)
        let result = manager.validate_produce(pid, epoch1, 0, 0, 101);
        assert_eq!(result, SequenceResult::Valid);
    }

    #[test]
    fn test_multiple_partitions() {
        let manager = IdempotentProducerManager::new();
        let (pid, epoch) = manager.init_producer(None);

        // Partition 0
        let result = manager.validate_produce(pid, epoch, 0, 0, 100);
        assert_eq!(result, SequenceResult::Valid);

        // Partition 1 (independent sequence)
        let result = manager.validate_produce(pid, epoch, 1, 0, 200);
        assert_eq!(result, SequenceResult::Valid);

        // Partition 0 seq=1
        let result = manager.validate_produce(pid, epoch, 0, 1, 101);
        assert_eq!(result, SequenceResult::Valid);

        // Partition 1 seq=1
        let result = manager.validate_produce(pid, epoch, 1, 1, 201);
        assert_eq!(result, SequenceResult::Valid);
    }

    #[test]
    fn test_unknown_producer() {
        let manager = IdempotentProducerManager::new();

        let result = manager.validate_produce(9999, 0, 0, 0, 100);
        assert_eq!(result, SequenceResult::UnknownProducer);
    }

    #[test]
    fn test_cleanup_idle_producers() {
        let manager = IdempotentProducerManager::with_idle_timeout(Duration::from_millis(1));

        let (pid, _) = manager.init_producer(None);
        assert!(manager.has_producer(pid));

        // Wait for idle timeout
        std::thread::sleep(Duration::from_millis(10));

        let removed = manager.cleanup_idle_producers();
        assert_eq!(removed, 1);
        assert!(!manager.has_producer(pid));
    }

    #[test]
    fn test_stats() {
        let manager = IdempotentProducerManager::new();

        let stats = manager.stats();
        assert_eq!(stats.active_producers, 0);
        assert_eq!(stats.total_partition_states, 0);

        let (pid, epoch) = manager.init_producer(None);
        manager.validate_produce(pid, epoch, 0, 0, 100);
        manager.validate_produce(pid, epoch, 1, 0, 200);

        let stats = manager.stats();
        assert_eq!(stats.active_producers, 1);
        assert_eq!(stats.total_partition_states, 2);
    }

    #[test]
    fn test_epoch_upgrade_clears_state() {
        let manager = IdempotentProducerManager::new();
        let (pid, epoch0) = manager.init_producer(None);

        // Send message with epoch 0
        manager.validate_produce(pid, epoch0, 0, 0, 100);
        manager.validate_produce(pid, epoch0, 0, 1, 101);

        // Higher epoch comes in (new instance)
        let new_epoch: ProducerEpoch = 5;
        let result = manager.validate_produce(pid, new_epoch, 0, 0, 200);
        assert_eq!(result, SequenceResult::Valid);

        // State was cleared, so seq=0 is valid again
        let metadata = manager.get_producer(pid).unwrap();
        assert_eq!(metadata.epoch, 5);
    }
}
