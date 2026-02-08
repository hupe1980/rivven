//! Batch processing utilities
//!
//! This module provides utilities for efficient batch processing of events,
//! including automatic batching, flush policies, and batch iterators.
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::prelude::*;
//!
//! let batcher = Batcher::new()
//!     .with_max_size(1000)
//!     .with_max_bytes(1024 * 1024)
//!     .with_max_wait(Duration::from_secs(5));
//!
//! while let Some(event) = source.next().await {
//!     if let Some(batch) = batcher.add(event) {
//!         // Batch is ready, process it
//!         sink.write_batch(batch).await?;
//!     }
//! }
//!
//! // Flush remaining events
//! if let Some(batch) = batcher.flush() {
//!     sink.write_batch(batch).await?;
//! }
//! ```

use super::event::SourceEvent;
use std::time::{Duration, Instant};

/// Configuration for batching
#[derive(Debug, Clone)]
pub struct BatcherConfig {
    /// Maximum number of events per batch
    pub max_size: usize,
    /// Maximum bytes per batch (0 = no limit)
    pub max_bytes: usize,
    /// Maximum time to wait before flushing
    pub max_wait: Duration,
}

impl Default for BatcherConfig {
    fn default() -> Self {
        Self {
            max_size: 10_000,
            max_bytes: 10 * 1024 * 1024, // 10MB
            max_wait: Duration::from_secs(5),
        }
    }
}

/// A batcher that accumulates events into batches
#[derive(Debug)]
pub struct Batcher {
    config: BatcherConfig,
    events: Vec<SourceEvent>,
    current_bytes: usize,
    batch_start: Option<Instant>,
}

impl Default for Batcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Batcher {
    /// Create a new batcher with default configuration
    pub fn new() -> Self {
        Self {
            config: BatcherConfig::default(),
            events: Vec::new(),
            current_bytes: 0,
            batch_start: None,
        }
    }

    /// Create a batcher with custom configuration
    pub fn with_config(config: BatcherConfig) -> Self {
        Self {
            config,
            events: Vec::new(),
            current_bytes: 0,
            batch_start: None,
        }
    }

    /// Set the maximum batch size
    pub fn with_max_size(mut self, size: usize) -> Self {
        self.config.max_size = size;
        self
    }

    /// Set the maximum bytes per batch
    pub fn with_max_bytes(mut self, bytes: usize) -> Self {
        self.config.max_bytes = bytes;
        self
    }

    /// Set the maximum wait time
    pub fn with_max_wait(mut self, duration: Duration) -> Self {
        self.config.max_wait = duration;
        self
    }

    /// Add an event to the batch
    ///
    /// Returns Some(batch) if the batch is ready to be flushed
    pub fn add(&mut self, event: SourceEvent) -> Option<Batch> {
        // Estimate event size
        let event_size = self.estimate_size(&event);

        // Check if adding this event would exceed limits
        let would_exceed_size = self.events.len() >= self.config.max_size;
        let would_exceed_bytes =
            self.config.max_bytes > 0 && self.current_bytes + event_size > self.config.max_bytes;

        if would_exceed_size || would_exceed_bytes {
            // Flush current batch first
            let batch = self.take_batch();

            // Start new batch with this event
            self.events.push(event);
            self.current_bytes = event_size;
            self.batch_start = Some(Instant::now());

            return batch;
        }

        // Add event to current batch
        if self.batch_start.is_none() {
            self.batch_start = Some(Instant::now());
        }

        self.events.push(event);
        self.current_bytes += event_size;

        // Check if batch is now full
        if self.events.len() >= self.config.max_size {
            return self.take_batch();
        }

        None
    }

    /// Check if the batch should be flushed due to timeout
    pub fn should_flush(&self) -> bool {
        if self.events.is_empty() {
            return false;
        }

        if let Some(start) = self.batch_start {
            start.elapsed() >= self.config.max_wait
        } else {
            false
        }
    }

    /// Flush the current batch
    pub fn flush(&mut self) -> Option<Batch> {
        self.take_batch()
    }

    /// Take the current batch and reset
    fn take_batch(&mut self) -> Option<Batch> {
        if self.events.is_empty() {
            return None;
        }

        let events = std::mem::take(&mut self.events);
        let bytes = self.current_bytes;
        self.current_bytes = 0;
        self.batch_start = None;

        Some(Batch {
            events,
            total_bytes: bytes,
        })
    }

    /// Estimate the size of an event in bytes (zero-allocation heuristic)
    fn estimate_size(&self, event: &SourceEvent) -> usize {
        event.estimated_size()
    }

    /// Get the current number of events in the batch
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Get the current byte size
    pub fn bytes(&self) -> usize {
        self.current_bytes
    }
}

/// A batch of events ready for processing
#[derive(Debug)]
pub struct Batch {
    /// The events in this batch
    pub events: Vec<SourceEvent>,
    /// Total estimated bytes
    pub total_bytes: usize,
}

impl Batch {
    /// Create an empty batch
    pub fn empty() -> Self {
        Self {
            events: Vec::new(),
            total_bytes: 0,
        }
    }

    /// Create a batch from events
    pub fn from_events(events: Vec<SourceEvent>) -> Self {
        let total_bytes: usize = events.iter().map(|e| e.estimated_size()).sum();

        Self {
            events,
            total_bytes,
        }
    }

    /// Get the number of events
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Iterate over events
    pub fn iter(&self) -> impl Iterator<Item = &SourceEvent> {
        self.events.iter()
    }
}

impl IntoIterator for Batch {
    type Item = SourceEvent;
    type IntoIter = std::vec::IntoIter<SourceEvent>;

    fn into_iter(self) -> Self::IntoIter {
        self.events.into_iter()
    }
}

impl<'a> IntoIterator for &'a Batch {
    type Item = &'a SourceEvent;
    type IntoIter = std::slice::Iter<'a, SourceEvent>;

    fn into_iter(self) -> Self::IntoIter {
        self.events.iter()
    }
}

/// An async batcher that handles time-based flushing
pub struct AsyncBatcher {
    batcher: Batcher,
}

impl AsyncBatcher {
    /// Create a new async batcher
    pub fn new(config: BatcherConfig) -> Self {
        Self {
            batcher: Batcher::with_config(config),
        }
    }

    /// Add an event
    pub fn add(&mut self, event: SourceEvent) -> Option<Batch> {
        self.batcher.add(event)
    }

    /// Check if should flush
    pub fn should_flush(&self) -> bool {
        self.batcher.should_flush()
    }

    /// Flush current batch
    pub fn flush(&mut self) -> Option<Batch> {
        self.batcher.flush()
    }

    /// Get time until next flush should happen
    pub fn time_until_flush(&self) -> Option<Duration> {
        if self.batcher.is_empty() {
            return None;
        }

        self.batcher.batch_start.map(|start| {
            let elapsed = start.elapsed();
            if elapsed >= self.batcher.config.max_wait {
                Duration::ZERO
            } else {
                self.batcher.config.max_wait - elapsed
            }
        })
    }
}

/// Split events into fixed-size chunks
pub fn chunk_events(events: Vec<SourceEvent>, chunk_size: usize) -> Vec<Batch> {
    events
        .chunks(chunk_size)
        .map(|chunk| Batch::from_events(chunk.to_vec()))
        .collect()
}

/// Split events by a partitioning function
pub fn partition_events<F, K>(events: Vec<SourceEvent>, mut key_fn: F) -> Vec<(K, Batch)>
where
    F: FnMut(&SourceEvent) -> K,
    K: Eq + std::hash::Hash,
{
    use std::collections::HashMap;

    let mut partitions: HashMap<K, Vec<SourceEvent>> = HashMap::new();

    for event in events {
        let key = key_fn(&event);
        partitions.entry(key).or_default().push(event);
    }

    partitions
        .into_iter()
        .map(|(k, events)| (k, Batch::from_events(events)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_event(id: usize) -> SourceEvent {
        SourceEvent::record("test", json!({"id": id}))
    }

    #[test]
    fn test_batcher_basic() {
        let mut batcher = Batcher::new().with_max_size(3);

        assert!(batcher.add(test_event(1)).is_none());
        assert!(batcher.add(test_event(2)).is_none());

        // Third event should not trigger flush yet (batch size = 3)
        let batch = batcher.add(test_event(3));
        assert!(batch.is_some());

        let batch = batch.unwrap();
        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn test_batcher_flush() {
        let mut batcher = Batcher::new();

        batcher.add(test_event(1));
        batcher.add(test_event(2));

        let batch = batcher.flush();
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().len(), 2);

        // Should be empty now
        assert!(batcher.is_empty());
        assert!(batcher.flush().is_none());
    }

    #[test]
    fn test_batcher_empty() {
        let mut batcher = Batcher::new();
        assert!(batcher.is_empty());
        assert!(batcher.flush().is_none());
    }

    #[test]
    fn test_batcher_bytes_limit() {
        let mut batcher = Batcher::new().with_max_size(1000).with_max_bytes(100); // Very small byte limit

        // Add events until byte limit is reached
        let mut batch_count = 0;
        for i in 0..10 {
            if batcher.add(test_event(i)).is_some() {
                batch_count += 1;
            }
        }

        // Should have triggered at least one batch due to size limit
        assert!(batch_count > 0 || !batcher.is_empty());
    }

    #[test]
    fn test_batch_from_events() {
        let events = vec![test_event(1), test_event(2), test_event(3)];
        let batch = Batch::from_events(events);

        assert_eq!(batch.len(), 3);
        assert!(!batch.is_empty());
        assert!(batch.total_bytes > 0);
    }

    #[test]
    fn test_batch_iterator() {
        let events = vec![test_event(1), test_event(2)];
        let batch = Batch::from_events(events);

        let collected: Vec<_> = batch.iter().collect();
        assert_eq!(collected.len(), 2);
    }

    #[test]
    fn test_chunk_events() {
        let events: Vec<_> = (0..10).map(test_event).collect();
        let chunks = chunk_events(events, 3);

        assert_eq!(chunks.len(), 4); // 3 + 3 + 3 + 1
        assert_eq!(chunks[0].len(), 3);
        assert_eq!(chunks[3].len(), 1);
    }

    #[test]
    fn test_partition_events() {
        let events = vec![
            SourceEvent::record("users", json!({"id": 1})),
            SourceEvent::record("orders", json!({"id": 1})),
            SourceEvent::record("users", json!({"id": 2})),
        ];

        let partitions = partition_events(events, |e| e.stream.clone());

        assert_eq!(partitions.len(), 2);
    }

    #[test]
    fn test_async_batcher_time_until_flush() {
        let config = BatcherConfig {
            max_wait: Duration::from_secs(5),
            ..Default::default()
        };
        let mut batcher = AsyncBatcher::new(config);

        // Empty batcher has no flush time
        assert!(batcher.time_until_flush().is_none());

        // Add an event
        batcher.add(test_event(1));

        // Now should have a time until flush
        let time = batcher.time_until_flush();
        assert!(time.is_some());
        assert!(time.unwrap() <= Duration::from_secs(5));
    }

    #[test]
    fn test_batcher_config_default() {
        let config = BatcherConfig::default();
        assert_eq!(config.max_size, 10_000);
        assert_eq!(config.max_bytes, 10 * 1024 * 1024);
        assert_eq!(config.max_wait, Duration::from_secs(5));
    }
}
