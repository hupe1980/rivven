//! High-level consumer API for Rivven (§10.1 fix)
//!
//! Provides a [`Consumer`] struct that wraps the low-level [`Client`] with:
//! - Topic subscription with automatic partition assignment
//! - Offset tracking per (topic, partition) pair
//! - Auto-commit of consumed offsets to the server
//! - Long-polling to avoid tight fetch loops
//! - Configurable batch sizes and poll intervals
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_client::consumer::{Consumer, ConsumerConfig};
//!
//! let config = ConsumerConfig::builder()
//!     .bootstrap_server("127.0.0.1:9092")
//!     .group_id("my-group")
//!     .topics(vec!["events".to_string()])
//!     .build();
//!
//! let mut consumer = Consumer::new(config).await?;
//!
//! loop {
//!     let records = consumer.poll().await?;
//!     for record in &records {
//!         println!("topic={} partition={} offset={}: {:?}",
//!             record.topic, record.partition, record.offset, record.value);
//!     }
//!     // Offsets are auto-committed periodically, or call:
//!     consumer.commit().await?;
//! }
//! ```
//!
//! # Consumer Group Protocol
//!
//! Full JoinGroup/SyncGroup/Heartbeat/LeaveGroup rebalancing requires
//! additional wire protocol extensions (not yet available). This consumer
//! uses a **static partition assignment** model: all partitions of
//! subscribed topics are assigned to this consumer. For multi-consumer
//! groups, use explicit partition assignment via [`ConsumerConfig::partitions`].

use crate::client::Client;
use crate::error::Result;
use rivven_protocol::MessageData;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Configuration for the high-level consumer.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Bootstrap server address (host:port)
    pub bootstrap_server: String,
    /// Consumer group ID for offset management
    pub group_id: String,
    /// Topics to subscribe to
    pub topics: Vec<String>,
    /// Explicit partition assignments (topic → partitions).
    /// If empty, all partitions of each subscribed topic are consumed.
    pub partitions: HashMap<String, Vec<u32>>,
    /// Maximum messages per partition per poll
    pub max_poll_records: usize,
    /// Long-poll wait time in milliseconds (0 = immediate return)
    pub max_poll_interval_ms: u64,
    /// Auto-commit interval (None = manual commit only)
    pub auto_commit_interval: Option<Duration>,
    /// Transaction isolation level (0 = read_uncommitted, 1 = read_committed)
    pub isolation_level: u8,
    /// Authentication credentials (optional)
    pub auth: Option<ConsumerAuthConfig>,
}

/// Authentication configuration for the consumer.
#[derive(Debug, Clone)]
pub struct ConsumerAuthConfig {
    pub username: String,
    pub password: String,
}

/// Builder for [`ConsumerConfig`].
pub struct ConsumerConfigBuilder {
    bootstrap_server: Option<String>,
    group_id: Option<String>,
    topics: Vec<String>,
    partitions: HashMap<String, Vec<u32>>,
    max_poll_records: usize,
    max_poll_interval_ms: u64,
    auto_commit_interval: Option<Duration>,
    isolation_level: u8,
    auth: Option<ConsumerAuthConfig>,
}

impl ConsumerConfigBuilder {
    pub fn new() -> Self {
        Self {
            bootstrap_server: None,
            group_id: None,
            topics: Vec::new(),
            partitions: HashMap::new(),
            max_poll_records: 500,
            max_poll_interval_ms: 5000,
            auto_commit_interval: Some(Duration::from_secs(5)),
            isolation_level: 0,
            auth: None,
        }
    }

    pub fn bootstrap_server(mut self, server: impl Into<String>) -> Self {
        self.bootstrap_server = Some(server.into());
        self
    }

    pub fn group_id(mut self, group: impl Into<String>) -> Self {
        self.group_id = Some(group.into());
        self
    }

    pub fn topics(mut self, topics: Vec<String>) -> Self {
        self.topics = topics;
        self
    }

    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.topics.push(topic.into());
        self
    }

    /// Assign specific partitions for a topic (static assignment).
    pub fn assign(mut self, topic: impl Into<String>, partitions: Vec<u32>) -> Self {
        self.partitions.insert(topic.into(), partitions);
        self
    }

    pub fn max_poll_records(mut self, n: usize) -> Self {
        self.max_poll_records = n;
        self
    }

    pub fn max_poll_interval_ms(mut self, ms: u64) -> Self {
        self.max_poll_interval_ms = ms;
        self
    }

    pub fn auto_commit_interval(mut self, interval: Option<Duration>) -> Self {
        self.auto_commit_interval = interval;
        self
    }

    pub fn enable_auto_commit(mut self, enabled: bool) -> Self {
        if enabled {
            self.auto_commit_interval = Some(Duration::from_secs(5));
        } else {
            self.auto_commit_interval = None;
        }
        self
    }

    pub fn isolation_level(mut self, level: u8) -> Self {
        self.isolation_level = level;
        self
    }

    /// Use read_committed isolation (only see committed transactional messages).
    pub fn read_committed(mut self) -> Self {
        self.isolation_level = 1;
        self
    }

    pub fn auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = Some(ConsumerAuthConfig {
            username: username.into(),
            password: password.into(),
        });
        self
    }

    pub fn build(self) -> ConsumerConfig {
        ConsumerConfig {
            bootstrap_server: self
                .bootstrap_server
                .unwrap_or_else(|| "127.0.0.1:9092".into()),
            group_id: self.group_id.unwrap_or_else(|| "default-group".into()),
            topics: self.topics,
            partitions: self.partitions,
            max_poll_records: self.max_poll_records,
            max_poll_interval_ms: self.max_poll_interval_ms,
            auto_commit_interval: self.auto_commit_interval,
            isolation_level: self.isolation_level,
            auth: self.auth,
        }
    }
}

impl Default for ConsumerConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsumerConfig {
    pub fn builder() -> ConsumerConfigBuilder {
        ConsumerConfigBuilder::new()
    }
}

/// A consumed record with topic/partition metadata.
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    /// Topic the record was consumed from
    pub topic: String,
    /// Partition number
    pub partition: u32,
    /// Record offset within the partition
    pub offset: u64,
    /// Record data
    pub data: MessageData,
}

/// High-level consumer that manages offset tracking and auto-commit.
///
/// Wraps one or more partitions across subscribed topics, polling them
/// round-robin and tracking the latest consumed offset per partition.
pub struct Consumer {
    client: Client,
    config: ConsumerConfig,
    /// Current offset per (topic, partition) — next offset to fetch
    offsets: HashMap<(String, u32), u64>,
    /// Resolved partition assignments: topic → Vec<partition_id>
    assignments: HashMap<String, Vec<u32>>,
    /// Flattened assignment list cached to avoid cloning on every poll
    assignment_list: Vec<(String, u32)>,
    /// Last auto-commit time
    last_commit: Instant,
    /// Whether initial assignment discovery has been done
    initialized: bool,
}

impl Consumer {
    /// Create and connect a new consumer.
    ///
    /// Connects to the bootstrap server, authenticates if configured,
    /// and discovers partition assignments for subscribed topics.
    pub async fn new(config: ConsumerConfig) -> Result<Self> {
        let mut client = Client::connect(&config.bootstrap_server).await?;

        // Authenticate if credentials are provided
        if let Some(ref auth) = config.auth {
            client
                .authenticate_scram(&auth.username, &auth.password)
                .await?;
        }

        let mut consumer = Self {
            client,
            config,
            offsets: HashMap::new(),
            assignments: HashMap::new(),
            assignment_list: Vec::new(),
            last_commit: Instant::now(),
            initialized: false,
        };

        consumer.discover_assignments().await?;

        info!(
            group_id = %consumer.config.group_id,
            topics = ?consumer.config.topics,
            partitions = ?consumer.assignments,
            "Consumer initialized"
        );

        Ok(consumer)
    }

    /// Discover partition assignments for subscribed topics.
    ///
    /// For each topic, queries the server for the partition count and
    /// assigns all partitions (or uses explicit assignments from config).
    async fn discover_assignments(&mut self) -> Result<()> {
        for topic in &self.config.topics {
            if let Some(explicit) = self.config.partitions.get(topic) {
                // Use explicit partition assignment
                self.assignments.insert(topic.clone(), explicit.clone());
            } else {
                // Discover partitions from server
                match self.client.get_metadata(topic.as_str()).await {
                    Ok((_name, partition_count)) => {
                        let partitions: Vec<u32> = (0..partition_count).collect();
                        self.assignments.insert(topic.clone(), partitions);
                    }
                    Err(e) => {
                        warn!(
                            topic = %topic,
                            error = %e,
                            "Failed to discover partitions, will retry on next poll"
                        );
                    }
                }
            }
        }

        // Initialize offsets from server (committed offsets)
        for (topic, partitions) in &self.assignments {
            for &partition in partitions {
                let key = (topic.clone(), partition);
                if self.offsets.contains_key(&key) {
                    continue;
                }
                // Try to load committed offset from server
                match self
                    .client
                    .get_offset(&self.config.group_id, topic, partition)
                    .await
                {
                    Ok(Some(offset)) => {
                        debug!(
                            topic = %topic,
                            partition,
                            offset,
                            "Resumed from committed offset"
                        );
                        self.offsets.insert(key, offset);
                    }
                    Ok(None) => {
                        // No committed offset — start from 0
                        self.offsets.insert(key, 0);
                    }
                    Err(e) => {
                        debug!(
                            topic = %topic,
                            partition,
                            error = %e,
                            "Failed to load committed offset, starting from 0"
                        );
                        self.offsets.insert(key, 0);
                    }
                }
            }
        }

        self.initialized = true;

        // Rebuild cached flattened assignment list
        self.assignment_list = self
            .assignments
            .iter()
            .flat_map(|(t, ps)| ps.iter().map(move |&p| (t.clone(), p)))
            .collect();

        Ok(())
    }

    /// Poll for new records across all assigned partitions.
    ///
    /// Fetches records from each partition round-robin, using long-polling
    /// all assigned partitions are fetched in each poll cycle. If no data is
    /// immediately available and long-polling is enabled, a single long-poll
    /// request is issued on the first partition to avoid tight spin loops.
    ///
    /// If auto-commit is enabled and the interval has elapsed, offsets are
    /// committed before returning.
    pub async fn poll(&mut self) -> Result<Vec<ConsumerRecord>> {
        if !self.initialized {
            self.discover_assignments().await?;
        }

        let mut records = Vec::new();
        let isolation_level = if self.config.isolation_level > 0 {
            Some(self.config.isolation_level)
        } else {
            None
        };

        // Phase 1: Immediate (non-blocking) fetch from all partitions
        for (topic, partition) in &self.assignment_list {
            let key = (topic.clone(), *partition);
            let offset = self.offsets.get(&key).copied().unwrap_or(0);

            let messages = self
                .client
                .consume_with_isolation(
                    topic.as_str(),
                    *partition,
                    offset,
                    self.config.max_poll_records,
                    isolation_level,
                )
                .await?;

            if !messages.is_empty() {
                let max_offset = messages.iter().map(|m| m.offset).max().unwrap_or(offset);
                self.offsets.insert(key, max_offset + 1);

                records.extend(messages.into_iter().map(|data| ConsumerRecord {
                    topic: topic.clone(),
                    partition: *partition,
                    offset: data.offset,
                    data,
                }));
            }
        }

        // Phase 2: If nothing was returned and long-polling is enabled,
        // issue a single long-poll on the first partition to avoid a busy loop.
        if records.is_empty() && self.config.max_poll_interval_ms > 0 {
            if let Some((topic, partition)) = self.assignment_list.first() {
                let key = (topic.clone(), *partition);
                let offset = self.offsets.get(&key).copied().unwrap_or(0);

                let messages = self
                    .client
                    .consume_long_poll(
                        topic.as_str(),
                        *partition,
                        offset,
                        self.config.max_poll_records,
                        isolation_level,
                        self.config.max_poll_interval_ms,
                    )
                    .await?;

                if !messages.is_empty() {
                    let max_offset = messages.iter().map(|m| m.offset).max().unwrap_or(offset);
                    self.offsets.insert(key, max_offset + 1);

                    records.extend(messages.into_iter().map(|data| ConsumerRecord {
                        topic: topic.clone(),
                        partition: *partition,
                        offset: data.offset,
                        data,
                    }));
                }
            }
        }

        // Auto-commit if interval has elapsed
        if let Some(interval) = self.config.auto_commit_interval {
            if self.last_commit.elapsed() >= interval {
                if let Err(e) = self.commit().await {
                    warn!(error = %e, "Auto-commit failed");
                }
            }
        }

        Ok(records)
    }

    /// Commit current offsets to the server.
    ///
    /// Sends a `CommitOffset` request for every tracked (topic, partition)
    /// pair. Failures are logged but do not abort the remaining commits.
    pub async fn commit(&mut self) -> Result<()> {
        let mut last_error = None;

        for ((topic, partition), offset) in &self.offsets {
            if let Err(e) = self
                .client
                .commit_offset(&self.config.group_id, topic, *partition, *offset)
                .await
            {
                warn!(
                    topic = %topic,
                    partition,
                    offset,
                    error = %e,
                    "Failed to commit offset"
                );
                last_error = Some(e);
            }
        }

        self.last_commit = Instant::now();

        match last_error {
            Some(e) => Err(e),
            None => {
                debug!(
                    group_id = %self.config.group_id,
                    partitions = self.offsets.len(),
                    "Offsets committed"
                );
                Ok(())
            }
        }
    }

    /// Seek a specific partition to a given offset.
    ///
    /// The next `poll()` will fetch from this offset for the specified partition.
    pub fn seek(&mut self, topic: impl Into<String>, partition: u32, offset: u64) {
        self.offsets.insert((topic.into(), partition), offset);
    }

    /// Seek all partitions of a topic to the beginning (offset 0).
    pub fn seek_to_beginning(&mut self, topic: &str) {
        if let Some(partitions) = self.assignments.get(topic) {
            for &p in partitions {
                self.offsets.insert((topic.to_string(), p), 0);
            }
        }
    }

    /// Get the current offset position for a (topic, partition) pair.
    pub fn position(&self, topic: &str, partition: u32) -> Option<u64> {
        self.offsets.get(&(topic.to_string(), partition)).copied()
    }

    /// Get current partition assignments.
    pub fn assignments(&self) -> &HashMap<String, Vec<u32>> {
        &self.assignments
    }

    /// Get the consumer group ID.
    pub fn group_id(&self) -> &str {
        &self.config.group_id
    }

    /// Close the consumer, committing final offsets if auto-commit is enabled.
    pub async fn close(mut self) -> Result<()> {
        if self.config.auto_commit_interval.is_some() {
            self.commit().await?;
        }
        info!(group_id = %self.config.group_id, "Consumer closed");
        Ok(())
    }
}
