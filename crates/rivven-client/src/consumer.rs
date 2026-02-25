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
use crate::error::{Error, Result};
use rivven_protocol::MessageData;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Configuration for the high-level consumer.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Bootstrap server addresses (host:port).
    ///
    /// On initial connect and reconnect, the consumer tries each server
    /// in round-robin order until one succeeds. Accepts a single server
    /// or multiple for failover.
    pub bootstrap_servers: Vec<String>,
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
    /// Interval for re-discovering partition assignments (default: 5 min).
    /// Set to `Duration::MAX` to disable periodic re-discovery.
    pub metadata_refresh_interval: Duration,
    /// Initial reconnect backoff delay in milliseconds (default: 100)
    pub reconnect_backoff_ms: u64,
    /// Maximum reconnect backoff delay in milliseconds (default: 10 000)
    pub reconnect_backoff_max_ms: u64,
}

/// Authentication configuration for the consumer.
#[derive(Debug, Clone)]
pub struct ConsumerAuthConfig {
    pub username: String,
    pub password: String,
}

/// Builder for [`ConsumerConfig`].
pub struct ConsumerConfigBuilder {
    bootstrap_servers: Vec<String>,
    group_id: Option<String>,
    topics: Vec<String>,
    partitions: HashMap<String, Vec<u32>>,
    max_poll_records: usize,
    max_poll_interval_ms: u64,
    auto_commit_interval: Option<Duration>,
    isolation_level: u8,
    auth: Option<ConsumerAuthConfig>,
    metadata_refresh_interval: Duration,
    reconnect_backoff_ms: u64,
    reconnect_backoff_max_ms: u64,
}

impl ConsumerConfigBuilder {
    pub fn new() -> Self {
        Self {
            bootstrap_servers: vec!["127.0.0.1:9092".to_string()],
            group_id: None,
            topics: Vec::new(),
            partitions: HashMap::new(),
            max_poll_records: 500,
            max_poll_interval_ms: 5000,
            auto_commit_interval: Some(Duration::from_secs(5)),
            isolation_level: 0,
            auth: None,
            metadata_refresh_interval: Duration::from_secs(300),
            reconnect_backoff_ms: 100,
            reconnect_backoff_max_ms: 10_000,
        }
    }

    /// Set a single bootstrap server (convenience for `bootstrap_servers`).
    pub fn bootstrap_server(mut self, server: impl Into<String>) -> Self {
        self.bootstrap_servers = vec![server.into()];
        self
    }

    /// Set multiple bootstrap servers for failover.
    pub fn bootstrap_servers(mut self, servers: Vec<String>) -> Self {
        self.bootstrap_servers = servers;
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

    /// Set the interval for periodic partition re-discovery.
    pub fn metadata_refresh_interval(mut self, interval: Duration) -> Self {
        self.metadata_refresh_interval = interval;
        self
    }

    /// Set initial reconnect backoff delay in milliseconds.
    pub fn reconnect_backoff_ms(mut self, ms: u64) -> Self {
        self.reconnect_backoff_ms = ms;
        self
    }

    /// Set maximum reconnect backoff delay in milliseconds.
    pub fn reconnect_backoff_max_ms(mut self, ms: u64) -> Self {
        self.reconnect_backoff_max_ms = ms;
        self
    }

    pub fn build(self) -> ConsumerConfig {
        ConsumerConfig {
            bootstrap_servers: self.bootstrap_servers,
            group_id: self.group_id.unwrap_or_else(|| "default-group".into()),
            topics: self.topics,
            partitions: self.partitions,
            max_poll_records: self.max_poll_records,
            max_poll_interval_ms: self.max_poll_interval_ms,
            auto_commit_interval: self.auto_commit_interval,
            isolation_level: self.isolation_level,
            auth: self.auth,
            metadata_refresh_interval: self.metadata_refresh_interval,
            reconnect_backoff_ms: self.reconnect_backoff_ms,
            reconnect_backoff_max_ms: self.reconnect_backoff_max_ms,
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
    /// Last partition discovery time
    last_discovery: Instant,
    /// Whether initial assignment discovery has been done
    initialized: bool,
}

impl Consumer {
    /// Create and connect a new consumer.
    ///
    /// Connects to the first available bootstrap server, authenticates if
    /// configured, and discovers partition assignments for subscribed topics.
    ///
    /// ## Auto-commit semantics
    ///
    /// When `auto_commit_interval` is set, offsets are committed periodically
    /// at the **next-fetch** position. This provides **at-most-once** semantics:
    /// if the application crashes between `poll()` returning and the records
    /// being processed, those records will be skipped on restart.
    ///
    /// For **at-least-once** semantics, disable auto-commit and call
    /// `commit()` explicitly after processing each batch.
    pub async fn new(config: ConsumerConfig) -> Result<Self> {
        let servers = &config.bootstrap_servers;
        if servers.is_empty() {
            return Err(Error::ConnectionError(
                "No bootstrap servers configured".to_string(),
            ));
        }

        let mut last_error = None;
        let mut client = None;
        for server in servers {
            match Client::connect(server).await {
                Ok(c) => {
                    client = Some(c);
                    break;
                }
                Err(e) => {
                    warn!(server = %server, error = %e, "Failed to connect to bootstrap server");
                    last_error = Some(e);
                }
            }
        }
        let mut client = client.ok_or_else(|| {
            last_error.unwrap_or_else(|| {
                Error::ConnectionError("No bootstrap servers available".to_string())
            })
        })?;

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
            last_discovery: Instant::now(),
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
    /// Automatically reconnects with exponential backoff on connection
    /// errors and periodically re-discovers partition
    /// assignments.
    pub async fn poll(&mut self) -> Result<Vec<ConsumerRecord>> {
        match self.poll_inner().await {
            Ok(records) => Ok(records),
            Err(e) if Self::is_connection_error(&e) => {
                warn!(error = %e, "Connection error during poll, attempting reconnect");
                self.reconnect().await?;
                self.poll_inner().await
            }
            Err(e) => Err(e),
        }
    }

    /// Inner poll implementation without reconnection wrapper.
    async fn poll_inner(&mut self) -> Result<Vec<ConsumerRecord>> {
        if !self.initialized {
            self.discover_assignments().await?;
        }

        // Periodically re-discover partition assignments so
        // that newly added partitions are picked up automatically.
        if self.last_discovery.elapsed() >= self.config.metadata_refresh_interval {
            if let Err(e) = self.discover_assignments().await {
                warn!(error = %e, "Failed to re-discover assignments, continuing with existing");
            }
            self.last_discovery = Instant::now();
        }

        let mut records = Vec::new();
        let isolation_level = if self.config.isolation_level > 0 {
            Some(self.config.isolation_level)
        } else {
            None
        };

        // Phase 1: Pipelined (non-blocking) fetch from all partitions.
        // Sends all consume requests at once, then reads all responses —
        // eliminates per-partition round-trip latency.
        if !self.assignment_list.is_empty() {
            let fetches: Vec<(&str, u32, u64, usize, Option<u8>)> = self
                .assignment_list
                .iter()
                .map(|(topic, partition)| {
                    let key = (topic.clone(), *partition);
                    let offset = self.offsets.get(&key).copied().unwrap_or(0);
                    (
                        topic.as_str(),
                        *partition,
                        offset,
                        self.config.max_poll_records,
                        isolation_level,
                    )
                })
                .collect();

            let results = self.client.consume_pipelined(&fetches).await?;

            for (i, result) in results.into_iter().enumerate() {
                let (topic, partition) = &self.assignment_list[i];
                match result {
                    Ok(messages) if !messages.is_empty() => {
                        let key = (topic.clone(), *partition);
                        let max_offset = messages.iter().map(|m| m.offset).max().unwrap_or(0);
                        self.offsets.insert(key, max_offset + 1);

                        records.extend(messages.into_iter().map(|data| ConsumerRecord {
                            topic: topic.clone(),
                            partition: *partition,
                            offset: data.offset,
                            data,
                        }));
                    }
                    Err(e) => {
                        warn!(
                            topic = %topic,
                            partition = partition,
                            error = %e,
                            "Pipelined fetch error, skipping partition"
                        );
                    }
                    _ => {} // empty result — no data
                }
            }
        }

        // Phase 2: If nothing was returned and long-polling is enabled,
        // issue a single long-poll to avoid a busy loop. We rotate the
        // assignment list so each call long-polls a different partition,
        // preventing starvation where only the first partition is polled.
        if records.is_empty() && self.config.max_poll_interval_ms > 0 {
            if !self.assignment_list.is_empty() {
                self.assignment_list.rotate_left(1);
            }
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
                if let Err(e) = self.commit_inner().await {
                    warn!(error = %e, "Auto-commit failed");
                }
            }
        }

        Ok(records)
    }

    /// Commit current offsets to the server.
    ///
    /// Automatically reconnects on connection errors.
    pub async fn commit(&mut self) -> Result<()> {
        match self.commit_inner().await {
            Ok(()) => Ok(()),
            Err(e) if Self::is_connection_error(&e) => {
                warn!(error = %e, "Connection error during commit, attempting reconnect");
                self.reconnect().await?;
                self.commit_inner().await
            }
            Err(e) => Err(e),
        }
    }

    /// Inner commit implementation without reconnection wrapper.
    ///
    /// Uses request pipelining to send all offset commits at once, then
    /// reads all responses. Collects all errors instead of only the last.
    async fn commit_inner(&mut self) -> Result<()> {
        if self.offsets.is_empty() {
            return Ok(());
        }

        // Build commit requests
        let commits: Vec<(String, u32, u64)> = self
            .offsets
            .iter()
            .map(|((topic, partition), offset)| (topic.clone(), *partition, *offset))
            .collect();

        let mut errors = Vec::new();

        // Use pipelining: send all commit requests back-to-back, then read responses
        if self.client.is_poisoned() {
            // Stream is desynchronized — sequential fallback would also fail.
            // Trigger reconnect by returning connection error immediately.
            return Err(Error::ConnectionError(
                "Client stream is desynchronized — reconnect required".into(),
            ));
        }

        {
            let results = self
                .client
                .commit_offsets_pipelined(&self.config.group_id, &commits)
                .await;

            match results {
                Ok(per_partition) => {
                    for (i, result) in per_partition.into_iter().enumerate() {
                        if let Err(e) = result {
                            let (topic, partition, offset) = &commits[i];
                            warn!(
                                topic = %topic, partition, offset, error = %e,
                                "Failed to commit offset"
                            );
                            errors.push(e);
                        }
                    }
                }
                Err(e) => {
                    // Transport-level failure — all commits failed
                    errors.push(e);
                }
            }
        }

        self.last_commit = Instant::now();

        if errors.is_empty() {
            debug!(
                group_id = %self.config.group_id,
                partitions = self.offsets.len(),
                "Offsets committed"
            );
            Ok(())
        } else {
            // Return the first error (all are logged above)
            Err(errors.into_iter().next().unwrap())
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

    // ========================================================================
    // Reconnection
    // ========================================================================

    /// Attempt to reconnect to a bootstrap server with exponential backoff.
    ///
    /// Tries each configured bootstrap server in round-robin order.
    async fn reconnect(&mut self) -> Result<()> {
        let mut backoff = Duration::from_millis(self.config.reconnect_backoff_ms);
        let max_backoff = Duration::from_millis(self.config.reconnect_backoff_max_ms);
        let servers = &self.config.bootstrap_servers;

        if servers.is_empty() {
            return Err(Error::ConnectionError(
                "No bootstrap servers configured".to_string(),
            ));
        }

        for attempt in 1..=10u32 {
            // Round-robin across bootstrap servers
            let server = &servers[(attempt as usize - 1) % servers.len()];
            info!(
                attempt,
                server = %server,
                "Attempting to reconnect"
            );
            match Client::connect(server).await {
                Ok(mut new_client) => {
                    // Re-authenticate if credentials are configured
                    if let Some(ref auth) = self.config.auth {
                        if let Err(e) = new_client
                            .authenticate_scram(&auth.username, &auth.password)
                            .await
                        {
                            warn!(error = %e, attempt, "Re-authentication failed during reconnect");
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(max_backoff);
                            continue;
                        }
                    }
                    self.client = new_client;
                    info!("Consumer reconnected successfully to {}", server);
                    return Ok(());
                }
                Err(e) => {
                    warn!(error = %e, attempt, server = %server, "Reconnect attempt failed");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(max_backoff);
                }
            }
        }
        Err(Error::ConnectionError(format!(
            "Failed to reconnect to any of {:?} after 10 attempts",
            servers
        )))
    }

    /// Check whether an error indicates a broken connection.
    fn is_connection_error(e: &Error) -> bool {
        matches!(
            e,
            Error::ConnectionError(_)
                | Error::IoError(_, _)
                | Error::Timeout
                | Error::TimeoutWithMessage(_)
                | Error::ProtocolError(_)
                | Error::ResponseTooLarge(_, _)
        )
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
