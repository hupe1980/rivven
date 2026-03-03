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
//! When no explicit partition assignments are configured, the consumer
//! uses server-side group coordination:
//!
//! 1. **JoinGroup** — register with the coordinator, receive generation ID
//! 2. **SyncGroup** — leader computes assignments, all members receive theirs
//! 3. **Heartbeat** — periodic keep-alive during `poll()`
//! 4. **LeaveGroup** — graceful departure on `close()`
//!
//! For explicit partition assignment (static model), set
//! [`ConsumerConfig::partitions`] to bypass the coordination protocol.

use crate::client::Client;
use crate::error::{Error, Result};
use rivven_protocol::MessageData;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// A topic-partition pair used in rebalance callbacks.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: Arc<str>,
    pub partition: u32,
}

/// Callback interface for consumer group rebalance events (CLIENT-08).
///
/// Implement this trait to receive notifications when partitions are
/// revoked or assigned during a rebalance. This is critical for:
/// - Committing offsets before partitions are revoked (exactly-once)
/// - Initializing state when new partitions are assigned
/// - Cleaning up resources when partitions are lost
///
/// # Example
///
/// ```rust,ignore
/// struct MyListener;
///
/// #[async_trait::async_trait]
/// impl RebalanceListener for MyListener {
///     async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) {
///         // Commit offsets for revoked partitions
///     }
///     async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) {
///         // Initialize state for new partitions
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait RebalanceListener: Send + Sync {
    /// Called before partitions are revoked from this consumer.
    ///
    /// Use this to commit offsets or flush state for the given partitions.
    /// This is invoked synchronously — the rebalance blocks until this returns.
    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]);

    /// Called after new partitions are assigned to this consumer.
    ///
    /// Use this to initialize partition-specific state or seek to custom offsets.
    async fn on_partitions_assigned(&self, partitions: &[TopicPartition]);
}

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
    pub max_poll_records: u32,
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
    /// Maximum number of reconnect attempts before giving up (default: 10)
    pub max_reconnect_attempts: u32,
    /// Session timeout for group coordination in milliseconds (default: 10 000).
    /// If the coordinator does not receive a heartbeat within this interval,
    /// it considers the member dead and triggers a rebalance.
    pub session_timeout_ms: u32,
    /// Rebalance timeout in milliseconds (default: 30 000).
    /// Maximum time the coordinator waits for all members to join during a rebalance.
    pub rebalance_timeout_ms: u32,
    /// Heartbeat interval in milliseconds (default: 3 000).
    /// Should be no more than 1/3 of `session_timeout_ms`.
    pub heartbeat_interval_ms: u64,
    /// TLS configuration (optional). When set, the consumer connects
    /// over TLS instead of plaintext.
    #[cfg(feature = "tls")]
    pub tls_config: Option<rivven_core::tls::TlsConfig>,
    /// TLS server name for certificate verification.
    /// Required when `tls_config` is `Some`.
    #[cfg(feature = "tls")]
    pub tls_server_name: Option<String>,
}

/// Authentication configuration for the consumer.
#[derive(Clone)]
pub struct ConsumerAuthConfig {
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for ConsumerAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerAuthConfig")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

/// Builder for [`ConsumerConfig`].
pub struct ConsumerConfigBuilder {
    bootstrap_servers: Vec<String>,
    group_id: Option<String>,
    topics: Vec<String>,
    partitions: HashMap<String, Vec<u32>>,
    max_poll_records: u32,
    max_poll_interval_ms: u64,
    auto_commit_interval: Option<Duration>,
    isolation_level: u8,
    auth: Option<ConsumerAuthConfig>,
    metadata_refresh_interval: Duration,
    reconnect_backoff_ms: u64,
    reconnect_backoff_max_ms: u64,
    max_reconnect_attempts: u32,
    session_timeout_ms: u32,
    rebalance_timeout_ms: u32,
    heartbeat_interval_ms: u64,
    #[cfg(feature = "tls")]
    tls_config: Option<rivven_core::tls::TlsConfig>,
    #[cfg(feature = "tls")]
    tls_server_name: Option<String>,
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
            max_reconnect_attempts: 10,
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 30_000,
            heartbeat_interval_ms: 3_000,
            #[cfg(feature = "tls")]
            tls_config: None,
            #[cfg(feature = "tls")]
            tls_server_name: None,
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

    pub fn max_poll_records(mut self, n: u32) -> Self {
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

    /// Set maximum number of reconnect attempts (default: 10).
    pub fn max_reconnect_attempts(mut self, attempts: u32) -> Self {
        self.max_reconnect_attempts = attempts;
        self
    }

    /// Set session timeout for group coordination (default: 10 000 ms).
    pub fn session_timeout_ms(mut self, ms: u32) -> Self {
        self.session_timeout_ms = ms;
        self
    }

    /// Set rebalance timeout (default: 30 000 ms).
    pub fn rebalance_timeout_ms(mut self, ms: u32) -> Self {
        self.rebalance_timeout_ms = ms;
        self
    }

    /// Set heartbeat interval (default: 3 000 ms, should be ≤ 1/3 of session timeout).
    pub fn heartbeat_interval_ms(mut self, ms: u64) -> Self {
        self.heartbeat_interval_ms = ms;
        self
    }

    /// Set TLS configuration for encrypted connections (CLIENT-06).
    #[cfg(feature = "tls")]
    pub fn tls(
        mut self,
        tls_config: rivven_core::tls::TlsConfig,
        server_name: impl Into<String>,
    ) -> Self {
        self.tls_config = Some(tls_config);
        self.tls_server_name = Some(server_name.into());
        self
    }

    pub fn build(self) -> ConsumerConfig {
        // Enforce heartbeat ≤ 1/3 of session timeout (Kafka best practice).
        // If the user configured an unnecessarily long heartbeat, clamp it
        // automatically instead of silently allowing session expiry.
        let max_heartbeat = (self.session_timeout_ms as u64) / 3;
        let heartbeat_interval_ms = if self.heartbeat_interval_ms > max_heartbeat {
            tracing::warn!(
                configured = self.heartbeat_interval_ms,
                clamped_to = max_heartbeat,
                session_timeout_ms = self.session_timeout_ms,
                "heartbeat_interval_ms exceeds 1/3 of session_timeout_ms, clamping"
            );
            max_heartbeat
        } else {
            self.heartbeat_interval_ms
        };

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
            max_reconnect_attempts: self.max_reconnect_attempts,
            session_timeout_ms: self.session_timeout_ms,
            rebalance_timeout_ms: self.rebalance_timeout_ms,
            heartbeat_interval_ms,
            #[cfg(feature = "tls")]
            tls_config: self.tls_config,
            #[cfg(feature = "tls")]
            tls_server_name: self.tls_server_name,
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
    /// Topic the record was consumed from (cheap `Arc` clone per record)
    pub topic: Arc<str>,
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
    offsets: HashMap<(Arc<str>, u32), u64>,
    /// Resolved partition assignments: topic → Vec<partition_id>
    assignments: HashMap<String, Vec<u32>>,
    /// Flattened assignment list cached to avoid cloning on every poll
    assignment_list: Vec<(Arc<str>, u32)>,
    /// Last auto-commit time
    last_commit: Instant,
    /// Last partition discovery time
    last_discovery: Instant,
    /// Whether initial assignment discovery has been done
    initialized: bool,
    /// Member ID assigned by the group coordinator (empty for static assignment)
    member_id: String,
    /// Current generation ID from the coordinator
    generation_id: u32,
    /// Whether this consumer is the group leader (computes assignments in SyncGroup)
    is_leader: bool,
    /// Last heartbeat time (for periodic heartbeats during poll)
    last_heartbeat: Instant,
    /// Whether this consumer uses server-side group coordination
    uses_coordination: bool,
    /// Set when a fetch response or background heartbeat signals rebalance,
    /// triggering a rejoin on the next poll (CLIENT-07 / CLIENT-02).
    /// Shared with the background heartbeat task via `Arc`.
    needs_rejoin: Arc<AtomicBool>,
    /// Optional rebalance listener for partition revocation/assignment callbacks (CLIENT-08).
    rebalance_listener: Option<Arc<dyn RebalanceListener>>,
    /// Background heartbeat task handle (CLIENT-02).
    /// Aborted on close, reconnect, or rebalance.
    heartbeat_handle: Option<tokio::task::JoinHandle<()>>,
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
            // Connect with TLS when configured (CLIENT-06), otherwise plaintext.
            #[cfg(feature = "tls")]
            let connect_result = if let (Some(ref tls_cfg), Some(ref sni)) =
                (&config.tls_config, &config.tls_server_name)
            {
                Client::connect_tls(server, tls_cfg, sni).await
            } else {
                Client::connect(server).await
            };
            #[cfg(not(feature = "tls"))]
            let connect_result = Client::connect(server).await;

            match connect_result {
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

        let uses_coordination = config.partitions.is_empty();

        let mut consumer = Self {
            client,
            config,
            offsets: HashMap::new(),
            assignments: HashMap::new(),
            assignment_list: Vec::new(),
            last_commit: Instant::now(),
            last_discovery: Instant::now(),
            initialized: false,
            member_id: String::new(),
            generation_id: 0,
            is_leader: false,
            last_heartbeat: Instant::now(),
            uses_coordination,
            needs_rejoin: Arc::new(AtomicBool::new(false)),
            rebalance_listener: None,
            heartbeat_handle: None,
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

    /// Register a rebalance listener for partition revocation/assignment events.
    ///
    /// The listener is invoked during `discover_assignments()`:
    /// - `on_partitions_revoked` is called with the old assignment before reassignment
    /// - `on_partitions_assigned` is called with the new assignment after reassignment
    pub fn set_rebalance_listener(&mut self, listener: Arc<dyn RebalanceListener>) {
        self.rebalance_listener = Some(listener);
    }

    /// Spawn (or restart) the background heartbeat task (CLIENT-02).
    ///
    /// The task opens its own TCP connection to the first available
    /// bootstrap server, authenticates if needed, then sends periodic
    /// heartbeats independently of the poll loop. This matches Kafka's
    /// dedicated `HeartbeatThread` design.
    ///
    /// If the heartbeat detects `REBALANCE_IN_PROGRESS` or a connection
    /// error, it sets `needs_rejoin` so the next `poll()` triggers a
    /// group rejoin.
    async fn spawn_heartbeat_task(&mut self) {
        // Abort any existing heartbeat task from a previous generation.
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
        }

        // Nothing to heartbeat without a member ID.
        if self.member_id.is_empty() || !self.uses_coordination {
            return;
        }

        let group_id = self.config.group_id.clone();
        let member_id = self.member_id.clone();
        let generation_id = self.generation_id;
        let interval = Duration::from_millis(self.config.heartbeat_interval_ms);
        let needs_rejoin = self.needs_rejoin.clone();
        let servers = self.config.bootstrap_servers.clone();
        let auth = self.config.auth.clone();

        self.heartbeat_handle = Some(tokio::spawn(async move {
            // Establish a dedicated connection for heartbeats.
            let mut hb_client = None;
            for server in &servers {
                match Client::connect(server).await {
                    Ok(mut c) => {
                        if let Some(ref auth) = auth {
                            if let Err(e) =
                                c.authenticate_scram(&auth.username, &auth.password).await
                            {
                                warn!(
                                    server = %server,
                                    error = %e,
                                    "Heartbeat connection auth failed, trying next server"
                                );
                                continue;
                            }
                        }
                        hb_client = Some(c);
                        break;
                    }
                    Err(e) => {
                        warn!(
                            server = %server,
                            error = %e,
                            "Heartbeat connection failed, trying next server"
                        );
                    }
                }
            }

            let Some(mut client) = hb_client else {
                warn!("Could not establish heartbeat connection to any server, signaling rejoin");
                needs_rejoin.store(true, Ordering::Release);
                return;
            };

            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // first tick is immediate, skip it

            loop {
                ticker.tick().await;
                match client.heartbeat(&group_id, generation_id, &member_id).await {
                    Ok(27) => {
                        // REBALANCE_IN_PROGRESS
                        info!(
                            group_id = %group_id,
                            "Background heartbeat: rebalance in progress, signaling rejoin"
                        );
                        needs_rejoin.store(true, Ordering::Release);
                    }
                    Ok(_) => {
                        // OK — heartbeat accepted
                    }
                    Err(e) => {
                        warn!(
                            group_id = %group_id,
                            error = %e,
                            "Background heartbeat failed, signaling rejoin"
                        );
                        needs_rejoin.store(true, Ordering::Release);
                        // Keep running — may be a transient error.
                        // If the generation is stale, the next poll() will
                        // rejoin and spawn a fresh heartbeat task.
                    }
                }
            }
        }));
    }

    /// Discover partition assignments for subscribed topics.
    ///
    /// When server-side group coordination is active (`uses_coordination`),
    /// performs the JoinGroup/SyncGroup protocol with the coordinator.
    /// Otherwise, queries metadata for each topic and assigns all partitions
    /// (or uses explicit assignments from config).
    async fn discover_assignments(&mut self) -> Result<()> {
        // Capture old assignments for rebalance callbacks (CLIENT-08).
        let old_tps: Vec<TopicPartition> = self
            .assignments
            .iter()
            .flat_map(|(t, ps)| {
                let arc: Arc<str> = Arc::from(t.as_str());
                ps.iter().map(move |&p| TopicPartition {
                    topic: arc.clone(),
                    partition: p,
                })
            })
            .collect();

        // Invoke on_partitions_revoked before changing assignments
        if !old_tps.is_empty() {
            if let Some(ref listener) = self.rebalance_listener {
                listener.on_partitions_revoked(&old_tps).await;
            }
        }

        if self.uses_coordination {
            self.discover_via_coordination().await?;
            // Spawn a dedicated background heartbeat task so heartbeats
            // are decoupled from the poll loop (CLIENT-02).
            self.spawn_heartbeat_task().await;
        } else {
            self.discover_via_metadata().await?;
        }

        // Clean up stale offsets for partitions we no longer own
        // (prevents unbounded memory growth across rebalances).
        let owned_keys: std::collections::HashSet<(Arc<str>, u32)> = self
            .assignments
            .iter()
            .flat_map(|(t, ps)| {
                let arc: Arc<str> = Arc::from(t.as_str());
                ps.iter().map(move |&p| (arc.clone(), p))
            })
            .collect();
        self.offsets.retain(|k, _| owned_keys.contains(k));

        // Initialize offsets from server (committed offsets)
        for (topic, partitions) in &self.assignments {
            for &partition in partitions {
                let key: (Arc<str>, u32) = (Arc::from(topic.as_str()), partition);
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

        // Rebuild cached flattened assignment list (one Arc<str> per topic,
        // cloned cheaply for each partition).
        self.assignment_list = self
            .assignments
            .iter()
            .flat_map(|(t, ps)| {
                let arc: Arc<str> = Arc::from(t.as_str());
                ps.iter().map(move |&p| (arc.clone(), p))
            })
            .collect();

        // Invoke on_partitions_assigned with the new assignment (CLIENT-08)
        if let Some(ref listener) = self.rebalance_listener {
            let new_tps: Vec<TopicPartition> = self
                .assignment_list
                .iter()
                .map(|(t, p)| TopicPartition {
                    topic: t.clone(),
                    partition: *p,
                })
                .collect();
            if !new_tps.is_empty() {
                listener.on_partitions_assigned(&new_tps).await;
            }
        }

        Ok(())
    }

    /// Discover assignments via server-side consumer group coordination.
    ///
    /// 1. **JoinGroup** — register with the coordinator; receive member ID,
    ///    generation ID, and the list of group members (if leader).
    /// 2. **SyncGroup** — the leader computes a range-based assignment for
    ///    all members and submits it; every member receives its own
    ///    partition list.
    async fn discover_via_coordination(&mut self) -> Result<()> {
        // Step 1: JoinGroup
        let (generation_id, _protocol_type, member_id, leader_id, members) = self
            .client
            .join_group(
                &self.config.group_id,
                &self.member_id,
                self.config.session_timeout_ms,
                self.config.rebalance_timeout_ms,
                "consumer",
                self.config.topics.clone(),
            )
            .await?;

        self.member_id = member_id.clone();
        self.generation_id = generation_id;
        self.is_leader = member_id == leader_id;

        info!(
            group_id = %self.config.group_id,
            member_id = %self.member_id,
            generation_id,
            is_leader = self.is_leader,
            member_count = members.len(),
            "Joined consumer group"
        );

        // Step 2: SyncGroup
        // Leader computes range-based assignments for all members.
        let group_assignments = if self.is_leader {
            self.compute_range_assignments(&members).await?
        } else {
            Vec::new()
        };

        let my_assignments = self
            .client
            .sync_group(
                &self.config.group_id,
                generation_id,
                &self.member_id,
                group_assignments,
            )
            .await?;

        // Apply returned assignments
        self.assignments.clear();
        for (topic, partitions) in my_assignments {
            debug!(
                topic = %topic,
                partitions = ?partitions,
                "Received partition assignment"
            );
            self.assignments.insert(topic, partitions);
        }

        self.last_heartbeat = Instant::now();

        Ok(())
    }

    /// Compute range-based partition assignments for all group members.
    ///
    /// For each topic, fetches the partition count from the server, then
    /// distributes partitions evenly across the members that subscribe
    /// to that topic.
    async fn compute_range_assignments(
        &mut self,
        members: &[(String, Vec<String>)],
    ) -> Result<Vec<(String, Vec<(String, Vec<u32>)>)>> {
        // Collect all unique topics across all members
        let mut all_topics: Vec<String> = members
            .iter()
            .flat_map(|(_, subs)| subs.iter().cloned())
            .collect();
        all_topics.sort();
        all_topics.dedup();

        // member_id → Vec<(topic, Vec<partition>)>
        let mut result_map: HashMap<String, Vec<(String, Vec<u32>)>> = members
            .iter()
            .map(|(mid, _)| (mid.clone(), Vec::new()))
            .collect();

        for topic in &all_topics {
            // Find members subscribed to this topic
            let mut subscribed: Vec<&str> = members
                .iter()
                .filter(|(_, subs)| subs.iter().any(|s| s == topic))
                .map(|(mid, _)| mid.as_str())
                .collect();
            subscribed.sort(); // deterministic ordering

            let partition_count = match self.client.get_metadata(topic.as_str()).await {
                Ok((_name, count)) => count,
                Err(e) => {
                    warn!(topic = %topic, error = %e, "Failed to get metadata for assignment");
                    continue;
                }
            };

            if subscribed.is_empty() || partition_count == 0 {
                continue;
            }

            // Range assignment: distribute partitions as evenly as possible
            let n_members = subscribed.len() as u32;
            let per_member = partition_count / n_members;
            let remainder = partition_count % n_members;

            let mut offset = 0u32;
            for (i, mid) in subscribed.iter().enumerate() {
                let extra = if (i as u32) < remainder { 1 } else { 0 };
                let count = per_member + extra;
                let partitions: Vec<u32> = (offset..offset + count).collect();
                offset += count;

                if let Some(entry) = result_map.get_mut(*mid) {
                    entry.push((topic.clone(), partitions));
                }
            }
        }

        Ok(result_map.into_iter().collect())
    }

    /// Discover assignments via metadata queries (static model).
    ///
    /// For each topic, queries the server for the partition count and
    /// assigns all partitions (or uses explicit assignments from config).
    async fn discover_via_metadata(&mut self) -> Result<()> {
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

        // If the background heartbeat or a fetch response signalled a
        // rebalance, rejoin the group before proceeding (CLIENT-02 / CLIENT-07).
        if self.needs_rejoin.load(Ordering::Acquire) && self.uses_coordination {
            info!(
                group_id = %self.config.group_id,
                "Rejoining group due to rebalance signal"
            );
            self.discover_assignments().await?;
            self.needs_rejoin.store(false, Ordering::Release);
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
            let fetches: Vec<(&str, u32, u64, u32, Option<u8>)> = self
                .assignment_list
                .iter()
                .map(|(topic, partition)| {
                    let key = (topic.clone(), *partition);
                    let offset = self.offsets.get(&key).copied().unwrap_or(0);
                    (
                        &**topic,
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
                        // Detect rebalance-related errors and set the needs_rejoin
                        // flag so the next poll triggers a group rejoin (CLIENT-07).
                        let err_str = e.to_string();
                        if err_str.contains("UNKNOWN_MEMBER_ID")
                            || err_str.contains("ILLEGAL_GENERATION")
                            || err_str.contains("REBALANCE_IN_PROGRESS")
                        {
                            warn!(
                                topic = %topic,
                                partition = partition,
                                error = %e,
                                "Rebalance signal in fetch response, will rejoin group"
                            );
                            self.needs_rejoin.store(true, Ordering::Release);
                        } else {
                            warn!(
                                topic = %topic,
                                partition = partition,
                                error = %e,
                                "Pipelined fetch error, skipping partition"
                            );
                        }
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

                // Cap long-poll timeout so it returns before the next
                // heartbeat is due, preventing session expiry during
                // long-polls (CLIENT-02).
                let max_wait = if self.uses_coordination {
                    self.config.max_poll_interval_ms.min(
                        self.config
                            .heartbeat_interval_ms
                            .saturating_sub(500)
                            .max(500),
                    )
                } else {
                    self.config.max_poll_interval_ms
                };

                let messages = self
                    .client
                    .consume_long_poll(
                        topic.to_string(),
                        *partition,
                        offset,
                        self.config.max_poll_records,
                        isolation_level,
                        max_wait,
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
            .map(|((topic, partition), offset)| (topic.to_string(), *partition, *offset))
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
            Err(errors.into_iter().next().expect("errors is non-empty"))
        }
    }

    /// Seek a specific partition to a given offset.
    ///
    /// The next `poll()` will fetch from this offset for the specified partition.
    pub fn seek(&mut self, topic: impl Into<String>, partition: u32, offset: u64) {
        let arc: Arc<str> = Arc::from(topic.into());
        self.offsets.insert((arc, partition), offset);
    }

    /// Seek all partitions of a topic to the beginning (offset 0).
    pub fn seek_to_beginning(&mut self, topic: &str) {
        if let Some(partitions) = self.assignments.get(topic) {
            let arc: Arc<str> = Arc::from(topic);
            for &p in partitions {
                self.offsets.insert((arc.clone(), p), 0);
            }
        }
    }

    /// Get the current offset position for a (topic, partition) pair.
    pub fn position(&self, topic: &str, partition: u32) -> Option<u64> {
        self.offsets.get(&(Arc::<str>::from(topic), partition)).copied()
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
        // Abort background heartbeat — it holds a stale connection and
        // generation. A new one will be spawned after rejoining.
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
        }

        let mut backoff = Duration::from_millis(self.config.reconnect_backoff_ms);
        let max_backoff = Duration::from_millis(self.config.reconnect_backoff_max_ms);
        let servers = &self.config.bootstrap_servers;

        if servers.is_empty() {
            return Err(Error::ConnectionError(
                "No bootstrap servers configured".to_string(),
            ));
        }

        for attempt in 1..=self.config.max_reconnect_attempts {
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

                    // Rejoin the consumer group on the new connection.
                    // The server has no state for this member on a new TCP
                    // connection, so we must re-discover assignments.
                    if self.uses_coordination {
                        if let Err(e) = self.discover_assignments().await {
                            warn!(error = %e, "Failed to rejoin group after reconnect");
                        }
                    }

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
            "Failed to reconnect to any of {:?} after {} attempts",
            servers, self.config.max_reconnect_attempts
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

    /// Close the consumer, committing final offsets and leaving the group.
    pub async fn close(mut self) -> Result<()> {
        // Stop background heartbeat first (CLIENT-02).
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
        }

        if self.config.auto_commit_interval.is_some() {
            self.commit().await?;
        }

        // Leave group if using server-side coordination
        if self.uses_coordination && !self.member_id.is_empty() {
            if let Err(e) = self
                .client
                .leave_group(&self.config.group_id, &self.member_id)
                .await
            {
                warn!(
                    error = %e,
                    group_id = %self.config.group_id,
                    member_id = %self.member_id,
                    "Failed to leave group gracefully"
                );
            } else {
                info!(
                    group_id = %self.config.group_id,
                    member_id = %self.member_id,
                    "Left consumer group"
                );
            }
        }

        info!(group_id = %self.config.group_id, "Consumer closed");
        Ok(())
    }
}
