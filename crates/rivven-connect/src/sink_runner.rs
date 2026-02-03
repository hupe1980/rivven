//! Sink runner - consumes from broker topics, writes to external systems
//!
//! Features:
//! - Automatic reconnection with exponential backoff
//! - Consumer group offset tracking
//! - Status tracking for health checks
//! - Graceful shutdown with offset commit
//! - Configurable rate limiting for backpressure

use crate::broker_client::SharedBrokerClient;
use crate::config::{ConnectConfig, SinkConfig, StartOffset};
use crate::error::{ConnectError, ConnectorStatus, Result};
use crate::rate_limiter::{RateLimiterStats, TokenBucketRateLimiter};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error, info, warn};

/// Sink runner state
pub struct SinkRunner {
    name: String,
    config: SinkConfig,
    broker: SharedBrokerClient,
    status: RwLock<ConnectorStatus>,
    events_consumed: AtomicU64,
    events_written: AtomicU64,
    errors_count: AtomicU64,
    /// Current offset per topic/partition
    offsets: RwLock<HashMap<(String, u32), u64>>,
    /// Rate limiter for controlling throughput
    rate_limiter: TokenBucketRateLimiter,
}

// Methods for health monitoring - reserved for future integration with health.rs
#[allow(dead_code)]
impl SinkRunner {
    /// Get current status
    pub async fn status(&self) -> ConnectorStatus {
        *self.status.read().await
    }

    /// Get events written count
    pub fn events_written(&self) -> u64 {
        self.events_written.load(Ordering::Relaxed)
    }

    /// Get error count
    pub fn errors_count(&self) -> u64 {
        self.errors_count.load(Ordering::Relaxed)
    }

    /// Get rate limiter statistics
    pub fn rate_limiter_stats(&self) -> RateLimiterStats {
        self.rate_limiter.stats()
    }
}

impl SinkRunner {
    /// Create a new sink runner
    pub fn new(name: String, config: SinkConfig, broker: SharedBrokerClient) -> Self {
        // Create rate limiter from config
        let rate_limiter_config = config.rate_limit.to_rate_limiter_config();
        let rate_limiter = TokenBucketRateLimiter::new(rate_limiter_config);

        if config.rate_limit.is_enabled() {
            info!(
                "Sink '{}' rate limiting enabled: {} events/sec",
                name, config.rate_limit.events_per_second
            );
        }

        Self {
            name,
            config,
            broker,
            status: RwLock::new(ConnectorStatus::Starting),
            events_consumed: AtomicU64::new(0),
            events_written: AtomicU64::new(0),
            errors_count: AtomicU64::new(0),
            offsets: RwLock::new(HashMap::new()),
            rate_limiter,
        }
    }

    /// Get events consumed count
    pub fn events_consumed(&self) -> u64 {
        self.events_consumed.load(Ordering::Relaxed)
    }

    /// Run the sink connector
    pub async fn run(&self, mut shutdown_rx: broadcast::Receiver<()>) -> Result<()> {
        info!(
            "Sink '{}' starting, consuming from topics: {:?}",
            self.name, self.config.topics
        );

        // Initialize offsets for each topic
        self.initialize_offsets().await?;

        // Run connector-specific logic
        let result = match self.config.connector.as_str() {
            "stdout" => self.run_stdout_sink(&mut shutdown_rx).await,
            "s3" => self.run_s3_sink(&mut shutdown_rx).await,
            "http" => self.run_http_sink(&mut shutdown_rx).await,
            other => Err(ConnectError::config(format!(
                "Unknown sink connector type: {}",
                other
            ))),
        };

        // Log rate limiter stats on shutdown
        let stats = self.rate_limiter.stats();
        if stats.enabled && stats.events_throttled > 0 {
            info!(
                "Sink '{}' rate limiter stats: {} events throttled, {}ms total wait time",
                self.name, stats.events_throttled, stats.total_wait_ms
            );
        }

        // Commit final offsets on shutdown
        if let Err(e) = self.commit_all_offsets().await {
            warn!("Sink '{}' failed to commit final offsets: {}", self.name, e);
        }

        *self.status.write().await = match &result {
            Ok(()) => ConnectorStatus::Stopped,
            Err(e) if e.is_shutdown() => ConnectorStatus::Stopped,
            Err(_) => ConnectorStatus::Failed,
        };

        result
    }

    /// Initialize offsets for all topics
    async fn initialize_offsets(&self) -> Result<()> {
        let consumer_group = &self.config.consumer_group;
        let mut offsets = self.offsets.write().await;

        for topic in &self.config.topics {
            // For now, assume partition 0. Multi-partition support can be added.
            let partition = 0u32;
            let key = (topic.clone(), partition);

            // First try to get committed offset
            let committed = self
                .broker
                .get_offset(consumer_group, topic, partition)
                .await
                .ok()
                .flatten();

            let start_offset = match committed {
                Some(offset) => {
                    info!(
                        "Sink '{}' resuming topic '{}' from committed offset {}",
                        self.name, topic, offset
                    );
                    offset
                }
                None => {
                    // No committed offset, use start_offset config
                    let offset = self.resolve_start_offset(topic).await?;
                    info!(
                        "Sink '{}' starting topic '{}' from offset {} ({:?})",
                        self.name, topic, offset, self.config.start_offset
                    );
                    offset
                }
            };

            offsets.insert(key, start_offset);
        }

        Ok(())
    }

    /// Resolve the starting offset based on configuration
    async fn resolve_start_offset(&self, topic: &str) -> Result<u64> {
        match &self.config.start_offset {
            StartOffset::Earliest => Ok(0),
            StartOffset::Latest => {
                // Query the broker for the latest offset
                match self.broker.get_offset_bounds(topic, 0).await {
                    Ok((_earliest, latest)) => {
                        info!(
                            "Sink '{}': Starting from latest offset {} for topic '{}'",
                            self.name, latest, topic
                        );
                        Ok(latest)
                    }
                    Err(e) => {
                        warn!(
                            "Sink '{}': Failed to get latest offset for topic '{}': {}. Starting from 0.",
                            self.name, topic, e
                        );
                        Ok(0)
                    }
                }
            }
            StartOffset::Timestamp(ts_str) => {
                // Parse the ISO 8601 timestamp string
                let timestamp_ms = match chrono::DateTime::parse_from_rfc3339(ts_str) {
                    Ok(dt) => dt.timestamp_millis(),
                    Err(e) => {
                        warn!(
                            "Sink '{}': Invalid timestamp '{}': {}. Starting from earliest.",
                            self.name, ts_str, e
                        );
                        return Ok(0);
                    }
                };

                // Query the broker for the offset at or after the given timestamp
                match self
                    .broker
                    .get_offset_for_timestamp(topic, 0, timestamp_ms)
                    .await
                {
                    Ok(Some(offset)) => {
                        info!(
                            "Sink '{}': Starting from offset {} (timestamp >= {}) for topic '{}'",
                            self.name, offset, ts_str, topic
                        );
                        Ok(offset)
                    }
                    Ok(None) => {
                        // No messages found with timestamp >= ts, start from latest
                        warn!(
                            "Sink '{}': No messages found with timestamp >= {} for topic '{}'. Starting from latest.",
                            self.name, ts_str, topic
                        );
                        match self.broker.get_offset_bounds(topic, 0).await {
                            Ok((_earliest, latest)) => Ok(latest),
                            Err(_) => Ok(0),
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Sink '{}': Failed to get offset for timestamp {} on topic '{}': {}. Starting from earliest.",
                            self.name, ts_str, topic, e
                        );
                        Ok(0)
                    }
                }
            }
        }
    }

    /// Consume messages from a topic/partition
    async fn consume(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_messages: usize,
    ) -> Result<Vec<rivven_client::MessageData>> {
        self.broker
            .consume_batch(topic, partition, offset, max_messages)
            .await
    }

    /// Commit offset for a topic/partition
    async fn commit_offset(&self, topic: &str, partition: u32, offset: u64) -> Result<()> {
        self.broker
            .commit_offset(&self.config.consumer_group, topic, partition, offset)
            .await
    }

    /// Commit all current offsets
    async fn commit_all_offsets(&self) -> Result<()> {
        let offsets = self.offsets.read().await;
        for ((topic, partition), offset) in offsets.iter() {
            self.commit_offset(topic, *partition, *offset).await?;
        }
        Ok(())
    }

    /// Stdout sink - prints events to console (useful for debugging)
    async fn run_stdout_sink(&self, shutdown_rx: &mut broadcast::Receiver<()>) -> Result<()> {
        use super::prelude::*;
        use crate::connectors::stdout::{format_event, OutputFormat, StdoutSinkConfig};
        use serde::Deserialize;

        #[derive(Deserialize, Default)]
        struct LegacyStdoutConfig {
            #[serde(default = "default_format")]
            format: String,
            #[serde(default = "default_batch_size")]
            batch_size: usize,
            #[serde(default = "default_true")]
            include_metadata: bool,
            #[serde(default = "default_true")]
            include_timestamp: bool,
            #[serde(default = "default_true")]
            color: bool,
        }

        fn default_format() -> String {
            "pretty".to_string()
        }
        fn default_batch_size() -> usize {
            100
        }
        fn default_true() -> bool {
            true
        }

        let legacy_config: LegacyStdoutConfig =
            serde_yaml::from_value(self.config.config.clone()).unwrap_or_default();

        // Convert to SDK config
        let sdk_config = StdoutSinkConfig {
            format: match legacy_config.format.as_str() {
                "json" => OutputFormat::Json,
                "tsv" => OutputFormat::Tsv,
                "text" => OutputFormat::Text,
                _ => OutputFormat::Pretty,
            },
            include_metadata: legacy_config.include_metadata,
            include_timestamp: legacy_config.include_timestamp,
            color: legacy_config.color,
            rate_limit: 0,
            avro_schema: None,
            confluent_wire_format: false,
        };

        *self.status.write().await = ConnectorStatus::Running;
        let poll_interval = tokio::time::Duration::from_millis(100);

        info!("Sink '{}' ready, consuming events", self.name);

        loop {
            // Check for shutdown
            if shutdown_rx.try_recv().is_ok() {
                info!(
                    "Sink '{}' shutting down after {} events",
                    self.name,
                    self.events_consumed()
                );
                return Ok(());
            }

            let mut received_any = false;

            // Poll each topic
            for topic in &self.config.topics {
                let partition = 0u32;
                let key = (topic.clone(), partition);

                let current_offset = {
                    let offsets = self.offsets.read().await;
                    *offsets.get(&key).unwrap_or(&0)
                };

                match self
                    .consume(topic, partition, current_offset, legacy_config.batch_size)
                    .await
                {
                    Ok(messages) if !messages.is_empty() => {
                        received_any = true;
                        let mut max_offset = current_offset;
                        let batch_size = messages.len() as u64;

                        // Apply rate limiting before processing the batch
                        let wait_time = self.rate_limiter.acquire(batch_size).await;
                        if !wait_time.is_zero() {
                            debug!(
                                "Sink '{}' rate limited: waited {:?} for {} events",
                                self.name, wait_time, batch_size
                            );
                        }

                        for msg in messages {
                            self.events_consumed.fetch_add(1, Ordering::Relaxed);
                            max_offset = max_offset.max(msg.offset);

                            // Convert broker message to SDK SourceEvent for formatting
                            let event = if let Ok(json) =
                                serde_json::from_slice::<serde_json::Value>(&msg.value)
                            {
                                SourceEvent {
                                    event_type: SourceEventType::Record,
                                    stream: topic.clone(),
                                    namespace: None,
                                    timestamp: chrono::Utc::now(),
                                    data: json,
                                    metadata: Default::default(),
                                }
                            } else {
                                SourceEvent {
                                    event_type: SourceEventType::Record,
                                    stream: topic.clone(),
                                    namespace: None,
                                    timestamp: chrono::Utc::now(),
                                    data: serde_json::json!({
                                        "raw": String::from_utf8_lossy(&msg.value)
                                    }),
                                    metadata: Default::default(),
                                }
                            };

                            // Use SDK formatter (no Avro codec for legacy runner)
                            let output = format_event(&event, &sdk_config, None);
                            println!("{}", output);

                            self.events_written.fetch_add(1, Ordering::Relaxed);
                        }

                        // Update offset
                        let next_offset = max_offset + 1;
                        {
                            let mut offsets = self.offsets.write().await;
                            offsets.insert(key.clone(), next_offset);
                        }

                        // Commit offset periodically (every 100 messages)
                        if self.events_consumed().is_multiple_of(100) {
                            if let Err(e) = self.commit_offset(topic, partition, next_offset).await
                            {
                                warn!("Sink '{}' failed to commit offset: {}", self.name, e);
                            }
                        }
                    }
                    Ok(_) => {
                        // No messages
                    }
                    Err(e) => {
                        self.errors_count.fetch_add(1, Ordering::Relaxed);
                        error!("Sink '{}' consume error: {}", self.name, e);
                        *self.status.write().await = ConnectorStatus::Unhealthy;
                    }
                }
            }

            if !received_any {
                tokio::time::sleep(poll_interval).await;
            }
        }
    }

    /// S3 sink - writes events to S3 in batches
    async fn run_s3_sink(&self, _shutdown_rx: &mut broadcast::Receiver<()>) -> Result<()> {
        Err(ConnectError::sink(
            &self.name,
            "S3 sink not yet implemented",
        ))
    }

    /// HTTP sink - posts events to HTTP endpoint
    async fn run_http_sink(&self, _shutdown_rx: &mut broadcast::Receiver<()>) -> Result<()> {
        Err(ConnectError::sink(
            &self.name,
            "HTTP sink not yet implemented",
        ))
    }
}

/// Shared sink runner for health checks
#[allow(dead_code)]
pub type SharedSinkRunner = Arc<SinkRunner>;

/// Run a sink connector
pub async fn run_sink(
    name: &str,
    sink_config: &SinkConfig,
    config: &ConnectConfig,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> Result<()> {
    use crate::broker_client::BrokerClient;

    // Create broker client with retry config
    let broker = Arc::new(BrokerClient::new(
        config.broker.clone(),
        config.settings.retry.clone(),
    ));

    // Connect to broker
    broker.connect().await?;

    // Create and run sink
    let runner = SinkRunner::new(name.to_string(), sink_config.clone(), broker.clone());

    runner.run(shutdown_rx.resubscribe()).await
}
