//! Sink runner - consumes from broker topics, writes to external systems
//!
//! Features:
//! - Automatic reconnection with exponential backoff
//! - Consumer group offset tracking
//! - Status tracking for health checks
//! - Graceful shutdown with offset commit
//! - Configurable rate limiting for backpressure

use crate::broker_client::SharedBrokerClient;
use crate::config::{ConnectConfig, SinkConfig, StartOffset, TransformStepConfig};
use crate::connectors::{create_sink_registry, SinkRegistry};
use crate::error::{ConnectError, ConnectorStatus, Result};
use crate::rate_limiter::{RateLimiterStats, TokenBucketRateLimiter};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error, info, warn};

/// Convert a broker message timestamp (milliseconds since epoch) into a
/// `DateTime<Utc>`. Falls back to `Utc::now()` when the value cannot be
/// represented as a valid datetime.
fn timestamp_from_millis(ts_ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ts_ms).unwrap_or_else(Utc::now)
}

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
    /// Transform steps applied to consumed data before writing
    #[allow(dead_code)]
    transforms: Vec<TransformStepConfig>,
    /// Sink connector registry for dynamic dispatch (registry-based connectors)
    sink_registry: Arc<SinkRegistry>,
}

// Methods for health monitoring
#[allow(dead_code)] // Wired into health endpoint in future
impl SinkRunner {
    /// Get current status
    pub(crate) async fn status(&self) -> ConnectorStatus {
        *self.status.read().await
    }

    /// Get events written count
    pub(crate) fn events_written(&self) -> u64 {
        self.events_written.load(Ordering::Relaxed)
    }

    /// Get error count
    pub(crate) fn errors_count(&self) -> u64 {
        self.errors_count.load(Ordering::Relaxed)
    }

    /// Get rate limiter statistics
    pub(crate) fn rate_limiter_stats(&self) -> RateLimiterStats {
        self.rate_limiter.stats()
    }
}

impl SinkRunner {
    /// Create a new sink runner
    pub fn new(
        name: String,
        config: SinkConfig,
        broker: SharedBrokerClient,
        sink_registry: Arc<SinkRegistry>,
    ) -> Self {
        // Create rate limiter from config
        let rate_limiter_config = config.rate_limit.to_rate_limiter_config();
        let rate_limiter = TokenBucketRateLimiter::new(rate_limiter_config);

        if config.rate_limit.is_enabled() {
            info!(
                "Sink '{}' rate limiting enabled: {} events/sec",
                name, config.rate_limit.events_per_second
            );
        }

        let transforms = config.transforms.clone();

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
            transforms,
            sink_registry,
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

        // Run connector-specific logic — registry-first dispatch.
        // All registered connectors (stdout, s3, http-webhook, etc.) are resolved
        // through the SinkRegistry. Only the built-in HTTP batch sink falls back to
        // an inline implementation when not in the registry.
        let connector = self.config.connector.as_str();
        let result = if let Some(factory) = self.sink_registry.get(connector) {
            self.run_registry_sink(factory, &mut shutdown_rx).await
        } else if connector == "http" {
            // Built-in HTTP batch sink (no factory yet — reqwest always available)
            self.run_http_sink(&mut shutdown_rx).await
        } else {
            let available: Vec<&str> = self.sink_registry.list().iter().map(|(n, _)| *n).collect();
            Err(ConnectError::config(format!(
                "Unknown sink connector type: '{}'. Available: {:?}",
                connector, available
            )))
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

    /// Initialize offsets for all topics and their partitions
    async fn initialize_offsets(&self) -> Result<()> {
        let consumer_group = &self.config.consumer_group;
        let mut offsets = self.offsets.write().await;

        for topic in &self.config.topics {
            // Query broker for partition count
            let partition_count = self
                .broker
                .get_topic_partition_count(topic)
                .await
                .unwrap_or_else(|_| {
                    warn!(
                        "Sink '{}' could not determine partition count for '{}', defaulting to 1",
                        self.name, topic
                    );
                    1
                });

            for partition in 0..partition_count {
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
                            "Sink '{}' resuming topic '{}' partition {} from committed offset {}",
                            self.name, topic, partition, offset
                        );
                        offset
                    }
                    None => {
                        // No committed offset, use start_offset config
                        let offset = self.resolve_start_offset(topic).await?;
                        info!(
                            "Sink '{}' starting topic '{}' partition {} from offset {} ({:?})",
                            self.name, topic, partition, offset, self.config.start_offset
                        );
                        offset
                    }
                };

                offsets.insert(key, start_offset);
            }
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
                            Err(e) => {
                                warn!(
                                    "Sink '{}': Failed to get offset bounds for topic '{}': {}. Starting from 0.",
                                    self.name, topic, e
                                );
                                Ok(0)
                            }
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
    #[allow(dead_code)]
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

        let legacy_config: LegacyStdoutConfig = serde_yaml::from_value(self.config.config.clone())
            .map_err(|e| {
                ConnectError::Config(format!("Failed to parse stdout sink config: {}", e))
            })?;

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
        // Adaptive backoff — start at 1ms, double up to 100ms when idle, reset on data
        const BACKOFF_MIN_MS: u64 = 1;
        const BACKOFF_MAX_MS: u64 = 100;
        let mut backoff_ms: u64 = BACKOFF_MIN_MS;

        // Track consecutive errors. When this reaches the
        // threshold the connector transitions to Failed and stops, rather
        // than silently retrying forever and potentially losing data.
        const MAX_CONSECUTIVE_ERRORS: u64 = 50;
        let mut consecutive_errors: u64 = 0;

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

            // Poll each topic-partition (offsets contains all topic-partition pairs)
            let topic_partitions: Vec<(String, u32)> = {
                let offsets = self.offsets.read().await;
                offsets.keys().cloned().collect()
            };

            for (topic, partition) in &topic_partitions {
                let key = (topic.clone(), *partition);

                let current_offset = {
                    let offsets = self.offsets.read().await;
                    *offsets.get(&key).unwrap_or(&0)
                };

                match self
                    .consume(topic, *partition, current_offset, legacy_config.batch_size)
                    .await
                {
                    Ok(messages) if !messages.is_empty() => {
                        received_any = true;
                        consecutive_errors = 0; // reset on success
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
                            // Preserve the original broker message timestamp
                            let event_ts = timestamp_from_millis(msg.timestamp);

                            let event = if let Ok(json) =
                                serde_json::from_slice::<serde_json::Value>(&msg.value)
                            {
                                SourceEvent {
                                    event_type: SourceEventType::Record,
                                    stream: topic.clone(),
                                    namespace: None,
                                    timestamp: event_ts,
                                    data: json,
                                    metadata: Default::default(),
                                }
                            } else {
                                SourceEvent {
                                    event_type: SourceEventType::Record,
                                    stream: topic.clone(),
                                    namespace: None,
                                    timestamp: event_ts,
                                    data: serde_json::json!({
                                        "raw": String::from_utf8_lossy(&msg.value)
                                    }),
                                    metadata: Default::default(),
                                }
                            };

                            // use structured logging instead of stdout
                            let output = format_event(&event, &sdk_config, None);
                            info!(target: "rivven_connect::sink", "{}", output);

                            self.events_written.fetch_add(1, Ordering::Relaxed);
                        }

                        // Update offset
                        let next_offset = max_offset + 1;
                        {
                            let mut offsets = self.offsets.write().await;
                            offsets.insert(key.clone(), next_offset);
                        }

                        // recover health status on successful consume
                        {
                            let status = self.status.read().await;
                            if *status == ConnectorStatus::Unhealthy {
                                drop(status);
                                *self.status.write().await = ConnectorStatus::Running;
                                info!("Sink '{}' recovered to Running", self.name);
                            }
                        }
                    }
                    Ok(_) => {
                        // No messages
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        self.errors_count.fetch_add(1, Ordering::Relaxed);
                        error!(
                            "Sink '{}' consume error ({}/{}): {}",
                            self.name, consecutive_errors, MAX_CONSECUTIVE_ERRORS, e
                        );
                        *self.status.write().await = ConnectorStatus::Unhealthy;

                        // Fail after too many consecutive errors
                        // instead of silently retrying forever.
                        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                            error!(
                                "Sink '{}' exceeded {} consecutive errors — transitioning to Failed",
                                self.name, MAX_CONSECUTIVE_ERRORS
                            );
                            *self.status.write().await = ConnectorStatus::Failed;
                            return Err(ConnectError::sink(
                                &self.name,
                                format!(
                                    "Too many consecutive consume errors ({}). Last error: {}",
                                    consecutive_errors, e
                                ),
                            ));
                        }
                    }
                }
            }

            // Commit ALL dirty partition offsets periodically (every 100 events)
            if received_any && self.events_consumed().is_multiple_of(100) {
                if let Err(e) = self.commit_all_offsets().await {
                    warn!("Sink '{}' failed to commit offsets: {}", self.name, e);
                }
            }

            if !received_any {
                tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(BACKOFF_MAX_MS);
            } else {
                backoff_ms = BACKOFF_MIN_MS;
            }
        }
    }

    /// S3 sink - writes events to S3 in batches
    ///
    /// Requires the `s3` feature with `object_store` dependency.
    #[allow(dead_code)]
    async fn run_s3_sink(&self, _shutdown_rx: &mut broadcast::Receiver<()>) -> Result<()> {
        Err(ConnectError::sink(
            &self.name,
            "S3 sink requires the 's3' feature to be enabled. \
             Compile with `--features s3` and configure 'bucket', 'region', and 'prefix' in connector config.",
        ))
    }

    /// HTTP sink - posts events to an HTTP endpoint
    ///
    /// Config fields:
    /// - `url` (required): Target URL to POST events to
    /// - `batch_size` (optional, default 100): Events per request
    /// - `content_type` (optional, default "application/json"): Content-Type header
    /// - `headers` (optional): Additional HTTP headers as key-value pairs
    async fn run_http_sink(&self, shutdown_rx: &mut broadcast::Receiver<()>) -> Result<()> {
        let url: String = self
            .config
            .config
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectError::config("HTTP sink requires 'url' in connector config"))?
            .to_string();

        // validate URL scheme to prevent SSRF / data exfiltration
        let parsed_url = url::Url::parse(&url)
            .map_err(|e| ConnectError::config(format!("Invalid HTTP sink URL '{}': {}", url, e)))?;
        match parsed_url.scheme() {
            "http" | "https" => {}
            scheme => {
                return Err(ConnectError::config(format!(
                    "HTTP sink URL must use http or https scheme, got '{}'",
                    scheme
                )));
            }
        }

        let batch_size: usize = self
            .config
            .config
            .get("batch_size")
            .and_then(|v| v.as_u64())
            .unwrap_or(100) as usize;

        let content_type: String = self
            .config
            .config
            .get("content_type")
            .and_then(|v| v.as_str())
            .unwrap_or("application/json")
            .to_string();

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| {
                ConnectError::sink(&self.name, format!("Failed to create HTTP client: {}", e))
            })?;

        *self.status.write().await = ConnectorStatus::Running;
        info!(
            "HTTP sink '{}' started, posting to {} (batch_size={})",
            self.name, url, batch_size
        );

        let poll_interval = std::time::Duration::from_millis(
            self.config
                .config
                .get("poll_interval_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(100),
        );

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("HTTP sink '{}' shutting down", self.name);
                    return Ok(());
                }
                _ = tokio::time::sleep(poll_interval) => {}
            }

            // Iterate all topic-partition pairs
            let topic_partitions: Vec<(String, u32)> = {
                let offsets = self.offsets.read().await;
                offsets.keys().cloned().collect()
            };

            for (ref topic, partition) in &topic_partitions {
                let key = (topic.clone(), *partition);
                let current_offset = {
                    let offsets = self.offsets.read().await;
                    offsets.get(&key).copied().unwrap_or(0)
                };

                match self
                    .consume(topic, *partition, current_offset, batch_size)
                    .await
                {
                    Ok(messages) if !messages.is_empty() => {
                        let count = messages.len();
                        let last_offset =
                            messages.last().map(|m| m.offset).unwrap_or(current_offset);

                        // Serialize batch as JSON array
                        let payload: Vec<serde_json::Value> = messages
                            .iter()
                            .map(|m| {
                                serde_json::json!({
                                    "topic": topic,
                                    "partition": partition,
                                    "offset": m.offset,
                                    "key": m.key.as_ref().map(|k| String::from_utf8_lossy(k).to_string()),
                                    "value": String::from_utf8_lossy(&m.value).to_string(),
                                    "timestamp": m.timestamp,
                                })
                            })
                            .collect();

                        let body = serde_json::to_vec(&payload).map_err(|e| {
                            ConnectError::sink(&self.name, format!("JSON serialize error: {}", e))
                        })?;

                        // POST to endpoint
                        let resp = client
                            .post(&url)
                            .header("Content-Type", &content_type)
                            .body(body)
                            .send()
                            .await
                            .map_err(|e| {
                                ConnectError::sink(&self.name, format!("HTTP POST failed: {}", e))
                            })?;

                        if !resp.status().is_success() {
                            warn!(
                                "HTTP sink '{}' received status {} from {}",
                                self.name,
                                resp.status(),
                                url
                            );
                        }

                        // Update offset
                        self.offsets.write().await.insert(key, last_offset + 1);
                        self.events_consumed
                            .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);

                        // Periodic offset commit
                        if self
                            .events_consumed
                            .load(std::sync::atomic::Ordering::Relaxed)
                            .is_multiple_of(1000)
                        {
                            if let Err(e) = self.commit_all_offsets().await {
                                warn!("Sink '{}': periodic offset commit failed: {}", self.name, e);
                            }
                        }
                    }
                    Ok(_) => {} // No messages
                    Err(e) => {
                        warn!("HTTP sink '{}' consume error: {}", self.name, e);
                    }
                }
            }
        }
    }

    /// Run a registry-based sink connector via AnySink trait dispatch
    ///
    /// This enables any connector registered in the SinkRegistry to be used
    /// without hardcoded match arms. The factory creates a type-erased AnySink
    /// which receives events consumed from the broker topics.
    async fn run_registry_sink(
        &self,
        factory: &Arc<dyn crate::connectors::SinkFactory>,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<()> {
        use super::prelude::*;
        use futures::StreamExt;

        let sink = factory.create()?;

        // Check connectivity first
        let check = sink.check_raw(&self.config.config).await?;
        if !check.success {
            return Err(ConnectError::sink(
                &self.name,
                format!(
                    "Connectivity check failed: {}",
                    check.message.unwrap_or_default()
                ),
            ));
        }

        *self.status.write().await = ConnectorStatus::Running;
        info!(
            "Sink '{}' started via registry (connector: {})",
            self.name, self.config.connector
        );

        // Adaptive backoff — start at 1ms, double up to 100ms when idle, reset on data
        const BACKOFF_MIN_MS: u64 = 1;
        const BACKOFF_MAX_MS: u64 = 100;
        let mut backoff_ms: u64 = BACKOFF_MIN_MS;
        // Track consecutive errors for consistent failure behavior
        const MAX_CONSECUTIVE_ERRORS: u64 = 50;
        let mut consecutive_errors: u64 = 0;
        let batch_size: usize = self
            .config
            .config
            .get("batch_size")
            .and_then(|v| v.as_u64())
            .unwrap_or(100) as usize;

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

            let topic_partitions: Vec<(String, u32)> = {
                let offsets = self.offsets.read().await;
                offsets.keys().cloned().collect()
            };

            for (topic, partition) in &topic_partitions {
                let key = (topic.clone(), *partition);

                let current_offset = {
                    let offsets = self.offsets.read().await;
                    *offsets.get(&key).unwrap_or(&0)
                };

                match self
                    .consume(topic, *partition, current_offset, batch_size)
                    .await
                {
                    Ok(messages) if !messages.is_empty() => {
                        received_any = true;
                        let mut max_offset = current_offset;
                        let count = messages.len() as u64;

                        // Apply rate limiting
                        let wait_time = self.rate_limiter.acquire(count).await;
                        if !wait_time.is_zero() {
                            debug!(
                                "Sink '{}' rate limited: waited {:?} for {} events",
                                self.name, wait_time, count
                            );
                        }

                        // Convert consumed messages into a SourceEvent stream for the AnySink
                        let events: Vec<SourceEvent> = messages
                            .iter()
                            .map(|msg| {
                                max_offset = max_offset.max(msg.offset);
                                self.events_consumed.fetch_add(1, Ordering::Relaxed);

                                let data = serde_json::from_slice::<serde_json::Value>(&msg.value)
                                    .unwrap_or_else(|_| {
                                        serde_json::json!({
                                            "raw": String::from_utf8_lossy(&msg.value)
                                        })
                                    });

                                SourceEvent {
                                    event_type: SourceEventType::Record,
                                    stream: topic.clone(),
                                    namespace: None,
                                    timestamp: timestamp_from_millis(msg.timestamp),
                                    data,
                                    metadata: Default::default(),
                                }
                            })
                            .collect();

                        let event_stream = futures::stream::iter(events).boxed();

                        match sink.write_raw(&self.config.config, event_stream).await {
                            Ok(result) => {
                                self.events_written
                                    .fetch_add(result.records_written, Ordering::Relaxed);
                            }
                            Err(e) => {
                                consecutive_errors += 1;
                                error!(
                                    "Sink '{}' write error ({}/{}): {}",
                                    self.name, consecutive_errors, MAX_CONSECUTIVE_ERRORS, e
                                );
                                self.errors_count.fetch_add(1, Ordering::Relaxed);
                                *self.status.write().await = ConnectorStatus::Unhealthy;
                                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                                    *self.status.write().await = ConnectorStatus::Failed;
                                    return Err(ConnectError::sink(
                                        &self.name,
                                        format!(
                                            "Too many consecutive write errors ({}). Last: {}",
                                            consecutive_errors, e
                                        ),
                                    ));
                                }
                            }
                        }

                        // Update offset
                        let next_offset = max_offset + 1;
                        {
                            let mut offsets = self.offsets.write().await;
                            offsets.insert(key.clone(), next_offset);
                        }

                        // recover health status on successful consume
                        consecutive_errors = 0;
                        {
                            let status = self.status.read().await;
                            if *status == ConnectorStatus::Unhealthy {
                                drop(status);
                                *self.status.write().await = ConnectorStatus::Running;
                                info!("Sink '{}' recovered to Running", self.name);
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        consecutive_errors += 1;
                        self.errors_count.fetch_add(1, Ordering::Relaxed);
                        error!(
                            "Sink '{}' consume error ({}/{}): {}",
                            self.name, consecutive_errors, MAX_CONSECUTIVE_ERRORS, e
                        );
                        *self.status.write().await = ConnectorStatus::Unhealthy;
                        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                            *self.status.write().await = ConnectorStatus::Failed;
                            return Err(ConnectError::sink(
                                &self.name,
                                format!(
                                    "Too many consecutive consume errors ({}). Last: {}",
                                    consecutive_errors, e
                                ),
                            ));
                        }
                    }
                }
            }

            // Commit ALL dirty partition offsets periodically (every 100 events)
            if received_any && self.events_consumed().is_multiple_of(100) {
                if let Err(e) = self.commit_all_offsets().await {
                    warn!("Sink '{}' failed to commit offsets: {}", self.name, e);
                }
            }

            if !received_any {
                tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(BACKOFF_MAX_MS);
            } else {
                backoff_ms = BACKOFF_MIN_MS;
            }
        }
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

    // Build the full sink registry for dynamic connector dispatch
    let sink_registry = Arc::new(create_sink_registry());

    // Create and run sink
    let runner = SinkRunner::new(
        name.to_string(),
        sink_config.clone(),
        broker.clone(),
        sink_registry,
    );

    runner.run(shutdown_rx.resubscribe()).await
}
