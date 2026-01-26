//! # CDC Heartbeat
//!
//! Heartbeat mechanism for CDC connectors to maintain replication slot health.
//!
//! ## Why Heartbeats Matter
//!
//! - **PostgreSQL**: Without heartbeats, inactive replication slots accumulate WAL files
//! - **MySQL**: Keeps binlog position fresh during periods of no changes
//! - **Health Monitoring**: Detects stalled connections
//!
//! ## Features
//!
//! - Configurable heartbeat interval
//! - Optional heartbeat topic for downstream consumers
//! - Lag detection and alerting
//! - Automatic WAL advancement (PostgreSQL)
//!
//! ## Usage
//!
//! ```rust,ignore
//! use rivven_cdc::common::heartbeat::{Heartbeat, HeartbeatConfig};
//!
//! let heartbeat = Heartbeat::new(HeartbeatConfig::default());
//! heartbeat.start().await;
//!
//! // Check if heartbeat is healthy
//! if heartbeat.is_healthy() {
//!     println!("CDC is healthy, lag: {:?}", heartbeat.lag());
//! }
//! ```

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Heartbeat configuration.
#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    /// Heartbeat interval (default: 10 seconds)
    pub interval: Duration,
    /// Topic to publish heartbeat events (optional)
    pub topic: Option<String>,
    /// Maximum allowed lag before unhealthy (default: 5 minutes)
    pub max_lag: Duration,
    /// Whether to emit heartbeat events to topic
    pub emit_events: bool,
    /// Action prefix for heartbeat events
    pub action_prefix: String,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            topic: None,
            max_lag: Duration::from_secs(300), // 5 minutes
            emit_events: false,
            action_prefix: "__debezium-heartbeat".to_string(),
        }
    }
}

impl HeartbeatConfig {
    /// Create a new config builder.
    pub fn builder() -> HeartbeatConfigBuilder {
        HeartbeatConfigBuilder::default()
    }
}

/// Builder for HeartbeatConfig.
#[derive(Default)]
pub struct HeartbeatConfigBuilder {
    config: HeartbeatConfig,
}

impl HeartbeatConfigBuilder {
    /// Set heartbeat interval.
    pub fn interval(mut self, interval: Duration) -> Self {
        self.config.interval = interval;
        self
    }

    /// Set heartbeat topic.
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.config.topic = Some(topic.into());
        self
    }

    /// Set maximum lag.
    pub fn max_lag(mut self, max_lag: Duration) -> Self {
        self.config.max_lag = max_lag;
        self
    }

    /// Enable event emission.
    pub fn emit_events(mut self, enabled: bool) -> Self {
        self.config.emit_events = enabled;
        self
    }

    /// Set action prefix.
    pub fn action_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.config.action_prefix = prefix.into();
        self
    }

    /// Build the config.
    pub fn build(self) -> HeartbeatConfig {
        self.config
    }
}

/// Heartbeat event emitted to topic.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HeartbeatEvent {
    /// Timestamp when heartbeat was generated
    pub timestamp: i64,
    /// Source database identifier
    pub source: String,
    /// Current position/LSN
    pub position: String,
    /// Connector name
    pub connector: String,
    /// Sequence number
    pub sequence: u64,
}

impl HeartbeatEvent {
    /// Create a new heartbeat event.
    pub fn new(source: &str, position: &str, connector: &str, sequence: u64) -> Self {
        Self {
            timestamp: chrono::Utc::now().timestamp_millis(),
            source: source.to_string(),
            position: position.to_string(),
            connector: connector.to_string(),
            sequence,
        }
    }
}

/// Heartbeat statistics.
#[derive(Debug, Default)]
pub struct HeartbeatStats {
    /// Total heartbeats sent
    heartbeats_sent: AtomicU64,
    /// Last heartbeat timestamp (epoch millis)
    last_heartbeat_ts: AtomicI64,
    /// Last position update timestamp
    last_position_ts: AtomicI64,
    /// Current lag in milliseconds
    current_lag_ms: AtomicI64,
    /// Times heartbeat was missed
    missed_heartbeats: AtomicU64,
    /// Is currently healthy
    is_healthy: AtomicBool,
}

impl HeartbeatStats {
    /// Record a heartbeat.
    pub fn record_heartbeat(&self) {
        self.heartbeats_sent.fetch_add(1, Ordering::Relaxed);
        self.last_heartbeat_ts
            .store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
    }

    /// Record position update.
    pub fn record_position_update(&self, lag_ms: i64) {
        self.last_position_ts
            .store(chrono::Utc::now().timestamp_millis(), Ordering::Relaxed);
        self.current_lag_ms.store(lag_ms, Ordering::Relaxed);
    }

    /// Record missed heartbeat.
    pub fn record_missed(&self) {
        self.missed_heartbeats.fetch_add(1, Ordering::Relaxed);
    }

    /// Set health status.
    pub fn set_healthy(&self, healthy: bool) {
        self.is_healthy.store(healthy, Ordering::Relaxed);
    }

    /// Get total heartbeats sent.
    pub fn heartbeats_sent(&self) -> u64 {
        self.heartbeats_sent.load(Ordering::Relaxed)
    }

    /// Get last heartbeat timestamp.
    pub fn last_heartbeat_ts(&self) -> i64 {
        self.last_heartbeat_ts.load(Ordering::Relaxed)
    }

    /// Get current lag in milliseconds.
    pub fn current_lag_ms(&self) -> i64 {
        self.current_lag_ms.load(Ordering::Relaxed)
    }

    /// Get missed heartbeats count.
    pub fn missed_heartbeats(&self) -> u64 {
        self.missed_heartbeats.load(Ordering::Relaxed)
    }

    /// Check if healthy.
    pub fn is_healthy(&self) -> bool {
        self.is_healthy.load(Ordering::Relaxed)
    }
}

/// Position tracker for the heartbeat.
#[derive(Debug, Clone, Default)]
pub struct PositionInfo {
    /// Current LSN/position
    pub position: String,
    /// Database server ID
    pub server_id: String,
    /// Timestamp of position
    pub timestamp: i64,
}

/// Heartbeat manager for CDC connectors.
pub struct Heartbeat {
    config: HeartbeatConfig,
    stats: Arc<HeartbeatStats>,
    position: RwLock<PositionInfo>,
    connector_name: String,
    running: AtomicBool,
    sequence: AtomicU64,
    started_at: RwLock<Option<Instant>>,
}

impl Heartbeat {
    /// Create a new heartbeat manager.
    pub fn new(config: HeartbeatConfig, connector_name: impl Into<String>) -> Self {
        Self {
            config,
            stats: Arc::new(HeartbeatStats::default()),
            position: RwLock::new(PositionInfo::default()),
            connector_name: connector_name.into(),
            running: AtomicBool::new(false),
            sequence: AtomicU64::new(0),
            started_at: RwLock::new(None),
        }
    }

    /// Get statistics.
    pub fn stats(&self) -> &Arc<HeartbeatStats> {
        &self.stats
    }

    /// Check if heartbeat is healthy.
    pub fn is_healthy(&self) -> bool {
        self.stats.is_healthy()
    }

    /// Get current lag as Duration.
    pub fn lag(&self) -> Duration {
        Duration::from_millis(self.stats.current_lag_ms() as u64)
    }

    /// Check if running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Update the current position.
    pub async fn update_position(&self, position: &str, server_id: &str) {
        let now = chrono::Utc::now().timestamp_millis();
        let mut pos = self.position.write().await;

        // Calculate lag from previous position update
        let lag_ms = if pos.timestamp > 0 {
            now - pos.timestamp
        } else {
            0
        };

        pos.position = position.to_string();
        pos.server_id = server_id.to_string();
        pos.timestamp = now;
        drop(pos);

        self.stats.record_position_update(lag_ms);

        // Check health based on lag
        let healthy = lag_ms < self.config.max_lag.as_millis() as i64;
        self.stats.set_healthy(healthy);

        if !healthy {
            warn!(
                "CDC lag exceeds threshold: {}ms > {}ms",
                lag_ms,
                self.config.max_lag.as_millis()
            );
        }

        debug!("Position updated: {}, lag: {}ms", position, lag_ms);
    }

    /// Generate a heartbeat event.
    pub async fn beat(&self) -> HeartbeatEvent {
        let pos = self.position.read().await;
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);

        self.stats.record_heartbeat();
        self.stats.set_healthy(true);

        let event = HeartbeatEvent::new(&pos.server_id, &pos.position, &self.connector_name, seq);

        info!(
            "Heartbeat #{}: position={}, connector={}",
            seq, pos.position, self.connector_name
        );

        event
    }

    /// Start the heartbeat background task.
    /// Returns a channel receiver for heartbeat events.
    pub async fn start(&self) -> tokio::sync::mpsc::Receiver<HeartbeatEvent> {
        let (tx, rx) = tokio::sync::mpsc::channel(16);

        self.running.store(true, Ordering::Relaxed);
        *self.started_at.write().await = Some(Instant::now());
        self.stats.set_healthy(true);

        let interval = self.config.interval;
        let emit_events = self.config.emit_events;
        let stats = self.stats.clone();

        // Clone what we need for the spawned task
        let connector_name = self.connector_name.clone();
        let position = PositionInfo::default();
        let position_arc = Arc::new(RwLock::new(position));

        // Spawn background heartbeat task
        let stats_clone = stats.clone();
        let position_clone = position_arc.clone();

        // Use a shared atomic for running state
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            let sequence = AtomicU64::new(0);

            loop {
                interval_timer.tick().await;

                if !running_clone.load(Ordering::Relaxed) {
                    break;
                }

                let pos = position_clone.read().await;
                let seq = sequence.fetch_add(1, Ordering::Relaxed);

                stats_clone.record_heartbeat();

                let event =
                    HeartbeatEvent::new(&pos.server_id, &pos.position, &connector_name, seq);
                drop(pos);

                if emit_events && tx.send(event).await.is_err() {
                    // Receiver dropped
                    break;
                }
            }
        });

        rx
    }

    /// Stop the heartbeat.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
        info!("Heartbeat stopped for connector: {}", self.connector_name);
    }

    /// Get the heartbeat topic name.
    pub fn topic(&self) -> Option<&str> {
        self.config.topic.as_deref()
    }

    /// Get uptime since start.
    pub async fn uptime(&self) -> Option<Duration> {
        self.started_at.read().await.map(|s| s.elapsed())
    }

    /// Check and update health based on last activity.
    pub fn check_health(&self) -> bool {
        let last_ts = self.stats.last_heartbeat_ts();
        if last_ts == 0 {
            return true; // Not started yet
        }

        let now = chrono::Utc::now().timestamp_millis();
        let since_last = Duration::from_millis((now - last_ts) as u64);

        // Unhealthy if more than 3 intervals without heartbeat
        let healthy = since_last < self.config.interval * 3;
        self.stats.set_healthy(healthy);

        if !healthy {
            self.stats.record_missed();
            warn!(
                "Heartbeat missed: last was {:?} ago (threshold: {:?})",
                since_last,
                self.config.interval * 3
            );
        }

        healthy
    }
}

/// Heartbeat callback for integrating with CDC sources.
pub trait HeartbeatCallback: Send + Sync {
    /// Called to send a heartbeat request to the database.
    fn send_heartbeat(&self) -> impl std::future::Future<Output = Result<(), String>> + Send;

    /// Get current position for heartbeat event.
    fn get_position(&self) -> impl std::future::Future<Output = String> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heartbeat_config_default() {
        let config = HeartbeatConfig::default();
        assert_eq!(config.interval, Duration::from_secs(10));
        assert_eq!(config.max_lag, Duration::from_secs(300));
        assert!(config.topic.is_none());
        assert!(!config.emit_events);
    }

    #[test]
    fn test_heartbeat_config_builder() {
        let config = HeartbeatConfig::builder()
            .interval(Duration::from_secs(5))
            .topic("heartbeat-topic")
            .max_lag(Duration::from_secs(60))
            .emit_events(true)
            .action_prefix("custom-prefix")
            .build();

        assert_eq!(config.interval, Duration::from_secs(5));
        assert_eq!(config.topic, Some("heartbeat-topic".to_string()));
        assert_eq!(config.max_lag, Duration::from_secs(60));
        assert!(config.emit_events);
        assert_eq!(config.action_prefix, "custom-prefix");
    }

    #[test]
    fn test_heartbeat_event() {
        let event = HeartbeatEvent::new("pg-server", "0/16B3748", "my-connector", 1);

        assert_eq!(event.source, "pg-server");
        assert_eq!(event.position, "0/16B3748");
        assert_eq!(event.connector, "my-connector");
        assert_eq!(event.sequence, 1);
        assert!(event.timestamp > 0);
    }

    #[test]
    fn test_heartbeat_stats() {
        let stats = HeartbeatStats::default();

        assert_eq!(stats.heartbeats_sent(), 0);
        assert!(!stats.is_healthy());

        stats.set_healthy(true);
        assert!(stats.is_healthy());

        stats.record_heartbeat();
        assert_eq!(stats.heartbeats_sent(), 1);
        assert!(stats.last_heartbeat_ts() > 0);

        stats.record_position_update(100);
        assert_eq!(stats.current_lag_ms(), 100);

        stats.record_missed();
        assert_eq!(stats.missed_heartbeats(), 1);
    }

    #[tokio::test]
    async fn test_heartbeat_creation() {
        let config = HeartbeatConfig::default();
        let heartbeat = Heartbeat::new(config, "test-connector");

        assert!(!heartbeat.is_running());
        assert!(!heartbeat.is_healthy());
        assert_eq!(heartbeat.lag(), Duration::ZERO);
    }

    #[tokio::test]
    async fn test_heartbeat_position_update() {
        let config = HeartbeatConfig::builder()
            .max_lag(Duration::from_secs(60))
            .build();
        let heartbeat = Heartbeat::new(config, "test-connector");

        heartbeat.update_position("0/16B3748", "pg-server-1").await;

        assert!(heartbeat.is_healthy());

        let pos = heartbeat.position.read().await;
        assert_eq!(pos.position, "0/16B3748");
        assert_eq!(pos.server_id, "pg-server-1");
    }

    #[tokio::test]
    async fn test_heartbeat_beat() {
        let config = HeartbeatConfig::default();
        let heartbeat = Heartbeat::new(config, "test-connector");

        heartbeat.update_position("0/1234", "server-1").await;

        let event = heartbeat.beat().await;

        assert_eq!(event.connector, "test-connector");
        assert_eq!(event.position, "0/1234");
        assert_eq!(event.source, "server-1");
        assert_eq!(event.sequence, 0);
        assert_eq!(heartbeat.stats().heartbeats_sent(), 1);
    }

    #[tokio::test]
    async fn test_heartbeat_health_check() {
        let config = HeartbeatConfig::builder()
            .interval(Duration::from_millis(10))
            .build();
        let heartbeat = Heartbeat::new(config, "test-connector");

        // Before any heartbeat - healthy (not started)
        assert!(heartbeat.check_health());

        // After a beat
        heartbeat.beat().await;
        assert!(heartbeat.check_health());
        assert!(heartbeat.is_healthy());
    }

    #[tokio::test]
    async fn test_heartbeat_lag_detection() {
        let config = HeartbeatConfig::builder()
            .max_lag(Duration::from_millis(50))
            .build();
        let heartbeat = Heartbeat::new(config, "test-connector");

        // First update
        heartbeat.update_position("0/1000", "server").await;

        // Simulate lag by waiting
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Second update - should calculate lag
        heartbeat.update_position("0/2000", "server").await;

        // Lag should be small (around 10ms)
        let lag = heartbeat.stats().current_lag_ms();
        assert!(lag > 0);
        assert!(lag < 100); // Should be around 10ms
    }

    #[tokio::test]
    async fn test_heartbeat_stop() {
        let config = HeartbeatConfig::default();
        let heartbeat = Heartbeat::new(config, "test-connector");

        heartbeat.running.store(true, Ordering::Relaxed);
        assert!(heartbeat.is_running());

        heartbeat.stop();
        assert!(!heartbeat.is_running());
    }

    #[tokio::test]
    async fn test_heartbeat_topic() {
        let config = HeartbeatConfig::builder()
            .topic("my-heartbeat-topic")
            .build();
        let heartbeat = Heartbeat::new(config, "connector");

        assert_eq!(heartbeat.topic(), Some("my-heartbeat-topic"));

        let config_no_topic = HeartbeatConfig::default();
        let heartbeat_no_topic = Heartbeat::new(config_no_topic, "connector");
        assert_eq!(heartbeat_no_topic.topic(), None);
    }

    #[tokio::test]
    async fn test_heartbeat_event_serialization() {
        let event = HeartbeatEvent::new("pg", "0/ABCD", "conn", 42);

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"source\":\"pg\""));
        assert!(json.contains("\"position\":\"0/ABCD\""));
        assert!(json.contains("\"connector\":\"conn\""));
        assert!(json.contains("\"sequence\":42"));

        let parsed: HeartbeatEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.source, "pg");
        assert_eq!(parsed.sequence, 42);
    }

    #[tokio::test]
    async fn test_multiple_position_updates() {
        let config = HeartbeatConfig::default();
        let heartbeat = Heartbeat::new(config, "test");

        for i in 0..5 {
            heartbeat
                .update_position(&format!("0/{}", i * 1000), "server")
                .await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let pos = heartbeat.position.read().await;
        assert_eq!(pos.position, "0/4000");
    }

    #[test]
    fn test_position_info_default() {
        let pos = PositionInfo::default();
        assert!(pos.position.is_empty());
        assert!(pos.server_id.is_empty());
        assert_eq!(pos.timestamp, 0);
    }
}
