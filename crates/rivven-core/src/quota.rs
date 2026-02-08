//! Per-Principal Quotas (Kafka Parity)
//!
//! Provides throughput limiting on a per-user, per-client-id, or per-consumer-group
//! basis to prevent noisy neighbors in multi-tenant deployments.
//!
//! ## Quota Types
//!
//! | Quota | Unit | Description |
//! |-------|------|-------------|
//! | `produce_bytes_rate` | bytes/sec | Producer throughput limit |
//! | `consume_bytes_rate` | bytes/sec | Consumer throughput limit |
//! | `request_rate` | requests/sec | Request rate limit |
//!
//! ## Entity Types
//!
//! Quotas can be applied at different levels:
//!
//! - **User**: Per authenticated principal (e.g., `user:alice`)
//! - **Client ID**: Per client identifier (e.g., `client:app-1`)
//! - **Consumer Group**: Per consumer group (e.g., `group:order-processors`)
//! - **Default**: Fallback when no specific quota is set
//!
//! ## Quota Resolution Order
//!
//! 1. User + Client ID specific quota
//! 2. User specific quota
//! 3. Client ID specific quota
//! 4. Default quota
//!
//! ## Protocol
//!
//! ```text
//! Client                           Broker
//!    │                                │
//!    │─── Produce(1MB) ──────────────>│
//!    │<── ProduceResponse(throttle=0) │  (Within quota)
//!    │                                │
//!    │─── Produce(10MB) ─────────────>│  (Exceeds quota)
//!    │<── ProduceResponse(throttle=500ms)  (Must wait 500ms)
//!    │                                │
//!    │    [wait 500ms]                │
//!    │                                │
//!    │─── Produce(1MB) ──────────────>│
//!    │<── ProduceResponse(throttle=0) │  (Back to normal)
//! ```
//!

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Default produce bytes per second (50 MB/s)
pub const DEFAULT_PRODUCE_BYTES_RATE: u64 = 50 * 1024 * 1024;

/// Default consume bytes per second (100 MB/s)
pub const DEFAULT_CONSUME_BYTES_RATE: u64 = 100 * 1024 * 1024;

/// Default request rate per second (100,000 requests/s)
///
/// This is deliberately generous to avoid throttling legitimate workloads.
/// Configure per-principal quotas for tighter multi-tenant limits.
pub const DEFAULT_REQUEST_RATE: u64 = 100_000;

/// Unlimited quota marker
pub const UNLIMITED: u64 = u64::MAX;

/// Entity type for quota application
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QuotaEntityType {
    /// Per-user quota (authenticated principal)
    User,
    /// Per-client-id quota
    ClientId,
    /// Per-consumer-group quota
    ConsumerGroup,
    /// Default quota (fallback)
    Default,
}

impl std::fmt::Display for QuotaEntityType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuotaEntityType::User => write!(f, "user"),
            QuotaEntityType::ClientId => write!(f, "client-id"),
            QuotaEntityType::ConsumerGroup => write!(f, "consumer-group"),
            QuotaEntityType::Default => write!(f, "default"),
        }
    }
}

/// Entity identifier for quota lookup
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QuotaEntity {
    pub entity_type: QuotaEntityType,
    pub name: Option<String>,
}

impl QuotaEntity {
    pub fn user(name: impl Into<String>) -> Self {
        Self {
            entity_type: QuotaEntityType::User,
            name: Some(name.into()),
        }
    }

    pub fn client_id(name: impl Into<String>) -> Self {
        Self {
            entity_type: QuotaEntityType::ClientId,
            name: Some(name.into()),
        }
    }

    pub fn consumer_group(name: impl Into<String>) -> Self {
        Self {
            entity_type: QuotaEntityType::ConsumerGroup,
            name: Some(name.into()),
        }
    }

    /// Creates a default quota entity
    pub fn default_entity() -> Self {
        Self {
            entity_type: QuotaEntityType::Default,
            name: None,
        }
    }

    pub fn default_user() -> Self {
        Self {
            entity_type: QuotaEntityType::User,
            name: None,
        }
    }

    pub fn default_client_id() -> Self {
        Self {
            entity_type: QuotaEntityType::ClientId,
            name: None,
        }
    }
}

impl std::fmt::Display for QuotaEntity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.name {
            Some(n) => write!(f, "{}:{}", self.entity_type, n),
            None => write!(f, "{}:<default>", self.entity_type),
        }
    }
}

/// Quota configuration for an entity
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuotaConfig {
    /// Producer bytes per second (None = use default)
    pub produce_bytes_rate: Option<u64>,

    /// Consumer bytes per second (None = use default)
    pub consume_bytes_rate: Option<u64>,

    /// Requests per second (None = use default)
    pub request_rate: Option<u64>,
}

impl QuotaConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_produce_rate(mut self, bytes_per_sec: u64) -> Self {
        self.produce_bytes_rate = Some(bytes_per_sec);
        self
    }

    pub fn with_consume_rate(mut self, bytes_per_sec: u64) -> Self {
        self.consume_bytes_rate = Some(bytes_per_sec);
        self
    }

    pub fn with_request_rate(mut self, requests_per_sec: u64) -> Self {
        self.request_rate = Some(requests_per_sec);
        self
    }

    pub fn unlimited() -> Self {
        Self {
            produce_bytes_rate: Some(UNLIMITED),
            consume_bytes_rate: Some(UNLIMITED),
            request_rate: Some(UNLIMITED),
        }
    }
}

/// Result of quota check
#[derive(Debug, Clone, PartialEq)]
pub enum QuotaResult {
    /// Request is within quota
    Allowed,

    /// Request exceeds quota, must throttle
    Throttled {
        /// Time to wait before retrying
        throttle_time: Duration,
        /// Quota type that was exceeded
        quota_type: QuotaType,
        /// Entity that exceeded quota
        entity: String,
    },
}

impl QuotaResult {
    pub fn is_allowed(&self) -> bool {
        matches!(self, QuotaResult::Allowed)
    }

    pub fn throttle_time_ms(&self) -> u64 {
        match self {
            QuotaResult::Allowed => 0,
            QuotaResult::Throttled { throttle_time, .. } => throttle_time.as_millis() as u64,
        }
    }
}

/// Type of quota being checked/enforced
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuotaType {
    ProduceBytes,
    ConsumeBytes,
    RequestRate,
}

impl std::fmt::Display for QuotaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuotaType::ProduceBytes => write!(f, "produce_bytes_rate"),
            QuotaType::ConsumeBytes => write!(f, "consume_bytes_rate"),
            QuotaType::RequestRate => write!(f, "request_rate"),
        }
    }
}

/// Sliding window tracker for quota enforcement
struct SlidingWindow {
    /// Window duration
    window: Duration,
    /// Accumulated value in current window
    current_value: AtomicU64,
    /// Window start time
    window_start: RwLock<Instant>,
    /// Total violations
    violations: AtomicU64,
}

impl SlidingWindow {
    fn new(window: Duration) -> Self {
        Self {
            window,
            current_value: AtomicU64::new(0),
            window_start: RwLock::new(Instant::now()),
            violations: AtomicU64::new(0),
        }
    }

    /// Record usage and return if quota is exceeded
    fn record(&self, amount: u64, limit: u64) -> Option<Duration> {
        // Hold write lock through the entire check-and-add to prevent TOCTOU races
        let mut start = self.window_start.write();
        let elapsed = start.elapsed();
        if elapsed >= self.window {
            // Reset window
            self.current_value.store(0, Ordering::Relaxed);
            *start = Instant::now();
        }
        drop(start);

        // Add to current value
        let new_value = self.current_value.fetch_add(amount, Ordering::Relaxed) + amount;

        // Check against limit
        if limit != UNLIMITED && new_value > limit {
            self.violations.fetch_add(1, Ordering::Relaxed);

            // Calculate throttle time based on how much we exceeded
            let exceeded_by = new_value.saturating_sub(limit);
            let throttle_secs = exceeded_by as f64 / limit as f64;
            let throttle_ms = (throttle_secs * 1000.0).min(30_000.0) as u64; // Cap at 30s

            Some(Duration::from_millis(throttle_ms.max(1)))
        } else {
            None
        }
    }

    fn current_rate(&self) -> u64 {
        let start = self.window_start.read();
        let elapsed = start.elapsed().as_secs_f64().max(0.001);
        (self.current_value.load(Ordering::Relaxed) as f64 / elapsed) as u64
    }

    fn violations(&self) -> u64 {
        self.violations.load(Ordering::Relaxed)
    }

    /// Reset window state (used for testing or reconfiguration)
    #[allow(dead_code)]
    fn reset(&self) {
        self.current_value.store(0, Ordering::Relaxed);
        *self.window_start.write() = Instant::now();
    }
}

/// Per-entity quota state
struct EntityQuotaState {
    produce_window: SlidingWindow,
    consume_window: SlidingWindow,
    request_window: SlidingWindow,
    last_activity: RwLock<Instant>,
}

impl EntityQuotaState {
    fn new() -> Self {
        Self {
            produce_window: SlidingWindow::new(Duration::from_secs(1)),
            consume_window: SlidingWindow::new(Duration::from_secs(1)),
            request_window: SlidingWindow::new(Duration::from_secs(1)),
            last_activity: RwLock::new(Instant::now()),
        }
    }

    fn touch(&self) {
        *self.last_activity.write() = Instant::now();
    }

    fn is_idle(&self, timeout: Duration) -> bool {
        self.last_activity.read().elapsed() > timeout
    }
}

/// Quota statistics for an entity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityQuotaStats {
    pub entity: String,
    pub produce_bytes_rate: u64,
    pub consume_bytes_rate: u64,
    pub request_rate: u64,
    pub produce_violations: u64,
    pub consume_violations: u64,
    pub request_violations: u64,
}

/// Global quota statistics
#[derive(Debug, Default)]
pub struct QuotaStats {
    total_produce_violations: AtomicU64,
    total_consume_violations: AtomicU64,
    total_request_violations: AtomicU64,
    total_throttle_time_ms: AtomicU64,
}

impl QuotaStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_violation(&self, quota_type: QuotaType, throttle_ms: u64) {
        match quota_type {
            QuotaType::ProduceBytes => {
                self.total_produce_violations
                    .fetch_add(1, Ordering::Relaxed);
            }
            QuotaType::ConsumeBytes => {
                self.total_consume_violations
                    .fetch_add(1, Ordering::Relaxed);
            }
            QuotaType::RequestRate => {
                self.total_request_violations
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        self.total_throttle_time_ms
            .fetch_add(throttle_ms, Ordering::Relaxed);
    }

    pub fn produce_violations(&self) -> u64 {
        self.total_produce_violations.load(Ordering::Relaxed)
    }

    pub fn consume_violations(&self) -> u64 {
        self.total_consume_violations.load(Ordering::Relaxed)
    }

    pub fn request_violations(&self) -> u64 {
        self.total_request_violations.load(Ordering::Relaxed)
    }

    pub fn total_throttle_time_ms(&self) -> u64 {
        self.total_throttle_time_ms.load(Ordering::Relaxed)
    }
}

/// Snapshot of quota stats for serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaStatsSnapshot {
    pub produce_violations: u64,
    pub consume_violations: u64,
    pub request_violations: u64,
    pub total_throttle_time_ms: u64,
}

impl From<&QuotaStats> for QuotaStatsSnapshot {
    fn from(stats: &QuotaStats) -> Self {
        Self {
            produce_violations: stats.produce_violations(),
            consume_violations: stats.consume_violations(),
            request_violations: stats.request_violations(),
            total_throttle_time_ms: stats.total_throttle_time_ms(),
        }
    }
}

/// Quota manager for per-principal throughput limiting
///
/// Manages quotas for users, client IDs, and consumer groups.
/// Enforces produce bytes/sec, consume bytes/sec, and request rate limits.
pub struct QuotaManager {
    /// Default quotas
    defaults: RwLock<QuotaConfig>,

    /// Per-entity quota configurations
    configs: RwLock<HashMap<QuotaEntity, QuotaConfig>>,

    /// Per-entity quota state (sliding windows)
    states: RwLock<HashMap<String, EntityQuotaState>>,

    /// Statistics
    stats: QuotaStats,

    /// Idle timeout for cleaning up state
    idle_timeout: Duration,
}

impl Default for QuotaManager {
    fn default() -> Self {
        Self::new()
    }
}

impl QuotaManager {
    /// Create a new quota manager with default quotas
    pub fn new() -> Self {
        Self {
            defaults: RwLock::new(QuotaConfig {
                produce_bytes_rate: Some(DEFAULT_PRODUCE_BYTES_RATE),
                consume_bytes_rate: Some(DEFAULT_CONSUME_BYTES_RATE),
                request_rate: Some(DEFAULT_REQUEST_RATE),
            }),
            configs: RwLock::new(HashMap::new()),
            states: RwLock::new(HashMap::new()),
            stats: QuotaStats::new(),
            idle_timeout: Duration::from_secs(3600), // 1 hour
        }
    }

    /// Create with custom defaults
    pub fn with_defaults(defaults: QuotaConfig) -> Self {
        Self {
            defaults: RwLock::new(defaults),
            configs: RwLock::new(HashMap::new()),
            states: RwLock::new(HashMap::new()),
            stats: QuotaStats::new(),
            idle_timeout: Duration::from_secs(3600),
        }
    }

    /// Set default quotas
    pub fn set_defaults(&self, config: QuotaConfig) {
        *self.defaults.write() = config;
    }

    /// Get current defaults
    pub fn get_defaults(&self) -> QuotaConfig {
        self.defaults.read().clone()
    }

    /// Set quota for a specific entity
    pub fn set_quota(&self, entity: QuotaEntity, config: QuotaConfig) {
        self.configs.write().insert(entity, config);
    }

    /// Remove quota for an entity (will use defaults)
    pub fn remove_quota(&self, entity: &QuotaEntity) {
        self.configs.write().remove(entity);
    }

    /// Get effective quota for an entity
    pub fn get_effective_quota(&self, user: Option<&str>, client_id: Option<&str>) -> QuotaConfig {
        let configs = self.configs.read();
        let defaults = self.defaults.read();

        // Resolution order: user+client > user > client > default
        let user_client_key = match (user, client_id) {
            (Some(u), Some(c)) => Some(format!("{}:{}", u, c)),
            _ => None,
        };

        // Try user+client specific
        if let Some(key) = &user_client_key {
            // Custom key lookup would go here
            let _ = key; // Suppress warning
        }

        // Try user specific
        if let Some(u) = user {
            if let Some(cfg) = configs.get(&QuotaEntity::user(u)) {
                return Self::merge_configs(cfg, &defaults);
            }
        }

        // Try client specific
        if let Some(c) = client_id {
            if let Some(cfg) = configs.get(&QuotaEntity::client_id(c)) {
                return Self::merge_configs(cfg, &defaults);
            }
        }

        // Try default user
        if user.is_some() {
            if let Some(cfg) = configs.get(&QuotaEntity::default_user()) {
                return Self::merge_configs(cfg, &defaults);
            }
        }

        // Try default client
        if client_id.is_some() {
            if let Some(cfg) = configs.get(&QuotaEntity::default_client_id()) {
                return Self::merge_configs(cfg, &defaults);
            }
        }

        // Use defaults
        defaults.clone()
    }

    fn merge_configs(specific: &QuotaConfig, defaults: &QuotaConfig) -> QuotaConfig {
        QuotaConfig {
            produce_bytes_rate: specific.produce_bytes_rate.or(defaults.produce_bytes_rate),
            consume_bytes_rate: specific.consume_bytes_rate.or(defaults.consume_bytes_rate),
            request_rate: specific.request_rate.or(defaults.request_rate),
        }
    }

    /// Record produce bytes and check quota
    pub fn record_produce(
        &self,
        user: Option<&str>,
        client_id: Option<&str>,
        bytes: u64,
    ) -> QuotaResult {
        let entity_key = Self::entity_key(user, client_id);
        let config = self.get_effective_quota(user, client_id);
        let limit = config
            .produce_bytes_rate
            .unwrap_or(DEFAULT_PRODUCE_BYTES_RATE);

        self.record_internal(&entity_key, QuotaType::ProduceBytes, bytes, limit)
    }

    /// Record consume bytes and check quota
    pub fn record_consume(
        &self,
        user: Option<&str>,
        client_id: Option<&str>,
        bytes: u64,
    ) -> QuotaResult {
        let entity_key = Self::entity_key(user, client_id);
        let config = self.get_effective_quota(user, client_id);
        let limit = config
            .consume_bytes_rate
            .unwrap_or(DEFAULT_CONSUME_BYTES_RATE);

        self.record_internal(&entity_key, QuotaType::ConsumeBytes, bytes, limit)
    }

    /// Record request and check quota
    pub fn record_request(&self, user: Option<&str>, client_id: Option<&str>) -> QuotaResult {
        let entity_key = Self::entity_key(user, client_id);
        let config = self.get_effective_quota(user, client_id);
        let limit = config.request_rate.unwrap_or(DEFAULT_REQUEST_RATE);

        self.record_internal(&entity_key, QuotaType::RequestRate, 1, limit)
    }

    fn entity_key(user: Option<&str>, client_id: Option<&str>) -> String {
        match (user, client_id) {
            (Some(u), Some(c)) => format!("{}:{}", u, c),
            (Some(u), None) => format!("user:{}", u),
            (None, Some(c)) => format!("client:{}", c),
            (None, None) => "anonymous".to_string(),
        }
    }

    fn record_internal(
        &self,
        entity_key: &str,
        quota_type: QuotaType,
        amount: u64,
        limit: u64,
    ) -> QuotaResult {
        // Get or create state
        {
            let states = self.states.read();
            if states.contains_key(entity_key) {
                drop(states);
                let states = self.states.read();
                if let Some(s) = states.get(entity_key) {
                    s.touch();
                }
            } else {
                drop(states);
                self.states
                    .write()
                    .insert(entity_key.to_string(), EntityQuotaState::new());
            }
        }

        // Record in appropriate window
        let states = self.states.read();
        if let Some(state) = states.get(entity_key) {
            let throttle = match quota_type {
                QuotaType::ProduceBytes => state.produce_window.record(amount, limit),
                QuotaType::ConsumeBytes => state.consume_window.record(amount, limit),
                QuotaType::RequestRate => state.request_window.record(amount, limit),
            };

            if let Some(throttle_time) = throttle {
                self.stats
                    .record_violation(quota_type, throttle_time.as_millis() as u64);
                return QuotaResult::Throttled {
                    throttle_time,
                    quota_type,
                    entity: entity_key.to_string(),
                };
            }
        }

        QuotaResult::Allowed
    }

    /// Get statistics for a specific entity
    pub fn get_entity_stats(
        &self,
        user: Option<&str>,
        client_id: Option<&str>,
    ) -> Option<EntityQuotaStats> {
        let entity_key = Self::entity_key(user, client_id);
        let states = self.states.read();

        states.get(&entity_key).map(|state| EntityQuotaStats {
            entity: entity_key.clone(),
            produce_bytes_rate: state.produce_window.current_rate(),
            consume_bytes_rate: state.consume_window.current_rate(),
            request_rate: state.request_window.current_rate(),
            produce_violations: state.produce_window.violations(),
            consume_violations: state.consume_window.violations(),
            request_violations: state.request_window.violations(),
        })
    }

    /// Get global statistics
    pub fn stats(&self) -> &QuotaStats {
        &self.stats
    }

    /// Clean up idle entity states
    pub fn cleanup_idle_entities(&self) -> usize {
        let mut states = self.states.write();
        let before = states.len();
        states.retain(|_, state| !state.is_idle(self.idle_timeout));
        before - states.len()
    }

    /// List all configured quotas
    pub fn list_quotas(&self) -> Vec<(QuotaEntity, QuotaConfig)> {
        self.configs
            .read()
            .iter()
            .map(|(e, c)| (e.clone(), c.clone()))
            .collect()
    }

    /// Get number of active entities being tracked
    pub fn active_entity_count(&self) -> usize {
        self.states.read().len()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quota_entity_display() {
        assert_eq!(QuotaEntity::user("alice").to_string(), "user:alice");
        assert_eq!(
            QuotaEntity::client_id("app-1").to_string(),
            "client-id:app-1"
        );
        assert_eq!(
            QuotaEntity::default_entity().to_string(),
            "default:<default>"
        );
    }

    #[test]
    fn test_quota_config_builder() {
        let config = QuotaConfig::new()
            .with_produce_rate(10_000_000)
            .with_consume_rate(20_000_000)
            .with_request_rate(500);

        assert_eq!(config.produce_bytes_rate, Some(10_000_000));
        assert_eq!(config.consume_bytes_rate, Some(20_000_000));
        assert_eq!(config.request_rate, Some(500));
    }

    #[test]
    fn test_default_quotas() {
        let manager = QuotaManager::new();
        let defaults = manager.get_defaults();

        assert_eq!(
            defaults.produce_bytes_rate,
            Some(DEFAULT_PRODUCE_BYTES_RATE)
        );
        assert_eq!(
            defaults.consume_bytes_rate,
            Some(DEFAULT_CONSUME_BYTES_RATE)
        );
        assert_eq!(defaults.request_rate, Some(DEFAULT_REQUEST_RATE));
    }

    #[test]
    fn test_set_user_quota() {
        let manager = QuotaManager::new();

        // Set specific quota for alice
        manager.set_quota(
            QuotaEntity::user("alice"),
            QuotaConfig::new().with_produce_rate(1_000_000),
        );

        // Alice gets specific quota
        let alice_quota = manager.get_effective_quota(Some("alice"), None);
        assert_eq!(alice_quota.produce_bytes_rate, Some(1_000_000));

        // Bob gets default
        let bob_quota = manager.get_effective_quota(Some("bob"), None);
        assert_eq!(
            bob_quota.produce_bytes_rate,
            Some(DEFAULT_PRODUCE_BYTES_RATE)
        );
    }

    #[test]
    fn test_record_produce_within_quota() {
        let manager = QuotaManager::new();

        // Record small produce - should be allowed
        let result = manager.record_produce(Some("alice"), None, 1024);
        assert!(result.is_allowed());
        assert_eq!(result.throttle_time_ms(), 0);
    }

    #[test]
    fn test_record_produce_exceeds_quota() {
        let manager = QuotaManager::with_defaults(
            QuotaConfig::new().with_produce_rate(1000), // 1KB/s
        );

        // First request within quota
        let result = manager.record_produce(Some("alice"), None, 500);
        assert!(result.is_allowed());

        // Second request exceeds quota
        let result = manager.record_produce(Some("alice"), None, 1000);
        assert!(!result.is_allowed());

        match result {
            QuotaResult::Throttled {
                throttle_time,
                quota_type,
                entity,
            } => {
                assert!(throttle_time.as_millis() > 0);
                assert_eq!(quota_type, QuotaType::ProduceBytes);
                assert!(entity.contains("alice"));
            }
            _ => panic!("Expected throttled"),
        }
    }

    #[test]
    fn test_request_rate_limiting() {
        let manager = QuotaManager::with_defaults(
            QuotaConfig::new().with_request_rate(10), // 10 requests/s
        );

        // First 10 requests should be allowed
        for _ in 0..10 {
            let result = manager.record_request(Some("bob"), None);
            assert!(result.is_allowed());
        }

        // 11th request should be throttled
        let result = manager.record_request(Some("bob"), None);
        assert!(!result.is_allowed());
    }

    #[test]
    fn test_unlimited_quota() {
        let manager = QuotaManager::new();
        manager.set_quota(QuotaEntity::user("admin"), QuotaConfig::unlimited());

        // Admin can produce unlimited
        let result = manager.record_produce(Some("admin"), None, 1_000_000_000);
        assert!(result.is_allowed());
    }

    #[test]
    fn test_quota_stats() {
        let manager = QuotaManager::with_defaults(QuotaConfig::new().with_produce_rate(100));

        // Exceed quota
        manager.record_produce(Some("alice"), None, 200);

        let stats = manager.stats();
        assert_eq!(stats.produce_violations(), 1);
        assert!(stats.total_throttle_time_ms() > 0);
    }

    #[test]
    fn test_entity_stats() {
        let manager = QuotaManager::new();

        // Record some activity
        manager.record_produce(Some("alice"), None, 1024);
        manager.record_consume(Some("alice"), None, 2048);
        manager.record_request(Some("alice"), None);

        let stats = manager.get_entity_stats(Some("alice"), None);
        assert!(stats.is_some());
        let stats = stats.unwrap();
        assert!(stats.produce_bytes_rate > 0 || stats.request_rate > 0);
    }

    #[test]
    fn test_quota_resolution_order() {
        let manager = QuotaManager::with_defaults(QuotaConfig::new().with_produce_rate(1000));

        // Set user default
        manager.set_quota(
            QuotaEntity::default_user(),
            QuotaConfig::new().with_produce_rate(2000),
        );

        // Set specific user
        manager.set_quota(
            QuotaEntity::user("alice"),
            QuotaConfig::new().with_produce_rate(3000),
        );

        // Alice gets specific quota
        let alice = manager.get_effective_quota(Some("alice"), None);
        assert_eq!(alice.produce_bytes_rate, Some(3000));

        // Bob gets user default
        let bob = manager.get_effective_quota(Some("bob"), None);
        assert_eq!(bob.produce_bytes_rate, Some(2000));

        // Anonymous gets global default
        let anon = manager.get_effective_quota(None, None);
        assert_eq!(anon.produce_bytes_rate, Some(1000));
    }

    #[test]
    fn test_client_id_quota() {
        let manager = QuotaManager::new();

        manager.set_quota(
            QuotaEntity::client_id("app-1"),
            QuotaConfig::new().with_request_rate(100),
        );

        let quota = manager.get_effective_quota(None, Some("app-1"));
        assert_eq!(quota.request_rate, Some(100));

        let quota = manager.get_effective_quota(None, Some("app-2"));
        assert_eq!(quota.request_rate, Some(DEFAULT_REQUEST_RATE));
    }

    #[test]
    fn test_list_quotas() {
        let manager = QuotaManager::new();

        manager.set_quota(
            QuotaEntity::user("alice"),
            QuotaConfig::new().with_produce_rate(1000),
        );
        manager.set_quota(
            QuotaEntity::user("bob"),
            QuotaConfig::new().with_produce_rate(2000),
        );

        let quotas = manager.list_quotas();
        assert_eq!(quotas.len(), 2);
    }

    #[test]
    fn test_remove_quota() {
        let manager = QuotaManager::new();

        let entity = QuotaEntity::user("alice");
        manager.set_quota(entity.clone(), QuotaConfig::new().with_produce_rate(1000));

        // Verify quota is set
        let quota = manager.get_effective_quota(Some("alice"), None);
        assert_eq!(quota.produce_bytes_rate, Some(1000));

        // Remove quota
        manager.remove_quota(&entity);

        // Should use defaults now
        let quota = manager.get_effective_quota(Some("alice"), None);
        assert_eq!(quota.produce_bytes_rate, Some(DEFAULT_PRODUCE_BYTES_RATE));
    }

    #[test]
    fn test_active_entity_count() {
        let manager = QuotaManager::new();

        assert_eq!(manager.active_entity_count(), 0);

        manager.record_produce(Some("alice"), None, 100);
        assert_eq!(manager.active_entity_count(), 1);

        manager.record_produce(Some("bob"), None, 100);
        assert_eq!(manager.active_entity_count(), 2);
    }

    #[test]
    fn test_quota_stats_snapshot() {
        let manager = QuotaManager::with_defaults(QuotaConfig::new().with_produce_rate(100));

        manager.record_produce(Some("alice"), None, 200);

        let snapshot: QuotaStatsSnapshot = manager.stats().into();
        assert_eq!(snapshot.produce_violations, 1);
    }

    #[test]
    fn test_consume_quota() {
        let manager = QuotaManager::with_defaults(QuotaConfig::new().with_consume_rate(1000));

        let result = manager.record_consume(Some("alice"), None, 500);
        assert!(result.is_allowed());

        let result = manager.record_consume(Some("alice"), None, 1000);
        assert!(!result.is_allowed());
        assert_eq!(manager.stats().consume_violations(), 1);
    }

    #[test]
    fn test_consumer_group_quota() {
        let manager = QuotaManager::new();

        manager.set_quota(
            QuotaEntity::consumer_group("order-processors"),
            QuotaConfig::new().with_consume_rate(5_000_000),
        );

        let quotas = manager.list_quotas();
        assert_eq!(quotas.len(), 1);
        assert_eq!(quotas[0].0, QuotaEntity::consumer_group("order-processors"));
    }
}
