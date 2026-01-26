//! # CDC Health Monitoring
//!
//! Health checks and auto-recovery for CDC sources.
//!
//! ## Features
//!
//! - **Connection Monitoring**: Detect disconnections
//! - **Lag Monitoring**: Track replication lag
//! - **Auto-Recovery**: Automatic reconnection with backoff
//! - **Health Checks**: Liveness and readiness probes
//!
//! ## Usage
//!
//! ```ignore
//! use rivven_cdc::common::{HealthMonitor, HealthConfig};
//!
//! let config = HealthConfig::default();
//! let monitor = HealthMonitor::new(config);
//!
//! // Register health check
//! monitor.register_check("database", || async {
//!     // Check database connection
//!     Ok(())
//! });
//!
//! // Start monitoring
//! monitor.start().await;
//!
//! // Check health
//! let status = monitor.status().await;
//! println!("Healthy: {}", status.is_healthy);
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, info, warn};

/// Health check result.
#[derive(Debug, Clone)]
pub enum HealthCheckResult {
    /// Check passed
    Healthy,
    /// Check passed with warning
    Degraded(String),
    /// Check failed
    Unhealthy(String),
}

impl HealthCheckResult {
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthCheckResult::Healthy)
    }

    pub fn is_degraded(&self) -> bool {
        matches!(self, HealthCheckResult::Degraded(_))
    }

    pub fn is_unhealthy(&self) -> bool {
        matches!(self, HealthCheckResult::Unhealthy(_))
    }
}

/// Type alias for async health check functions.
pub type HealthCheckFn =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = HealthCheckResult> + Send>> + Send + Sync>;

/// Configuration for health monitoring.
#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Interval between health checks
    pub check_interval: Duration,
    /// Maximum allowed replication lag
    pub max_lag_ms: u64,
    /// Number of failed checks before unhealthy
    pub failure_threshold: u32,
    /// Number of successful checks to recover
    pub success_threshold: u32,
    /// Timeout for individual health checks
    pub check_timeout: Duration,
    /// Enable auto-recovery
    pub auto_recovery: bool,
    /// Initial recovery delay
    pub recovery_delay: Duration,
    /// Maximum recovery delay
    pub max_recovery_delay: Duration,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(10),
            max_lag_ms: 30_000, // 30 seconds
            failure_threshold: 3,
            success_threshold: 2,
            check_timeout: Duration::from_secs(5),
            auto_recovery: true,
            recovery_delay: Duration::from_secs(1),
            max_recovery_delay: Duration::from_secs(60),
        }
    }
}

impl HealthConfig {
    pub fn strict() -> Self {
        Self {
            check_interval: Duration::from_secs(5),
            max_lag_ms: 10_000,
            failure_threshold: 2,
            success_threshold: 3,
            ..Default::default()
        }
    }

    pub fn relaxed() -> Self {
        Self {
            check_interval: Duration::from_secs(30),
            max_lag_ms: 60_000,
            failure_threshold: 5,
            success_threshold: 1,
            ..Default::default()
        }
    }
}

/// Overall health status.
#[derive(Debug, Clone)]
pub struct HealthStatus {
    /// Whether the system is healthy
    pub is_healthy: bool,
    /// Whether the system is ready to serve
    pub is_ready: bool,
    /// Individual check results
    pub checks: HashMap<String, CheckStatus>,
    /// Current replication lag
    pub lag_ms: u64,
    /// Last successful check time
    pub last_healthy: Option<Instant>,
    /// Number of consecutive failures
    pub consecutive_failures: u32,
    /// Number of consecutive successes
    pub consecutive_successes: u32,
    /// Uptime
    pub uptime: Duration,
}

/// Status of an individual health check.
#[derive(Debug, Clone)]
pub struct CheckStatus {
    pub name: String,
    pub result: HealthCheckResult,
    pub last_check: Instant,
    pub duration: Duration,
}

/// Health monitor that tracks system health and coordinates recovery.
pub struct HealthMonitor {
    config: HealthConfig,
    checks: RwLock<HashMap<String, HealthCheckFn>>,
    state: Arc<MonitorState>,
    start_time: Instant,
}

struct MonitorState {
    healthy: AtomicBool,
    ready: AtomicBool,
    lag_ms: AtomicU64,
    consecutive_failures: AtomicU64,
    consecutive_successes: AtomicU64,
    last_healthy: RwLock<Option<Instant>>,
    check_results: RwLock<HashMap<String, CheckStatus>>,
    #[allow(dead_code)] // Reserved for recovery coordinator integration
    recovery_in_progress: AtomicBool,
}

impl HealthMonitor {
    /// Create a new health monitor.
    pub fn new(config: HealthConfig) -> Self {
        Self {
            config,
            checks: RwLock::new(HashMap::new()),
            state: Arc::new(MonitorState {
                healthy: AtomicBool::new(false),
                ready: AtomicBool::new(false),
                lag_ms: AtomicU64::new(0),
                consecutive_failures: AtomicU64::new(0),
                consecutive_successes: AtomicU64::new(0),
                last_healthy: RwLock::new(None),
                check_results: RwLock::new(HashMap::new()),
                recovery_in_progress: AtomicBool::new(false),
            }),
            start_time: Instant::now(),
        }
    }

    /// Register a health check.
    pub async fn register_check<F, Fut>(&self, name: &str, check: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HealthCheckResult> + Send + 'static,
    {
        let mut checks = self.checks.write().await;
        checks.insert(name.to_string(), Box::new(move || Box::pin(check())));
        debug!("Registered health check: {}", name);
    }

    /// Unregister a health check.
    pub async fn unregister_check(&self, name: &str) {
        let mut checks = self.checks.write().await;
        checks.remove(name);
    }

    /// Run all health checks once.
    pub async fn check(&self) -> HealthStatus {
        let checks = self.checks.read().await;
        let mut results = HashMap::new();
        let mut all_healthy = true;

        for (name, check_fn) in checks.iter() {
            let start = Instant::now();

            let result = tokio::time::timeout(self.config.check_timeout, check_fn())
                .await
                .unwrap_or_else(|_| HealthCheckResult::Unhealthy("Timeout".to_string()));

            let duration = start.elapsed();

            if !result.is_healthy() {
                all_healthy = false;
                if result.is_unhealthy() {
                    warn!("Health check '{}' failed: {:?}", name, result);
                }
            }

            let status = CheckStatus {
                name: name.clone(),
                result: result.clone(),
                last_check: Instant::now(),
                duration,
            };

            results.insert(name.clone(), status);
        }

        // Check lag threshold
        let lag_ms = self.state.lag_ms.load(Ordering::Relaxed);
        if lag_ms > self.config.max_lag_ms {
            all_healthy = false;
            results.insert(
                "replication_lag".to_string(),
                CheckStatus {
                    name: "replication_lag".to_string(),
                    result: HealthCheckResult::Unhealthy(format!(
                        "Lag {}ms exceeds threshold {}ms",
                        lag_ms, self.config.max_lag_ms
                    )),
                    last_check: Instant::now(),
                    duration: Duration::ZERO,
                },
            );
        }

        // Update state
        if all_healthy {
            self.state
                .consecutive_successes
                .fetch_add(1, Ordering::Relaxed);
            self.state.consecutive_failures.store(0, Ordering::Relaxed);

            let successes = self.state.consecutive_successes.load(Ordering::Relaxed);
            if successes >= self.config.success_threshold as u64 {
                self.state.healthy.store(true, Ordering::Relaxed);
                self.state.ready.store(true, Ordering::Relaxed);
                *self.state.last_healthy.write().await = Some(Instant::now());
            }
        } else {
            self.state
                .consecutive_failures
                .fetch_add(1, Ordering::Relaxed);
            self.state.consecutive_successes.store(0, Ordering::Relaxed);

            let failures = self.state.consecutive_failures.load(Ordering::Relaxed);
            if failures >= self.config.failure_threshold as u64 {
                self.state.healthy.store(false, Ordering::Relaxed);
            }
        }

        // Store results
        *self.state.check_results.write().await = results.clone();

        HealthStatus {
            is_healthy: self.state.healthy.load(Ordering::Relaxed),
            is_ready: self.state.ready.load(Ordering::Relaxed),
            checks: results,
            lag_ms,
            last_healthy: *self.state.last_healthy.read().await,
            consecutive_failures: self.state.consecutive_failures.load(Ordering::Relaxed) as u32,
            consecutive_successes: self.state.consecutive_successes.load(Ordering::Relaxed) as u32,
            uptime: self.start_time.elapsed(),
        }
    }

    /// Get current health status without running checks.
    pub async fn status(&self) -> HealthStatus {
        let results = self.state.check_results.read().await.clone();
        HealthStatus {
            is_healthy: self.state.healthy.load(Ordering::Relaxed),
            is_ready: self.state.ready.load(Ordering::Relaxed),
            checks: results,
            lag_ms: self.state.lag_ms.load(Ordering::Relaxed),
            last_healthy: *self.state.last_healthy.read().await,
            consecutive_failures: self.state.consecutive_failures.load(Ordering::Relaxed) as u32,
            consecutive_successes: self.state.consecutive_successes.load(Ordering::Relaxed) as u32,
            uptime: self.start_time.elapsed(),
        }
    }

    /// Update replication lag.
    pub fn set_lag(&self, lag_ms: u64) {
        self.state.lag_ms.store(lag_ms, Ordering::Relaxed);
    }

    /// Check if system is healthy.
    pub fn is_healthy(&self) -> bool {
        self.state.healthy.load(Ordering::Relaxed)
    }

    /// Check if system is ready.
    pub fn is_ready(&self) -> bool {
        self.state.ready.load(Ordering::Relaxed)
    }

    /// Mark system as connected (healthy).
    pub fn mark_connected(&self) {
        self.state.healthy.store(true, Ordering::Relaxed);
        self.state.ready.store(true, Ordering::Relaxed);
        self.state.consecutive_failures.store(0, Ordering::Relaxed);
        info!("System marked as connected and healthy");
    }

    /// Mark system as disconnected (unhealthy).
    pub fn mark_disconnected(&self) {
        self.state.healthy.store(false, Ordering::Relaxed);
        warn!("System marked as disconnected");
    }

    /// Spawn background health check task.
    pub fn spawn_monitor(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let config = self.config.clone();
        tokio::spawn(async move {
            let mut interval = interval(config.check_interval);
            loop {
                interval.tick().await;
                self.check().await;
            }
        })
    }
}

/// Recovery coordinator for handling reconnection with exponential backoff.
pub struct RecoveryCoordinator {
    config: HealthConfig,
    current_delay: RwLock<Duration>,
    attempts: AtomicU64,
    in_progress: AtomicBool,
}

impl RecoveryCoordinator {
    pub fn new(config: HealthConfig) -> Self {
        Self {
            current_delay: RwLock::new(config.recovery_delay),
            config,
            attempts: AtomicU64::new(0),
            in_progress: AtomicBool::new(false),
        }
    }

    /// Attempt recovery with a recovery function.
    pub async fn recover<F, Fut>(&self, recover_fn: F) -> bool
    where
        F: Fn() -> Fut,
        Fut: Future<Output = bool>,
    {
        if self
            .in_progress
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            debug!("Recovery already in progress");
            return false;
        }

        let attempt = self.attempts.fetch_add(1, Ordering::Relaxed) + 1;
        let delay = *self.current_delay.read().await;

        info!(
            "Starting recovery attempt {} after {:?} delay",
            attempt, delay
        );

        tokio::time::sleep(delay).await;

        let success = recover_fn().await;

        if success {
            info!("Recovery successful on attempt {}", attempt);
            self.reset().await;
        } else {
            warn!("Recovery failed on attempt {}", attempt);
            self.increase_delay().await;
        }

        self.in_progress.store(false, Ordering::Release);
        success
    }

    /// Increase delay with exponential backoff.
    async fn increase_delay(&self) {
        let mut delay = self.current_delay.write().await;
        let new_delay = (*delay * 2).min(self.config.max_recovery_delay);
        *delay = new_delay;
    }

    /// Reset recovery state after success.
    async fn reset(&self) {
        *self.current_delay.write().await = self.config.recovery_delay;
        self.attempts.store(0, Ordering::Relaxed);
    }

    /// Get current attempt count.
    pub fn attempts(&self) -> u64 {
        self.attempts.load(Ordering::Relaxed)
    }

    /// Check if recovery is in progress.
    pub fn is_recovering(&self) -> bool {
        self.in_progress.load(Ordering::Relaxed)
    }
}

/// Kubernetes-compatible health endpoint responses.
impl HealthStatus {
    /// Liveness probe (is the process alive?).
    pub fn liveness(&self) -> (u16, &'static str) {
        if self.consecutive_failures < 10 {
            (200, "OK")
        } else {
            (503, "Service Unavailable")
        }
    }

    /// Readiness probe (can we serve traffic?).
    pub fn readiness(&self) -> (u16, &'static str) {
        if self.is_ready {
            (200, "OK")
        } else {
            (503, "Service Unavailable")
        }
    }

    /// Format as JSON for health endpoints.
    pub fn to_json(&self) -> String {
        let checks: HashMap<String, String> = self
            .checks
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    match &v.result {
                        HealthCheckResult::Healthy => "healthy".to_string(),
                        HealthCheckResult::Degraded(msg) => format!("degraded: {}", msg),
                        HealthCheckResult::Unhealthy(msg) => format!("unhealthy: {}", msg),
                    },
                )
            })
            .collect();

        serde_json::json!({
            "status": if self.is_healthy { "healthy" } else { "unhealthy" },
            "ready": self.is_ready,
            "lag_ms": self.lag_ms,
            "uptime_secs": self.uptime.as_secs(),
            "consecutive_failures": self.consecutive_failures,
            "checks": checks
        })
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_check_result() {
        assert!(HealthCheckResult::Healthy.is_healthy());
        assert!(!HealthCheckResult::Healthy.is_degraded());
        assert!(!HealthCheckResult::Healthy.is_unhealthy());

        assert!(!HealthCheckResult::Degraded("test".into()).is_healthy());
        assert!(HealthCheckResult::Degraded("test".into()).is_degraded());

        assert!(HealthCheckResult::Unhealthy("test".into()).is_unhealthy());
    }

    #[test]
    fn test_health_config_presets() {
        let strict = HealthConfig::strict();
        assert_eq!(strict.failure_threshold, 2);

        let relaxed = HealthConfig::relaxed();
        assert_eq!(relaxed.failure_threshold, 5);
    }

    #[tokio::test]
    async fn test_health_monitor_basic() {
        let monitor = HealthMonitor::new(HealthConfig::default());

        // Initially not healthy (no successful checks)
        assert!(!monitor.is_healthy());

        // Register a healthy check
        monitor
            .register_check("test", || async { HealthCheckResult::Healthy })
            .await;

        // Run checks multiple times to pass threshold
        for _ in 0..3 {
            monitor.check().await;
        }

        assert!(monitor.is_healthy());
    }

    #[tokio::test]
    async fn test_health_monitor_failure() {
        let config = HealthConfig {
            failure_threshold: 2,
            success_threshold: 1,
            ..Default::default()
        };
        let monitor = HealthMonitor::new(config);

        // Start healthy
        monitor.mark_connected();
        assert!(monitor.is_healthy());

        // Register failing check
        monitor
            .register_check("failing", || async {
                HealthCheckResult::Unhealthy("test failure".into())
            })
            .await;

        // Run until threshold
        monitor.check().await;
        assert!(monitor.is_healthy()); // Still healthy after 1 failure

        monitor.check().await;
        assert!(!monitor.is_healthy()); // Unhealthy after 2 failures
    }

    #[tokio::test]
    async fn test_lag_monitoring() {
        let config = HealthConfig {
            max_lag_ms: 1000,
            failure_threshold: 1,
            ..Default::default()
        };
        let monitor = HealthMonitor::new(config);
        monitor.mark_connected();

        // Set acceptable lag
        monitor.set_lag(500);
        let status = monitor.check().await;
        assert!(status.is_healthy);

        // Set excessive lag
        monitor.set_lag(2000);
        let status = monitor.check().await;
        assert!(!status.is_healthy);
        assert!(status.checks.contains_key("replication_lag"));
    }

    #[tokio::test]
    async fn test_recovery_coordinator() {
        let config = HealthConfig {
            recovery_delay: Duration::from_millis(10),
            max_recovery_delay: Duration::from_millis(100),
            ..Default::default()
        };
        let coordinator = RecoveryCoordinator::new(config);

        // Successful recovery
        let success = coordinator.recover(|| async { true }).await;
        assert!(success);
        assert_eq!(coordinator.attempts(), 0); // Reset after success

        // Failed recovery
        let success = coordinator.recover(|| async { false }).await;
        assert!(!success);
    }

    #[tokio::test]
    async fn test_health_status_probes() {
        let status = HealthStatus {
            is_healthy: true,
            is_ready: true,
            checks: HashMap::new(),
            lag_ms: 100,
            last_healthy: Some(Instant::now()),
            consecutive_failures: 0,
            consecutive_successes: 5,
            uptime: Duration::from_secs(3600),
        };

        assert_eq!(status.liveness(), (200, "OK"));
        assert_eq!(status.readiness(), (200, "OK"));

        let not_ready = HealthStatus {
            is_ready: false,
            ..status.clone()
        };
        assert_eq!(not_ready.readiness(), (503, "Service Unavailable"));
    }

    #[test]
    fn test_health_status_json() {
        let status = HealthStatus {
            is_healthy: true,
            is_ready: true,
            checks: HashMap::new(),
            lag_ms: 100,
            last_healthy: Some(Instant::now()),
            consecutive_failures: 0,
            consecutive_successes: 5,
            uptime: Duration::from_secs(60),
        };

        let json = status.to_json();
        assert!(json.contains("\"status\":\"healthy\""));
        assert!(json.contains("\"ready\":true"));
    }

    #[tokio::test]
    async fn test_mark_connected_disconnected() {
        let monitor = HealthMonitor::new(HealthConfig::default());

        monitor.mark_connected();
        assert!(monitor.is_healthy());
        assert!(monitor.is_ready());

        monitor.mark_disconnected();
        assert!(!monitor.is_healthy());
    }
}
