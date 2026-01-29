//! Mock implementations for testing

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

/// A mock event sink that captures events for verification
#[derive(Debug, Clone)]
pub struct MockEventSink {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
    event_count: Arc<AtomicU64>,
}

/// A captured event from the mock sink
#[derive(Debug, Clone)]
pub struct CapturedEvent {
    pub offset: u64,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: HashMap<String, String>,
    pub timestamp: i64,
}

impl MockEventSink {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            event_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Capture an event
    pub async fn capture(&self, offset: u64, key: Option<Bytes>, value: Bytes) {
        self.capture_with_headers(offset, key, value, HashMap::new())
            .await;
    }

    /// Capture an event with headers
    pub async fn capture_with_headers(
        &self,
        offset: u64,
        key: Option<Bytes>,
        value: Bytes,
        headers: HashMap<String, String>,
    ) {
        let event = CapturedEvent {
            offset,
            key,
            value,
            headers,
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        self.events.lock().await.push(event);
        self.event_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Get the current event count
    pub fn count(&self) -> u64 {
        self.event_count.load(Ordering::SeqCst)
    }

    /// Get all captured events
    pub async fn events(&self) -> Vec<CapturedEvent> {
        self.events.lock().await.clone()
    }

    /// Clear all captured events
    pub async fn clear(&self) {
        self.events.lock().await.clear();
        self.event_count.store(0, Ordering::SeqCst);
    }

    /// Wait for a specific number of events
    pub async fn wait_for_count(&self, count: u64, timeout: std::time::Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if self.count() >= count {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        false
    }
}

impl Default for MockEventSink {
    fn default() -> Self {
        Self::new()
    }
}

/// A mock metrics collector
#[derive(Debug, Default)]
pub struct MockMetrics {
    counters: Arc<Mutex<HashMap<String, u64>>>,
    gauges: Arc<Mutex<HashMap<String, f64>>>,
    histograms: Arc<Mutex<HashMap<String, Vec<f64>>>>,
}

impl MockMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn increment(&self, name: &str, value: u64) {
        let mut counters = self.counters.lock().await;
        *counters.entry(name.to_string()).or_insert(0) += value;
    }

    pub async fn gauge(&self, name: &str, value: f64) {
        let mut gauges = self.gauges.lock().await;
        gauges.insert(name.to_string(), value);
    }

    pub async fn histogram(&self, name: &str, value: f64) {
        let mut histograms = self.histograms.lock().await;
        histograms.entry(name.to_string()).or_default().push(value);
    }

    pub async fn get_counter(&self, name: &str) -> Option<u64> {
        self.counters.lock().await.get(name).copied()
    }

    pub async fn get_gauge(&self, name: &str) -> Option<f64> {
        self.gauges.lock().await.get(name).copied()
    }

    pub async fn get_histogram(&self, name: &str) -> Option<Vec<f64>> {
        self.histograms.lock().await.get(name).cloned()
    }
}

/// Fault injection utilities for chaos testing
pub mod fault_injection {
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::sync::Arc;

    /// A fault injector that can simulate various failure modes
    #[derive(Debug, Clone)]
    pub struct FaultInjector {
        /// Whether to fail the next operation
        fail_next: Arc<AtomicBool>,
        /// Number of operations to delay
        delay_count: Arc<AtomicU32>,
        /// Delay duration in milliseconds
        delay_ms: Arc<AtomicU32>,
        /// Failure rate (0-100)
        failure_rate: Arc<AtomicU32>,
    }

    impl FaultInjector {
        pub fn new() -> Self {
            Self {
                fail_next: Arc::new(AtomicBool::new(false)),
                delay_count: Arc::new(AtomicU32::new(0)),
                delay_ms: Arc::new(AtomicU32::new(0)),
                failure_rate: Arc::new(AtomicU32::new(0)),
            }
        }

        /// Set the injector to fail the next operation
        pub fn fail_next(&self) {
            self.fail_next.store(true, Ordering::SeqCst);
        }

        /// Set a random failure rate (0-100%)
        pub fn set_failure_rate(&self, rate: u32) {
            self.failure_rate.store(rate.min(100), Ordering::SeqCst);
        }

        /// Set delay for the next N operations
        pub fn set_delay(&self, count: u32, delay_ms: u32) {
            self.delay_count.store(count, Ordering::SeqCst);
            self.delay_ms.store(delay_ms, Ordering::SeqCst);
        }

        /// Check if the current operation should fail
        pub fn should_fail(&self) -> bool {
            // Check explicit fail_next
            if self.fail_next.swap(false, Ordering::SeqCst) {
                return true;
            }

            // Check random failure rate
            let rate = self.failure_rate.load(Ordering::SeqCst);
            if rate > 0 {
                let random = rand::random::<u32>() % 100;
                return random < rate;
            }

            false
        }

        /// Get delay for current operation (returns 0 if no delay)
        pub fn get_delay(&self) -> std::time::Duration {
            let count = self.delay_count.load(Ordering::SeqCst);
            if count > 0 {
                self.delay_count.fetch_sub(1, Ordering::SeqCst);
                let ms = self.delay_ms.load(Ordering::SeqCst);
                return std::time::Duration::from_millis(ms as u64);
            }
            std::time::Duration::ZERO
        }

        /// Apply fault injection to an operation
        pub async fn maybe_fail<T, E>(
            &self,
            operation: impl std::future::Future<Output = Result<T, E>>,
        ) -> Result<T, E>
        where
            E: From<std::io::Error>,
        {
            // Apply delay if configured
            let delay = self.get_delay();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }

            // Check if we should fail
            if self.should_fail() {
                return Err(std::io::Error::other("Injected fault").into());
            }

            operation.await
        }
    }

    impl Default for FaultInjector {
        fn default() -> Self {
            Self::new()
        }
    }
}
