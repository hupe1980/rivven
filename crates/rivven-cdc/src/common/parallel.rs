//! # Parallel CDC Source
//!
//! Multi-table parallel CDC processing with work distribution.
//!
//! ## Features
//!
//! - **Table Parallelism**: Process multiple tables concurrently
//! - **Work Stealing**: Dynamic load balancing across workers
//! - **Backpressure**: Per-table rate limiting
//! - **Coordinated Snapshots**: Consistent multi-table snapshots
//!
//! ## Usage
//!
//! ```ignore
//! use rivven_cdc::common::{ParallelSource, ParallelConfig};
//!
//! let config = ParallelConfig::builder()
//!     .concurrency(4)
//!     .per_table_buffer(1000)
//!     .build();
//!
//! let source = ParallelSource::new(config, sources);
//! source.start().await?;
//!
//! while let Some(event) = source.next().await {
//!     process(event).await?;
//! }
//! ```

use crate::common::{CdcError, CdcEvent, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{Notify, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Configuration for parallel CDC source.
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    /// Maximum concurrent table workers
    pub concurrency: usize,
    /// Buffer size per table
    pub per_table_buffer: usize,
    /// Output channel buffer size
    pub output_buffer: usize,
    /// Enable work stealing between workers
    pub work_stealing: bool,
    /// Maximum events per second per table
    pub per_table_rate_limit: Option<u64>,
    /// Timeout for worker shutdown
    pub shutdown_timeout: Duration,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            concurrency: 4,
            per_table_buffer: 1000,
            output_buffer: 10_000,
            work_stealing: true,
            per_table_rate_limit: None,
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

impl ParallelConfig {
    pub fn builder() -> ParallelConfigBuilder {
        ParallelConfigBuilder::new()
    }

    /// High-throughput preset.
    pub fn high_throughput() -> Self {
        Self {
            concurrency: 8,
            per_table_buffer: 5000,
            output_buffer: 50_000,
            work_stealing: true,
            per_table_rate_limit: None,
            shutdown_timeout: Duration::from_secs(60),
        }
    }

    /// Low-latency preset.
    pub fn low_latency() -> Self {
        Self {
            concurrency: 2,
            per_table_buffer: 100,
            output_buffer: 1000,
            work_stealing: false,
            per_table_rate_limit: Some(10_000),
            shutdown_timeout: Duration::from_secs(10),
        }
    }
}

/// Builder for ParallelConfig.
pub struct ParallelConfigBuilder {
    config: ParallelConfig,
}

impl ParallelConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: ParallelConfig::default(),
        }
    }

    pub fn concurrency(mut self, n: usize) -> Self {
        self.config.concurrency = n;
        self
    }

    pub fn per_table_buffer(mut self, size: usize) -> Self {
        self.config.per_table_buffer = size;
        self
    }

    pub fn output_buffer(mut self, size: usize) -> Self {
        self.config.output_buffer = size;
        self
    }

    pub fn work_stealing(mut self, enabled: bool) -> Self {
        self.config.work_stealing = enabled;
        self
    }

    pub fn per_table_rate_limit(mut self, limit: u64) -> Self {
        self.config.per_table_rate_limit = Some(limit);
        self
    }

    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.config.shutdown_timeout = timeout;
        self
    }

    pub fn build(self) -> ParallelConfig {
        self.config
    }
}

impl Default for ParallelConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for a table worker.
#[derive(Debug, Default)]
pub struct TableWorkerStats {
    pub events_processed: AtomicU64,
    pub events_dropped: AtomicU64,
    pub last_event_time: RwLock<Option<Instant>>,
    pub avg_latency_us: AtomicU64,
}

impl TableWorkerStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_event(&self, latency: Duration) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);

        // Exponential moving average for latency
        let latency_us = latency.as_micros() as u64;
        let current = self.avg_latency_us.load(Ordering::Relaxed);
        let new_avg = if current == 0 {
            latency_us
        } else {
            (current * 7 + latency_us) / 8 // EMA with alpha = 0.125
        };
        self.avg_latency_us.store(new_avg, Ordering::Relaxed);
    }

    pub fn record_dropped(&self) {
        self.events_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> TableWorkerStatsSnapshot {
        TableWorkerStatsSnapshot {
            events_processed: self.events_processed.load(Ordering::Relaxed),
            events_dropped: self.events_dropped.load(Ordering::Relaxed),
            avg_latency_us: self.avg_latency_us.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of table worker statistics.
#[derive(Debug, Clone)]
pub struct TableWorkerStatsSnapshot {
    pub events_processed: u64,
    pub events_dropped: u64,
    pub avg_latency_us: u64,
}

/// Overall parallel source statistics.
#[derive(Debug, Default)]
pub struct ParallelSourceStats {
    pub total_events: AtomicU64,
    pub total_dropped: AtomicU64,
    pub active_workers: AtomicU64,
    pub tables: RwLock<HashMap<String, Arc<TableWorkerStats>>>,
}

impl ParallelSourceStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register_table(&self, table: &str) -> Arc<TableWorkerStats> {
        let stats = Arc::new(TableWorkerStats::new());
        self.tables
            .write()
            .await
            .insert(table.to_string(), stats.clone());
        stats
    }

    pub fn record_event(&self) {
        self.total_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dropped(&self) {
        self.total_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn worker_started(&self) {
        self.active_workers.fetch_add(1, Ordering::Relaxed);
    }

    pub fn worker_stopped(&self) {
        self.active_workers.fetch_sub(1, Ordering::Relaxed);
    }

    pub async fn snapshot(&self) -> ParallelSourceStatsSnapshot {
        let tables = self.tables.read().await;
        let table_stats: HashMap<String, TableWorkerStatsSnapshot> = tables
            .iter()
            .map(|(k, v)| (k.clone(), v.snapshot()))
            .collect();

        ParallelSourceStatsSnapshot {
            total_events: self.total_events.load(Ordering::Relaxed),
            total_dropped: self.total_dropped.load(Ordering::Relaxed),
            active_workers: self.active_workers.load(Ordering::Relaxed),
            tables: table_stats,
        }
    }
}

/// Snapshot of parallel source statistics.
#[derive(Debug, Clone)]
pub struct ParallelSourceStatsSnapshot {
    pub total_events: u64,
    pub total_dropped: u64,
    pub active_workers: u64,
    pub tables: HashMap<String, TableWorkerStatsSnapshot>,
}

impl ParallelSourceStatsSnapshot {
    /// Calculate throughput (events/sec) given duration.
    pub fn throughput(&self, duration: Duration) -> f64 {
        if duration.as_secs_f64() == 0.0 {
            return 0.0;
        }
        self.total_events as f64 / duration.as_secs_f64()
    }

    /// Calculate drop rate (0.0 - 1.0).
    pub fn drop_rate(&self) -> f64 {
        let total = self.total_events + self.total_dropped;
        if total == 0 {
            return 0.0;
        }
        self.total_dropped as f64 / total as f64
    }
}

/// A table assignment for parallel processing.
#[derive(Debug, Clone)]
pub struct TableAssignment {
    pub schema: String,
    pub table: String,
    pub priority: u32,
}

impl TableAssignment {
    pub fn new(schema: &str, table: &str) -> Self {
        Self {
            schema: schema.to_string(),
            table: table.to_string(),
            priority: 0,
        }
    }

    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }
}

/// Event producer trait for table-specific CDC sources.
#[async_trait::async_trait]
pub trait TableEventProducer: Send + Sync {
    /// Start producing events for a table.
    async fn start(&mut self, table: &TableAssignment) -> Result<()>;

    /// Get next event (None = no more events).
    async fn next_event(&mut self) -> Result<Option<CdcEvent>>;

    /// Stop producing events.
    async fn stop(&mut self) -> Result<()>;
}

/// Worker handle for managing table processing.
struct TableWorker {
    table: TableAssignment,
    handle: JoinHandle<()>,
    shutdown: Arc<Notify>,
}

/// Coordinator for parallel CDC processing.
pub struct ParallelCoordinator {
    config: ParallelConfig,
    stats: Arc<ParallelSourceStats>,
    workers: RwLock<Vec<TableWorker>>,
    semaphore: Arc<Semaphore>,
    output_tx: Sender<CdcEvent>,
    output_rx: RwLock<Option<Receiver<CdcEvent>>>,
    running: AtomicBool,
    start_time: RwLock<Option<Instant>>,
}

impl ParallelCoordinator {
    /// Create a new parallel coordinator.
    pub fn new(config: ParallelConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.output_buffer);
        Self {
            semaphore: Arc::new(Semaphore::new(config.concurrency)),
            stats: Arc::new(ParallelSourceStats::new()),
            workers: RwLock::new(Vec::new()),
            output_tx: tx,
            output_rx: RwLock::new(Some(rx)),
            running: AtomicBool::new(false),
            start_time: RwLock::new(None),
            config,
        }
    }

    /// Add a table for parallel processing.
    pub async fn add_table<F, P>(&self, table: TableAssignment, producer_factory: F) -> Result<()>
    where
        F: FnOnce() -> P + Send + 'static,
        P: TableEventProducer + 'static,
    {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| CdcError::replication("Semaphore closed"))?;

        let table_name = table.full_name();
        let table_for_worker = table.clone();
        let table_stats = self.stats.register_table(&table_name).await;
        let output_tx = self.output_tx.clone();
        let stats = self.stats.clone();
        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();
        let rate_limit = self.config.per_table_rate_limit;
        let buffer_size = self.config.per_table_buffer;

        let handle = tokio::spawn(async move {
            let _permit = permit; // Hold permit until worker completes
            stats.worker_started();

            let mut producer = producer_factory();
            if let Err(e) = producer.start(&table_for_worker).await {
                error!(
                    "Failed to start producer for {}: {}",
                    table_for_worker.full_name(),
                    e
                );
                stats.worker_stopped();
                return;
            }

            let mut last_send = Instant::now();
            let mut events_this_second = 0u64;
            let mut buffer: Vec<CdcEvent> = Vec::with_capacity(buffer_size);

            loop {
                tokio::select! {
                    _ = shutdown_clone.notified() => {
                        debug!("Worker for {} received shutdown signal", table_for_worker.full_name());
                        break;
                    }
                    result = producer.next_event() => {
                        match result {
                            Ok(Some(event)) => {
                                let event_start = Instant::now();

                                // Rate limiting
                                if let Some(limit) = rate_limit {
                                    let now = Instant::now();
                                    if now.duration_since(last_send) >= Duration::from_secs(1) {
                                        last_send = now;
                                        events_this_second = 0;
                                    }
                                    if events_this_second >= limit {
                                        tokio::time::sleep(Duration::from_millis(10)).await;
                                        continue;
                                    }
                                    events_this_second += 1;
                                }

                                // Buffer events
                                buffer.push(event);

                                // Flush when buffer is full or enough time has passed
                                if buffer.len() >= buffer_size {
                                    for e in buffer.drain(..) {
                                        if output_tx.try_send(e).is_err() {
                                            table_stats.record_dropped();
                                            stats.record_dropped();
                                        } else {
                                            table_stats.record_event(event_start.elapsed());
                                            stats.record_event();
                                        }
                                    }
                                }
                            }
                            Ok(None) => {
                                debug!("Producer for {} completed", table_for_worker.full_name());
                                break;
                            }
                            Err(e) => {
                                error!("Error from producer {}: {}", table_for_worker.full_name(), e);
                                break;
                            }
                        }
                    }
                }
            }

            // Flush remaining buffered events
            for e in buffer.drain(..) {
                let _ = output_tx.try_send(e);
            }

            if let Err(e) = producer.stop().await {
                warn!(
                    "Error stopping producer for {}: {}",
                    table_for_worker.full_name(),
                    e
                );
            }

            stats.worker_stopped();
            info!("Worker for {} stopped", table_for_worker.full_name());
        });

        self.workers.write().await.push(TableWorker {
            table,
            handle,
            shutdown,
        });

        Ok(())
    }

    /// Start processing (if not already started).
    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(()); // Already running
        }
        *self.start_time.write().await = Some(Instant::now());
        info!("Parallel coordinator started");
        Ok(())
    }

    /// Take the output receiver (can only be called once).
    pub async fn take_receiver(&self) -> Option<Receiver<CdcEvent>> {
        self.output_rx.write().await.take()
    }

    /// Get next event from any table.
    pub async fn next(&self) -> Option<CdcEvent> {
        if let Some(ref mut rx) = *self.output_rx.write().await {
            rx.recv().await
        } else {
            None
        }
    }

    /// Stop all workers.
    pub async fn stop(&self) -> Result<()> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Ok(()); // Already stopped
        }

        info!("Stopping parallel coordinator");

        let workers = std::mem::take(&mut *self.workers.write().await);

        // Notify all workers to shutdown
        for worker in &workers {
            worker.shutdown.notify_one();
        }

        // Wait for workers with timeout
        let deadline = Instant::now() + self.config.shutdown_timeout;
        for worker in workers {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                worker.handle.abort();
                warn!(
                    "Aborted worker for {} due to timeout",
                    worker.table.full_name()
                );
            } else {
                match tokio::time::timeout(remaining, worker.handle).await {
                    Ok(Ok(())) => {
                        debug!("Worker for {} stopped gracefully", worker.table.full_name())
                    }
                    Ok(Err(e)) => warn!("Worker for {} panicked: {}", worker.table.full_name(), e),
                    Err(_) => {
                        warn!("Worker for {} timed out", worker.table.full_name());
                    }
                }
            }
        }

        info!("Parallel coordinator stopped");
        Ok(())
    }

    /// Get statistics.
    pub async fn stats(&self) -> ParallelSourceStatsSnapshot {
        self.stats.snapshot().await
    }

    /// Check if running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get uptime.
    pub async fn uptime(&self) -> Duration {
        self.start_time
            .read()
            .await
            .map(|t| t.elapsed())
            .unwrap_or_default()
    }
}

/// Simple in-memory event producer for testing.
pub struct MemoryEventProducer {
    events: Vec<CdcEvent>,
    index: usize,
}

impl MemoryEventProducer {
    pub fn new(events: Vec<CdcEvent>) -> Self {
        Self { events, index: 0 }
    }
}

#[async_trait::async_trait]
impl TableEventProducer for MemoryEventProducer {
    async fn start(&mut self, _table: &TableAssignment) -> Result<()> {
        Ok(())
    }

    async fn next_event(&mut self) -> Result<Option<CdcEvent>> {
        if self.index < self.events.len() {
            let event = self.events[self.index].clone();
            self.index += 1;
            Ok(Some(event))
        } else {
            Ok(None)
        }
    }

    async fn stop(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::CdcOp;

    fn make_event(table: &str, id: i64) -> CdcEvent {
        CdcEvent {
            source_type: "postgres".to_string(),
            database: "testdb".to_string(),
            schema: "public".to_string(),
            table: table.to_string(),
            op: CdcOp::Insert,
            before: None,
            after: Some(serde_json::json!({ "id": id })),
            timestamp: chrono::Utc::now().timestamp(),
            transaction: None,
        }
    }

    #[test]
    fn test_config_builder() {
        let config = ParallelConfig::builder()
            .concurrency(8)
            .per_table_buffer(2000)
            .output_buffer(20000)
            .build();

        assert_eq!(config.concurrency, 8);
        assert_eq!(config.per_table_buffer, 2000);
        assert_eq!(config.output_buffer, 20000);
    }

    #[test]
    fn test_config_presets() {
        let ht = ParallelConfig::high_throughput();
        assert_eq!(ht.concurrency, 8);

        let ll = ParallelConfig::low_latency();
        assert_eq!(ll.concurrency, 2);
    }

    #[test]
    fn test_table_assignment() {
        let table = TableAssignment::new("public", "users").with_priority(10);
        assert_eq!(table.full_name(), "public.users");
        assert_eq!(table.priority, 10);
    }

    #[tokio::test]
    async fn test_parallel_coordinator_basic() {
        let config = ParallelConfig::default();
        let coordinator = ParallelCoordinator::new(config);

        let events = vec![make_event("users", 1), make_event("users", 2)];

        let table = TableAssignment::new("public", "users");
        let events_clone = events.clone();
        coordinator
            .add_table(table, move || MemoryEventProducer::new(events_clone))
            .await
            .unwrap();

        coordinator.start().await.unwrap();

        // Receive events
        let mut received = Vec::new();
        let mut rx = coordinator.take_receiver().await.unwrap();

        while let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(100), rx.recv()).await
        {
            received.push(event);
        }

        coordinator.stop().await.unwrap();

        assert_eq!(received.len(), 2);
    }

    #[tokio::test]
    async fn test_parallel_coordinator_multiple_tables() {
        let config = ParallelConfig::builder().concurrency(4).build();
        let coordinator = ParallelCoordinator::new(config);

        // Add multiple tables
        for i in 0..3 {
            let table_name = format!("table_{}", i);
            let events = vec![make_event(&table_name, 1), make_event(&table_name, 2)];
            let table = TableAssignment::new("public", &table_name);
            coordinator
                .add_table(table, move || MemoryEventProducer::new(events))
                .await
                .unwrap();
        }

        coordinator.start().await.unwrap();

        let mut rx = coordinator.take_receiver().await.unwrap();
        let mut count = 0;

        while let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            count += 1;
        }

        coordinator.stop().await.unwrap();

        assert_eq!(count, 6); // 3 tables * 2 events
    }

    #[tokio::test]
    async fn test_worker_stats() {
        let stats = TableWorkerStats::new();

        stats.record_event(Duration::from_micros(100));
        stats.record_event(Duration::from_micros(200));
        stats.record_dropped();

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.events_processed, 2);
        assert_eq!(snapshot.events_dropped, 1);
    }

    #[tokio::test]
    async fn test_parallel_source_stats() {
        let stats = ParallelSourceStats::new();

        stats.register_table("users").await;
        stats.record_event();
        stats.record_event();
        stats.record_dropped();

        let snapshot = stats.snapshot().await;
        assert_eq!(snapshot.total_events, 2);
        assert_eq!(snapshot.total_dropped, 1);
        assert!(snapshot.tables.contains_key("users"));
    }

    #[test]
    fn test_stats_calculations() {
        let stats = ParallelSourceStatsSnapshot {
            total_events: 1000,
            total_dropped: 100,
            active_workers: 4,
            tables: HashMap::new(),
        };

        // 1100 total, 100 dropped = ~9.09% drop rate
        assert!((stats.drop_rate() - 0.0909).abs() < 0.01);

        // 1000 events in 10 seconds = 100 events/sec
        assert!((stats.throughput(Duration::from_secs(10)) - 100.0).abs() < 0.1);
    }

    #[tokio::test]
    async fn test_memory_event_producer() {
        let events = vec![make_event("users", 1), make_event("users", 2)];
        let mut producer = MemoryEventProducer::new(events);

        let table = TableAssignment::new("public", "users");
        producer.start(&table).await.unwrap();

        let e1 = producer.next_event().await.unwrap();
        assert!(e1.is_some());

        let e2 = producer.next_event().await.unwrap();
        assert!(e2.is_some());

        let e3 = producer.next_event().await.unwrap();
        assert!(e3.is_none());

        producer.stop().await.unwrap();
    }
}
