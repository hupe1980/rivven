//! # CDC Pipeline Abstraction
//!
//! Composable, type-safe CDC pipelines with transforms, filters, and routing.
//!
//! ## Features
//!
//! - **Transform Chain**: Apply multiple transforms in sequence
//! - **Branching**: Route events to multiple destinations
//! - **Error Handling**: Dead letter queue for failed events
//! - **Async Processing**: Non-blocking pipeline execution
//!
//! ## Usage
//!
//! ```ignore
//! use rivven_cdc::common::{Pipeline, Transform, CdcEvent};
//!
//! let pipeline = Pipeline::new()
//!     .filter(|e| e.table != "audit_log")
//!     .transform(|mut e| {
//!         e.metadata.insert("processed_at".into(), chrono::Utc::now().to_rfc3339().into());
//!         e
//!     })
//!     .branch(|e| format!("cdc.{}.{}", e.schema, e.table))
//!     .with_dead_letter_queue("cdc.dlq")
//!     .build();
//!
//! // Process events
//! let results = pipeline.process(events).await;
//! ```

use crate::common::{CdcEvent, CdcOp};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

/// A transform function that modifies CDC events.
pub type TransformFn = Box<dyn Fn(CdcEvent) -> Option<CdcEvent> + Send + Sync>;

/// An async transform function.
pub type AsyncTransformFn = Box<
    dyn Fn(CdcEvent) -> Pin<Box<dyn Future<Output = Option<CdcEvent>> + Send>> + Send + Sync,
>;

/// A filter predicate for CDC events.
pub type FilterFn = Box<dyn Fn(&CdcEvent) -> bool + Send + Sync>;

/// A routing function that determines destination for events.
pub type RouterFn = Box<dyn Fn(&CdcEvent) -> String + Send + Sync>;

/// Pipeline stage representing a single operation.
#[derive(Clone)]
pub enum PipelineStage {
    /// Synchronous transform
    Transform(Arc<TransformFn>),
    /// Asynchronous transform
    AsyncTransform(Arc<AsyncTransformFn>),
    /// Filter events
    Filter(Arc<FilterFn>),
    /// Route to topic
    Route(Arc<RouterFn>),
}

/// Statistics for pipeline execution.
#[derive(Debug, Default)]
pub struct PipelineStats {
    /// Total events processed
    pub events_processed: AtomicU64,
    /// Events that passed all stages
    pub events_output: AtomicU64,
    /// Events filtered out
    pub events_filtered: AtomicU64,
    /// Events sent to DLQ
    pub events_dlq: AtomicU64,
    /// Transform errors
    pub transform_errors: AtomicU64,
}

impl PipelineStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_processed(&self) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_output(&self) {
        self.events_output.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_filtered(&self) {
        self.events_filtered.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dlq(&self) {
        self.events_dlq.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.transform_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> PipelineStatsSnapshot {
        PipelineStatsSnapshot {
            events_processed: self.events_processed.load(Ordering::Relaxed),
            events_output: self.events_output.load(Ordering::Relaxed),
            events_filtered: self.events_filtered.load(Ordering::Relaxed),
            events_dlq: self.events_dlq.load(Ordering::Relaxed),
            transform_errors: self.transform_errors.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of pipeline statistics.
#[derive(Debug, Clone)]
pub struct PipelineStatsSnapshot {
    pub events_processed: u64,
    pub events_output: u64,
    pub events_filtered: u64,
    pub events_dlq: u64,
    pub transform_errors: u64,
}

impl PipelineStatsSnapshot {
    /// Calculate success rate (0.0 - 1.0).
    pub fn success_rate(&self) -> f64 {
        if self.events_processed == 0 {
            return 1.0;
        }
        self.events_output as f64 / self.events_processed as f64
    }

    /// Calculate filter rate (0.0 - 1.0).
    pub fn filter_rate(&self) -> f64 {
        if self.events_processed == 0 {
            return 0.0;
        }
        self.events_filtered as f64 / self.events_processed as f64
    }
}

/// Result of processing an event through the pipeline.
#[derive(Debug, Clone)]
pub enum ProcessResult {
    /// Event passed through, with optional routing destination
    Output { event: CdcEvent, destination: Option<String> },
    /// Event was filtered out
    Filtered,
    /// Event was sent to dead letter queue
    DeadLetter { event: CdcEvent, reason: String },
}

/// Builder for constructing CDC pipelines.
pub struct PipelineBuilder {
    stages: Vec<PipelineStage>,
    dlq_topic: Option<String>,
    name: String,
}

impl PipelineBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            stages: Vec::new(),
            dlq_topic: None,
            name: name.to_string(),
        }
    }

    /// Add a synchronous transform stage.
    pub fn transform<F>(mut self, f: F) -> Self
    where
        F: Fn(CdcEvent) -> Option<CdcEvent> + Send + Sync + 'static,
    {
        self.stages.push(PipelineStage::Transform(Arc::new(Box::new(f))));
        self
    }

    /// Add an async transform stage.
    pub fn async_transform<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(CdcEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<CdcEvent>> + Send + 'static,
    {
        self.stages.push(PipelineStage::AsyncTransform(Arc::new(Box::new(
            move |e| Box::pin(f(e)),
        ))));
        self
    }

    /// Add a filter stage.
    pub fn filter<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&CdcEvent) -> bool + Send + Sync + 'static,
    {
        self.stages.push(PipelineStage::Filter(Arc::new(Box::new(predicate))));
        self
    }

    /// Add a routing stage.
    pub fn route<F>(mut self, router: F) -> Self
    where
        F: Fn(&CdcEvent) -> String + Send + Sync + 'static,
    {
        self.stages.push(PipelineStage::Route(Arc::new(Box::new(router))));
        self
    }

    /// Set dead letter queue topic.
    pub fn with_dlq(mut self, topic: &str) -> Self {
        self.dlq_topic = Some(topic.to_string());
        self
    }

    /// Build the pipeline.
    pub fn build(self) -> Pipeline {
        Pipeline {
            stages: self.stages,
            dlq_topic: self.dlq_topic,
            stats: Arc::new(PipelineStats::new()),
            name: self.name,
        }
    }
}

/// A CDC processing pipeline.
pub struct Pipeline {
    stages: Vec<PipelineStage>,
    dlq_topic: Option<String>,
    stats: Arc<PipelineStats>,
    name: String,
}

impl Pipeline {
    /// Create a new pipeline builder.
    pub fn builder(name: &str) -> PipelineBuilder {
        PipelineBuilder::new(name)
    }

    /// Process a single event through the pipeline.
    pub async fn process_one(&self, event: CdcEvent) -> ProcessResult {
        self.stats.record_processed();
        let mut current = event;
        let mut destination = None;

        for stage in &self.stages {
            match stage {
                PipelineStage::Transform(f) => {
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(current.clone()))) {
                        Ok(Some(e)) => current = e,
                        Ok(None) => {
                            self.stats.record_filtered();
                            return ProcessResult::Filtered;
                        }
                        Err(_) => {
                            self.stats.record_error();
                            self.stats.record_dlq();
                            return ProcessResult::DeadLetter {
                                event: current,
                                reason: "Transform panicked".to_string(),
                            };
                        }
                    }
                }
                PipelineStage::AsyncTransform(f) => {
                    match f(current.clone()).await {
                        Some(e) => current = e,
                        None => {
                            self.stats.record_filtered();
                            return ProcessResult::Filtered;
                        }
                    }
                }
                PipelineStage::Filter(predicate) => {
                    if !predicate(&current) {
                        self.stats.record_filtered();
                        return ProcessResult::Filtered;
                    }
                }
                PipelineStage::Route(router) => {
                    destination = Some(router(&current));
                }
            }
        }

        self.stats.record_output();
        ProcessResult::Output {
            event: current,
            destination,
        }
    }

    /// Process multiple events through the pipeline.
    pub async fn process(&self, events: Vec<CdcEvent>) -> Vec<ProcessResult> {
        let mut results = Vec::with_capacity(events.len());
        for event in events {
            results.push(self.process_one(event).await);
        }
        results
    }

    /// Process events in parallel (up to concurrency limit).
    pub async fn process_parallel(
        &self,
        events: Vec<CdcEvent>,
        concurrency: usize,
    ) -> Vec<ProcessResult> {
        use futures::stream::{self, StreamExt};

        stream::iter(events)
            .map(|e| self.process_one(e))
            .buffer_unordered(concurrency)
            .collect()
            .await
    }

    /// Get pipeline statistics.
    pub fn stats(&self) -> PipelineStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get pipeline name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get dead letter queue topic.
    pub fn dlq_topic(&self) -> Option<&str> {
        self.dlq_topic.as_deref()
    }
}

/// Common transforms for CDC events.
pub mod transforms {
    use super::*;

    /// Mask sensitive fields in the event.
    pub fn mask_fields(
        fields: Vec<String>,
    ) -> impl Fn(CdcEvent) -> Option<CdcEvent> + Send + Sync {
        move |mut event| {
            let mask = serde_json::json!("***MASKED***");
            
            if let Some(ref mut after) = event.after {
                if let Some(obj) = after.as_object_mut() {
                    for field in &fields {
                        if obj.contains_key(field) {
                            obj.insert(field.clone(), mask.clone());
                        }
                    }
                }
            }
            
            if let Some(ref mut before) = event.before {
                if let Some(obj) = before.as_object_mut() {
                    for field in &fields {
                        if obj.contains_key(field) {
                            obj.insert(field.clone(), mask.clone());
                        }
                    }
                }
            }
            
            Some(event)
        }
    }

    /// Rename a field in the event.
    pub fn rename_field(
        from: String,
        to: String,
    ) -> impl Fn(CdcEvent) -> Option<CdcEvent> + Send + Sync {
        move |mut event| {
            if let Some(ref mut after) = event.after {
                if let Some(obj) = after.as_object_mut() {
                    if let Some(value) = obj.remove(&from) {
                        obj.insert(to.clone(), value);
                    }
                }
            }
            
            if let Some(ref mut before) = event.before {
                if let Some(obj) = before.as_object_mut() {
                    if let Some(value) = obj.remove(&from) {
                        obj.insert(to.clone(), value);
                    }
                }
            }
            
            Some(event)
        }
    }

    /// Convert operation type (e.g., treat Snapshot as Insert).
    pub fn normalize_op() -> impl Fn(CdcEvent) -> Option<CdcEvent> + Send + Sync {
        |mut event| {
            if event.op == CdcOp::Snapshot {
                event.op = CdcOp::Insert;
            }
            Some(event)
        }
    }

    /// Filter to only keep specific operations.
    pub fn only_ops(ops: Vec<CdcOp>) -> impl Fn(&CdcEvent) -> bool + Send + Sync {
        move |event| ops.contains(&event.op)
    }

    /// Filter to exclude specific tables.
    pub fn exclude_tables(tables: Vec<String>) -> impl Fn(&CdcEvent) -> bool + Send + Sync {
        move |event| !tables.contains(&event.table)
    }

    /// Route events by schema and table.
    pub fn route_by_table(prefix: String) -> impl Fn(&CdcEvent) -> String + Send + Sync {
        move |event| format!("{}.{}.{}", prefix, event.schema, event.table)
    }
}

/// Pipeline registry for managing multiple pipelines.
pub struct PipelineRegistry {
    pipelines: RwLock<HashMap<String, Arc<Pipeline>>>,
}

impl PipelineRegistry {
    pub fn new() -> Self {
        Self {
            pipelines: RwLock::new(HashMap::new()),
        }
    }

    /// Register a pipeline.
    pub async fn register(&self, pipeline: Pipeline) {
        let name = pipeline.name.clone();
        let mut pipelines = self.pipelines.write().await;
        pipelines.insert(name.clone(), Arc::new(pipeline));
        debug!("Registered pipeline: {}", name);
    }

    /// Get a pipeline by name.
    pub async fn get(&self, name: &str) -> Option<Arc<Pipeline>> {
        let pipelines = self.pipelines.read().await;
        pipelines.get(name).cloned()
    }

    /// Remove a pipeline.
    pub async fn remove(&self, name: &str) -> Option<Arc<Pipeline>> {
        let mut pipelines = self.pipelines.write().await;
        pipelines.remove(name)
    }

    /// List all pipeline names.
    pub async fn list(&self) -> Vec<String> {
        let pipelines = self.pipelines.read().await;
        pipelines.keys().cloned().collect()
    }

    /// Get stats for all pipelines.
    pub async fn all_stats(&self) -> HashMap<String, PipelineStatsSnapshot> {
        let pipelines = self.pipelines.read().await;
        pipelines
            .iter()
            .map(|(name, p)| (name.clone(), p.stats()))
            .collect()
    }
}

impl Default for PipelineRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::CdcOp;

    fn make_event(table: &str, op: CdcOp) -> CdcEvent {
        CdcEvent {
            source_type: "postgres".to_string(),
            database: "testdb".to_string(),
            schema: "public".to_string(),
            table: table.to_string(),
            op,
            before: None,
            after: Some(serde_json::json!({
                "id": 1,
                "email": "test@example.com"
            })),
            timestamp: chrono::Utc::now().timestamp(),
            transaction: None,
        }
    }

    #[tokio::test]
    async fn test_pipeline_basic() {
        let pipeline = Pipeline::builder("test")
            .transform(|e| Some(e))
            .build();

        let event = make_event("users", CdcOp::Insert);
        let result = pipeline.process_one(event.clone()).await;

        match result {
            ProcessResult::Output { event: e, .. } => {
                assert_eq!(e.table, "users");
            }
            _ => panic!("Expected output"),
        }
    }

    #[tokio::test]
    async fn test_pipeline_filter() {
        let pipeline = Pipeline::builder("test")
            .filter(|e| e.table != "audit_log")
            .build();

        // Should pass
        let event1 = make_event("users", CdcOp::Insert);
        assert!(matches!(
            pipeline.process_one(event1).await,
            ProcessResult::Output { .. }
        ));

        // Should filter
        let event2 = make_event("audit_log", CdcOp::Insert);
        assert!(matches!(
            pipeline.process_one(event2).await,
            ProcessResult::Filtered
        ));
    }

    #[tokio::test]
    async fn test_pipeline_transform() {
        let pipeline = Pipeline::builder("test")
            .transform(transforms::rename_field("email".to_string(), "user_email".to_string()))
            .build();

        let event = make_event("users", CdcOp::Insert);
        let result = pipeline.process_one(event).await;

        match result {
            ProcessResult::Output { event, .. } => {
                let after = event.after.unwrap();
                assert!(after.get("user_email").is_some());
                assert!(after.get("email").is_none());
            }
            _ => panic!("Expected output"),
        }
    }

    #[tokio::test]
    async fn test_pipeline_route() {
        let pipeline = Pipeline::builder("test")
            .route(transforms::route_by_table("cdc".to_string()))
            .build();

        let event = make_event("users", CdcOp::Insert);
        let result = pipeline.process_one(event).await;

        match result {
            ProcessResult::Output { destination, .. } => {
                assert_eq!(destination, Some("cdc.public.users".to_string()));
            }
            _ => panic!("Expected output"),
        }
    }

    #[tokio::test]
    async fn test_pipeline_mask_fields() {
        let pipeline = Pipeline::builder("test")
            .transform(transforms::mask_fields(vec!["email".to_string()]))
            .build();

        let event = make_event("users", CdcOp::Insert);
        let result = pipeline.process_one(event).await;

        match result {
            ProcessResult::Output { event, .. } => {
                let after = event.after.unwrap();
                assert_eq!(after.get("email"), Some(&serde_json::json!("***MASKED***")));
                assert_eq!(after.get("id"), Some(&serde_json::json!(1))); // Unchanged
            }
            _ => panic!("Expected output"),
        }
    }

    #[tokio::test]
    async fn test_pipeline_chain() {
        let pipeline = Pipeline::builder("test")
            .filter(|e| e.op != CdcOp::Delete)
            .transform(transforms::normalize_op())
            .transform(transforms::rename_field("email".to_string(), "user_email".to_string()))
            .route(transforms::route_by_table("events".to_string()))
            .build();

        // Snapshot should become Insert
        let event = make_event("users", CdcOp::Snapshot);
        let result = pipeline.process_one(event).await;

        match result {
            ProcessResult::Output { event, destination } => {
                assert_eq!(event.op, CdcOp::Insert);
                let after = event.after.unwrap();
                assert!(after.get("user_email").is_some());
                assert_eq!(destination, Some("events.public.users".to_string()));
            }
            _ => panic!("Expected output"),
        }

        // Delete should be filtered
        let delete_event = make_event("users", CdcOp::Delete);
        assert!(matches!(
            pipeline.process_one(delete_event).await,
            ProcessResult::Filtered
        ));
    }

    #[tokio::test]
    async fn test_pipeline_stats() {
        let pipeline = Pipeline::builder("test")
            .filter(|e| e.table != "filtered")
            .build();

        let events = vec![
            make_event("users", CdcOp::Insert),
            make_event("filtered", CdcOp::Insert),
            make_event("orders", CdcOp::Update),
        ];

        pipeline.process(events).await;

        let stats = pipeline.stats();
        assert_eq!(stats.events_processed, 3);
        assert_eq!(stats.events_output, 2);
        assert_eq!(stats.events_filtered, 1);
    }

    #[tokio::test]
    async fn test_pipeline_parallel() {
        let pipeline = Pipeline::builder("test")
            .transform(|e| Some(e))
            .build();

        let events: Vec<_> = (0..100)
            .map(|i| make_event(&format!("table_{}", i), CdcOp::Insert))
            .collect();

        let results = pipeline.process_parallel(events, 10).await;
        assert_eq!(results.len(), 100);

        let stats = pipeline.stats();
        assert_eq!(stats.events_processed, 100);
        assert_eq!(stats.events_output, 100);
    }

    #[tokio::test]
    async fn test_pipeline_registry() {
        let registry = PipelineRegistry::new();

        let pipeline1 = Pipeline::builder("pipeline-1").build();
        let pipeline2 = Pipeline::builder("pipeline-2").build();

        registry.register(pipeline1).await;
        registry.register(pipeline2).await;

        let names = registry.list().await;
        assert_eq!(names.len(), 2);

        let p1 = registry.get("pipeline-1").await;
        assert!(p1.is_some());
        assert_eq!(p1.unwrap().name(), "pipeline-1");

        registry.remove("pipeline-1").await;
        assert!(registry.get("pipeline-1").await.is_none());
    }

    #[tokio::test]
    async fn test_async_transform() {
        let pipeline = Pipeline::builder("async-test")
            .async_transform(|mut event| async move {
                // Simulate async operation
                tokio::time::sleep(tokio::time::Duration::from_micros(10)).await;
                // Modify the event in some way
                event.source_type = "processed".to_string();
                Some(event)
            })
            .build();

        let event = make_event("users", CdcOp::Insert);
        let result = pipeline.process_one(event).await;

        match result {
            ProcessResult::Output { event, .. } => {
                assert_eq!(event.source_type, "processed");
            }
            _ => panic!("Expected output"),
        }
    }

    #[test]
    fn test_stats_rates() {
        let stats = PipelineStatsSnapshot {
            events_processed: 100,
            events_output: 80,
            events_filtered: 20,
            events_dlq: 0,
            transform_errors: 0,
        };

        assert!((stats.success_rate() - 0.8).abs() < 0.001);
        assert!((stats.filter_rate() - 0.2).abs() < 0.001);
    }

    #[test]
    fn test_only_ops_filter() {
        let filter = transforms::only_ops(vec![CdcOp::Insert, CdcOp::Update]);
        
        let insert = make_event("users", CdcOp::Insert);
        let delete = make_event("users", CdcOp::Delete);
        
        assert!(filter(&insert));
        assert!(!filter(&delete));
    }
}
