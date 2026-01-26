//! Datagen Source connector
//!
//! A synthetic data generator for testing and demos. Generates events without
//! requiring any external system.
//!
//! # Patterns
//!
//! - `sequence`: Sequential integers (0, 1, 2, ...)
//! - `random`: Random JSON objects with configurable fields
//! - `users`: Fake user records with names, emails, etc.
//! - `orders`: Fake e-commerce orders
//! - `events`: Generic event stream with type/payload
//! - `metrics`: Time-series metric data
//!
//! # Example Configuration
//!
//! ```yaml
//! sources:
//!   test-data:
//!     connector: datagen
//!     topic: demo-events
//!     config:
//!       pattern: orders
//!       events_per_second: 10
//!       max_events: 1000
//! ```

use crate::connectors::{AnySource, SourceFactory};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use super::super::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use validator::Validate;

/// Datagen source configuration
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct DatagenConfig {
    /// Data pattern to generate
    #[serde(default)]
    pub pattern: DataPattern,

    /// Events per second (0 = as fast as possible)
    #[serde(default = "default_events_per_second")]
    #[validate(range(max = 100_000))]
    pub events_per_second: u32,

    /// Maximum events to generate (0 = unlimited)
    #[serde(default)]
    pub max_events: u64,

    /// Stream name for generated events
    #[serde(default = "default_stream_name")]
    #[validate(length(min = 1, max = 255))]
    pub stream_name: String,

    /// Namespace for events (optional)
    pub namespace: Option<String>,

    /// Include sequence number in event data
    #[serde(default = "default_true")]
    pub include_sequence: bool,

    /// Include timestamp in event data
    #[serde(default = "default_true")]
    pub include_timestamp: bool,

    /// Custom fields to add to every event
    #[serde(default)]
    pub custom_fields: std::collections::HashMap<String, serde_json::Value>,

    /// Seed for random number generator (for reproducible tests)
    pub seed: Option<u64>,

    /// Batch size for CDC patterns (generates insert/update/delete cycles)
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 10_000))]
    pub batch_size: u32,

    /// CDC simulation mode - generate INSERT/UPDATE/DELETE cycles
    /// When enabled, generates realistic CDC event streams with:
    /// - 60% inserts, 30% updates, 10% deletes
    /// - Updates reference previously inserted records
    /// - Deletes remove previously inserted records
    #[serde(default)]
    pub cdc_mode: bool,
}

fn default_events_per_second() -> u32 {
    10
}

fn default_stream_name() -> String {
    "datagen".to_string()
}

fn default_true() -> bool {
    true
}

fn default_batch_size() -> u32 {
    100
}

impl Default for DatagenConfig {
    fn default() -> Self {
        Self {
            pattern: DataPattern::default(),
            events_per_second: default_events_per_second(),
            max_events: 0,
            stream_name: default_stream_name(),
            namespace: None,
            include_sequence: true,
            include_timestamp: true,
            custom_fields: std::collections::HashMap::new(),
            seed: None,
            batch_size: default_batch_size(),
            cdc_mode: false,
        }
    }
}

/// Data generation patterns
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DataPattern {
    /// Sequential integers (0, 1, 2, ...)
    Sequence,

    /// Random JSON objects
    #[default]
    Random,

    /// Fake user records
    Users,

    /// Fake e-commerce orders
    Orders,

    /// Generic events with type and payload
    Events,

    /// Time-series metrics
    Metrics,

    /// Simple key-value pairs
    KeyValue,
}

impl std::fmt::Display for DataPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sequence => write!(f, "sequence"),
            Self::Random => write!(f, "random"),
            Self::Users => write!(f, "users"),
            Self::Orders => write!(f, "orders"),
            Self::Events => write!(f, "events"),
            Self::Metrics => write!(f, "metrics"),
            Self::KeyValue => write!(f, "key_value"),
        }
    }
}

/// Simple pseudo-random number generator (xorshift64)
/// Avoids external dependencies and is reproducible with seed
struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 0x853c49e6748fea9b } else { seed },
        }
    }

    fn from_time() -> Self {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x853c49e6748fea9b);
        Self::new(seed)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() as f64) / (u64::MAX as f64)
    }

    fn range(&mut self, min: u64, max: u64) -> u64 {
        if min >= max {
            return min;
        }
        min + (self.next_u64() % (max - min))
    }

    fn range_i32(&mut self, min: i32, max: i32) -> i32 {
        if min >= max {
            return min;
        }
        min + (self.next_u32() as i32).abs() % (max - min)
    }

    fn choose<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        let idx = self.range(0, items.len() as u64) as usize;
        &items[idx]
    }

    fn bool(&mut self) -> bool {
        self.next_u64().is_multiple_of(2)
    }
}

/// CDC operation type for simulation mode
#[derive(Debug, Clone, Copy, PartialEq)]
enum CdcOperation {
    Insert,
    Update,
    Delete,
}

/// Data generator state
struct DataGenerator {
    rng: Rng,
    sequence: AtomicU64,
    config: DatagenConfig,
    /// Track generated record IDs for CDC updates/deletes
    /// Bounded to prevent unbounded memory growth
    cdc_record_pool: Vec<(String, serde_json::Value)>,
    /// Max records to track for CDC operations
    cdc_pool_max_size: usize,
}

impl DataGenerator {
    fn new(config: DatagenConfig) -> Self {
        let rng = config.seed.map(Rng::new).unwrap_or_else(Rng::from_time);
        Self {
            rng,
            sequence: AtomicU64::new(0),
            config,
            cdc_record_pool: Vec::with_capacity(1000),
            cdc_pool_max_size: 10_000, // Bounded to prevent memory issues
        }
    }

    fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::Relaxed)
    }

    /// Determine CDC operation based on distribution and pool state
    fn next_cdc_operation(&mut self) -> CdcOperation {
        if self.cdc_record_pool.is_empty() {
            // Must insert first
            return CdcOperation::Insert;
        }

        // Distribution: 60% insert, 30% update, 10% delete
        let roll = self.rng.range(0, 100);
        if roll < 60 {
            CdcOperation::Insert
        } else if roll < 90 {
            CdcOperation::Update
        } else {
            CdcOperation::Delete
        }
    }

    /// Generate a CDC-style event (insert/update/delete)
    fn generate_cdc(&mut self) -> SourceEvent {
        let seq = self.next_sequence();
        let op = self.next_cdc_operation();
        let stream_name = self.config.stream_name.clone();
        let namespace = self.config.namespace.clone();

        match op {
            CdcOperation::Insert => {
                let data = self.generate_data_for_pattern(seq);
                let id = self.extract_id(&data);

                // Track for future updates/deletes (bounded)
                if self.cdc_record_pool.len() < self.cdc_pool_max_size {
                    self.cdc_record_pool.push((id, data.clone()));
                } else {
                    // Replace random entry to maintain bounded pool
                    let idx = self.rng.range(0, self.cdc_record_pool.len() as u64) as usize;
                    self.cdc_record_pool[idx] = (id, data.clone());
                }

                let mut event = SourceEvent::insert(&stream_name, data);
                event.namespace = namespace;
                self.add_cdc_metadata(&mut event, seq, "insert");
                event
            }
            CdcOperation::Update => {
                // Pick random record from pool
                let idx = self.rng.range(0, self.cdc_record_pool.len() as u64) as usize;
                let (id, before) = self.cdc_record_pool[idx].clone();

                // Generate updated version
                let mut after = self.generate_data_for_pattern(seq);
                // Keep same ID
                if let serde_json::Value::Object(ref mut map) = after {
                    self.set_id(map, &id);
                }

                // Update pool
                self.cdc_record_pool[idx] = (id, after.clone());

                let mut event = SourceEvent::update(&stream_name, Some(before), after);
                event.namespace = namespace;
                self.add_cdc_metadata(&mut event, seq, "update");
                event
            }
            CdcOperation::Delete => {
                // Remove random record from pool
                let idx = self.rng.range(0, self.cdc_record_pool.len() as u64) as usize;
                let (_, before) = self.cdc_record_pool.swap_remove(idx);

                let mut event = SourceEvent::delete(&stream_name, before);
                event.namespace = namespace;
                self.add_cdc_metadata(&mut event, seq, "delete");
                event
            }
        }
    }

    fn generate_data_for_pattern(&mut self, seq: u64) -> serde_json::Value {
        let now = Utc::now();
        let mut data = match self.config.pattern {
            DataPattern::Sequence => self.generate_sequence(seq),
            DataPattern::Random => self.generate_random(seq),
            DataPattern::Users => self.generate_user(seq),
            DataPattern::Orders => self.generate_order(seq),
            DataPattern::Events => self.generate_event(seq),
            DataPattern::Metrics => self.generate_metric(seq, now),
            DataPattern::KeyValue => self.generate_key_value(seq),
        };

        // Add optional fields
        if let serde_json::Value::Object(ref mut map) = data {
            if self.config.include_sequence {
                map.insert("_sequence".to_string(), serde_json::json!(seq));
            }
            if self.config.include_timestamp {
                map.insert("_timestamp".to_string(), serde_json::json!(now.to_rfc3339()));
            }
            for (key, value) in &self.config.custom_fields {
                map.insert(key.clone(), value.clone());
            }
        }

        data
    }

    fn extract_id(&self, data: &serde_json::Value) -> String {
        // Try common ID field names
        if let serde_json::Value::Object(map) = data {
            for key in &["user_id", "order_id", "event_id", "metric_id", "key", "id", "value"] {
                if let Some(serde_json::Value::String(id)) = map.get(*key) {
                    return id.clone();
                }
                if let Some(serde_json::Value::Number(n)) = map.get(*key) {
                    return n.to_string();
                }
            }
        }
        // Fallback to random ID
        format!("id_{:016x}", self.sequence.load(Ordering::Relaxed))
    }

    fn set_id(&self, map: &mut serde_json::Map<String, serde_json::Value>, id: &str) {
        // Set ID on appropriate field based on pattern
        for key in &["user_id", "order_id", "event_id", "key", "id"] {
            if map.contains_key(*key) {
                map.insert(key.to_string(), serde_json::json!(id));
                return;
            }
        }
    }

    fn add_cdc_metadata(&self, event: &mut SourceEvent, seq: u64, op: &str) {
        event.metadata.extra.insert(
            "cdc".to_string(),
            serde_json::json!({
                "operation": op,
                "sequence": seq,
                "source": "datagen",
                "simulated": true,
            }),
        );
    }

    fn generate(&mut self) -> serde_json::Value {
        let seq = self.next_sequence();
        let now = Utc::now();

        let mut data = match self.config.pattern {
            DataPattern::Sequence => self.generate_sequence(seq),
            DataPattern::Random => self.generate_random(seq),
            DataPattern::Users => self.generate_user(seq),
            DataPattern::Orders => self.generate_order(seq),
            DataPattern::Events => self.generate_event(seq),
            DataPattern::Metrics => self.generate_metric(seq, now),
            DataPattern::KeyValue => self.generate_key_value(seq),
        };

        // Add optional fields
        if let serde_json::Value::Object(ref mut map) = data {
            if self.config.include_sequence {
                map.insert("_sequence".to_string(), serde_json::json!(seq));
            }
            if self.config.include_timestamp {
                map.insert("_timestamp".to_string(), serde_json::json!(now.to_rfc3339()));
            }
            // Add custom fields
            for (key, value) in &self.config.custom_fields {
                map.insert(key.clone(), value.clone());
            }
        }

        data
    }

    fn generate_sequence(&self, seq: u64) -> serde_json::Value {
        serde_json::json!({
            "value": seq
        })
    }

    fn generate_random(&mut self, _seq: u64) -> serde_json::Value {
        serde_json::json!({
            "id": format!("{:016x}", self.rng.next_u64()),
            "value": self.rng.range(0, 1000),
            "ratio": (self.rng.next_f64() * 100.0).round() / 100.0,
            "active": self.rng.bool(),
            "category": self.rng.choose(&["A", "B", "C", "D"]).to_string(),
        })
    }

    fn generate_user(&mut self, seq: u64) -> serde_json::Value {
        const FIRST_NAMES: &[&str] = &[
            "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Henry",
            "Ivy", "Jack", "Kate", "Leo", "Mia", "Noah", "Olivia", "Peter",
        ];
        const LAST_NAMES: &[&str] = &[
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
            "Davis", "Martinez", "Anderson", "Taylor", "Thomas", "Moore", "White",
        ];
        const DOMAINS: &[&str] = &[
            "example.com", "test.org", "demo.net", "mail.io", "acme.co",
        ];
        const COUNTRIES: &[&str] = &[
            "US", "UK", "DE", "FR", "JP", "AU", "CA", "BR", "IN", "MX",
        ];

        let first = self.rng.choose(FIRST_NAMES);
        let last = self.rng.choose(LAST_NAMES);
        let domain = self.rng.choose(DOMAINS);
        let country = self.rng.choose(COUNTRIES);

        serde_json::json!({
            "user_id": format!("user_{:08}", seq),
            "first_name": first,
            "last_name": last,
            "email": format!("{}.{}@{}", first.to_lowercase(), last.to_lowercase(), domain),
            "age": self.rng.range_i32(18, 80),
            "country": country,
            "premium": self.rng.bool(),
            "signup_date": self.random_date(),
        })
    }

    fn generate_order(&mut self, seq: u64) -> serde_json::Value {
        const PRODUCTS: &[&str] = &[
            "Widget Pro", "Gadget X", "Super Tool", "Mega Device", "Ultra Kit",
            "Power Pack", "Smart Hub", "Data Box", "Cloud Unit", "Sync Module",
        ];
        const STATUSES: &[&str] = &[
            "pending", "confirmed", "processing", "shipped", "delivered", "cancelled",
        ];
        const CURRENCIES: &[&str] = &["USD", "EUR", "GBP", "JPY", "AUD"];

        let product = self.rng.choose(PRODUCTS);
        let quantity = self.rng.range(1, 10) as u32;
        let unit_price = (self.rng.next_f64() * 200.0 + 10.0).round() / 100.0 * 100.0;
        let total = unit_price * quantity as f64;

        serde_json::json!({
            "order_id": format!("ORD-{:08}", seq),
            "customer_id": format!("user_{:08}", self.rng.range(1, 10000)),
            "product": product,
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total,
            "currency": self.rng.choose(CURRENCIES),
            "status": self.rng.choose(STATUSES),
            "created_at": self.random_recent_timestamp(),
        })
    }

    fn generate_event(&mut self, seq: u64) -> serde_json::Value {
        const EVENT_TYPES: &[&str] = &[
            "page_view", "click", "signup", "purchase", "logout", "error",
            "search", "add_to_cart", "remove_from_cart", "checkout",
        ];
        const SOURCES: &[&str] = &[
            "web", "mobile_ios", "mobile_android", "api", "webhook",
        ];

        let event_type = self.rng.choose(EVENT_TYPES);

        serde_json::json!({
            "event_id": format!("evt_{:016x}", self.rng.next_u64()),
            "event_type": event_type,
            "sequence": seq,
            "source": self.rng.choose(SOURCES),
            "user_id": format!("user_{:08}", self.rng.range(1, 10000)),
            "session_id": format!("sess_{:012x}", self.rng.next_u64() >> 16),
            "payload": self.generate_event_payload(event_type),
        })
    }

    fn generate_event_payload(&mut self, event_type: &str) -> serde_json::Value {
        match event_type {
            "page_view" => serde_json::json!({
                "url": format!("/page/{}", self.rng.range(1, 100)),
                "referrer": if self.rng.bool() { Some("https://search.example.com") } else { None },
                "duration_ms": self.rng.range(100, 30000),
            }),
            "click" => serde_json::json!({
                "element_id": format!("btn_{}", self.rng.range(1, 50)),
                "x": self.rng.range(0, 1920),
                "y": self.rng.range(0, 1080),
            }),
            "purchase" => serde_json::json!({
                "amount": (self.rng.next_f64() * 500.0).round() / 100.0 * 100.0,
                "items": self.rng.range(1, 10),
            }),
            _ => serde_json::json!({}),
        }
    }

    fn generate_metric(&mut self, seq: u64, now: DateTime<Utc>) -> serde_json::Value {
        const METRIC_NAMES: &[&str] = &[
            "cpu_usage", "memory_used", "disk_io", "network_rx", "network_tx",
            "requests_per_sec", "latency_ms", "error_rate", "queue_depth",
        ];
        const HOSTS: &[&str] = &[
            "web-01", "web-02", "api-01", "api-02", "db-01", "cache-01",
        ];

        let metric = self.rng.choose(METRIC_NAMES);
        let host = self.rng.choose(HOSTS);

        let value = match *metric {
            "cpu_usage" | "memory_used" | "error_rate" => self.rng.next_f64() * 100.0,
            "latency_ms" => self.rng.next_f64() * 500.0 + 1.0,
            "requests_per_sec" => self.rng.next_f64() * 10000.0,
            _ => self.rng.next_f64() * 1000.0,
        };

        serde_json::json!({
            "metric_id": seq,
            "name": metric,
            "value": (value * 100.0).round() / 100.0,
            "unit": self.metric_unit(metric),
            "host": host,
            "tags": {
                "env": self.rng.choose(&["prod", "staging", "dev"]),
                "region": self.rng.choose(&["us-east", "us-west", "eu-west", "ap-south"]),
            },
            "timestamp": now.to_rfc3339(),
        })
    }

    fn metric_unit(&self, metric: &str) -> &'static str {
        match metric {
            "cpu_usage" | "memory_used" | "error_rate" => "percent",
            "latency_ms" => "milliseconds",
            "requests_per_sec" => "req/s",
            "disk_io" | "network_rx" | "network_tx" => "bytes/s",
            _ => "count",
        }
    }

    fn generate_key_value(&mut self, seq: u64) -> serde_json::Value {
        serde_json::json!({
            "key": format!("key_{:08}", seq),
            "value": format!("value_{:016x}", self.rng.next_u64()),
        })
    }

    fn random_date(&mut self) -> String {
        let days_ago = self.rng.range(0, 365 * 3) as i64;
        let date = Utc::now() - chrono::Duration::days(days_ago);
        date.format("%Y-%m-%d").to_string()
    }

    fn random_recent_timestamp(&mut self) -> String {
        let secs_ago = self.rng.range(0, 86400 * 7) as i64;
        let ts = Utc::now() - chrono::Duration::seconds(secs_ago);
        ts.to_rfc3339()
    }
}

/// Datagen Source implementation
pub struct DatagenSource;

impl DatagenSource {
    /// Create a new DatagenSource
    pub fn new() -> Self {
        Self
    }
}

impl Default for DatagenSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Source for DatagenSource {
    type Config = DatagenConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("datagen", env!("CARGO_PKG_VERSION"))
            .description("Synthetic data generator for testing and demos")
            .author("Rivven Team")
            .license("Apache-2.0")
            .documentation_url("https://rivven.dev/docs/connectors/datagen")
            .config_schema::<DatagenConfig>()
            .build()
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        // Validate the configuration
        config
            .validate()
            .map_err(|e| ConnectorError::Config(format!("Invalid config: {}", e)))?;

        Ok(CheckResult::success())
    }

    async fn discover(&self, config: &Self::Config) -> Result<Catalog> {
        let schema = self.schema_for_pattern(&config.pattern);
        let mut stream = Stream::new(&config.stream_name, schema)
            .sync_modes(vec![SyncMode::FullRefresh, SyncMode::Incremental]);

        if let Some(ref ns) = config.namespace {
            stream = stream.namespace(ns.clone());
        }

        Ok(Catalog::new().add_stream(stream))
    }

    async fn read(
        &self,
        config: &Self::Config,
        _catalog: &ConfiguredCatalog,
        _state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        let config = config.clone();

        let stream = async_stream::stream! {
            let mut generator = DataGenerator::new(config.clone());
            let interval = if config.events_per_second > 0 {
                Some(std::time::Duration::from_secs_f64(1.0 / config.events_per_second as f64))
            } else {
                None
            };

            let mut count = 0u64;
            let mut last_event = std::time::Instant::now();

            tracing::info!(
                "Datagen starting: pattern={:?}, cdc_mode={}, rate={} events/sec",
                config.pattern,
                config.cdc_mode,
                config.events_per_second
            );

            loop {
                // Check max_events limit
                if config.max_events > 0 && count >= config.max_events {
                    tracing::info!("Datagen reached max_events limit: {}", config.max_events);
                    break;
                }

                // Rate limiting
                if let Some(interval) = interval {
                    let elapsed = last_event.elapsed();
                    if elapsed < interval {
                        tokio::time::sleep(interval - elapsed).await;
                    }
                    last_event = std::time::Instant::now();
                }

                // Generate event - CDC mode or regular record mode
                let event = if config.cdc_mode {
                    generator.generate_cdc()
                } else {
                    let data = generator.generate();
                    let mut event = SourceEvent::record(&config.stream_name, data);
                    event.namespace = config.namespace.clone();
                    event
                };

                count += 1;
                yield Ok(event);

                // Periodic yield to prevent blocking
                if count.is_multiple_of(100) {
                    tokio::task::yield_now().await;
                }
            }
        };

        Ok(Box::pin(stream))
    }
}

impl DatagenSource {
    fn schema_for_pattern(&self, pattern: &DataPattern) -> serde_json::Value {
        match pattern {
            DataPattern::Sequence => serde_json::json!({
                "type": "object",
                "properties": {
                    "value": { "type": "integer" },
                    "_sequence": { "type": "integer" },
                    "_timestamp": { "type": "string", "format": "date-time" }
                }
            }),
            DataPattern::Users => serde_json::json!({
                "type": "object",
                "properties": {
                    "user_id": { "type": "string" },
                    "first_name": { "type": "string" },
                    "last_name": { "type": "string" },
                    "email": { "type": "string", "format": "email" },
                    "age": { "type": "integer" },
                    "country": { "type": "string" },
                    "premium": { "type": "boolean" },
                    "signup_date": { "type": "string", "format": "date" }
                }
            }),
            DataPattern::Orders => serde_json::json!({
                "type": "object",
                "properties": {
                    "order_id": { "type": "string" },
                    "customer_id": { "type": "string" },
                    "product": { "type": "string" },
                    "quantity": { "type": "integer" },
                    "unit_price": { "type": "number" },
                    "total_amount": { "type": "number" },
                    "currency": { "type": "string" },
                    "status": { "type": "string" },
                    "created_at": { "type": "string", "format": "date-time" }
                }
            }),
            DataPattern::Events => serde_json::json!({
                "type": "object",
                "properties": {
                    "event_id": { "type": "string" },
                    "event_type": { "type": "string" },
                    "sequence": { "type": "integer" },
                    "source": { "type": "string" },
                    "user_id": { "type": "string" },
                    "session_id": { "type": "string" },
                    "payload": { "type": "object" }
                }
            }),
            DataPattern::Metrics => serde_json::json!({
                "type": "object",
                "properties": {
                    "metric_id": { "type": "integer" },
                    "name": { "type": "string" },
                    "value": { "type": "number" },
                    "unit": { "type": "string" },
                    "host": { "type": "string" },
                    "tags": { "type": "object" },
                    "timestamp": { "type": "string", "format": "date-time" }
                }
            }),
            DataPattern::KeyValue | DataPattern::Random => serde_json::json!({
                "type": "object",
                "properties": {
                    "key": { "type": "string" },
                    "value": { "type": "string" }
                }
            }),
        }
    }
}

/// Factory for creating DatagenSource instances
pub struct DatagenSourceFactory;

impl SourceFactory for DatagenSourceFactory {
    fn spec(&self) -> ConnectorSpec {
        DatagenSource::spec()
    }

    fn create(&self) -> Box<dyn AnySource> {
        Box::new(DatagenSourceWrapper(DatagenSource::new()))
    }
}

/// Wrapper for type-erased source operations
struct DatagenSourceWrapper(DatagenSource);

#[async_trait]
impl AnySource for DatagenSourceWrapper {
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult> {
        let typed_config: DatagenConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid config: {}", e)))?;

        typed_config
            .validate()
            .map_err(|e| ConnectorError::Config(format!("Config validation failed: {}", e)))?;

        self.0.check(&typed_config).await
    }

    async fn discover_raw(&self, config: &serde_yaml::Value) -> Result<Catalog> {
        let typed_config: DatagenConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid config: {}", e)))?;
        self.0.discover(&typed_config).await
    }

    async fn read_raw(
        &self,
        config: &serde_yaml::Value,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        let typed_config: DatagenConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid config: {}", e)))?;

        typed_config
            .validate()
            .map_err(|e| ConnectorError::Config(format!("Config validation failed: {}", e)))?;

        self.0.read(&typed_config, catalog, state).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[test]
    fn test_default_config() {
        let config = DatagenConfig::default();
        assert_eq!(config.events_per_second, 10);
        assert_eq!(config.max_events, 0);
        assert_eq!(config.stream_name, "datagen");
        assert!(config.include_sequence);
        assert!(config.include_timestamp);
    }

    #[test]
    fn test_rng_deterministic() {
        let mut rng1 = Rng::new(12345);
        let mut rng2 = Rng::new(12345);

        for _ in 0..100 {
            assert_eq!(rng1.next_u64(), rng2.next_u64());
        }
    }

    #[test]
    fn test_generator_sequence() {
        let config = DatagenConfig {
            pattern: DataPattern::Sequence,
            include_sequence: false,
            include_timestamp: false,
            ..Default::default()
        };
        let mut generator = DataGenerator::new(config);

        for i in 0..5 {
            let data = generator.generate();
            assert_eq!(data["value"], i);
        }
    }

    #[test]
    fn test_generator_users() {
        let config = DatagenConfig {
            pattern: DataPattern::Users,
            seed: Some(42),
            ..Default::default()
        };
        let mut generator = DataGenerator::new(config);
        let data = generator.generate();

        assert!(data["user_id"].is_string());
        assert!(data["first_name"].is_string());
        assert!(data["last_name"].is_string());
        assert!(data["email"].is_string());
        assert!(data["age"].is_i64());
        assert!(data["country"].is_string());
        assert!(data["premium"].is_boolean());
    }

    #[test]
    fn test_generator_orders() {
        let config = DatagenConfig {
            pattern: DataPattern::Orders,
            seed: Some(42),
            ..Default::default()
        };
        let mut generator = DataGenerator::new(config);
        let data = generator.generate();

        assert!(data["order_id"].is_string());
        assert!(data["customer_id"].is_string());
        assert!(data["product"].is_string());
        assert!(data["quantity"].is_u64());
        assert!(data["unit_price"].is_f64());
        assert!(data["total_amount"].is_f64());
    }

    #[test]
    fn test_generator_events() {
        let config = DatagenConfig {
            pattern: DataPattern::Events,
            seed: Some(42),
            ..Default::default()
        };
        let mut generator = DataGenerator::new(config);
        let data = generator.generate();

        assert!(data["event_id"].is_string());
        assert!(data["event_type"].is_string());
        assert!(data["source"].is_string());
        assert!(data["user_id"].is_string());
    }

    #[test]
    fn test_generator_metrics() {
        let config = DatagenConfig {
            pattern: DataPattern::Metrics,
            seed: Some(42),
            ..Default::default()
        };
        let mut generator = DataGenerator::new(config);
        let data = generator.generate();

        assert!(data["metric_id"].is_u64());
        assert!(data["name"].is_string());
        assert!(data["value"].is_f64());
        assert!(data["unit"].is_string());
        assert!(data["host"].is_string());
    }

    #[test]
    fn test_custom_fields() {
        let mut custom = std::collections::HashMap::new();
        custom.insert("env".to_string(), serde_json::json!("test"));
        custom.insert("version".to_string(), serde_json::json!(1));

        let config = DatagenConfig {
            pattern: DataPattern::Sequence,
            custom_fields: custom,
            include_sequence: false,
            include_timestamp: false,
            ..Default::default()
        };
        let mut generator = DataGenerator::new(config);
        let data = generator.generate();

        assert_eq!(data["env"], "test");
        assert_eq!(data["version"], 1);
    }

    #[test]
    fn test_config_validation() {
        let config = DatagenConfig {
            events_per_second: 150_000, // Over limit
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[tokio::test]
    async fn test_source_check() {
        let source = DatagenSource::new();
        let config = DatagenConfig::default();

        let result = source.check(&config).await.unwrap();
        assert!(result.is_success());
    }

    #[tokio::test]
    async fn test_source_discover() {
        let source = DatagenSource::new();
        let config = DatagenConfig {
            stream_name: "test_stream".to_string(),
            ..Default::default()
        };

        let catalog = source.discover(&config).await.unwrap();
        assert!(!catalog.streams.is_empty());
        assert_eq!(catalog.streams[0].name, "test_stream");
    }

    #[tokio::test]
    async fn test_source_read_limited() {
        let source = DatagenSource::new();
        let config = DatagenConfig {
            pattern: DataPattern::Sequence,
            max_events: 5,
            events_per_second: 0, // As fast as possible
            ..Default::default()
        };

        let catalog = ConfiguredCatalog::default();
        let mut stream = source.read(&config, &catalog, None).await.unwrap();

        let mut count = 0;
        while let Some(result) = stream.next().await {
            let event = result.unwrap();
            assert_eq!(event.stream, "datagen");
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[test]
    fn test_factory_spec() {
        let factory = DatagenSourceFactory;
        let spec = factory.spec();
        assert_eq!(spec.connector_type, "datagen");
    }

    #[test]
    fn test_factory_create() {
        let factory = DatagenSourceFactory;
        // Factory creates a boxed source, validation happens in check_raw/read_raw
        let _source = factory.create();
    }

    #[tokio::test]
    async fn test_factory_check_raw_valid() {
        let factory = DatagenSourceFactory;
        let source = factory.create();
        let config = serde_yaml::from_str(
            r#"
            pattern: orders
            events_per_second: 100
            max_events: 1000
            "#,
        )
        .unwrap();

        let result = source.check_raw(&config).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_success());
    }

    #[tokio::test]
    async fn test_factory_check_raw_invalid() {
        let factory = DatagenSourceFactory;
        let source = factory.create();
        let config = serde_yaml::from_str(
            r#"
            events_per_second: 999999
            "#,
        )
        .unwrap();

        let result = source.check_raw(&config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cdc_mode_generates_operations() {
        let source = DatagenSource::new();
        let config = DatagenConfig {
            pattern: DataPattern::Users,
            max_events: 100,
            events_per_second: 0,
            cdc_mode: true,
            seed: Some(42),
            ..Default::default()
        };

        let catalog = ConfiguredCatalog::default();
        let mut stream = source.read(&config, &catalog, None).await.unwrap();

        let mut inserts = 0;
        let mut updates = 0;
        let mut deletes = 0;

        while let Some(result) = stream.next().await {
            let event = result.unwrap();
            match event.event_type {
                SourceEventType::Insert => inserts += 1,
                SourceEventType::Update => updates += 1,
                SourceEventType::Delete => deletes += 1,
                _ => {}
            }
        }

        // Should have mix of operations
        assert!(inserts > 0, "Should have inserts");
        // After initial inserts, should have updates and deletes
        assert!(updates > 0 || deletes > 0, "Should have updates or deletes after building pool");
        assert_eq!(inserts + updates + deletes, 100);
    }

    #[test]
    fn test_cdc_operation_distribution() {
        let config = DatagenConfig {
            cdc_mode: true,
            seed: Some(12345),
            ..Default::default()
        };
        let mut generator = DataGenerator::new(config);

        // Generate some inserts first to populate pool
        for _ in 0..10 {
            let _ = generator.generate_cdc();
        }

        // Now check distribution over many operations
        let mut inserts = 0;
        let mut updates = 0;
        let mut deletes = 0;

        for _ in 0..1000 {
            match generator.next_cdc_operation() {
                CdcOperation::Insert => inserts += 1,
                CdcOperation::Update => updates += 1,
                CdcOperation::Delete => deletes += 1,
            }
        }

        // Should roughly match 60/30/10 distribution (with some variance)
        let insert_ratio = inserts as f64 / 1000.0;
        let update_ratio = updates as f64 / 1000.0;
        let delete_ratio = deletes as f64 / 1000.0;

        assert!(insert_ratio > 0.5 && insert_ratio < 0.7, "Insert ratio ~60%: {}", insert_ratio);
        assert!(update_ratio > 0.2 && update_ratio < 0.4, "Update ratio ~30%: {}", update_ratio);
        assert!(delete_ratio > 0.05 && delete_ratio < 0.15, "Delete ratio ~10%: {}", delete_ratio);
    }

    #[test]
    fn test_cdc_pool_bounded() {
        let config = DatagenConfig {
            cdc_mode: true,
            seed: Some(42),
            ..Default::default()
        };
        let mut generator = DataGenerator::new(config);
        generator.cdc_pool_max_size = 100; // Small pool for testing

        // Generate many inserts
        for _ in 0..500 {
            let _ = generator.generate_cdc();
        }

        // Pool should be bounded
        assert!(generator.cdc_record_pool.len() <= generator.cdc_pool_max_size);
    }
}
