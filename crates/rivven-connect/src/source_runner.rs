//! Source runner - reads from external systems, publishes to broker topics
//!
//! Features:
//! - Automatic reconnection with exponential backoff
//! - Status tracking for health checks
//! - Graceful shutdown support
//! - Per-source metrics
//! - Auto-create topics with configurable settings
//! - Dynamic topic routing for CDC connectors

use crate::broker_client::SharedBrokerClient;
use crate::config::{ConnectConfig, SourceConfig, TopicSettings, TransformStepConfig};
use crate::connectors::{create_source_registry, SourceRegistry};
use crate::error::{ConnectError, ConnectorStatus, Result};
use crate::rate_limiter::TokenBucketRateLimiter;
use crate::schema::{SchemaRegistryClient, SchemaRegistryConfig};
use crate::topic_resolver::TopicResolver;
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock, Semaphore};
use tracing::{debug, error, info, warn};

/// Dead letter envelope wrapping a failed event with error context.
///
/// Published as JSON to the configured `dead_letter_topic` when a source
/// event cannot be delivered after all retry attempts. Preserves the
/// original payload and captures enough metadata for operators to
/// diagnose and replay the event.
#[derive(Debug, serde::Serialize)]
pub struct DlqEnvelope {
    /// Original destination topic
    pub original_topic: String,
    /// Source connector name
    pub connector_name: String,
    /// Original event payload (base64-encoded for binary safety)
    pub payload_b64: String,
    /// Error message from the last failed attempt
    pub error: String,
    /// Number of delivery attempts made
    pub retry_count: u32,
    /// ISO-8601 timestamp when the event was dead-lettered
    pub timestamp: String,
}

/// Source runner state
pub struct SourceRunner {
    name: String,
    config: SourceConfig,
    global_topic_settings: TopicSettings,
    broker: SharedBrokerClient,
    status: RwLock<ConnectorStatus>,
    events_published: AtomicU64,
    errors_count: AtomicU64,
    /// Topic resolver for CDC connectors (when topic_routing is configured)
    topic_resolver: Option<TopicResolver>,
    /// Rate limiter for publish throughput
    rate_limiter: TokenBucketRateLimiter,
    /// In-flight request limiter (backpressure)
    in_flight_semaphore: Option<Arc<Semaphore>>,
    /// Schema registry client (if schema_registry_url is configured)
    #[allow(dead_code)]
    schema_registry: Option<Arc<SchemaRegistryClient>>,
    /// Transform steps to apply before publishing
    transforms: Vec<TransformStepConfig>,
    /// Source connector registry for dynamic dispatch (registry-based connectors)
    source_registry: Arc<SourceRegistry>,
    /// Dead letter topic for events that fail to publish after all retries
    dead_letter_topic: Option<String>,
    /// Counter for events routed to DLQ
    events_dead_lettered: AtomicU64,
}

// Methods for health monitoring
#[allow(dead_code)] // Wired into health endpoint in future
impl SourceRunner {
    /// Get current status
    pub(crate) async fn status(&self) -> ConnectorStatus {
        *self.status.read().await
    }

    /// Get error count
    pub(crate) fn errors_count(&self) -> u64 {
        self.errors_count.load(Ordering::Relaxed)
    }
}

impl SourceRunner {
    /// Create a new source runner
    pub fn new(
        name: String,
        config: SourceConfig,
        global_topic_settings: TopicSettings,
        broker: SharedBrokerClient,
        source_registry: Arc<SourceRegistry>,
    ) -> Result<Self> {
        // Extract topic_routing from connector-specific config for CDC connectors
        let topic_routing_pattern = Self::extract_topic_routing(&config);

        // Initialize topic resolver if topic_routing is configured
        let topic_resolver = topic_routing_pattern.and_then(|pattern| {
            match TopicResolver::new(&pattern) {
                Ok(resolver) => {
                    info!(
                        "Source '{}': topic routing enabled with pattern '{}'",
                        name, pattern
                    );
                    Some(resolver)
                }
                Err(e) => {
                    // This should have been caught by config validation,
                    // but log and fall back to static topic
                    error!(
                        "Source '{}': invalid topic_routing pattern '{}': {}. Using static topic '{}' instead",
                        name, pattern, e, config.topic
                    );
                    None
                }
            }
        });

        // Rate limiter for publish throughput
        let rate_limiter_config = config.rate_limit.to_rate_limiter_config();
        let rate_limiter = TokenBucketRateLimiter::new(rate_limiter_config);

        // In-flight request semaphore for backpressure
        let in_flight_semaphore = if config.rate_limit.max_in_flight > 0 {
            Some(Arc::new(Semaphore::new(config.rate_limit.max_in_flight)))
        } else {
            None
        };

        // Initialize schema registry client if configured
        let schema_registry = config.schema_registry_url.as_ref().and_then(|url| {
            let sr_config = SchemaRegistryConfig::external(url);
            match SchemaRegistryClient::from_config(&sr_config) {
                Ok(client) => {
                    info!("Source '{}': schema registry enabled at {}", name, url);
                    Some(Arc::new(client))
                }
                Err(e) => {
                    warn!("Source '{}': failed to init schema registry: {}", name, e);
                    None
                }
            }
        });

        let transforms = config.transforms.clone();

        // Fail-fast: validate transform types at startup
        for step in &transforms {
            step.validate().map_err(|e| {
                ConnectError::config(format!("Source '{}': invalid transform: {}", name, e,))
            })?;
        }

        let dead_letter_topic = config.dead_letter_topic.clone();
        if let Some(ref dlq) = dead_letter_topic {
            info!("Source '{}': dead letter topic configured: {}", name, dlq);
        }

        Ok(Self {
            name,
            config,
            global_topic_settings,
            broker,
            status: RwLock::new(ConnectorStatus::Starting),
            events_published: AtomicU64::new(0),
            errors_count: AtomicU64::new(0),
            topic_resolver,
            rate_limiter,
            in_flight_semaphore,
            schema_registry,
            transforms,
            source_registry,
            dead_letter_topic,
            events_dead_lettered: AtomicU64::new(0),
        })
    }

    /// Extract topic_routing from CDC connector config
    fn extract_topic_routing(config: &SourceConfig) -> Option<String> {
        // Only CDC connectors support topic_routing
        match config.connector.as_str() {
            "postgres-cdc" => {
                // Try to parse as PostgresCdcConfig and extract topic_routing
                if let Ok(pg_config) = serde_yaml::from_value::<
                    crate::connectors::cdc::PostgresCdcConfig,
                >(config.config.clone())
                {
                    pg_config.topic_routing
                } else {
                    None
                }
            }
            "mysql-cdc" | "mariadb-cdc" => {
                // Try to parse as MySqlCdcConfig and extract topic_routing
                if let Ok(mysql_config) = serde_yaml::from_value::<
                    crate::connectors::cdc::MySqlCdcConfig,
                >(config.config.clone())
                {
                    mysql_config.topic_routing
                } else {
                    None
                }
            }
            _ => None, // Non-CDC connectors don't support topic_routing
        }
    }

    /// Get events published count
    pub fn events_published(&self) -> u64 {
        self.events_published.load(Ordering::Relaxed)
    }

    /// Get events dead-lettered count
    pub fn events_dead_lettered(&self) -> u64 {
        self.events_dead_lettered.load(Ordering::Relaxed)
    }

    /// Publish a failed event to the dead letter topic.
    ///
    /// Wraps the original payload in a [`DlqEnvelope`] with error context
    /// and publishes it with a short retry budget (3 attempts). If even
    /// the DLQ publish fails, returns an error — the pipeline should halt
    /// because data would be irretrievably lost.
    async fn publish_to_dlq(
        &self,
        dlq_topic: &str,
        original_topic: &str,
        payload: &[u8],
        error_msg: &str,
        retry_count: u32,
    ) -> Result<()> {
        use base64::Engine;

        let envelope = DlqEnvelope {
            original_topic: original_topic.to_string(),
            connector_name: self.name.clone(),
            payload_b64: base64::engine::general_purpose::STANDARD.encode(payload),
            error: error_msg.to_string(),
            retry_count,
            timestamp: chrono::Utc::now().to_rfc3339(),
        };

        let json = serde_json::to_vec(&envelope)
            .map_err(|e| ConnectError::Serialization(format!("DLQ envelope serialize: {}", e)))?;

        // Short retry budget for the DLQ publish itself
        let mut last_err = None;
        for attempt in 0..3u32 {
            match self.broker.publish(dlq_topic, json.clone()).await {
                Ok(_) => {
                    self.events_dead_lettered.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        "Source '{}': event dead-lettered to '{}' (original topic: '{}', retries: {})",
                        self.name, dlq_topic, original_topic, retry_count
                    );
                    return Ok(());
                }
                Err(e) => {
                    let backoff_ms = 100 * 2u64.pow(attempt);
                    warn!(
                        "Source '{}': DLQ publish attempt {}/3 to '{}' failed: {}",
                        self.name,
                        attempt + 1,
                        dlq_topic,
                        e
                    );
                    last_err = Some(e);
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                }
            }
        }

        Err(ConnectError::Permanent(format!(
            "DLQ publish to '{}' failed after 3 attempts: {}. Original event from topic '{}' is LOST.",
            dlq_topic,
            last_err.map(|e| e.to_string()).unwrap_or_default(),
            original_topic,
        )))
    }

    /// Run the source connector
    pub async fn run(&self, mut shutdown_rx: broadcast::Receiver<()>) -> Result<()> {
        info!(
            "Source '{}' starting, publishing to topic: {}",
            self.name, self.config.topic
        );

        // Ensure topic exists
        self.ensure_topic_exists().await?;

        // Run connector-specific logic — registry-first dispatch.
        // All registered connectors (datagen, postgres-cdc, mysql-cdc, etc.) are
        // resolved through the SourceRegistry. Only the built-in HTTP poller
        // falls back to an inline implementation when not in the registry.
        let connector = self.config.connector.as_str();
        let result = if let Some(factory) = self.source_registry.get(connector) {
            self.run_registry_source(factory, &mut shutdown_rx).await
        } else if connector == "http" {
            // Built-in HTTP poller (no factory yet — reqwest always available)
            self.run_http_source(&mut shutdown_rx).await
        } else {
            let available: Vec<&str> = self
                .source_registry
                .list()
                .iter()
                .map(|(n, _)| *n)
                .collect();
            Err(ConnectError::config(format!(
                "Unknown source connector type: '{}'. Available: {:?}",
                connector, available
            )))
        };

        *self.status.write().await = match &result {
            Ok(()) => ConnectorStatus::Stopped,
            Err(e) if e.is_shutdown() => ConnectorStatus::Stopped,
            Err(_) => ConnectorStatus::Failed,
        };

        result
    }

    /// Ensure the target topic exists (with auto-create if enabled)
    async fn ensure_topic_exists(&self) -> Result<()> {
        let topic = &self.config.topic;

        // Determine if auto-create is enabled for this source
        let auto_create = self
            .config
            .topic_config
            .as_ref()
            .and_then(|tc| tc.auto_create)
            .unwrap_or(self.global_topic_settings.auto_create);

        // Determine partition count
        let partitions = self
            .config
            .topic_config
            .as_ref()
            .and_then(|tc| tc.partitions)
            .unwrap_or(self.global_topic_settings.default_partitions);

        if !auto_create {
            // Check if topic exists when auto-create is disabled
            if self.global_topic_settings.require_topic_exists {
                match self.broker.topic_exists(topic).await {
                    Ok(true) => {
                        info!("Source '{}': topic '{}' exists", self.name, topic);
                        return Ok(());
                    }
                    Ok(false) => {
                        return Err(ConnectError::Topic(format!(
                            "Topic '{}' does not exist and auto_create is disabled",
                            topic
                        )));
                    }
                    Err(e) => {
                        warn!(
                            "Source '{}': failed to check topic existence: {}",
                            self.name, e
                        );
                        // Proceed anyway - topic might exist
                    }
                }
            }
            return Ok(());
        }

        // Auto-create enabled - create topic
        match self.broker.create_topic(topic, partitions).await {
            Ok(_) => {
                info!(
                    "Source '{}': created topic '{}' with {} partition(s)",
                    self.name, topic, partitions
                );
                Ok(())
            }
            Err(e) => {
                // Topic might already exist, which is fine
                let err_str = e.to_string().to_lowercase();
                if err_str.contains("already exists") || err_str.contains("exists") {
                    debug!("Source '{}': topic '{}' already exists", self.name, topic);
                    Ok(())
                } else {
                    warn!(
                        "Source '{}': topic creation returned: {} (may already exist)",
                        self.name, e
                    );
                    // Don't fail - topic might exist, we'll find out when we publish
                    Ok::<(), ConnectError>(())
                }
            }
        }?;

        // Also auto-create the dead letter topic if configured
        if let Some(ref dlq_topic) = self.dead_letter_topic {
            match self.broker.create_topic(dlq_topic, partitions).await {
                Ok(_) => {
                    info!(
                        "Source '{}': created DLQ topic '{}' with {} partition(s)",
                        self.name, dlq_topic, partitions
                    );
                }
                Err(e) => {
                    let err_str = e.to_string().to_lowercase();
                    if err_str.contains("already exists") || err_str.contains("exists") {
                        debug!(
                            "Source '{}': DLQ topic '{}' already exists",
                            self.name, dlq_topic
                        );
                    } else {
                        warn!(
                            "Source '{}': DLQ topic creation returned: {} (may already exist)",
                            self.name, e
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Publish an event to the broker
    async fn publish(&self, data: Bytes) -> Result<()> {
        let data = self.apply_transforms(data)?;
        // Empty bytes after transform = filtered event, skip
        if data.is_empty() {
            return Ok(());
        }
        self.publish_to_topic_inner(&self.config.topic, data).await
    }

    /// Publish an event to a specific topic with rate limiting and backpressure
    async fn publish_to_topic(&self, topic: &str, data: Bytes) -> Result<()> {
        // Apply transforms (ordered pipeline)
        let data = self.apply_transforms(data)?;

        // Empty bytes after transform = filtered event, skip
        if data.is_empty() {
            return Ok(());
        }

        self.publish_to_topic_inner(topic, data).await
    }

    /// Inner publish — rate limiting + backpressure + broker call
    async fn publish_to_topic_inner(&self, topic: &str, data: Bytes) -> Result<()> {
        // Rate limiting
        let wait = self.rate_limiter.acquire(1).await;
        if !wait.is_zero() {
            debug!("Source '{}' rate limited: waited {:?}", self.name, wait);
        }

        // Backpressure — wait for in-flight permit
        let _permit = if let Some(ref sem) = self.in_flight_semaphore {
            Some(
                sem.acquire()
                    .await
                    .map_err(|_| ConnectError::broker("In-flight semaphore closed"))?,
            )
        } else {
            None
        };

        match self.broker.publish(topic, data).await {
            Ok(_) => {
                self.events_published.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.errors_count.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Apply configured transform steps to event data
    fn apply_transforms(&self, data: Bytes) -> Result<Bytes> {
        if self.transforms.is_empty() {
            return Ok(data);
        }

        // Parse as JSON for field-level transforms
        let mut value: serde_json::Value = serde_json::from_slice(&data)
            .map_err(|e| ConnectError::Serialization(format!("Transform parse error: {}", e)))?;

        for step in &self.transforms {
            match step.transform_type.as_str() {
                "rename_field" => {
                    if let (Some(from), Some(to)) = (
                        step.config.get("from").and_then(|v| v.as_str()),
                        step.config.get("to").and_then(|v| v.as_str()),
                    ) {
                        if let Some(obj) = value.as_object_mut() {
                            if let Some(val) = obj.remove(from) {
                                obj.insert(to.to_string(), val);
                            }
                        }
                    }
                }
                "remove_field" => {
                    if let Some(field) = step.config.get("field").and_then(|v| v.as_str()) {
                        if let Some(obj) = value.as_object_mut() {
                            obj.remove(field);
                        }
                    }
                }
                "add_field" => {
                    if let (Some(field), Some(val)) = (
                        step.config.get("field").and_then(|v| v.as_str()),
                        step.config.get("value"),
                    ) {
                        if let Some(obj) = value.as_object_mut() {
                            // Convert serde_yaml::Value to serde_json::Value
                            let json_val = match serde_json::to_value(val) {
                                Ok(v) => v,
                                Err(e) => {
                                    warn!(field = field, error = %e, "Transform add_field: value conversion failed, inserting null");
                                    serde_json::Value::Null
                                }
                            };
                            obj.insert(field.to_string(), json_val);
                        }
                    }
                }
                "filter" => {
                    // Filter: if condition not met, skip this event
                    if let (Some(field), Some(equals)) = (
                        step.config.get("field").and_then(|v| v.as_str()),
                        step.config.get("equals").and_then(|v| v.as_str()),
                    ) {
                        let matches = value
                            .get(field)
                            .and_then(|v| v.as_str())
                            .map(|v| v == equals)
                            .unwrap_or(false);
                        if !matches {
                            // Return empty bytes to signal filtered event
                            return Ok(Bytes::new());
                        }
                    }
                }
                other => {
                    return Err(ConnectError::config(format!(
                        "Source '{}': unknown transform type '{}'",
                        self.name, other
                    )));
                }
            }
        }

        let out = serde_json::to_vec(&value).map_err(|e| {
            ConnectError::Serialization(format!("Transform serialize error: {}", e))
        })?;
        Ok(Bytes::from(out))
    }

    #[allow(dead_code)]
    #[cfg(feature = "postgres")]
    async fn run_postgres_cdc(&self, shutdown_rx: &mut broadcast::Receiver<()>) -> Result<()> {
        use crate::connectors::cdc::PostgresCdcConfig as SdkPgConfig;
        use rivven_cdc::common::CdcSource;
        use rivven_cdc::postgres::{PostgresCdc, PostgresCdcConfig};
        use validator::Validate;

        // Parse and validate using the SDK config struct
        let sdk_config: SdkPgConfig = serde_yaml::from_value(self.config.config.clone())
            .map_err(|e| ConnectError::config(format!("Invalid postgres config: {}", e)))?;

        // Validate configuration
        sdk_config
            .validate()
            .map_err(|e| ConnectError::config(format!("Config validation failed: {}", e)))?;

        // Build connection string - password is explicitly exposed here for connection
        // quote password to handle spaces and = characters
        let password_raw = sdk_config.password.expose();
        let escaped_password = password_raw.replace('\\', "\\\\").replace('\'', "\\'");
        let connection_string = format!(
            "host={} port={} dbname={} user={} password='{}'",
            sdk_config.host,
            sdk_config.port,
            sdk_config.database,
            sdk_config.user,
            escaped_password // Explicit password exposure for DB connection
        );

        let cdc_config = PostgresCdcConfig::builder()
            .connection_string(connection_string)
            .slot_name(sdk_config.slot_name.clone())
            .publication_name(sdk_config.publication_name.clone())
            .buffer_size(1000)
            .build()
            .map_err(|e| ConnectError::source(&self.name, e.to_string()))?;

        let mut cdc = PostgresCdc::new(cdc_config);

        cdc.start()
            .await
            .map_err(|e| ConnectError::source(&self.name, e.to_string()))?;

        let mut event_rx = cdc
            .take_event_receiver()
            .ok_or_else(|| ConnectError::source(&self.name, "Failed to get event receiver"))?;

        *self.status.write().await = ConnectorStatus::Running;
        info!(
            "Source '{}' connected to PostgreSQL, streaming CDC events",
            self.name
        );

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!(
                        "Source '{}' shutting down after {} events",
                        self.name,
                        self.events_published()
                    );
                    if let Err(e) = cdc.stop().await {
                        warn!(source = %self.name, error = %e, "CDC stop returned error during shutdown");
                    }
                    return Ok(());
                }
                event = event_rx.recv() => {
                    match event {
                        Some(cdc_event) => {
                            // Resolve target topic: use topic_routing if configured, else static topic
                            let target_topic = if let Some(resolver) = &self.topic_resolver {
                                use crate::topic_resolver::TopicMetadata;
                                let metadata = TopicMetadata::new(
                                    &cdc_event.database,
                                    &cdc_event.schema,
                                    &cdc_event.table,
                                );
                                resolver.resolve(&metadata)
                            } else {
                                self.config.topic.clone()
                            };

                            let json = serde_json::to_vec(&cdc_event)
                                .map_err(|e| ConnectError::Serialization(e.to_string()))?;

                            // Retry with exponential backoff (C-5 fix).
                            // CDC events MUST NOT be silently dropped — the CDC stream has
                            // already advanced past this event, so losing it means permanent
                            // data loss. We retry up to max_retries with bounded backoff.
                            let data = Bytes::from(json);
                            let max_retries: u32 = 10;
                            let mut backoff_ms: u64 = 100;
                            let max_backoff_ms: u64 = 30_000;
                            let backoff_multiplier: f64 = 2.0;
                            let mut attempt = 0u32;
                            let mut last_err = None;

                            loop {
                                match self.publish_to_topic(&target_topic, data.clone()).await {
                                    Ok(_) => {
                                        if attempt > 0 {
                                            info!(
                                                "Source '{}' publish to '{}' succeeded after {} retries",
                                                self.name, target_topic, attempt
                                            );
                                        }
                                        break;
                                    }
                                    Err(e) => {
                                        attempt += 1;
                                        if attempt > max_retries {
                                            error!(
                                                "Source '{}' publish to '{}' failed after {} retries: {}. \
                                                 CDC event LOST — manual recovery required.",
                                                self.name, target_topic, max_retries, e
                                            );
                                            *self.status.write().await = ConnectorStatus::Unhealthy;
                                            last_err = Some(e);
                                            break;
                                        }
                                        warn!(
                                            "Source '{}' publish to '{}' failed (attempt {}/{}): {}. \
                                             Retrying in {}ms...",
                                            self.name, target_topic, attempt, max_retries, e, backoff_ms
                                        );
                                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                                        backoff_ms = ((backoff_ms as f64 * backoff_multiplier) as u64)
                                            .min(max_backoff_ms);
                                    }
                                }
                            }

                            if let Some(e) = last_err {
                                // DLQ fallback: if configured, route the failed event to the
                                // dead letter topic so the CDC pipeline can continue without
                                // data loss. The original event is preserved for later replay.
                                if let Some(ref dlq_topic) = self.dead_letter_topic {
                                    self.publish_to_dlq(
                                        dlq_topic,
                                        &target_topic,
                                        &data,
                                        &e.to_string(),
                                        max_retries,
                                    )
                                    .await?;
                                    // Continue processing — event is safely in the DLQ
                                } else {
                                    // No DLQ configured — halt the pipeline to prevent
                                    // silent data loss
                                    return Err(ConnectError::Permanent(format!(
                                        "CDC event permanently lost after {} retries \
                                         (no dead_letter_topic configured): {}",
                                        max_retries, e
                                    )));
                                }
                            }

                            let count = self.events_published();
                            if count.is_multiple_of(1000) && count > 0 {
                                debug!("Source '{}' published {} events", self.name, count);
                            }
                        }
                        None => {
                            info!("Source '{}' CDC channel closed", self.name);
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    /// HTTP source - polls an HTTP endpoint and publishes responses as messages
    ///
    /// Config fields:
    /// - `url` (required): URL to poll via GET
    /// - `poll_interval_ms` (optional, default 5000): Polling interval in milliseconds
    /// - `headers` (optional): Additional HTTP headers as key-value pairs
    async fn run_http_source(&self, shutdown_rx: &mut broadcast::Receiver<()>) -> Result<()> {
        let url: String = self
            .config
            .config
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectError::config("HTTP source requires 'url' in connector config"))?
            .to_string();

        // validate URL scheme to prevent SSRF to internal endpoints
        let parsed_url = url::Url::parse(&url).map_err(|e| {
            ConnectError::config(format!("Invalid HTTP source URL '{}': {}", url, e))
        })?;
        match parsed_url.scheme() {
            "http" | "https" => {}
            scheme => {
                return Err(ConnectError::config(format!(
                    "HTTP source URL must use http or https scheme, got '{}'",
                    scheme
                )));
            }
        }

        let poll_interval_ms: u64 = self
            .config
            .config
            .get("poll_interval_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(5000);

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| {
                ConnectError::source(&self.name, format!("Failed to create HTTP client: {}", e))
            })?;

        *self.status.write().await = ConnectorStatus::Running;
        info!(
            "HTTP source '{}' started, polling {} every {}ms",
            self.name, url, poll_interval_ms
        );

        let poll_interval = std::time::Duration::from_millis(poll_interval_ms);

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("HTTP source '{}' shutting down", self.name);
                    return Ok(());
                }
                _ = tokio::time::sleep(poll_interval) => {}
            }

            match client.get(&url).send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        match resp.bytes().await {
                            Ok(body) if !body.is_empty() => {
                                let payload = body.clone();
                                let mut published = false;
                                for attempt in 0..3u32 {
                                    match self
                                        .broker
                                        .publish(&self.config.topic, body.to_vec())
                                        .await
                                    {
                                        Ok(_offset) => {
                                            self.events_published
                                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                            published = true;
                                            break;
                                        }
                                        Err(e) => {
                                            let backoff_ms = 100 * 2u64.pow(attempt);
                                            warn!(
                                                "HTTP source '{}' publish attempt {}/3 failed: {} (retrying in {}ms)",
                                                self.name, attempt + 1, e, backoff_ms
                                            );
                                            tokio::time::sleep(std::time::Duration::from_millis(
                                                backoff_ms,
                                            ))
                                            .await;
                                        }
                                    }
                                }
                                if !published {
                                    if let Some(dlq_topic) = &self.dead_letter_topic {
                                        let dlq_topic = dlq_topic.clone();
                                        warn!(
                                            "HTTP source '{}' routing failed event to DLQ topic '{}'",
                                            self.name, dlq_topic
                                        );
                                        self.publish_to_dlq(
                                            &dlq_topic,
                                            &self.config.topic,
                                            &payload,
                                            "publish failed after 3 retries",
                                            3,
                                        )
                                        .await?;
                                    } else {
                                        error!("HTTP source '{}' publish failed after 3 attempts (no DLQ configured)", self.name);
                                        self.errors_count.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                            Ok(_) => {} // Empty body, skip
                            Err(e) => {
                                warn!("HTTP source '{}' body read error: {}", self.name, e);
                            }
                        }
                    } else {
                        warn!(
                            "HTTP source '{}' received status {} from {}",
                            self.name,
                            resp.status(),
                            url
                        );
                    }
                }
                Err(e) => {
                    warn!("HTTP source '{}' request error: {}", self.name, e);
                }
            }
        }
    }

    #[allow(dead_code)]
    async fn run_datagen(&self, shutdown_rx: &mut broadcast::Receiver<()>) -> Result<()> {
        use super::prelude::*;
        use crate::connectors::datagen::{DatagenConfig, DatagenSource};
        use futures::StreamExt;

        // Parse and validate configuration
        let config: DatagenConfig = serde_yaml::from_value(self.config.config.clone())
            .map_err(|e| ConnectError::config(format!("Invalid datagen config: {}", e)))?;

        config
            .validate()
            .map_err(|e| ConnectError::config(format!("Config validation failed: {}", e)))?;

        let source = DatagenSource::new();
        let catalog = ConfiguredCatalog::default();

        let mut stream = source
            .read(&config, &catalog, None)
            .await
            .map_err(|e| ConnectError::source(&self.name, e.to_string()))?;

        *self.status.write().await = ConnectorStatus::Running;
        info!(
            "Source '{}' started datagen with pattern: {:?}, rate: {} events/sec",
            self.name, config.pattern, config.events_per_second
        );

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!(
                        "Source '{}' shutting down after {} events",
                        self.name,
                        self.events_published()
                    );
                    return Ok(());
                }
                event = stream.next() => {
                    match event {
                        Some(Ok(source_event)) => {
                            let json = serde_json::to_vec(&source_event)
                                .map_err(|e| ConnectError::Serialization(e.to_string()))?;

                            let payload = Bytes::from(json);
                            let mut published = false;
                            for attempt in 0..3u32 {
                                match self.publish(payload.clone()).await {
                                    Ok(()) => { published = true; break; }
                                    Err(e) => {
                                        let backoff_ms = 100 * 2u64.pow(attempt);
                                        warn!(
                                            "Source '{}' publish attempt {}/3 failed: {} (retrying in {}ms)",
                                            self.name, attempt + 1, e, backoff_ms
                                        );
                                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                                    }
                                }
                            }
                            if !published {
                                if let Some(ref dlq_topic) = self.dead_letter_topic {
                                    self.publish_to_dlq(
                                        dlq_topic,
                                        &self.config.topic,
                                        &payload,
                                        "publish failed after 3 retries",
                                        3,
                                    )
                                    .await?;
                                } else {
                                    error!("Source '{}' publish failed after 3 attempts", self.name);
                                    *self.status.write().await = ConnectorStatus::Unhealthy;
                                    return Err(ConnectError::Source {
                                        name: self.name.clone(),
                                        message: "publish failed after 3 retries".into(),
                                    });
                                }
                            }

                            let count = self.events_published();
                            if count.is_multiple_of(1000) && count > 0 {
                                debug!("Source '{}' published {} events", self.name, count);
                            }
                        }
                        Some(Err(e)) => {
                            error!("Source '{}' datagen error: {}", self.name, e);
                            self.errors_count.fetch_add(1, Ordering::Relaxed);
                        }
                        None => {
                            info!(
                                "Source '{}' datagen completed after {} events",
                                self.name,
                                self.events_published()
                            );
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    /// Run a registry-based source connector via AnySource trait dispatch
    ///
    /// This enables any connector registered in the SourceRegistry to be used
    /// without hardcoded match arms. The factory creates a type-erased AnySource
    /// which streams events through the same publish pipeline (rate limiting,
    /// backpressure, transforms).
    async fn run_registry_source(
        &self,
        factory: &Arc<dyn crate::connectors::SourceFactory>,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<()> {
        use super::prelude::*;
        use futures::StreamExt;

        let source = factory.create()?;

        // Check connectivity first
        let check = source.check_raw(&self.config.config).await?;
        if !check.success {
            return Err(ConnectError::source(
                &self.name,
                format!(
                    "Connectivity check failed: {}",
                    check.message.unwrap_or_default()
                ),
            ));
        }

        // Discover available streams
        let catalog = source.discover_raw(&self.config.config).await?;
        let configured_catalog = ConfiguredCatalog::from_catalog(&catalog);

        // Start reading events
        let mut stream = source
            .read_raw(&self.config.config, &configured_catalog, None)
            .await?;

        *self.status.write().await = ConnectorStatus::Running;
        info!(
            "Source '{}' started via registry (connector: {})",
            self.name, self.config.connector
        );

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!(
                        "Source '{}' shutting down after {} events",
                        self.name,
                        self.events_published()
                    );
                    return Ok(());
                }
                event = stream.next() => {
                    match event {
                        Some(Ok(source_event)) => {
                            let json = serde_json::to_vec(&source_event)
                                .map_err(|e| ConnectError::Serialization(e.to_string()))?;

                            let payload = Bytes::from(json);
                            let mut published = false;
                            for attempt in 0..3u32 {
                                match self.publish(payload.clone()).await {
                                    Ok(()) => { published = true; break; }
                                    Err(e) => {
                                        let backoff_ms = 100 * 2u64.pow(attempt);
                                        warn!(
                                            "Source '{}' publish attempt {}/3 failed: {} (retrying in {}ms)",
                                            self.name, attempt + 1, e, backoff_ms
                                        );
                                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                                    }
                                }
                            }
                            if !published {
                                if let Some(dlq_topic) = &self.dead_letter_topic {
                                    let dlq_topic = dlq_topic.clone();
                                    warn!(
                                        "Source '{}' routing failed event to DLQ topic '{}'",
                                        self.name, dlq_topic
                                    );
                                    self.publish_to_dlq(
                                        &dlq_topic,
                                        &self.config.topic,
                                        &payload,
                                        "publish failed after 3 retries",
                                        3,
                                    ).await?;
                                } else {
                                    error!("Source '{}' publish failed after 3 attempts (no DLQ configured)", self.name);
                                    *self.status.write().await = ConnectorStatus::Unhealthy;
                                    return Err(ConnectError::Source {
                                        name: self.name.clone(),
                                        message: "publish failed after 3 retries".into(),
                                    });
                                }
                            }

                            let count = self.events_published();
                            if count.is_multiple_of(1000) && count > 0 {
                                debug!("Source '{}' published {} events", self.name, count);
                            }
                        }
                        Some(Err(e)) => {
                            error!("Source '{}' registry connector error: {}", self.name, e);
                            self.errors_count.fetch_add(1, Ordering::Relaxed);
                        }
                        None => {
                            info!(
                                "Source '{}' registry connector completed after {} events",
                                self.name,
                                self.events_published()
                            );
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

/// Shared source runner for health checks
#[allow(dead_code)]
pub type SharedSourceRunner = Arc<SourceRunner>;

/// Run a source connector
pub async fn run_source(
    name: &str,
    source_config: &SourceConfig,
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

    // Build the full source registry for dynamic connector dispatch
    let source_registry = Arc::new(create_source_registry());

    // Create and run source with global topic settings
    let runner = SourceRunner::new(
        name.to_string(),
        source_config.clone(),
        config.settings.topic.clone(),
        broker.clone(),
        source_registry,
    )?;

    runner.run(shutdown_rx.resubscribe()).await
}
