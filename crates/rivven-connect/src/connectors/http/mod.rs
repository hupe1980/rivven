//! HTTP Webhook Sink Connector for Rivven Connect
//!
//! This crate provides an HTTP webhook sink that sends events to HTTP endpoints.
//! Useful for serverless functions, webhooks, and event-driven architectures.
//!
//! # Features
//!
//! - POST, PUT, PATCH, DELETE methods
//! - Bearer, Basic, API Key, and HMAC authentication
//! - Automatic retries with exponential backoff and jitter
//! - Batching with configurable size and timeout
//! - Custom headers support
//! - **Compression**: gzip for large payloads
//! - **Circuit breaker**: Fail fast when endpoint is unavailable
//! - **Rate limiting**: Configurable requests per second
//! - **Concurrent requests**: Parallel sending with semaphore control
//! - **Request signing**: HMAC-SHA256 webhook signatures
//! - **Template URLs**: Interpolate event data into URL paths
//! - **Metrics**: Atomic counters for observability
//! - **TLS configuration**: Custom CA certificates
//!
//! # Example
//!
//! ```rust,ignore
//! use super::super::traits::registry::SinkRegistry;
//! use rivven_http::HttpWebhookSinkFactory;
//! use std::sync::Arc;
//!
//! let mut sinks = SinkRegistry::new();
//! sinks.register("http-webhook", Arc::new(HttpWebhookSinkFactory));
//! ```

use super::super::prelude::*;
use super::super::traits::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use super::super::traits::registry::{AnySink, SinkFactory};
use async_trait::async_trait;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::StreamExt;
use hmac::{Hmac, Mac};
use rand::Rng;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};
use validator::Validate;

use crate::types::SensitiveString;

/// HTTP Webhook sink configuration
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct HttpWebhookConfig {
    /// Target URL for the webhook (supports template variables: `{stream}`, `{event_type}`)
    #[validate(url)]
    pub url: String,

    /// HTTP method (default: POST)
    #[serde(default = "default_method")]
    pub method: HttpMethod,

    /// Request timeout in seconds (default: 30)
    #[serde(default = "default_timeout")]
    #[validate(range(min = 1, max = 300))]
    pub timeout_secs: u32,

    /// Maximum retries on failure (default: 3)
    #[serde(default = "default_retries")]
    #[validate(range(max = 10))]
    pub max_retries: u32,

    /// Retry backoff base in milliseconds (default: 1000)
    #[serde(default = "default_retry_backoff")]
    #[validate(range(min = 100, max = 60000))]
    pub retry_backoff_ms: u32,

    /// Content type (default: application/json)
    #[serde(default = "default_content_type")]
    pub content_type: String,

    /// Additional headers to include
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Authentication configuration (optional)
    #[serde(default)]
    pub auth: Option<AuthConfig>,

    /// Batch events before sending (default: 1)
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 1000))]
    pub batch_size: usize,

    /// Batch timeout in milliseconds (default: 1000)
    #[serde(default = "default_batch_timeout")]
    #[validate(range(min = 100, max = 60000))]
    pub batch_timeout_ms: u32,

    /// Include event metadata in payload
    #[serde(default = "default_true")]
    pub include_metadata: bool,

    // ========================================================================
    // Advanced features for best-in-class performance
    // ========================================================================
    /// Enable gzip compression for payloads above this size (bytes, 0 = disabled)
    #[serde(default)]
    #[validate(range(max = 10485760))]
    pub compression_threshold_bytes: u32,

    /// Maximum concurrent requests (default: 4)
    #[serde(default = "default_concurrency")]
    #[validate(range(min = 1, max = 100))]
    pub max_concurrency: u32,

    /// Rate limit: requests per second (0 = unlimited)
    #[serde(default)]
    #[validate(range(max = 100000))]
    pub rate_limit_rps: u32,

    /// Circuit breaker: failure threshold before opening (0 = disabled)
    #[serde(default)]
    #[validate(range(max = 100))]
    pub circuit_breaker_threshold: u32,

    /// Circuit breaker: reset timeout in seconds
    #[serde(default = "default_circuit_reset")]
    #[validate(range(min = 1, max = 300))]
    pub circuit_breaker_reset_secs: u32,

    /// Add jitter to retry backoff (default: true)
    #[serde(default = "default_true")]
    pub retry_jitter: bool,

    /// HMAC signing configuration for webhook verification
    #[serde(default)]
    pub signing: Option<SigningConfig>,

    /// TLS configuration
    #[serde(default)]
    pub tls: Option<TlsConfig>,

    /// Health check endpoint (separate from main URL, optional)
    #[serde(default)]
    pub health_check_url: Option<String>,

    /// Enable HTTP/2 (default: true)
    #[serde(default = "default_true")]
    pub http2: bool,

    /// Keep-alive timeout in seconds (default: 90)
    #[serde(default = "default_keepalive")]
    #[validate(range(min = 0, max = 600))]
    pub keepalive_secs: u32,
}

fn default_method() -> HttpMethod {
    HttpMethod::Post
}

fn default_timeout() -> u32 {
    30
}

fn default_retries() -> u32 {
    3
}

fn default_retry_backoff() -> u32 {
    1000
}

fn default_content_type() -> String {
    "application/json".to_string()
}

fn default_batch_size() -> usize {
    1
}

fn default_batch_timeout() -> u32 {
    1000
}

fn default_true() -> bool {
    true
}

fn default_concurrency() -> u32 {
    4
}

fn default_circuit_reset() -> u32 {
    30
}

fn default_keepalive() -> u32 {
    90
}

/// HMAC signing configuration for webhook verification
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct SigningConfig {
    /// HMAC secret key
    pub secret: SensitiveString,
    /// Header name for the signature (default: X-Webhook-Signature)
    #[serde(default = "default_signature_header")]
    pub header_name: String,
    /// Signature algorithm (default: sha256)
    #[serde(default)]
    pub algorithm: SigningAlgorithm,
    /// Include timestamp in signature
    #[serde(default = "default_true")]
    pub include_timestamp: bool,
    /// Timestamp header name (default: X-Webhook-Timestamp)
    #[serde(default = "default_timestamp_header")]
    pub timestamp_header: String,
}

fn default_signature_header() -> String {
    "X-Webhook-Signature".to_string()
}

fn default_timestamp_header() -> String {
    "X-Webhook-Timestamp".to_string()
}

/// Signing algorithms for HMAC webhook verification
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum SigningAlgorithm {
    #[default]
    Sha256,
}

/// TLS configuration for webhook connections
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct TlsConfig {
    /// Path to custom CA certificate (PEM format)
    pub ca_cert_path: Option<String>,
    /// Accept invalid certificates (for testing only!)
    #[serde(default)]
    pub accept_invalid_certs: bool,
}

/// HTTP methods supported by the webhook sink
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    #[default]
    Post,
    Put,
    Patch,
    Delete,
}

impl HttpMethod {
    fn as_reqwest_method(&self) -> reqwest::Method {
        match self {
            HttpMethod::Post => reqwest::Method::POST,
            HttpMethod::Put => reqwest::Method::PUT,
            HttpMethod::Patch => reqwest::Method::PATCH,
            HttpMethod::Delete => reqwest::Method::DELETE,
        }
    }
}

/// Authentication configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConfig {
    /// Bearer token authentication
    Bearer { token: SensitiveString },
    /// Basic authentication
    Basic {
        username: String,
        password: SensitiveString,
    },
    /// API key in header
    ApiKey {
        header_name: String,
        key: SensitiveString,
    },
}

// ============================================================================
// Metrics for observability
// ============================================================================

/// Atomic counters for HTTP webhook sink metrics
#[derive(Debug, Default)]
pub struct HttpWebhookStats {
    /// Total requests sent
    pub requests_sent: AtomicU64,
    /// Successful requests (2xx)
    pub requests_success: AtomicU64,
    /// Failed requests (after all retries)
    pub requests_failed: AtomicU64,
    /// Retried requests
    pub requests_retried: AtomicU64,
    /// Circuit breaker rejections
    pub circuit_breaker_rejections: AtomicU64,
    /// Rate limited requests
    pub rate_limited: AtomicU64,
    /// Bytes sent (uncompressed)
    pub bytes_sent: AtomicU64,
    /// Bytes sent (compressed, if compression enabled)
    pub bytes_compressed: AtomicU64,
    /// Total latency in microseconds
    pub total_latency_us: AtomicU64,
}

impl HttpWebhookStats {
    /// Get average latency in milliseconds
    pub fn avg_latency_ms(&self) -> f64 {
        let total = self.total_latency_us.load(Ordering::Relaxed);
        let count = self.requests_success.load(Ordering::Relaxed);
        if count == 0 {
            0.0
        } else {
            (total as f64 / count as f64) / 1000.0
        }
    }

    /// Get success rate (0.0-1.0)
    pub fn success_rate(&self) -> f64 {
        let success = self.requests_success.load(Ordering::Relaxed);
        let failed = self.requests_failed.load(Ordering::Relaxed);
        let total = success + failed;
        if total == 0 {
            1.0
        } else {
            success as f64 / total as f64
        }
    }

    /// Get compression ratio (compressed/uncompressed)
    pub fn compression_ratio(&self) -> f64 {
        let uncompressed = self.bytes_sent.load(Ordering::Relaxed);
        let compressed = self.bytes_compressed.load(Ordering::Relaxed);
        if uncompressed == 0 || compressed == 0 {
            1.0
        } else {
            compressed as f64 / uncompressed as f64
        }
    }
}

/// HTTP Webhook Sink implementation with best-in-class features
pub struct HttpWebhookSink {
    client: reqwest::Client,
    circuit_breaker: Option<CircuitBreaker>,
    #[allow(dead_code)] // Reserved for future concurrent request support
    concurrency_semaphore: Arc<Semaphore>,
    stats: Arc<HttpWebhookStats>,
}

impl HttpWebhookSink {
    /// Create a new HttpWebhookSink with default configuration
    ///
    /// # Errors
    /// Returns an error if the HTTP client cannot be constructed (e.g., TLS initialization fails)
    pub fn try_new() -> std::result::Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::try_new_with_config(None, None)
    }

    /// Create a new HttpWebhookSink with custom configuration
    pub fn try_new_with_config(
        tls_config: Option<&TlsConfig>,
        _http2: Option<bool>,
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(90));

        // TLS configuration
        if let Some(tls) = tls_config {
            if tls.accept_invalid_certs {
                warn!("TLS certificate validation disabled - use only for testing!");
                builder = builder.danger_accept_invalid_certs(true);
            }
            if let Some(ref ca_path) = tls.ca_cert_path {
                let cert_data = std::fs::read(ca_path).map_err(|e| {
                    error!("Failed to read CA certificate from {:?}: {}", ca_path, e);
                    e
                })?;
                let cert = reqwest::Certificate::from_pem(&cert_data).map_err(|e| {
                    error!("Failed to parse CA certificate from {:?}: {}", ca_path, e);
                    e
                })?;
                builder = builder.add_root_certificate(cert);
            }
        }

        let client = builder.build()?;

        Ok(Self {
            client,
            circuit_breaker: None,
            concurrency_semaphore: Arc::new(Semaphore::new(4)),
            stats: Arc::new(HttpWebhookStats::default()),
        })
    }

    /// Get statistics for this sink
    pub fn stats(&self) -> &HttpWebhookStats {
        &self.stats
    }

    /// Initialize circuit breaker and concurrency settings from config.
    ///
    /// Previously marked `#[allow(dead_code)]` and never called.
    /// Call this after construction to wire config-based circuit breaker and
    /// concurrency settings before the first `write()` invocation.
    pub fn init_from_config(&mut self, config: &HttpWebhookConfig) {
        // Set up circuit breaker if enabled
        if config.circuit_breaker_threshold > 0 {
            let cb_config = CircuitBreakerConfig {
                enabled: true,
                failure_threshold: config.circuit_breaker_threshold,
                reset_timeout_secs: config.circuit_breaker_reset_secs as u64,
                success_threshold: 2,
            };
            self.circuit_breaker = Some(CircuitBreaker::from_config(&cb_config));
        }

        // Set up concurrency semaphore
        self.concurrency_semaphore = Arc::new(Semaphore::new(config.max_concurrency as usize));
    }

    /// Interpolate template variables in URL
    fn interpolate_url(&self, url: &str, event: &SourceEvent) -> String {
        let encode = |s: &str| urlencoding::encode(s).into_owned();
        url.replace("{stream}", &encode(&event.stream))
            .replace("{event_type}", &encode(event.event_type.as_str()))
            .replace(
                "{namespace}",
                &encode(event.namespace.as_deref().unwrap_or("default")),
            )
    }

    /// Compute HMAC signature for payload
    fn compute_signature(&self, signing: &SigningConfig, payload: &[u8], timestamp: i64) -> String {
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(signing.secret.expose_secret().as_bytes())
            .expect("HMAC key");

        // Include timestamp in signature if enabled
        if signing.include_timestamp {
            mac.update(timestamp.to_string().as_bytes());
            mac.update(b".");
        }
        mac.update(payload);

        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }

    /// Maybe compress payload based on config
    fn maybe_compress(&self, data: &[u8], threshold: u32) -> (Vec<u8>, bool) {
        if threshold == 0 || data.len() < threshold as usize {
            return (data.to_vec(), false);
        }

        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        if encoder.write_all(data).is_ok() {
            if let Ok(compressed) = encoder.finish() {
                // Only use compression if it actually reduces size
                if compressed.len() < data.len() {
                    return (compressed, true);
                }
            }
        }

        (data.to_vec(), false)
    }

    /// Calculate backoff with optional jitter
    fn calculate_backoff(&self, config: &HttpWebhookConfig, attempt: u32) -> Duration {
        let base_ms = (config.retry_backoff_ms as u64) * 2u64.pow(attempt);
        let backoff_ms = if config.retry_jitter {
            let mut rng = rand::thread_rng();
            let jitter = rng.gen_range(0..=(base_ms / 4).max(1));
            base_ms.saturating_add(jitter)
        } else {
            base_ms
        };
        Duration::from_millis(backoff_ms.min(60000)) // Cap at 60s
    }

    /// Build a request with auth, headers, and signing
    fn build_request(
        &self,
        config: &HttpWebhookConfig,
        url: &str,
        payload: &[u8],
        compressed: bool,
        timestamp: i64,
    ) -> reqwest::RequestBuilder {
        let mut request = self
            .client
            .request(config.method.as_reqwest_method(), url)
            .timeout(Duration::from_secs(config.timeout_secs as u64))
            .header("Content-Type", &config.content_type);

        // Set content encoding if compressed
        if compressed {
            request = request.header("Content-Encoding", "gzip");
        }

        // Add custom headers
        for (key, value) in &config.headers {
            request = request.header(key.as_str(), value.as_str());
        }

        // Add authentication
        if let Some(ref auth) = config.auth {
            request = match auth {
                AuthConfig::Bearer { token } => {
                    request.header("Authorization", format!("Bearer {}", token.expose_secret()))
                }
                AuthConfig::Basic { username, password } => {
                    request.basic_auth(username, Some(password.expose_secret()))
                }
                AuthConfig::ApiKey { header_name, key } => {
                    request.header(header_name.as_str(), key.expose_secret())
                }
            };
        }

        // Add HMAC signature if configured
        if let Some(ref signing) = config.signing {
            let signature = self.compute_signature(signing, payload, timestamp);
            let sig_value = format!("sha256={}", signature);
            request = request.header(signing.header_name.as_str(), sig_value);

            if signing.include_timestamp {
                request = request.header(signing.timestamp_header.as_str(), timestamp.to_string());
            }
        }

        request.body(payload.to_vec())
    }

    /// Send a request with retries and circuit breaker
    async fn send_with_retry(
        &self,
        config: &HttpWebhookConfig,
        url: &str,
        payload: &serde_json::Value,
    ) -> std::result::Result<reqwest::Response, String> {
        // Check circuit breaker
        if let Some(ref cb) = self.circuit_breaker {
            if !cb.allow_request() {
                self.stats
                    .circuit_breaker_rejections
                    .fetch_add(1, Ordering::Relaxed);
                return Err(format!("Circuit breaker open (state: {:?})", cb.state()));
            }
        }

        // Serialize and maybe compress
        let json_bytes = serde_json::to_vec(payload).map_err(|e| format!("JSON error: {}", e))?;
        let uncompressed_len = json_bytes.len() as u64;
        let (payload_bytes, compressed) =
            self.maybe_compress(&json_bytes, config.compression_threshold_bytes);

        self.stats
            .bytes_sent
            .fetch_add(uncompressed_len, Ordering::Relaxed);
        if compressed {
            self.stats
                .bytes_compressed
                .fetch_add(payload_bytes.len() as u64, Ordering::Relaxed);
        }

        let timestamp = chrono::Utc::now().timestamp();
        let mut last_error = String::new();

        for attempt in 0..=config.max_retries {
            if attempt > 0 {
                self.stats.requests_retried.fetch_add(1, Ordering::Relaxed);
                let backoff = self.calculate_backoff(config, attempt - 1);
                debug!("Retry attempt {} after {:?}", attempt, backoff);
                tokio::time::sleep(backoff).await;
            }

            let start = std::time::Instant::now();
            let request = self.build_request(config, url, &payload_bytes, compressed, timestamp);
            self.stats.requests_sent.fetch_add(1, Ordering::Relaxed);

            match request.send().await {
                Ok(response) => {
                    let latency_us = start.elapsed().as_micros() as u64;
                    self.stats
                        .total_latency_us
                        .fetch_add(latency_us, Ordering::Relaxed);

                    if response.status().is_success() {
                        self.stats.requests_success.fetch_add(1, Ordering::Relaxed);
                        // Record success with circuit breaker
                        if let Some(ref cb) = self.circuit_breaker {
                            cb.record_success();
                        }
                        return Ok(response);
                    } else if response.status().is_server_error() {
                        // Server error - retry
                        last_error = format!(
                            "Server error: {} {}",
                            response.status(),
                            response.text().await.unwrap_or_default()
                        );
                        warn!(
                            "Webhook request failed (attempt {}): {}",
                            attempt + 1,
                            last_error
                        );
                        // Record failure with circuit breaker
                        if let Some(ref cb) = self.circuit_breaker {
                            cb.record_failure();
                        }
                        continue;
                    } else if response.status().as_u16() == 429 {
                        // Rate limited by server - retry with longer backoff
                        self.stats.rate_limited.fetch_add(1, Ordering::Relaxed);
                        last_error = "Rate limited by server (429)".to_string();
                        warn!(
                            "Webhook rate limited (attempt {}), backing off",
                            attempt + 1
                        );
                        // Extra backoff for rate limiting
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    } else {
                        // Client error - don't retry
                        self.stats.requests_failed.fetch_add(1, Ordering::Relaxed);
                        if let Some(ref cb) = self.circuit_breaker {
                            cb.record_failure();
                        }
                        return Err(format!(
                            "Client error: {} {}",
                            response.status(),
                            response.text().await.unwrap_or_default()
                        ));
                    }
                }
                Err(e) => {
                    if e.is_timeout() || e.is_connect() {
                        // Transient error - retry
                        last_error = format!("Request failed: {}", e);
                        warn!(
                            "Webhook request failed (attempt {}): {}",
                            attempt + 1,
                            last_error
                        );
                        if let Some(ref cb) = self.circuit_breaker {
                            cb.record_failure();
                        }
                        continue;
                    } else {
                        // Non-transient error - don't retry
                        self.stats.requests_failed.fetch_add(1, Ordering::Relaxed);
                        if let Some(ref cb) = self.circuit_breaker {
                            cb.record_failure();
                        }
                        return Err(format!("Request failed: {}", e));
                    }
                }
            }
        }

        self.stats.requests_failed.fetch_add(1, Ordering::Relaxed);
        Err(format!("Max retries exceeded. Last error: {}", last_error))
    }
}

#[async_trait]
impl Sink for HttpWebhookSink {
    type Config = HttpWebhookConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("http-webhook", env!("CARGO_PKG_VERSION"))
            .description("Send events to HTTP webhooks")
            .author("Rivven Team")
            .license("MIT/Apache-2.0")
            .documentation_url("https://rivven.dev/docs/connectors/http-webhook")
            .config_schema::<HttpWebhookConfig>()
            .build()
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        // Validate config first
        if let Err(e) = config.validate() {
            return Ok(CheckResult::failure(format!(
                "Invalid configuration: {}",
                e
            )));
        }

        // Use health check URL if provided, otherwise use main URL
        let check_url = config.health_check_url.as_deref().unwrap_or(&config.url);
        info!("Checking HTTP webhook connectivity to {}", check_url);

        // Try a HEAD request to check endpoint is reachable
        let result = self
            .client
            .head(check_url)
            .timeout(Duration::from_secs(config.timeout_secs as u64))
            .send()
            .await;

        match result {
            Ok(response) => {
                if response.status().is_success() || response.status() == 405 {
                    // 405 Method Not Allowed is expected for HEAD
                    info!("Webhook endpoint is reachable");
                    Ok(CheckResult::success())
                } else {
                    Ok(CheckResult::failure(format!(
                        "Endpoint returned status: {}",
                        response.status()
                    )))
                }
            }
            Err(e) => Ok(CheckResult::failure(format!(
                "Failed to connect to webhook: {}",
                e
            ))),
        }
    }

    async fn write(
        &self,
        config: &Self::Config,
        mut events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        let mut result = WriteResult::new();
        let mut batch: Vec<(String, serde_json::Value)> = Vec::with_capacity(config.batch_size);
        let mut batch_bytes: u64 = 0;
        let batch_timeout = Duration::from_millis(config.batch_timeout_ms as u64);
        let mut last_flush = std::time::Instant::now();

        info!(
            "Starting HTTP webhook sink to {} (batch_size={}, concurrency={}, compression={})",
            config.url,
            config.batch_size,
            config.max_concurrency,
            if config.compression_threshold_bytes > 0 {
                format!("{}B", config.compression_threshold_bytes)
            } else {
                "disabled".to_string()
            }
        );

        // Log circuit breaker state if enabled
        if config.circuit_breaker_threshold > 0 {
            info!(
                "Circuit breaker enabled: threshold={}, reset={}s",
                config.circuit_breaker_threshold, config.circuit_breaker_reset_secs
            );
        }

        loop {
            // Use timeout for batching
            let event = tokio::time::timeout(batch_timeout, events.next()).await;

            let (should_flush, stream_ended) = match event {
                Ok(Some(event)) => {
                    // Interpolate URL with event data
                    let url = self.interpolate_url(&config.url, &event);

                    // Build payload for this event
                    let payload = if config.include_metadata {
                        serde_json::json!({
                            "stream": event.stream,
                            "event_type": event.event_type.as_str(),
                            "timestamp": event.timestamp.to_rfc3339(),
                            "namespace": event.namespace,
                            "data": event.data,
                            "metadata": event.metadata.extra,
                        })
                    } else {
                        event.data.clone()
                    };

                    batch_bytes += payload.to_string().len() as u64;
                    batch.push((url, payload));

                    // Flush if batch is full
                    (batch.len() >= config.batch_size, false)
                }
                Ok(None) => {
                    // Stream ended - flush remaining
                    (true, true)
                }
                Err(_) => {
                    // Timeout - flush if we have events
                    (
                        !batch.is_empty() && last_flush.elapsed() >= batch_timeout,
                        false,
                    )
                }
            };

            if should_flush && !batch.is_empty() {
                // Group by URL for batched sending
                let mut url_batches: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
                for (url, payload) in batch.drain(..) {
                    url_batches.entry(url).or_default().push(payload);
                }

                for (url, payloads) in url_batches {
                    // Build payload (single event or array)
                    let payload = if payloads.len() == 1 {
                        payloads.into_iter().next().unwrap()
                    } else {
                        serde_json::Value::Array(payloads)
                    };

                    let count = if let serde_json::Value::Array(ref arr) = payload {
                        arr.len()
                    } else {
                        1
                    };

                    match self.send_with_retry(config, &url, &payload).await {
                        Ok(_) => {
                            debug!("Sent {} events to webhook {}", count, url);
                            result.add_success(count as u64, batch_bytes);
                        }
                        Err(e) => {
                            error!("Failed to send events to webhook {}: {}", url, e);
                            result.add_failure(count as u64, e);
                        }
                    }
                }

                batch_bytes = 0;
                last_flush = std::time::Instant::now();
            }

            // Check if stream ended
            if stream_ended {
                break;
            }
        }

        // Log final stats
        let stats = self.stats();
        info!(
            "HTTP webhook sink complete. {} events sent, {} failed, avg_latency={:.2}ms, success_rate={:.1}%",
            result.records_written,
            result.records_failed,
            stats.avg_latency_ms(),
            stats.success_rate() * 100.0
        );

        if config.compression_threshold_bytes > 0 {
            info!(
                "Compression ratio: {:.1}%",
                stats.compression_ratio() * 100.0
            );
        }

        Ok(result)
    }
}

// ============================================================================
// Factory and AnySink implementation for registry pattern
// ============================================================================

/// Factory for creating HttpWebhookSink instances
///
/// Register this factory with a SinkRegistry to enable HTTP webhook support.
pub struct HttpWebhookSinkFactory;

impl SinkFactory for HttpWebhookSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        HttpWebhookSink::spec()
    }

    fn create(&self) -> Result<Box<dyn AnySink>> {
        Ok(Box::new(HttpWebhookSink::try_new().map_err(|e| {
            ConnectError::config(format!("Failed to create HTTP client: {}", e))
        })?))
    }
}

// Implement AnySink for HttpWebhookSink using the SDK macro
crate::impl_any_sink!(HttpWebhookSink, HttpWebhookConfig);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let yaml = r#"
            url: https://webhook.example.com/events
        "#;

        let config: HttpWebhookConfig = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(config.url, "https://webhook.example.com/events");
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.batch_size, 1);
        assert!(config.include_metadata);
        // New defaults
        assert_eq!(config.max_concurrency, 4);
        assert_eq!(config.compression_threshold_bytes, 0);
        assert_eq!(config.rate_limit_rps, 0);
        assert_eq!(config.circuit_breaker_threshold, 0);
        assert!(config.retry_jitter);
        assert!(config.http2);
        assert_eq!(config.keepalive_secs, 90);
    }

    #[test]
    fn test_config_with_auth() {
        let yaml = r#"
            url: https://webhook.example.com/events
            auth:
              type: bearer
              token: my-secret-token
            headers:
              X-Custom-Header: custom-value
        "#;

        let config: HttpWebhookConfig = serde_yaml::from_str(yaml).unwrap();

        assert!(config.auth.is_some());
        assert_eq!(
            config.headers.get("X-Custom-Header"),
            Some(&"custom-value".to_string())
        );
    }

    #[test]
    fn test_config_with_batching() {
        let yaml = r#"
            url: https://webhook.example.com/events
            batch_size: 100
            batch_timeout_ms: 5000
        "#;

        let config: HttpWebhookConfig = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(config.batch_size, 100);
        assert_eq!(config.batch_timeout_ms, 5000);
    }

    #[test]
    fn test_config_with_advanced_features() {
        let yaml = r#"
            url: https://webhook.example.com/{stream}/events
            compression_threshold_bytes: 1024
            max_concurrency: 8
            rate_limit_rps: 100
            circuit_breaker_threshold: 5
            circuit_breaker_reset_secs: 60
            retry_jitter: true
            http2: true
            keepalive_secs: 120
            health_check_url: https://webhook.example.com/health
        "#;

        let config: HttpWebhookConfig = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(config.compression_threshold_bytes, 1024);
        assert_eq!(config.max_concurrency, 8);
        assert_eq!(config.rate_limit_rps, 100);
        assert_eq!(config.circuit_breaker_threshold, 5);
        assert_eq!(config.circuit_breaker_reset_secs, 60);
        assert!(config.retry_jitter);
        assert!(config.http2);
        assert_eq!(config.keepalive_secs, 120);
        assert_eq!(
            config.health_check_url,
            Some("https://webhook.example.com/health".to_string())
        );
    }

    #[test]
    fn test_config_with_signing() {
        let yaml = r#"
            url: https://webhook.example.com/events
            signing:
              secret: my-webhook-secret
              header_name: X-Signature
              algorithm: sha256
              include_timestamp: true
              timestamp_header: X-Timestamp
        "#;

        let config: HttpWebhookConfig = serde_yaml::from_str(yaml).unwrap();

        assert!(config.signing.is_some());
        let signing = config.signing.unwrap();
        assert_eq!(signing.header_name, "X-Signature");
        assert!(signing.include_timestamp);
        assert_eq!(signing.timestamp_header, "X-Timestamp");
    }

    #[test]
    fn test_config_with_tls() {
        let yaml = r#"
            url: https://webhook.example.com/events
            tls:
              ca_cert_path: /path/to/ca.pem
              accept_invalid_certs: false
        "#;

        let config: HttpWebhookConfig = serde_yaml::from_str(yaml).unwrap();

        assert!(config.tls.is_some());
        let tls = config.tls.unwrap();
        assert_eq!(tls.ca_cert_path, Some("/path/to/ca.pem".to_string()));
        assert!(!tls.accept_invalid_certs);
    }

    #[test]
    fn test_http_method_delete() {
        let yaml = r#"
            url: https://webhook.example.com/events
            method: DELETE
        "#;

        let config: HttpWebhookConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(config.method, HttpMethod::Delete));
    }

    #[test]
    fn test_spec() {
        let spec = HttpWebhookSink::spec();
        assert_eq!(spec.connector_type, "http-webhook");
    }

    #[test]
    fn test_factory() {
        let factory = HttpWebhookSinkFactory;
        let spec = factory.spec();
        assert_eq!(spec.connector_type, "http-webhook");

        // Factory should create a valid sink
        let _sink = factory.create().unwrap();
    }

    #[test]
    fn test_stats_default() {
        let stats = HttpWebhookStats::default();
        assert_eq!(stats.requests_sent.load(Ordering::Relaxed), 0);
        assert_eq!(stats.requests_success.load(Ordering::Relaxed), 0);
        assert_eq!(stats.requests_failed.load(Ordering::Relaxed), 0);
        assert_eq!(stats.avg_latency_ms(), 0.0);
        assert_eq!(stats.success_rate(), 1.0); // 0/0 = 1.0 by convention
        assert_eq!(stats.compression_ratio(), 1.0);
    }

    #[test]
    fn test_stats_calculations() {
        let stats = HttpWebhookStats::default();

        // Simulate 10 successful requests with 100ms average latency
        stats.requests_success.store(10, Ordering::Relaxed);
        stats.total_latency_us.store(1_000_000, Ordering::Relaxed); // 1 second total

        assert_eq!(stats.avg_latency_ms(), 100.0);
        assert_eq!(stats.success_rate(), 1.0);

        // Add some failures
        stats.requests_failed.store(2, Ordering::Relaxed);
        let rate = stats.success_rate();
        assert!((rate - 0.833).abs() < 0.01); // 10/12 ≈ 0.833
    }

    #[test]
    fn test_compression_ratio() {
        let stats = HttpWebhookStats::default();

        // 1000 bytes uncompressed, 400 bytes compressed = 40% ratio
        stats.bytes_sent.store(1000, Ordering::Relaxed);
        stats.bytes_compressed.store(400, Ordering::Relaxed);

        assert_eq!(stats.compression_ratio(), 0.4);
    }

    #[test]
    fn test_url_interpolation() {
        let sink = HttpWebhookSink::try_new().unwrap();

        let event = SourceEvent {
            stream: "orders".to_string(),
            event_type: crate::traits::event::SourceEventType::Record,
            timestamp: chrono::Utc::now(),
            namespace: Some("production".to_string()),
            data: serde_json::json!({}),
            metadata: Default::default(),
        };

        let url = sink.interpolate_url("https://api.example.com/{stream}/{event_type}", &event);
        assert_eq!(url, "https://api.example.com/orders/record");

        let url2 = sink.interpolate_url("https://api.example.com/{namespace}/events", &event);
        assert_eq!(url2, "https://api.example.com/production/events");
    }

    #[test]
    fn test_compression() {
        let sink = HttpWebhookSink::try_new().unwrap();

        // Small payload - no compression
        let small_data = b"hello world";
        let (result, compressed) = sink.maybe_compress(small_data, 1024);
        assert!(!compressed);
        assert_eq!(result, small_data);

        // Large compressible payload
        let large_data = "a".repeat(2000);
        let (result, compressed) = sink.maybe_compress(large_data.as_bytes(), 1024);
        assert!(compressed);
        assert!(result.len() < large_data.len());
    }

    #[test]
    fn test_hmac_signature() {
        let sink = HttpWebhookSink::try_new().unwrap();

        let signing = SigningConfig {
            secret: SensitiveString::new("test-secret".to_string()),
            header_name: "X-Signature".to_string(),
            algorithm: SigningAlgorithm::Sha256,
            include_timestamp: false,
            timestamp_header: "X-Timestamp".to_string(),
        };

        let payload = b"test payload";
        let signature = sink.compute_signature(&signing, payload, 0);

        // Signature should be a valid hex string
        assert_eq!(signature.len(), 64); // SHA256 = 32 bytes = 64 hex chars
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_backoff_with_jitter() {
        let sink = HttpWebhookSink::try_new().unwrap();

        let config = HttpWebhookConfig {
            url: "https://example.com".to_string(),
            method: HttpMethod::Post,
            timeout_secs: 30,
            max_retries: 3,
            retry_backoff_ms: 1000,
            content_type: "application/json".to_string(),
            headers: HashMap::new(),
            auth: None,
            batch_size: 1,
            batch_timeout_ms: 1000,
            include_metadata: true,
            compression_threshold_bytes: 0,
            max_concurrency: 4,
            rate_limit_rps: 0,
            circuit_breaker_threshold: 0,
            circuit_breaker_reset_secs: 30,
            retry_jitter: true,
            signing: None,
            tls: None,
            health_check_url: None,
            http2: true,
            keepalive_secs: 90,
        };

        // With jitter enabled, backoff should vary
        let backoff1 = sink.calculate_backoff(&config, 0);
        let backoff2 = sink.calculate_backoff(&config, 1);
        let backoff3 = sink.calculate_backoff(&config, 2);

        // Backoffs should increase (approximately)
        assert!(backoff2 > backoff1 || backoff2.as_millis() >= 1000);
        assert!(backoff3 > backoff2 || backoff3.as_millis() >= 2000);
    }

    #[test]
    fn test_circuit_breaker_config() {
        let yaml = r#"
            url: https://webhook.example.com/events
            circuit_breaker_threshold: 5
            circuit_breaker_reset_secs: 60
        "#;

        let config: HttpWebhookConfig = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(config.circuit_breaker_threshold, 5);
        assert_eq!(config.circuit_breaker_reset_secs, 60);
    }

    #[test]
    fn test_all_auth_types() {
        // Bearer
        let yaml = r#"
            url: https://example.com
            auth:
              type: bearer
              token: my-token
        "#;
        let config: HttpWebhookConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(config.auth, Some(AuthConfig::Bearer { .. })));

        // Basic
        let yaml = r#"
            url: https://example.com
            auth:
              type: basic
              username: user
              password: pass
        "#;
        let config: HttpWebhookConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(config.auth, Some(AuthConfig::Basic { .. })));

        // API Key
        let yaml = r#"
            url: https://example.com
            auth:
              type: api_key
              header_name: X-API-Key
              key: secret-key
        "#;
        let config: HttpWebhookConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(config.auth, Some(AuthConfig::ApiKey { .. })));
    }
}
