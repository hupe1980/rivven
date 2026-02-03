//! HTTP Webhook Sink Connector for Rivven Connect
//!
//! This crate provides an HTTP webhook sink that sends events to HTTP endpoints.
//! Useful for serverless functions, webhooks, and event-driven architectures.
//!
//! # Features
//!
//! - POST, PUT, PATCH methods
//! - Bearer, Basic, and API Key authentication
//! - Automatic retries with exponential backoff
//! - Batching with configurable size and timeout
//! - Custom headers support
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
use super::super::traits::registry::{AnySink, SinkFactory};
use async_trait::async_trait;
use futures::StreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use validator::Validate;

use crate::types::SensitiveString;

/// HTTP Webhook sink configuration
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct HttpWebhookConfig {
    /// Target URL for the webhook
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

/// HTTP methods supported by the webhook sink
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    #[default]
    Post,
    Put,
    Patch,
}

impl HttpMethod {
    fn as_reqwest_method(&self) -> reqwest::Method {
        match self {
            HttpMethod::Post => reqwest::Method::POST,
            HttpMethod::Put => reqwest::Method::PUT,
            HttpMethod::Patch => reqwest::Method::PATCH,
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

/// HTTP Webhook Sink implementation
pub struct HttpWebhookSink {
    client: reqwest::Client,
}

impl HttpWebhookSink {
    /// Create a new HttpWebhookSink
    ///
    /// # Errors
    /// Returns an error if the HTTP client cannot be constructed (e.g., TLS initialization fails)
    pub fn try_new() -> std::result::Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        Ok(Self { client })
    }

    /// Create a new HttpWebhookSink, panicking on failure
    ///
    /// This is a convenience method for cases where failure is not expected.
    /// Prefer `try_new()` in production code.
    pub fn new() -> Self {
        Self::try_new().expect("Failed to create HTTP client - TLS initialization may have failed")
    }

    /// Build a request with auth and headers
    fn build_request(
        &self,
        config: &HttpWebhookConfig,
        payload: &serde_json::Value,
    ) -> reqwest::RequestBuilder {
        let mut request = self
            .client
            .request(config.method.as_reqwest_method(), &config.url)
            .timeout(Duration::from_secs(config.timeout_secs as u64))
            .header("Content-Type", &config.content_type)
            .json(payload);

        // Add custom headers
        for (key, value) in &config.headers {
            request = request.header(key.as_str(), value.as_str());
        }

        // Add authentication
        if let Some(ref auth) = config.auth {
            request = match auth {
                AuthConfig::Bearer { token } => {
                    request.header("Authorization", format!("Bearer {}", token.expose()))
                }
                AuthConfig::Basic { username, password } => {
                    request.basic_auth(username, Some(password.expose()))
                }
                AuthConfig::ApiKey { header_name, key } => {
                    request.header(header_name.as_str(), key.expose())
                }
            };
        }

        request
    }

    /// Send a request with retries
    async fn send_with_retry(
        &self,
        config: &HttpWebhookConfig,
        payload: &serde_json::Value,
    ) -> std::result::Result<reqwest::Response, String> {
        let mut last_error = String::new();

        for attempt in 0..=config.max_retries {
            if attempt > 0 {
                // Exponential backoff
                let backoff =
                    Duration::from_millis((config.retry_backoff_ms as u64) * 2u64.pow(attempt - 1));
                debug!("Retry attempt {} after {:?}", attempt, backoff);
                tokio::time::sleep(backoff).await;
            }

            let request = self.build_request(config, payload);

            match request.send().await {
                Ok(response) => {
                    if response.status().is_success() {
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
                        continue;
                    } else {
                        // Client error - don't retry
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
                        continue;
                    } else {
                        // Non-transient error - don't retry
                        return Err(format!("Request failed: {}", e));
                    }
                }
            }
        }

        Err(format!("Max retries exceeded. Last error: {}", last_error))
    }
}

impl Default for HttpWebhookSink {
    fn default() -> Self {
        Self::new()
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

        info!("Checking HTTP webhook connectivity to {}", config.url);

        // Try a HEAD request to check endpoint is reachable
        let result = self
            .client
            .head(&config.url)
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
        let mut batch: Vec<serde_json::Value> = Vec::with_capacity(config.batch_size);
        let mut batch_bytes: u64 = 0;
        let batch_timeout = Duration::from_millis(config.batch_timeout_ms as u64);
        let mut last_flush = std::time::Instant::now();

        info!(
            "Starting HTTP webhook sink to {} (batch_size={})",
            config.url, config.batch_size
        );

        loop {
            // Use timeout for batching
            let event = tokio::time::timeout(batch_timeout, events.next()).await;

            let (should_flush, stream_ended) = match event {
                Ok(Some(event)) => {
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
                    batch.push(payload);

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
                // Build payload (single event or array)
                let payload = if batch.len() == 1 {
                    batch.remove(0)
                } else {
                    serde_json::Value::Array(std::mem::take(&mut batch))
                };

                let count = if let serde_json::Value::Array(ref arr) = payload {
                    arr.len()
                } else {
                    1
                };

                match self.send_with_retry(config, &payload).await {
                    Ok(_) => {
                        debug!("Sent {} events to webhook", count);
                        result.add_success(count as u64, batch_bytes);
                    }
                    Err(e) => {
                        error!("Failed to send events to webhook: {}", e);
                        result.add_failure(count as u64, e);
                    }
                }

                batch.clear();
                batch_bytes = 0;
                last_flush = std::time::Instant::now();
            }

            // Check if stream ended
            if stream_ended {
                break;
            }
        }

        info!(
            "HTTP webhook sink complete. {} events sent, {} failed",
            result.records_written, result.records_failed
        );

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

    fn create(&self) -> Box<dyn AnySink> {
        Box::new(HttpWebhookSink::new())
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
        let _sink = factory.create();
    }
}
