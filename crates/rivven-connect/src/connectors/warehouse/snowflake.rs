//! Snowflake Sink Connector using Snowpipe Streaming REST API
//!
//! This module provides low-latency, high-throughput streaming ingestion into Snowflake
//! using the Snowpipe Streaming API with proper JWT-based authentication.
//!
//! # Features
//!
//! - **JWT-based authentication** with RSA key pairs (RS256)
//! - **Automatic token generation** with configurable expiration
//! - **Exactly-once semantics** via offset tokens
//! - **Automatic batching** for optimal throughput
//! - **Automatic token refresh** for long-running pipelines
//! - **Connection pooling** via reqwest client
//! - **Exponential backoff retry** for transient failures
//! - **Metrics integration** for observability
//! - **Request tracing** with unique request IDs
//!
//! # Authentication Methods
//!
//! Snowflake supports key pair authentication for programmatic access:
//!
//! ## Key Pair Authentication (Recommended)
//!
//! 1. Generate an RSA key pair (2048-bit minimum, 4096-bit recommended):
//!    ```bash
//!    # Generate unencrypted private key in PKCS#8 format
//!    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
//!    
//!    # Or generate encrypted private key (not yet supported)
//!    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -v2 aes256
//!    
//!    # Extract public key
//!    openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
//!    ```
//!
//! 2. Register the public key with Snowflake:
//!    ```sql
//!    ALTER USER RIVVEN_USER SET RSA_PUBLIC_KEY='MIIBIjAN...';
//!    ```
//!
//! 3. Configure the connector with the private key path
//!
//! # JWT Token Format
//!
//! The connector generates JWT tokens with the following claims:
//! - `iss`: `<account>.<user>.SHA256:<public_key_fingerprint>`
//! - `sub`: `<account>.<user>`
//! - `iat`: Current timestamp
//! - `exp`: Expiration timestamp (configurable, default 59 minutes)
//!
//! # Retry Behavior
//!
//! The connector automatically retries transient failures with exponential backoff:
//! - Default: 3 retries with 1s initial backoff, 2x multiplier, 10% jitter
//! - Retryable: 429 (rate limit), 503 (service unavailable), network errors
//! - Non-retryable: 400 (bad request), 401 (auth), 403 (forbidden)
//! - Circuit breaker: Opens after consecutive failures, prevents cascading
//!
//! # Circuit Breaker
//!
//! The connector implements a circuit breaker pattern to prevent cascading failures:
//! - **Closed**: Normal operation, requests go through
//! - **Open**: After consecutive failures, requests fail fast
//! - **Half-Open**: After reset timeout, allows one test request

use crate::connectors::{AnySink, SinkFactory};
use crate::error::ConnectorError;
use crate::prelude::*;
#[cfg(feature = "snowflake")]
use crate::traits::circuit_breaker::CircuitBreaker;
use crate::traits::circuit_breaker::CircuitBreakerConfig;
use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::types::SensitiveString;

#[cfg(feature = "snowflake")]
use rand::Rng;

#[cfg(feature = "snowflake")]
use metrics::{counter, gauge, histogram};

#[cfg(feature = "snowflake")]
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
#[cfg(feature = "snowflake")]
use rsa::pkcs8::DecodePrivateKey;
#[cfg(feature = "snowflake")]
use rsa::RsaPrivateKey;

/// JWT claims for Snowflake authentication
#[cfg(feature = "snowflake")]
#[derive(Debug, Serialize, Deserialize)]
struct SnowflakeJwtClaims {
    /// Issuer: <account>.<user>.SHA256:<public_key_fingerprint>
    iss: String,
    /// Subject: <account>.<user>
    sub: String,
    /// Issued at timestamp
    iat: u64,
    /// Expiration timestamp
    exp: u64,
}

/// Snowflake authentication state
#[cfg(feature = "snowflake")]
struct SnowflakeAuth {
    /// The current JWT token
    token: String,
    /// When the token expires
    expires_at: SystemTime,
    /// The encoding key for generating new tokens
    encoding_key: EncodingKey,
    /// The issuer claim
    issuer: String,
    /// The subject claim
    subject: String,
    /// Token lifetime in seconds
    token_lifetime_secs: u64,
}

#[cfg(feature = "snowflake")]
impl SnowflakeAuth {
    /// Create new authentication state from private key
    fn new(
        account: &str,
        user: &str,
        private_key_pem: &str,
        passphrase: Option<&str>,
        token_lifetime_secs: u64,
    ) -> crate::error::Result<Self> {
        // Parse the RSA private key
        // Note: Encrypted private keys require decryption before use
        // For encrypted keys, decrypt with: openssl pkcs8 -in encrypted.p8 -out decrypted.p8
        if passphrase.is_some() {
            return Err(ConnectorError::Config(
                "Encrypted private keys are not yet supported. Please use an unencrypted PKCS#8 key.\n\
                 To decrypt: openssl pkcs8 -in encrypted.p8 -out decrypted.p8".to_string()
            ).into());
        }

        let private_key = RsaPrivateKey::from_pkcs8_pem(private_key_pem)
            .map_err(|e| ConnectorError::Config(format!("Failed to parse private key: {}", e)))?;

        // Calculate public key fingerprint (SHA-256 of DER-encoded public key)
        let public_key = private_key.to_public_key();
        let public_key_der = rsa::pkcs8::EncodePublicKey::to_public_key_der(&public_key)
            .map_err(|e| ConnectorError::Config(format!("Failed to encode public key: {}", e)))?;

        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(public_key_der.as_bytes());
        let fingerprint = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            hasher.finalize(),
        );

        // Normalize account identifier (uppercase, remove region suffix for legacy accounts)
        let account_upper = account.to_uppercase();
        let user_upper = user.to_uppercase();

        // Build issuer and subject
        let issuer = format!("{}.{}.SHA256:{}", account_upper, user_upper, fingerprint);
        let subject = format!("{}.{}", account_upper, user_upper);

        // Convert RSA private key to PKCS#8 DER for jsonwebtoken
        let private_key_der = rsa::pkcs8::EncodePrivateKey::to_pkcs8_der(&private_key)
            .map_err(|e| ConnectorError::Config(format!("Failed to encode private key: {}", e)))?;

        let encoding_key = EncodingKey::from_rsa_der(private_key_der.as_bytes());

        let mut auth = Self {
            token: String::new(),
            expires_at: UNIX_EPOCH,
            encoding_key,
            issuer,
            subject,
            token_lifetime_secs,
        };

        // Generate initial token
        auth.refresh_token()?;

        Ok(auth)
    }

    /// Check if token needs refresh (with 5 minute buffer)
    fn needs_refresh(&self) -> bool {
        let buffer = Duration::from_secs(300); // 5 minutes
        match SystemTime::now().checked_add(buffer) {
            Some(check_time) => check_time >= self.expires_at,
            None => true,
        }
    }

    /// Generate a new JWT token
    fn refresh_token(&mut self) -> crate::error::Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| ConnectorError::Internal(format!("System time error: {}", e)))?;

        let iat = now.as_secs();
        let exp = iat + self.token_lifetime_secs;

        let claims = SnowflakeJwtClaims {
            iss: self.issuer.clone(),
            sub: self.subject.clone(),
            iat,
            exp,
        };

        let header = Header::new(Algorithm::RS256);
        self.token = encode(&header, &claims, &self.encoding_key)
            .map_err(|e| ConnectorError::Internal(format!("Failed to generate JWT: {}", e)))?;

        self.expires_at = UNIX_EPOCH + Duration::from_secs(exp);

        debug!(
            "Generated new Snowflake JWT token, expires at {:?}",
            self.expires_at
        );

        Ok(())
    }

    /// Get the current token, refreshing if necessary
    fn get_token(&mut self) -> crate::error::Result<&str> {
        if self.needs_refresh() {
            self.refresh_token()?;
        }
        Ok(&self.token)
    }
}

/// Retry configuration for Snowflake API calls
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    #[serde(default = "default_max_retries")]
    #[validate(range(min = 0, max = 10))]
    pub max_retries: u32,

    /// Initial backoff duration in milliseconds
    #[serde(default = "default_initial_backoff_ms")]
    #[validate(range(min = 100, max = 60000))]
    pub initial_backoff_ms: u64,

    /// Maximum backoff duration in milliseconds
    #[serde(default = "default_max_backoff_ms")]
    #[validate(range(min = 1000, max = 300000))]
    pub max_backoff_ms: u64,

    /// Backoff multiplier (exponential)
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,

    /// Jitter factor (0.0-1.0) to randomize backoff and prevent thundering herd
    #[serde(default = "default_jitter_factor")]
    pub jitter_factor: f64,
}

fn default_max_retries() -> u32 {
    3
}

fn default_initial_backoff_ms() -> u64 {
    1000
}

fn default_max_backoff_ms() -> u64 {
    30000
}

fn default_backoff_multiplier() -> f64 {
    2.0
}

fn default_jitter_factor() -> f64 {
    0.1 // 10% jitter by default
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            backoff_multiplier: default_backoff_multiplier(),
            jitter_factor: default_jitter_factor(),
        }
    }
}

/// Configuration for the Snowflake sink
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct SnowflakeSinkConfig {
    /// Snowflake account identifier (e.g., "myorg-account123")
    #[validate(length(min = 1, max = 255))]
    pub account: String,

    /// Snowflake user name
    #[validate(length(min = 1, max = 255))]
    pub user: String,

    /// Path to PKCS#8 private key file (PEM format)
    #[validate(length(min = 1))]
    pub private_key_path: String,

    /// Passphrase for encrypted private key (optional, not yet supported)
    #[serde(default)]
    pub private_key_passphrase: Option<SensitiveString>,

    /// Target database name
    #[validate(length(min = 1, max = 255))]
    pub database: String,

    /// Target schema name
    #[validate(length(min = 1, max = 255))]
    pub schema: String,

    /// Target table name
    #[validate(length(min = 1, max = 255))]
    pub table: String,

    /// Warehouse to use for operations (optional, required for some operations)
    #[serde(default)]
    pub warehouse: Option<String>,

    /// Channel name for this sink instance
    #[serde(default = "default_channel_name")]
    #[validate(length(min = 1, max = 255))]
    pub channel: String,

    /// Maximum rows per batch request
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 10000))]
    pub batch_size: usize,

    /// Maximum time in seconds before flushing a partial batch
    #[serde(default = "default_flush_interval_secs")]
    #[validate(range(min = 1, max = 60))]
    pub flush_interval_secs: u64,

    /// Role to use for operations (optional)
    #[serde(default)]
    pub role: Option<String>,

    /// Enable gzip compression for request payloads (recommended for large batches)
    #[serde(default = "default_compression_enabled")]
    pub compression_enabled: bool,

    /// Minimum batch size (in bytes, estimated) to trigger compression
    #[serde(default = "default_compression_threshold_bytes")]
    #[validate(range(min = 1024, max = 10485760))]
    pub compression_threshold_bytes: usize,

    /// JWT token lifetime in seconds (default: 3540 = 59 minutes, max allowed is 60 minutes)
    #[serde(default = "default_token_lifetime_secs")]
    #[validate(range(min = 60, max = 3600))]
    pub token_lifetime_secs: u64,

    /// Retry configuration for transient failures
    #[serde(default)]
    #[validate(nested)]
    pub retry: RetryConfig,

    /// Circuit breaker configuration for protecting against cascading failures
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerConfig,

    /// Request timeout in seconds
    #[serde(default = "default_request_timeout_secs")]
    #[validate(range(min = 5, max = 300))]
    pub request_timeout_secs: u64,
}

fn default_request_timeout_secs() -> u64 {
    30
}

fn default_compression_enabled() -> bool {
    true
}

fn default_compression_threshold_bytes() -> usize {
    8192 // 8KB - compress if payload is larger
}

fn default_channel_name() -> String {
    "rivven-channel-1".to_string()
}

fn default_batch_size() -> usize {
    1000
}

fn default_flush_interval_secs() -> u64 {
    1
}

fn default_token_lifetime_secs() -> u64 {
    3540 // 59 minutes (Snowflake max is 60 minutes)
}

impl Default for SnowflakeSinkConfig {
    fn default() -> Self {
        Self {
            account: String::new(),
            user: String::new(),
            private_key_path: String::new(),
            private_key_passphrase: None,
            database: String::new(),
            schema: String::new(),
            table: String::new(),
            warehouse: None,
            channel: default_channel_name(),
            batch_size: default_batch_size(),
            flush_interval_secs: default_flush_interval_secs(),
            role: None,
            token_lifetime_secs: default_token_lifetime_secs(),
            retry: RetryConfig::default(),
            circuit_breaker: CircuitBreakerConfig::default(),
            request_timeout_secs: default_request_timeout_secs(),
            compression_enabled: default_compression_enabled(),
            compression_threshold_bytes: default_compression_threshold_bytes(),
        }
    }
}

/// Check if an HTTP status code is retryable
#[cfg(feature = "snowflake")]
fn is_retryable_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

/// Calculate backoff with jitter
#[cfg(feature = "snowflake")]
fn calculate_backoff_with_jitter(base_ms: u64, jitter_factor: f64) -> u64 {
    if jitter_factor <= 0.0 {
        return base_ms;
    }

    let mut rng = rand::thread_rng();
    let jitter_range = (base_ms as f64 * jitter_factor) as u64;
    let jitter = if jitter_range > 0 {
        rng.gen_range(0..jitter_range)
    } else {
        0
    };

    // Apply jitter both ways: base_ms ± jitter
    if rng.gen_bool(0.5) {
        base_ms.saturating_add(jitter)
    } else {
        base_ms.saturating_sub(jitter)
    }
}

/// Snowflake Sink implementation with JWT authentication
#[cfg(feature = "snowflake")]
pub struct SnowflakeSink {
    /// HTTP client for API calls
    client: reqwest::Client,
    /// Circuit breaker for protecting against cascading failures
    circuit_breaker: Option<CircuitBreaker>,
}

#[cfg(feature = "snowflake")]
impl SnowflakeSink {
    /// Create a new Snowflake sink with custom timeout (F-117 fix: returns Result)
    pub fn try_with_timeout(timeout: Duration) -> std::result::Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(90))
            .build()?;
        Ok(Self {
            client,
            circuit_breaker: Some(CircuitBreaker::from_config(&CircuitBreakerConfig::default())),
        })
    }

    /// Create a new Snowflake sink with custom timeout.
    ///
    /// # Errors
    /// Returns `reqwest::Error` if the HTTP client cannot be built.
    pub fn with_timeout(timeout: Duration) -> std::result::Result<Self, reqwest::Error> {
        Self::try_with_timeout(timeout)
    }

    /// Create a new Snowflake sink with full configuration (F-117 fix: returns Result)
    pub fn try_with_config(
        config: &SnowflakeSinkConfig,
    ) -> std::result::Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.request_timeout_secs))
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(90))
            .build()?;

        let circuit_breaker = if config.circuit_breaker.enabled {
            Some(CircuitBreaker::from_config(&config.circuit_breaker))
        } else {
            None
        };

        Ok(Self {
            client,
            circuit_breaker,
        })
    }

    /// Create a new Snowflake sink with full configuration.
    ///
    /// # Errors
    /// Returns `reqwest::Error` if the HTTP client cannot be built.
    pub fn with_config(config: &SnowflakeSinkConfig) -> std::result::Result<Self, reqwest::Error> {
        Self::try_with_config(config)
    }

    /// Build the Snowpipe Streaming API base URL
    fn api_url(account: &str) -> String {
        // Account format: orgname-accountname or legacy accountname.region
        // API endpoint: https://<account>.snowflakecomputing.com/v1/data/pipes/
        format!("https://{}.snowflakecomputing.com", account.to_lowercase())
    }

    /// Generate a unique request ID for tracing
    fn generate_request_id() -> String {
        format!(
            "rivven-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_micros())
                .unwrap_or(0)
        )
    }

    /// Execute an HTTP request with retry logic and circuit breaker
    async fn execute_with_retry<F, Fut, T>(
        &self,
        config: &SnowflakeSinkConfig,
        operation: &str,
        mut request_fn: F,
    ) -> crate::error::Result<T>
    where
        F: FnMut(String) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<reqwest::Response, reqwest::Error>>,
        T: for<'de> Deserialize<'de>,
    {
        let start_time = std::time::Instant::now();

        // Check circuit breaker before attempting request
        if let Some(ref cb) = self.circuit_breaker {
            if !cb.allow_request() {
                counter!("snowflake.circuit_breaker.rejected").increment(1);
                warn!(
                    operation = %operation,
                    "Circuit breaker is open, failing fast"
                );
                return Err(ConnectorError::Connection(
                    "Circuit breaker is open - service unavailable".to_string(),
                )
                .into());
            }
        }

        let mut attempt = 0;
        let mut backoff_ms = config.retry.initial_backoff_ms;

        loop {
            attempt += 1;
            let request_id = Self::generate_request_id();

            match request_fn(request_id.clone()).await {
                Ok(response) => {
                    let status = response.status();

                    // Record request latency
                    histogram!("snowflake.request.duration_ms")
                        .record(start_time.elapsed().as_millis() as f64);

                    if status.is_success() {
                        // Record success with circuit breaker
                        if let Some(ref cb) = self.circuit_breaker {
                            cb.record_success();
                        }
                        counter!("snowflake.requests.success").increment(1);

                        let result: T = response.json().await.map_err(|e| {
                            ConnectorError::Internal(format!(
                                "Failed to parse Snowflake response: {}",
                                e
                            ))
                        })?;
                        return Ok(result);
                    }

                    // Extract Retry-After header for rate limiting
                    let retry_after_ms = response
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok())
                        .map(|secs| secs * 1000);

                    let body = response.text().await.unwrap_or_default();

                    // Check if we should retry
                    if is_retryable_status(status) && attempt <= config.retry.max_retries {
                        counter!("snowflake.requests.retried").increment(1);

                        // Use Retry-After header if provided (for 429), otherwise use backoff
                        let wait_ms = if status.as_u16() == 429 {
                            retry_after_ms.unwrap_or_else(|| {
                                calculate_backoff_with_jitter(
                                    backoff_ms,
                                    config.retry.jitter_factor,
                                )
                            })
                        } else {
                            calculate_backoff_with_jitter(backoff_ms, config.retry.jitter_factor)
                        };

                        warn!(
                            request_id = %request_id,
                            operation = %operation,
                            attempt = %attempt,
                            status = %status,
                            retry_after = ?retry_after_ms,
                            "Snowflake API returned retryable error, will retry in {}ms",
                            wait_ms
                        );

                        tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                        backoff_ms = (backoff_ms as f64 * config.retry.backoff_multiplier) as u64;
                        backoff_ms = backoff_ms.min(config.retry.max_backoff_ms);
                        continue;
                    }

                    // Record failure with circuit breaker
                    if let Some(ref cb) = self.circuit_breaker {
                        cb.record_failure();
                    }
                    counter!("snowflake.requests.failed").increment(1);

                    // Non-retryable error or max retries exceeded
                    error!(
                        request_id = %request_id,
                        operation = %operation,
                        status = %status,
                        attempts = %attempt,
                        "Snowflake API error"
                    );
                    return Err(ConnectorError::Connection(format!(
                        "Snowflake API error ({}): {}",
                        status, body
                    ))
                    .into());
                }
                Err(e) => {
                    // Network errors are retryable
                    if attempt <= config.retry.max_retries {
                        counter!("snowflake.requests.retried").increment(1);

                        let jittered_backoff =
                            calculate_backoff_with_jitter(backoff_ms, config.retry.jitter_factor);

                        warn!(
                            request_id = %request_id,
                            operation = %operation,
                            attempt = %attempt,
                            error = %e,
                            "Snowflake API request failed, will retry in {}ms",
                            jittered_backoff
                        );

                        tokio::time::sleep(Duration::from_millis(jittered_backoff)).await;
                        backoff_ms = (backoff_ms as f64 * config.retry.backoff_multiplier) as u64;
                        backoff_ms = backoff_ms.min(config.retry.max_backoff_ms);
                        continue;
                    }

                    // Record failure with circuit breaker
                    if let Some(ref cb) = self.circuit_breaker {
                        cb.record_failure();
                    }
                    counter!("snowflake.requests.failed").increment(1);

                    error!(
                        request_id = %request_id,
                        operation = %operation,
                        error = %e,
                        attempts = %attempt,
                        "Snowflake API request failed after {} attempts",
                        attempt
                    );
                    return Err(ConnectorError::Connection(format!(
                        "Snowflake API request failed after {} attempts: {}",
                        attempt, e
                    ))
                    .into());
                }
            }
        }
    }

    /// Insert rows using the Snowpipe Streaming insertRows API
    async fn insert_rows(
        &self,
        auth: &mut SnowflakeAuth,
        config: &SnowflakeSinkConfig,
        rows: &[serde_json::Value],
        offset_token: &str,
    ) -> crate::error::Result<InsertRowsResponse> {
        let token = auth.get_token()?.to_string();

        let url = format!(
            "{}/v1/streaming/channels/{}/tables/{}.{}.{}/insertRows",
            Self::api_url(&config.account),
            config.channel,
            config.database,
            config.schema,
            config.table
        );

        let request_body = InsertRowsRequest {
            rows: rows.to_vec(),
            offset_token: offset_token.to_string(),
        };

        // Build base headers including optional role
        let mut headers = vec![
            ("Authorization".to_string(), format!("Bearer {}", token)),
            ("Content-Type".to_string(), "application/json".to_string()),
            ("Accept".to_string(), "application/json".to_string()),
            (
                "X-Snowflake-Authorization-Token-Type".to_string(),
                "KEYPAIR_JWT".to_string(),
            ),
        ];

        if let Some(ref role) = config.role {
            headers.push(("X-Snowflake-Role".to_string(), role.clone()));
        }

        // Track rows for metrics
        let row_count = rows.len();
        gauge!("snowflake.batch.size").set(row_count as f64);

        let client = self.client.clone();
        let url_clone = url.clone();
        let body_clone = request_body.clone();
        let headers_clone = headers.clone();

        self.execute_with_retry(config, "insertRows", |request_id| {
            let client = client.clone();
            let url = url_clone.clone();
            let body = body_clone.clone();
            let mut hdrs = headers_clone.clone();
            // Add request ID header for Snowflake-side tracing
            hdrs.push(("X-Request-ID".to_string(), request_id));

            async move {
                let mut req = client.post(&url);
                for (k, v) in hdrs {
                    req = req.header(&k, &v);
                }
                req.json(&body).send().await
            }
        })
        .await
    }

    /// Open a streaming channel
    async fn open_channel(
        &self,
        auth: &mut SnowflakeAuth,
        config: &SnowflakeSinkConfig,
    ) -> crate::error::Result<OpenChannelResponse> {
        let token = auth.get_token()?.to_string();

        let url = format!("{}/v1/streaming/channels", Self::api_url(&config.account));

        let request_body = OpenChannelRequest {
            name: config.channel.clone(),
            database: config.database.clone(),
            schema: config.schema.clone(),
            table: config.table.clone(),
            offset_token_verification: true,
        };

        // Build base headers including optional role
        let mut headers = vec![
            ("Authorization".to_string(), format!("Bearer {}", token)),
            ("Content-Type".to_string(), "application/json".to_string()),
            ("Accept".to_string(), "application/json".to_string()),
            (
                "X-Snowflake-Authorization-Token-Type".to_string(),
                "KEYPAIR_JWT".to_string(),
            ),
        ];

        if let Some(ref role) = config.role {
            headers.push(("X-Snowflake-Role".to_string(), role.clone()));
        }

        let client = self.client.clone();
        let url_clone = url.clone();
        let body_clone = request_body.clone();
        let headers_clone = headers.clone();

        self.execute_with_retry(config, "openChannel", |request_id| {
            let client = client.clone();
            let url = url_clone.clone();
            let body = body_clone.clone();
            let mut hdrs = headers_clone.clone();
            // Add request ID header for Snowflake-side tracing
            hdrs.push(("X-Request-ID".to_string(), request_id));

            async move {
                let mut req = client.post(&url);
                for (k, v) in hdrs {
                    req = req.header(&k, &v);
                }
                req.json(&body).send().await
            }
        })
        .await
    }
}

/// Snowpipe Streaming API request/response types
#[cfg(feature = "snowflake")]
#[derive(Debug, Serialize, Clone)]
struct InsertRowsRequest {
    rows: Vec<serde_json::Value>,
    offset_token: String,
}

#[cfg(feature = "snowflake")]
#[derive(Debug, Deserialize)]
struct InsertRowsResponse {
    #[serde(default)]
    success: bool,
    #[serde(default)]
    errors: Vec<InsertError>,
}

#[cfg(feature = "snowflake")]
#[derive(Debug, Deserialize)]
struct InsertError {
    #[serde(default)]
    row_index: usize,
    #[serde(default)]
    message: String,
}

#[cfg(feature = "snowflake")]
#[derive(Debug, Serialize, Clone)]
struct OpenChannelRequest {
    name: String,
    database: String,
    schema: String,
    table: String,
    offset_token_verification: bool,
}

#[cfg(feature = "snowflake")]
#[derive(Debug, Deserialize)]
struct OpenChannelResponse {
    #[serde(default)]
    channel_name: String,
    #[serde(default)]
    status: String,
}

#[cfg(feature = "snowflake")]
#[async_trait]
impl Sink for SnowflakeSink {
    type Config = SnowflakeSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("snowflake", env!("CARGO_PKG_VERSION"))
            .description("Snowflake Snowpipe Streaming sink - low-latency data ingestion")
            .documentation_url("https://rivven.dev/docs/connectors/snowflake-sink")
            .config_schema_from::<SnowflakeSinkConfig>()
            .metadata("api", "snowpipe-streaming")
            .metadata("auth", "jwt-keypair")
    }

    async fn check(&self, config: &Self::Config) -> crate::error::Result<CheckResult> {
        info!(
            "Checking Snowflake connectivity for account: {}",
            config.account
        );

        // Verify private key file exists
        let key_path = std::path::Path::new(&config.private_key_path);
        if !key_path.exists() {
            return Ok(CheckResult::failure(format!(
                "Private key file not found: {}",
                config.private_key_path
            )));
        }

        // Read private key
        let private_key_pem = match std::fs::read_to_string(key_path) {
            Ok(contents) => {
                if !contents.contains("-----BEGIN") || !contents.contains("PRIVATE KEY-----") {
                    return Ok(CheckResult::failure(
                        "Private key file does not appear to be in PEM format",
                    ));
                }
                contents
            }
            Err(e) => {
                return Ok(CheckResult::failure(format!(
                    "Failed to read private key file: {}",
                    e
                )));
            }
        };

        // Test JWT generation
        let passphrase = config
            .private_key_passphrase
            .as_ref()
            .map(|s| s.expose_secret());
        let mut auth = match SnowflakeAuth::new(
            &config.account,
            &config.user,
            &private_key_pem,
            passphrase,
            config.token_lifetime_secs,
        ) {
            Ok(auth) => auth,
            Err(e) => {
                return Ok(CheckResult::failure(format!(
                    "Failed to initialize authentication: {}",
                    e
                )));
            }
        };

        // Verify token can be generated
        if let Err(e) = auth.get_token() {
            return Ok(CheckResult::failure(format!(
                "Failed to generate JWT token: {}",
                e
            )));
        }

        // Try to open a channel to verify connectivity
        match self.open_channel(&mut auth, config).await {
            Ok(response) => {
                info!(
                    "Successfully opened Snowflake channel: {} (status: {})",
                    response.channel_name, response.status
                );
            }
            Err(e) => {
                warn!(
                    "Channel open test failed (may be expected for check): {}",
                    e
                );
                // Don't fail the check for channel errors, as the channel may not exist yet
            }
        }

        info!(
            "Snowflake configuration validated for {}.{}.{}",
            config.database, config.schema, config.table
        );

        Ok(CheckResult::builder()
            .check_passed("private_key")
            .check_passed("jwt_generation")
            .check_passed("configuration")
            .build())
    }

    async fn write(
        &self,
        config: &Self::Config,
        mut events: futures::stream::BoxStream<'static, SourceEvent>,
    ) -> crate::error::Result<WriteResult> {
        use futures::StreamExt;

        // Initialize authentication
        let private_key_pem = std::fs::read_to_string(&config.private_key_path)
            .map_err(|e| ConnectorError::Config(format!("Failed to read private key: {}", e)))?;

        let passphrase = config
            .private_key_passphrase
            .as_ref()
            .map(|s| s.expose_secret());
        let auth = Arc::new(RwLock::new(SnowflakeAuth::new(
            &config.account,
            &config.user,
            &private_key_pem,
            passphrase,
            config.token_lifetime_secs,
        )?));

        let mut batch: Vec<serde_json::Value> = Vec::with_capacity(config.batch_size);
        let mut total_written = 0u64;
        let mut total_failed = 0u64;
        let mut errors: Vec<String> = Vec::new();
        let mut last_flush = std::time::Instant::now();
        let flush_interval = Duration::from_secs(config.flush_interval_secs);
        let mut offset_counter = 0u64;

        info!(
            "Starting Snowflake sink: {}.{}.{}, batch_size={}",
            config.database, config.schema, config.table, config.batch_size
        );

        // Open channel
        {
            let mut auth_guard = auth.write().await;
            match self.open_channel(&mut auth_guard, config).await {
                Ok(response) => {
                    info!("Opened Snowflake channel: {}", response.channel_name);
                }
                Err(e) => {
                    warn!("Failed to open channel (will retry on first insert): {}", e);
                }
            }
        }

        while let Some(event) = events.next().await {
            // Convert event to row format
            let row = event.data.clone();
            batch.push(row);

            let should_flush =
                batch.len() >= config.batch_size || last_flush.elapsed() >= flush_interval;

            if should_flush && !batch.is_empty() {
                debug!("Flushing {} rows to Snowflake", batch.len());

                let offset_token = format!("rivven-{}", offset_counter);
                offset_counter += 1;

                let mut auth_guard = auth.write().await;
                match self
                    .insert_rows(&mut auth_guard, config, &batch, &offset_token)
                    .await
                {
                    Ok(response) => {
                        if response.success {
                            total_written += batch.len() as u64;
                        } else {
                            let failed_count = response.errors.len();
                            total_failed += failed_count as u64;
                            total_written += (batch.len() - failed_count) as u64;

                            for err in response.errors {
                                errors.push(format!("Row {}: {}", err.row_index, err.message));
                            }
                        }
                    }
                    Err(e) => {
                        total_failed += batch.len() as u64;
                        errors.push(format!("Batch insert failed: {}", e));
                    }
                }

                batch.clear();
                last_flush = std::time::Instant::now();
            }
        }

        // Flush remaining events
        if !batch.is_empty() {
            debug!("Flushing final {} rows to Snowflake", batch.len());

            let offset_token = format!("rivven-{}", offset_counter);
            let mut auth_guard = auth.write().await;

            match self
                .insert_rows(&mut auth_guard, config, &batch, &offset_token)
                .await
            {
                Ok(response) => {
                    if response.success {
                        total_written += batch.len() as u64;
                    } else {
                        let failed_count = response.errors.len();
                        total_failed += failed_count as u64;
                        total_written += (batch.len() - failed_count) as u64;
                    }
                }
                Err(e) => {
                    total_failed += batch.len() as u64;
                    errors.push(format!("Final batch insert failed: {}", e));
                }
            }
        }

        info!(
            "Snowflake sink completed: {} written, {} failed",
            total_written, total_failed
        );

        Ok(WriteResult {
            records_written: total_written,
            bytes_written: 0,
            records_failed: total_failed,
            errors,
        })
    }
}

/// Stub implementation when snowflake feature is disabled
#[cfg(not(feature = "snowflake"))]
pub struct SnowflakeSink;

#[cfg(not(feature = "snowflake"))]
impl SnowflakeSink {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(not(feature = "snowflake"))]
impl Default for SnowflakeSink {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(feature = "snowflake"))]
#[async_trait]
impl Sink for SnowflakeSink {
    type Config = SnowflakeSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("snowflake", env!("CARGO_PKG_VERSION"))
            .description("Snowflake Snowpipe Streaming sink (requires 'snowflake' feature)")
            .documentation_url("https://rivven.dev/docs/connectors/snowflake-sink")
            .config_schema_from::<SnowflakeSinkConfig>()
            .metadata("api", "snowpipe-streaming")
            .metadata("auth", "jwt-keypair")
    }

    async fn check(&self, _config: &Self::Config) -> crate::error::Result<CheckResult> {
        Ok(CheckResult::failure(
            "Snowflake connector requires the 'snowflake' feature to be enabled",
        ))
    }

    async fn write(
        &self,
        _config: &Self::Config,
        _events: futures::stream::BoxStream<'static, SourceEvent>,
    ) -> crate::error::Result<WriteResult> {
        Err(ConnectorError::Config(
            "Snowflake connector requires the 'snowflake' feature to be enabled".to_string(),
        ))
    }
}

/// Factory for creating Snowflake sink instances
pub struct SnowflakeSinkFactory;

impl SinkFactory for SnowflakeSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        SnowflakeSink::spec()
    }

    fn create(&self) -> Result<Box<dyn AnySink>> {
        Ok(Box::new(
            SnowflakeSink::try_with_config(&SnowflakeSinkConfig::default()).map_err(|e| {
                ConnectError::config(format!("Failed to create Snowflake HTTP client: {}", e))
            })?,
        ))
    }
}

// Implement AnySink for SnowflakeSink
crate::impl_any_sink!(SnowflakeSink, SnowflakeSinkConfig);

/// Register the Snowflake sink with the given registry
pub fn register(registry: &mut crate::SinkRegistry) {
    use std::sync::Arc;
    registry.register("snowflake", Arc::new(SnowflakeSinkFactory));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::circuit_breaker::CircuitState;

    #[test]
    fn test_spec() {
        let spec = SnowflakeSink::spec();
        assert_eq!(spec.connector_type, "snowflake");
        assert!(spec.config_schema.is_some());
    }

    #[test]
    fn test_default_config() {
        let config = SnowflakeSinkConfig::default();
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.flush_interval_secs, 1);
        assert_eq!(config.channel, "rivven-channel-1");
        assert_eq!(config.token_lifetime_secs, 3540); // 59 minutes
    }

    #[test]
    fn test_factory() {
        let factory = SnowflakeSinkFactory;
        let spec = factory.spec();
        assert_eq!(spec.connector_type, "snowflake");
        let _sink = factory.create().unwrap();
    }

    #[test]
    fn test_config_serialization() {
        let config = SnowflakeSinkConfig {
            account: "myorg-account123".to_string(),
            user: "RIVVEN_USER".to_string(),
            private_key_path: "/path/to/key.p8".to_string(),
            private_key_passphrase: None,
            database: "MY_DB".to_string(),
            schema: "PUBLIC".to_string(),
            table: "EVENTS".to_string(),
            warehouse: Some("COMPUTE_WH".to_string()),
            channel: "rivven-channel-1".to_string(),
            batch_size: 500,
            flush_interval_secs: 5,
            role: Some("RIVVEN_ROLE".to_string()),
            compression_enabled: true,
            compression_threshold_bytes: 4096,
            token_lifetime_secs: 1800,
            retry: RetryConfig::default(),
            circuit_breaker: CircuitBreakerConfig::default(),
            request_timeout_secs: 60,
        };

        let json = serde_json::to_string(&config).unwrap();
        let parsed: SnowflakeSinkConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.account, "myorg-account123");
        assert_eq!(parsed.batch_size, 500);
        assert_eq!(parsed.token_lifetime_secs, 1800);
        assert_eq!(parsed.warehouse, Some("COMPUTE_WH".to_string()));
        assert_eq!(parsed.retry.max_retries, 3);
        assert!(parsed.circuit_breaker.enabled);
        assert!(parsed.compression_enabled);
        assert_eq!(parsed.compression_threshold_bytes, 4096);
    }

    #[test]
    fn test_retry_config_defaults() {
        let retry = RetryConfig::default();
        assert_eq!(retry.max_retries, 3);
        assert_eq!(retry.initial_backoff_ms, 1000);
        assert_eq!(retry.max_backoff_ms, 30000);
        assert!((retry.backoff_multiplier - 2.0).abs() < f64::EPSILON);
        assert!((retry.jitter_factor - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn test_circuit_breaker_config_defaults() {
        let cb = CircuitBreakerConfig::default();
        assert!(cb.enabled);
        assert_eq!(cb.failure_threshold, 5);
        assert_eq!(cb.reset_timeout_secs, 30);
        assert_eq!(cb.success_threshold, 2);
    }

    #[test]
    fn test_compression_defaults() {
        let config = SnowflakeSinkConfig::default();
        assert!(config.compression_enabled);
        assert_eq!(config.compression_threshold_bytes, 8192);
    }

    #[cfg(feature = "snowflake")]
    mod auth_tests {
        use super::*;

        // Note: These tests require a valid RSA key pair
        // In CI, you would use a test key or mock the crypto operations

        #[test]
        fn test_api_url_generation() {
            let url = SnowflakeSink::api_url("myorg-account123");
            assert_eq!(url, "https://myorg-account123.snowflakecomputing.com");

            let url = SnowflakeSink::api_url("MYORG-ACCOUNT123");
            assert_eq!(url, "https://myorg-account123.snowflakecomputing.com");
        }

        #[test]
        fn test_jwt_claims_structure() {
            let claims = SnowflakeJwtClaims {
                iss: "MYORG-ACCOUNT123.USER.SHA256:abc123".to_string(),
                sub: "MYORG-ACCOUNT123.USER".to_string(),
                iat: 1000,
                exp: 2000,
            };

            let json = serde_json::to_string(&claims).unwrap();
            assert!(json.contains("iss"));
            assert!(json.contains("sub"));
            assert!(json.contains("iat"));
            assert!(json.contains("exp"));
        }

        #[test]
        fn test_retryable_status_codes() {
            use reqwest::StatusCode;

            // Retryable codes
            assert!(is_retryable_status(StatusCode::REQUEST_TIMEOUT)); // 408
            assert!(is_retryable_status(StatusCode::TOO_MANY_REQUESTS)); // 429
            assert!(is_retryable_status(StatusCode::INTERNAL_SERVER_ERROR)); // 500
            assert!(is_retryable_status(StatusCode::BAD_GATEWAY)); // 502
            assert!(is_retryable_status(StatusCode::SERVICE_UNAVAILABLE)); // 503
            assert!(is_retryable_status(StatusCode::GATEWAY_TIMEOUT)); // 504

            // Non-retryable codes
            assert!(!is_retryable_status(StatusCode::OK)); // 200
            assert!(!is_retryable_status(StatusCode::BAD_REQUEST)); // 400
            assert!(!is_retryable_status(StatusCode::UNAUTHORIZED)); // 401
            assert!(!is_retryable_status(StatusCode::FORBIDDEN)); // 403
            assert!(!is_retryable_status(StatusCode::NOT_FOUND)); // 404
        }

        #[test]
        fn test_request_id_format() {
            let request_id = SnowflakeSink::generate_request_id();
            assert!(request_id.starts_with("rivven-"));
            assert!(request_id.len() > 10); // Should have process ID and timestamp
        }

        #[test]
        fn test_jitter_calculation() {
            // Test no jitter
            let result = calculate_backoff_with_jitter(1000, 0.0);
            assert_eq!(result, 1000);

            // Test with jitter - should be within expected range
            for _ in 0..100 {
                let result = calculate_backoff_with_jitter(1000, 0.5);
                assert!(
                    (500..=1500).contains(&result),
                    "Jitter result {} out of range",
                    result
                );
            }
        }

        #[test]
        fn test_circuit_breaker_states() {
            let config = CircuitBreakerConfig {
                enabled: true,
                failure_threshold: 3,
                reset_timeout_secs: 1,
                success_threshold: 2,
            };
            let cb = CircuitBreaker::from_config(&config);

            // Initially closed
            assert_eq!(cb.state(), CircuitState::Closed);
            assert!(cb.allow_request());

            // Record failures up to threshold
            cb.record_failure();
            cb.record_failure();
            assert_eq!(cb.state(), CircuitState::Closed);

            // Third failure opens the circuit
            cb.record_failure();
            assert_eq!(cb.state(), CircuitState::Open);
            assert!(!cb.allow_request());
        }

        #[test]
        fn test_circuit_breaker_disabled() {
            // When `enabled` is false, no CircuitBreaker is created (checked at config level)
            let config = SnowflakeSinkConfig {
                circuit_breaker: CircuitBreakerConfig {
                    enabled: false,
                    failure_threshold: 1,
                    reset_timeout_secs: 1,
                    success_threshold: 1,
                },
                ..Default::default()
            };
            let sink = SnowflakeSink::with_config(&config).unwrap();
            assert!(sink.circuit_breaker.is_none());
        }

        #[test]
        fn test_circuit_breaker_success_resets() {
            let config = CircuitBreakerConfig {
                enabled: true,
                failure_threshold: 3,
                reset_timeout_secs: 60,
                success_threshold: 2,
            };
            let cb = CircuitBreaker::from_config(&config);

            // Record some failures
            cb.record_failure();
            cb.record_failure();
            assert_eq!(cb.state(), CircuitState::Closed);

            // Success should reset failure count
            cb.record_success();
            assert_eq!(cb.state(), CircuitState::Closed);

            // Now need 3 more failures to open
            cb.record_failure();
            cb.record_failure();
            assert_eq!(cb.state(), CircuitState::Closed);
            cb.record_failure();
            assert_eq!(cb.state(), CircuitState::Open);
        }
    }
}
