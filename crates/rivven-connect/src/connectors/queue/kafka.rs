//! Apache Kafka source and sink connectors using krafka
//!
//! This module provides Kafka source (consumer) and sink (producer) connectors
//! backed by the [`krafka`] crate — a pure Rust, async-native Kafka client.
//!
//! # Features
//!
//! | Feature | Description |
//! |---------|-------------|
//! | Native consumer groups | Full consumer group protocol via krafka |
//! | Producer batching | Built-in linger/batch-size batching |
//! | SASL authentication | PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 on all clients |
//! | OAUTHBEARER | Token-based auth (RFC 7628 / KIP-255) |
//! | AWS MSK IAM | Native IAM authentication for Amazon MSK |
//! | TLS/SSL | Native TLS support |
//! | Compression | None, Gzip, Snappy, LZ4, Zstd |
//! | Idempotent producer | Exactly-once semantics support |
//! | Transactional producer | Optional transactional_id for EOS |
//! | Lock-free metrics | Atomic counters with zero contention |
//! | AdminClient health checks | Topic listing and partition discovery |
//! | Prometheus export | `to_prometheus_format()` for scraping |

use crate::error::{ConnectorError, Result};
use crate::traits::catalog::{Catalog, ConfiguredCatalog, Stream, SyncMode};
use crate::traits::event::SourceEvent;
use crate::traits::registry::{
    AnySource, AnySink, SinkFactory, SinkRegistry, SourceFactory, SourceRegistry,
};
use crate::traits::sink::{Sink, WriteResult};
use crate::traits::source::{CheckDetail, CheckResult, Source};
use crate::traits::spec::{ConnectorSpec, SyncModeSpec};
use crate::traits::state::State;


use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};
use validator::Validate;

// ============================================================================
// Configuration Enums
// ============================================================================

/// Security protocol for Kafka connections
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SecurityProtocol {
    /// No encryption or authentication
    Plaintext,
    /// TLS encryption without SASL
    Ssl,
    /// SASL authentication without TLS
    SaslPlaintext,
    /// SASL authentication with TLS
    SaslSsl,
}

impl Default for SecurityProtocol {
    fn default() -> Self {
        Self::Plaintext
    }
}

/// SASL authentication mechanism
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum SaslMechanism {
    /// PLAIN mechanism (username/password in cleartext)
    #[serde(rename = "PLAIN")]
    Plain,
    /// SCRAM-SHA-256 mechanism (salted challenge-response)
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256,
    /// SCRAM-SHA-512 mechanism (salted challenge-response)
    #[serde(rename = "SCRAM-SHA-512")]
    ScramSha512,
    /// OAUTHBEARER mechanism (token-based, RFC 7628 / KIP-255)
    #[serde(rename = "OAUTHBEARER")]
    OAuthBearer,
    /// AWS MSK IAM authentication
    #[serde(rename = "AWS_MSK_IAM")]
    AwsMskIam,
}

impl Default for SaslMechanism {
    fn default() -> Self {
        Self::Plain
    }
}

/// Kafka security configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize, Validate, JsonSchema)]
pub struct KafkaSecurityConfig {
    /// Security protocol
    #[serde(default)]
    pub protocol: SecurityProtocol,

    /// SASL mechanism (required when protocol is SASL_*)
    pub sasl_mechanism: Option<SaslMechanism>,

    /// SASL username (for PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
    pub sasl_username: Option<String>,

    /// SASL password (for PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
    pub sasl_password: Option<String>,

    /// OAuth bearer token (for OAUTHBEARER mechanism)
    pub oauth_token: Option<String>,

    /// AWS access key ID (for AWS_MSK_IAM mechanism)
    pub aws_access_key_id: Option<String>,

    /// AWS secret access key (for AWS_MSK_IAM mechanism)
    pub aws_secret_access_key: Option<String>,

    /// AWS region (for AWS_MSK_IAM mechanism, e.g. "us-east-1")
    pub aws_region: Option<String>,

    /// Path to CA certificate for TLS (PEM format)
    pub ssl_ca_cert: Option<String>,

    /// Path to client certificate for mTLS (PEM format)
    pub ssl_client_cert: Option<String>,

    /// Path to client key for mTLS (PEM format)
    pub ssl_client_key: Option<String>,
}

impl KafkaSecurityConfig {
    /// Returns `true` if this config requires SASL authentication.
    #[inline]
    pub fn requires_sasl(&self) -> bool {
        matches!(
            self.protocol,
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
        )
    }

    /// Build a krafka `AuthConfig` from this security config.
    ///
    /// Returns `Some(AuthConfig)` for SASL protocols, `None` for plaintext/SSL.
    fn to_auth_config(&self) -> Result<Option<krafka::auth::AuthConfig>> {
        if !self.requires_sasl() {
            return Ok(None);
        }

        let mechanism = self.sasl_mechanism.unwrap_or_default();
        let auth = match mechanism {
            SaslMechanism::Plain => {
                let username = self.require_username()?;
                let password = self.require_password()?;
                krafka::auth::AuthConfig::sasl_plain(username, password)
            }
            SaslMechanism::ScramSha256 => {
                let username = self.require_username()?;
                let password = self.require_password()?;
                krafka::auth::AuthConfig::sasl_scram_sha256(username, password)
            }
            SaslMechanism::ScramSha512 => {
                let username = self.require_username()?;
                let password = self.require_password()?;
                krafka::auth::AuthConfig::sasl_scram_sha512(username, password)
            }
            SaslMechanism::OAuthBearer => {
                let token = self.oauth_token.as_deref().ok_or_else(|| {
                    ConnectorError::Config(
                        "oauth_token is required for OAUTHBEARER mechanism".to_string(),
                    )
                })?;
                krafka::auth::AuthConfig::sasl_oauthbearer(token)
            }
            SaslMechanism::AwsMskIam => {
                // MSK IAM signing requires TLS — reject SASL_PLAINTEXT
                if self.protocol == SecurityProtocol::SaslPlaintext {
                    return Err(ConnectorError::Config(
                        "AWS_MSK_IAM requires SASL_SSL protocol; \
                         SASL_PLAINTEXT transmits IAM credentials without TLS".to_string(),
                    ).into());
                }
                let access_key = self.aws_access_key_id.as_deref().ok_or_else(|| {
                    ConnectorError::Config(
                        "aws_access_key_id is required for AWS_MSK_IAM mechanism".to_string(),
                    )
                })?;
                let secret_key = self.aws_secret_access_key.as_deref().ok_or_else(|| {
                    ConnectorError::Config(
                        "aws_secret_access_key is required for AWS_MSK_IAM mechanism".to_string(),
                    )
                })?;
                let region = self.aws_region.as_deref().ok_or_else(|| {
                    ConnectorError::Config(
                        "aws_region is required for AWS_MSK_IAM mechanism".to_string(),
                    )
                })?;
                krafka::auth::AuthConfig::aws_msk_iam(access_key, secret_key, region)
            }
        };
        Ok(Some(auth))
    }

    /// Require SASL username, returning a config error if absent.
    #[inline]
    fn require_username(&self) -> Result<&str> {
        self.sasl_username.as_deref().ok_or_else(|| {
            ConnectorError::Config(
                "sasl_username is required for SASL protocols".to_string(),
            ).into()
        })
    }

    /// Require SASL password, returning a config error if absent.
    #[inline]
    fn require_password(&self) -> Result<&str> {
        self.sasl_password.as_deref().ok_or_else(|| {
            ConnectorError::Config(
                "sasl_password is required for SASL protocols".to_string(),
            ).into()
        })
    }
}

/// Compression codec for Kafka messages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    /// No compression
    None,
    /// Gzip compression
    Gzip,
    /// Snappy compression
    Snappy,
    /// LZ4 compression
    Lz4,
    /// Zstd compression
    Zstd,
}

impl Default for CompressionCodec {
    fn default() -> Self {
        Self::None
    }
}

impl From<CompressionCodec> for krafka::protocol::Compression {
    fn from(codec: CompressionCodec) -> Self {
        match codec {
            CompressionCodec::None => krafka::protocol::Compression::None,
            CompressionCodec::Gzip => krafka::protocol::Compression::Gzip,
            CompressionCodec::Snappy => krafka::protocol::Compression::Snappy,
            CompressionCodec::Lz4 => krafka::protocol::Compression::Lz4,
            CompressionCodec::Zstd => krafka::protocol::Compression::Zstd,
        }
    }
}

/// Where to start reading from when no committed offset exists
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum StartOffset {
    /// Start from the earliest available message
    Earliest,
    /// Start from the latest message (new messages only)
    Latest,
}

impl Default for StartOffset {
    fn default() -> Self {
        Self::Earliest
    }
}

impl From<StartOffset> for krafka::consumer::AutoOffsetReset {
    fn from(offset: StartOffset) -> Self {
        match offset {
            StartOffset::Earliest => krafka::consumer::AutoOffsetReset::Earliest,
            StartOffset::Latest => krafka::consumer::AutoOffsetReset::Latest,
        }
    }
}

/// Acknowledgment level for produced messages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum AckLevel {
    /// No acknowledgment (fire-and-forget)
    None,
    /// Leader acknowledgment only
    Leader,
    /// All in-sync replicas must acknowledge
    All,
}

impl Default for AckLevel {
    fn default() -> Self {
        Self::All
    }
}

impl From<AckLevel> for krafka::producer::Acks {
    fn from(level: AckLevel) -> Self {
        match level {
            AckLevel::None => krafka::producer::Acks::None,
            AckLevel::Leader => krafka::producer::Acks::Leader,
            AckLevel::All => krafka::producer::Acks::All,
        }
    }
}

// ============================================================================
// Source Metrics
// ============================================================================

/// Snapshot of source metrics at a point in time
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KafkaSourceMetricsSnapshot {
    /// Total messages consumed
    pub messages_consumed: u64,
    /// Total bytes consumed
    pub bytes_consumed: u64,
    /// Total poll operations
    pub polls: u64,
    /// Empty polls (no messages returned)
    pub empty_polls: u64,
    /// Total errors encountered
    pub errors: u64,
    /// Total commit operations
    pub commits: u64,
    /// Failed commit operations
    pub commit_failures: u64,
    /// Total rebalance events
    pub rebalances: u64,
    /// Minimum batch size seen
    pub min_batch_size: u64,
    /// Maximum batch size seen
    pub max_batch_size: u64,
    /// Sum of all batch sizes (for computing average)
    pub batch_size_sum: u64,
    /// Number of non-empty batches (for computing average)
    pub batch_count: u64,
}

impl KafkaSourceMetricsSnapshot {
    /// Average batch size, or 0 if no batches were consumed
    #[inline]
    pub fn avg_batch_size(&self) -> f64 {
        if self.batch_count == 0 {
            0.0
        } else {
            self.batch_size_sum as f64 / self.batch_count as f64
        }
    }

    /// Consumption rate (messages per second) given a duration
    #[inline]
    pub fn rate(&self, elapsed: Duration) -> f64 {
        let secs = elapsed.as_secs_f64();
        if secs <= 0.0 {
            0.0
        } else {
            self.messages_consumed as f64 / secs
        }
    }

    /// Empty poll ratio (0.0–1.0)
    #[inline]
    pub fn empty_poll_rate(&self) -> f64 {
        if self.polls == 0 {
            0.0
        } else {
            self.empty_polls as f64 / self.polls as f64
        }
    }

    /// Format metrics in Prometheus exposition format
    pub fn to_prometheus_format(&self, prefix: &str) -> String {
        let mut out = String::with_capacity(1024);
        let p = prefix;

        let _ = write!(out,
            "# HELP {p}_messages_consumed_total Total messages consumed\n\
             # TYPE {p}_messages_consumed_total counter\n\
             {p}_messages_consumed_total {}\n",
            self.messages_consumed
        );
        let _ = write!(out,
            "# HELP {p}_bytes_consumed_total Total bytes consumed\n\
             # TYPE {p}_bytes_consumed_total counter\n\
             {p}_bytes_consumed_total {}\n",
            self.bytes_consumed
        );
        let _ = write!(out,
            "# HELP {p}_polls_total Total poll operations\n\
             # TYPE {p}_polls_total counter\n\
             {p}_polls_total {}\n",
            self.polls
        );
        let _ = write!(out,
            "# HELP {p}_empty_polls_total Empty poll operations\n\
             # TYPE {p}_empty_polls_total counter\n\
             {p}_empty_polls_total {}\n",
            self.empty_polls
        );
        let _ = write!(out,
            "# HELP {p}_errors_total Total errors\n\
             # TYPE {p}_errors_total counter\n\
             {p}_errors_total {}\n",
            self.errors
        );
        let _ = write!(out,
            "# HELP {p}_commits_total Total commit operations\n\
             # TYPE {p}_commits_total counter\n\
             {p}_commits_total {}\n",
            self.commits
        );
        let _ = write!(out,
            "# HELP {p}_commit_failures_total Failed commit operations\n\
             # TYPE {p}_commit_failures_total counter\n\
             {p}_commit_failures_total {}\n",
            self.commit_failures
        );
        let _ = write!(out,
            "# HELP {p}_rebalances_total Total rebalance events\n\
             # TYPE {p}_rebalances_total counter\n\
             {p}_rebalances_total {}\n",
            self.rebalances
        );
        let _ = write!(out,
            "# HELP {p}_batch_size_min Minimum batch size\n\
             # TYPE {p}_batch_size_min gauge\n\
             {p}_batch_size_min {}\n",
            self.min_batch_size
        );
        let _ = write!(out,
            "# HELP {p}_batch_size_max Maximum batch size\n\
             # TYPE {p}_batch_size_max gauge\n\
             {p}_batch_size_max {}\n",
            self.max_batch_size
        );
        let _ = write!(out,
            "# HELP {p}_batch_size_avg Average batch size\n\
             # TYPE {p}_batch_size_avg gauge\n\
             {p}_batch_size_avg {:.2}\n",
            self.avg_batch_size()
        );

        out
    }
}

/// Lock-free source metrics using atomic counters
pub struct KafkaSourceMetrics {
    messages_consumed: AtomicU64,
    bytes_consumed: AtomicU64,
    polls: AtomicU64,
    empty_polls: AtomicU64,
    errors: AtomicU64,
    commits: AtomicU64,
    commit_failures: AtomicU64,
    rebalances: AtomicU64,
    min_batch_size: AtomicU64,
    max_batch_size: AtomicU64,
    batch_size_sum: AtomicU64,
    batch_count: AtomicU64,
}

impl KafkaSourceMetrics {
    /// Create a new metrics instance with all counters at zero
    pub fn new() -> Self {
        Self {
            messages_consumed: AtomicU64::new(0),
            bytes_consumed: AtomicU64::new(0),
            polls: AtomicU64::new(0),
            empty_polls: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            commits: AtomicU64::new(0),
            commit_failures: AtomicU64::new(0),
            rebalances: AtomicU64::new(0),
            min_batch_size: AtomicU64::new(u64::MAX),
            max_batch_size: AtomicU64::new(0),
            batch_size_sum: AtomicU64::new(0),
            batch_count: AtomicU64::new(0),
        }
    }

    /// Record a consumed batch of messages
    #[inline]
    pub fn record_batch(&self, count: u64, bytes: u64) {
        self.messages_consumed.fetch_add(count, Ordering::Relaxed);
        self.bytes_consumed.fetch_add(bytes, Ordering::Relaxed);
        self.polls.fetch_add(1, Ordering::Relaxed);

        if count == 0 {
            self.empty_polls.fetch_add(1, Ordering::Relaxed);
        } else {
            self.batch_size_sum.fetch_add(count, Ordering::Relaxed);
            self.batch_count.fetch_add(1, Ordering::Relaxed);
            // CAS loop for min
            let mut current = self.min_batch_size.load(Ordering::Relaxed);
            while count < current {
                match self.min_batch_size.compare_exchange_weak(
                    current,
                    count,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }
            // CAS loop for max
            current = self.max_batch_size.load(Ordering::Relaxed);
            while count > current {
                match self.max_batch_size.compare_exchange_weak(
                    current,
                    count,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }
        }
    }

    /// Record an error
    #[inline]
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful commit
    #[inline]
    pub fn record_commit(&self) {
        self.commits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed commit
    #[inline]
    pub fn record_commit_failure(&self) {
        self.commit_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a rebalance event
    #[inline]
    pub fn record_rebalance(&self) {
        self.rebalances.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a snapshot of current metrics
    #[inline]
    pub fn snapshot(&self) -> KafkaSourceMetricsSnapshot {
        let min = self.min_batch_size.load(Ordering::Relaxed);
        KafkaSourceMetricsSnapshot {
            messages_consumed: self.messages_consumed.load(Ordering::Relaxed),
            bytes_consumed: self.bytes_consumed.load(Ordering::Relaxed),
            polls: self.polls.load(Ordering::Relaxed),
            empty_polls: self.empty_polls.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            commits: self.commits.load(Ordering::Relaxed),
            commit_failures: self.commit_failures.load(Ordering::Relaxed),
            rebalances: self.rebalances.load(Ordering::Relaxed),
            min_batch_size: if min == u64::MAX { 0 } else { min },
            max_batch_size: self.max_batch_size.load(Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum.load(Ordering::Relaxed),
            batch_count: self.batch_count.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters
    pub fn reset(&self) {
        self.messages_consumed.store(0, Ordering::Relaxed);
        self.bytes_consumed.store(0, Ordering::Relaxed);
        self.polls.store(0, Ordering::Relaxed);
        self.empty_polls.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.commits.store(0, Ordering::Relaxed);
        self.commit_failures.store(0, Ordering::Relaxed);
        self.rebalances.store(0, Ordering::Relaxed);
        self.min_batch_size.store(u64::MAX, Ordering::Relaxed);
        self.max_batch_size.store(0, Ordering::Relaxed);
        self.batch_size_sum.store(0, Ordering::Relaxed);
        self.batch_count.store(0, Ordering::Relaxed);
    }

    /// Snapshot and reset atomically (best-effort — counters are independent)
    pub fn snapshot_and_reset(&self) -> KafkaSourceMetricsSnapshot {
        let snap = self.snapshot();
        self.reset();
        snap
    }
}

impl Default for KafkaSourceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Sink Metrics
// ============================================================================

/// Snapshot of sink metrics at a point in time
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KafkaSinkMetricsSnapshot {
    /// Total messages produced
    pub messages_produced: u64,
    /// Total bytes produced
    pub bytes_produced: u64,
    /// Total send operations
    pub sends: u64,
    /// Total errors encountered
    pub errors: u64,
    /// Total flush operations
    pub flushes: u64,
    /// Total retries
    pub retries: u64,
    /// Minimum batch size seen
    pub min_batch_size: u64,
    /// Maximum batch size seen
    pub max_batch_size: u64,
    /// Sum of all batch sizes
    pub batch_size_sum: u64,
    /// Number of non-empty batches
    pub batch_count: u64,
}

impl KafkaSinkMetricsSnapshot {
    /// Average batch size, or 0 if no batches
    #[inline]
    pub fn avg_batch_size(&self) -> f64 {
        if self.batch_count == 0 {
            0.0
        } else {
            self.batch_size_sum as f64 / self.batch_count as f64
        }
    }

    /// Production rate (messages per second) given a duration
    #[inline]
    pub fn rate(&self, elapsed: Duration) -> f64 {
        let secs = elapsed.as_secs_f64();
        if secs <= 0.0 {
            0.0
        } else {
            self.messages_produced as f64 / secs
        }
    }

    /// Success rate (0.0–1.0)
    #[inline]
    pub fn success_rate(&self) -> f64 {
        let total = self.messages_produced + self.errors;
        if total == 0 {
            1.0
        } else {
            self.messages_produced as f64 / total as f64
        }
    }

    /// Format metrics in Prometheus exposition format
    pub fn to_prometheus_format(&self, prefix: &str) -> String {
        let mut out = String::with_capacity(1024);
        let p = prefix;

        let _ = write!(out,
            "# HELP {p}_messages_produced_total Total messages produced\n\
             # TYPE {p}_messages_produced_total counter\n\
             {p}_messages_produced_total {}\n",
            self.messages_produced
        );
        let _ = write!(out,
            "# HELP {p}_bytes_produced_total Total bytes produced\n\
             # TYPE {p}_bytes_produced_total counter\n\
             {p}_bytes_produced_total {}\n",
            self.bytes_produced
        );
        let _ = write!(out,
            "# HELP {p}_sends_total Total send operations\n\
             # TYPE {p}_sends_total counter\n\
             {p}_sends_total {}\n",
            self.sends
        );
        let _ = write!(out,
            "# HELP {p}_errors_total Total errors\n\
             # TYPE {p}_errors_total counter\n\
             {p}_errors_total {}\n",
            self.errors
        );
        let _ = write!(out,
            "# HELP {p}_flushes_total Total flush operations\n\
             # TYPE {p}_flushes_total counter\n\
             {p}_flushes_total {}\n",
            self.flushes
        );
        let _ = write!(out,
            "# HELP {p}_retries_total Total retries\n\
             # TYPE {p}_retries_total counter\n\
             {p}_retries_total {}\n",
            self.retries
        );
        let _ = write!(out,
            "# HELP {p}_batch_size_min Minimum batch size\n\
             # TYPE {p}_batch_size_min gauge\n\
             {p}_batch_size_min {}\n",
            self.min_batch_size
        );
        let _ = write!(out,
            "# HELP {p}_batch_size_max Maximum batch size\n\
             # TYPE {p}_batch_size_max gauge\n\
             {p}_batch_size_max {}\n",
            self.max_batch_size
        );
        let _ = write!(out,
            "# HELP {p}_batch_size_avg Average batch size\n\
             # TYPE {p}_batch_size_avg gauge\n\
             {p}_batch_size_avg {:.2}\n",
            self.avg_batch_size()
        );

        out
    }
}

/// Lock-free sink metrics using atomic counters
pub struct KafkaSinkMetrics {
    messages_produced: AtomicU64,
    bytes_produced: AtomicU64,
    sends: AtomicU64,
    errors: AtomicU64,
    flushes: AtomicU64,
    retries: AtomicU64,
    min_batch_size: AtomicU64,
    max_batch_size: AtomicU64,
    batch_size_sum: AtomicU64,
    batch_count: AtomicU64,
}

impl KafkaSinkMetrics {
    /// Create a new metrics instance with all counters at zero
    pub fn new() -> Self {
        Self {
            messages_produced: AtomicU64::new(0),
            bytes_produced: AtomicU64::new(0),
            sends: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            min_batch_size: AtomicU64::new(u64::MAX),
            max_batch_size: AtomicU64::new(0),
            batch_size_sum: AtomicU64::new(0),
            batch_count: AtomicU64::new(0),
        }
    }

    /// Record a produced batch
    #[inline]
    pub fn record_batch(&self, count: u64, bytes: u64) {
        self.messages_produced.fetch_add(count, Ordering::Relaxed);
        self.bytes_produced.fetch_add(bytes, Ordering::Relaxed);
        self.sends.fetch_add(1, Ordering::Relaxed);

        if count > 0 {
            self.batch_size_sum.fetch_add(count, Ordering::Relaxed);
            self.batch_count.fetch_add(1, Ordering::Relaxed);
            // CAS loop for min
            let mut current = self.min_batch_size.load(Ordering::Relaxed);
            while count < current {
                match self.min_batch_size.compare_exchange_weak(
                    current,
                    count,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }
            // CAS loop for max
            current = self.max_batch_size.load(Ordering::Relaxed);
            while count > current {
                match self.max_batch_size.compare_exchange_weak(
                    current,
                    count,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }
        }
    }

    /// Record an error
    #[inline]
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a flush
    #[inline]
    pub fn record_flush(&self) {
        self.flushes.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a retry
    #[inline]
    pub fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a snapshot of current metrics
    #[inline]
    pub fn snapshot(&self) -> KafkaSinkMetricsSnapshot {
        let min = self.min_batch_size.load(Ordering::Relaxed);
        KafkaSinkMetricsSnapshot {
            messages_produced: self.messages_produced.load(Ordering::Relaxed),
            bytes_produced: self.bytes_produced.load(Ordering::Relaxed),
            sends: self.sends.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            flushes: self.flushes.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            min_batch_size: if min == u64::MAX { 0 } else { min },
            max_batch_size: self.max_batch_size.load(Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum.load(Ordering::Relaxed),
            batch_count: self.batch_count.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters
    pub fn reset(&self) {
        self.messages_produced.store(0, Ordering::Relaxed);
        self.bytes_produced.store(0, Ordering::Relaxed);
        self.sends.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.flushes.store(0, Ordering::Relaxed);
        self.retries.store(0, Ordering::Relaxed);
        self.min_batch_size.store(u64::MAX, Ordering::Relaxed);
        self.max_batch_size.store(0, Ordering::Relaxed);
        self.batch_size_sum.store(0, Ordering::Relaxed);
        self.batch_count.store(0, Ordering::Relaxed);
    }

    /// Snapshot and reset
    pub fn snapshot_and_reset(&self) -> KafkaSinkMetricsSnapshot {
        let snap = self.snapshot();
        self.reset();
        snap
    }
}

impl Default for KafkaSinkMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Source Configuration
// ============================================================================

/// Configuration for the Kafka source connector
#[derive(Debug, Clone, Serialize, Deserialize, Validate, JsonSchema)]
pub struct KafkaSourceConfig {
    /// Comma-separated list of broker addresses (e.g., "localhost:9092")
    #[validate(length(min = 1, message = "At least one broker address is required"))]
    pub brokers: String,

    /// Topic to consume from
    #[validate(length(min = 1, message = "Topic name is required"))]
    pub topic: String,

    /// Consumer group ID
    #[serde(default = "default_consumer_group")]
    pub consumer_group: String,

    /// Where to start consuming when no committed offset exists
    #[serde(default)]
    pub start_offset: StartOffset,

    /// Security configuration
    #[serde(default)]
    #[validate(nested)]
    pub security: KafkaSecurityConfig,

    /// Maximum number of records per poll
    #[serde(default = "default_max_poll_records")]
    pub max_poll_records: u32,

    /// Maximum interval between polls before consumer is considered dead (ms)
    #[serde(default = "default_max_poll_interval_ms")]
    pub max_poll_interval_ms: u64,

    /// Session timeout for the consumer group (ms)
    #[serde(default = "default_session_timeout_ms")]
    pub session_timeout_ms: u64,

    /// Heartbeat interval (ms)
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,

    /// Whether to include Kafka headers in event metadata
    #[serde(default)]
    pub include_headers: bool,

    /// Whether to include the message key in event metadata
    #[serde(default)]
    pub include_key: bool,

    /// Enable auto-commit of offsets
    #[serde(default = "default_true")]
    pub enable_auto_commit: bool,

    /// Auto-commit interval (ms)
    #[serde(default = "default_auto_commit_interval_ms")]
    pub auto_commit_interval_ms: u64,

    /// Minimum bytes to fetch per request
    #[serde(default = "default_fetch_min_bytes")]
    pub fetch_min_bytes: u32,

    /// Maximum bytes to fetch per request
    #[serde(default = "default_fetch_max_bytes")]
    pub fetch_max_bytes: u32,

    /// Request timeout (ms)
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,

    /// Delay between empty polls (ms)
    #[serde(default = "default_empty_poll_delay_ms")]
    pub empty_poll_delay_ms: u64,

    /// Client ID for this consumer
    pub client_id: Option<String>,

    /// Isolation level for transactional reads.
    ///
    /// `read_uncommitted` (default) returns all messages including those from
    /// aborted transactions. `read_committed` returns only committed transactional
    /// messages (required for exactly-once consumers).
    #[serde(default)]
    pub isolation_level: IsolationLevel,
}

/// Isolation level for Kafka consumers (transactional read semantics)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IsolationLevel {
    /// Return all records including aborted transactions
    ReadUncommitted,
    /// Return only committed transactional records
    ReadCommitted,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        Self::ReadUncommitted
    }
}

impl From<IsolationLevel> for krafka::consumer::IsolationLevel {
    fn from(level: IsolationLevel) -> Self {
        match level {
            IsolationLevel::ReadUncommitted => krafka::consumer::IsolationLevel::ReadUncommitted,
            IsolationLevel::ReadCommitted => krafka::consumer::IsolationLevel::ReadCommitted,
        }
    }
}

fn default_consumer_group() -> String {
    "rivven-connect".to_string()
}
fn default_max_poll_records() -> u32 {
    500
}
fn default_max_poll_interval_ms() -> u64 {
    300_000
}
fn default_session_timeout_ms() -> u64 {
    30_000
}
fn default_heartbeat_interval_ms() -> u64 {
    3_000
}
fn default_true() -> bool {
    true
}
fn default_auto_commit_interval_ms() -> u64 {
    5_000
}
fn default_fetch_min_bytes() -> u32 {
    1
}
fn default_fetch_max_bytes() -> u32 {
    52_428_800 // 50 MB
}
fn default_request_timeout_ms() -> u64 {
    30_000
}
fn default_empty_poll_delay_ms() -> u64 {
    100
}

// ============================================================================
// Source Implementation
// ============================================================================

/// Kafka source connector
///
/// Consumes messages from a Kafka topic using krafka's native consumer groups.
pub struct KafkaSource;

impl KafkaSource {
    /// Build a krafka admin client for health checks and discovery
    async fn build_admin_client(
        config: &KafkaSourceConfig,
    ) -> Result<krafka::admin::AdminClient> {
        let mut builder = krafka::admin::AdminClient::builder()
            .bootstrap_servers(&config.brokers)
            .request_timeout(Duration::from_millis(config.request_timeout_ms));

        if let Some(ref client_id) = config.client_id {
            builder = builder.client_id(client_id);
        }

        if let Some(auth) = config.security.to_auth_config()? {
            builder = builder.auth(auth);
        }

        Ok(builder
            .build()
            .await
            .map_err(|e| ConnectorError::Connection(format!("Failed to connect to Kafka: {e}")))?)
    }

    /// Build a krafka consumer
    async fn build_consumer(
        config: &KafkaSourceConfig,
    ) -> Result<krafka::consumer::Consumer> {
        let mut builder = krafka::consumer::Consumer::builder()
            .bootstrap_servers(&config.brokers)
            .group_id(&config.consumer_group)
            .auto_offset_reset(config.start_offset.into())
            .enable_auto_commit(config.enable_auto_commit)
            .auto_commit_interval(Duration::from_millis(config.auto_commit_interval_ms))
            .session_timeout(Duration::from_millis(config.session_timeout_ms))
            .heartbeat_interval(Duration::from_millis(config.heartbeat_interval_ms))
            .max_poll_records(config.max_poll_records as i32)
            .max_poll_interval(Duration::from_millis(config.max_poll_interval_ms))
            .fetch_min_bytes(config.fetch_min_bytes as i32)
            .fetch_max_bytes(config.fetch_max_bytes as i32)
            .request_timeout(Duration::from_millis(config.request_timeout_ms))
            .isolation_level(config.isolation_level.into());

        if let Some(ref client_id) = config.client_id {
            builder = builder.client_id(client_id);
        }

        // Wire SASL/TLS auth to the consumer (krafka v0.2+ supports auth on all clients)
        if let Some(auth) = config.security.to_auth_config()? {
            builder = builder.auth(auth);
        }

        Ok(builder
            .build()
            .await
            .map_err(|e| ConnectorError::Connection(format!("Failed to create consumer: {e}")))?)
    }
}

#[async_trait]
impl Source for KafkaSource {
    type Config = KafkaSourceConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("kafka", env!("CARGO_PKG_VERSION"))
            .description("Apache Kafka source connector (krafka)")
            .metadata("protocol", "kafka")
            .sync_modes(vec![SyncModeSpec::FullRefresh, SyncModeSpec::Incremental])
            .build()
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        if let Err(e) = config.validate() {
            return Ok(CheckResult::builder()
                .check_failed("config", format!("Invalid configuration: {e}"))
                .build());
        }

        let start = Instant::now();

        // 1. Connectivity: Try to create an admin client
        let admin = match Self::build_admin_client(config).await {
            Ok(admin) => admin,
            Err(e) => {
                return Ok(CheckResult::builder()
                    .check_failed("connectivity", format!("Failed to connect: {e}"))
                    .build());
            }
        };
        let connectivity_ms = start.elapsed().as_millis() as u64;

        // 2. Topic exists: Check that the configured topic has partitions
        let topic_start = Instant::now();
        let topic_check = match admin.partition_count(&config.topic).await {
            Ok(Some(count)) if count > 0 => CheckDetail::passed("topic")
                .with_message(format!(
                    "Topic '{}' exists with {} partition(s)",
                    config.topic, count
                ))
                .with_duration_ms(topic_start.elapsed().as_millis() as u64),
            Ok(_) => CheckDetail::failed("topic", format!("Topic '{}' has 0 partitions", config.topic))
                .with_duration_ms(topic_start.elapsed().as_millis() as u64),
            Err(e) => CheckDetail::failed(
                "topic",
                format!("Topic '{}' not found or inaccessible: {e}", config.topic),
            )
            .with_duration_ms(topic_start.elapsed().as_millis() as u64),
        };

        Ok(CheckResult::builder()
            .check(
                CheckDetail::passed("connectivity")
                    .with_message(format!("Connected to {}", config.brokers))
                    .with_duration_ms(connectivity_ms),
            )
            .check(topic_check)
            .build())
    }

    async fn discover(&self, config: &Self::Config) -> Result<Catalog> {
        let admin = Self::build_admin_client(config).await?;

        let topics = admin.list_topics().await.map_err(|e| {
            ConnectorError::Connection(format!("Failed to list topics: {e}"))
        })?;

        let mut catalog = Catalog::new();
        for topic_name in topics {
            // Skip internal topics
            if topic_name.starts_with("__") {
                continue;
            }

            let partition_count = admin
                .partition_count(&topic_name)
                .await
                .ok()
                .flatten()
                .unwrap_or(0);

            let schema = serde_json::json!({
                "type": "object",
                "properties": {
                    "key": { "type": ["string", "null"] },
                    "value": { "type": ["string", "null"] },
                    "topic": { "type": "string" },
                    "partition": { "type": "integer" },
                    "offset": { "type": "integer" },
                    "timestamp": { "type": "integer" },
                    "headers": { "type": "object" }
                }
            });

            let mut stream = Stream::new(&topic_name, schema);
            stream.supported_sync_modes = vec![SyncMode::FullRefresh, SyncMode::Incremental];
            stream.source_defined_primary_key = Some(vec![
                vec!["topic".to_string()],
                vec!["partition".to_string()],
                vec!["offset".to_string()],
            ]);

            // Store partition count in metadata
            stream.metadata.insert(
                "partition_count".to_string(),
                serde_json::Value::Number(partition_count.into()),
            );

            catalog = catalog.add_stream(stream);
        }

        Ok(catalog)
    }

    async fn read(
        &self,
        config: &Self::Config,
        _catalog: &ConfiguredCatalog,
        _state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        let consumer = Self::build_consumer(config).await?;

        consumer.subscribe(&[&config.topic]).await.map_err(|e| {
            ConnectorError::Connection(format!("Failed to subscribe to topic: {e}"))
        })?;

        info!(
            topic = %config.topic,
            group = %config.consumer_group,
            "Kafka consumer subscribed"
        );

        let topic = config.topic.clone();
        let include_headers = config.include_headers;
        let include_key = config.include_key;
        let empty_poll_delay = Duration::from_millis(config.empty_poll_delay_ms);
        let metrics = std::sync::Arc::new(KafkaSourceMetrics::new());

        let stream = async_stream::stream! {
            // Reusable position buffer to avoid allocation per record
            let mut pos_buf = String::with_capacity(64);

            loop {
                match consumer.poll(Duration::from_millis(100)).await {
                    Ok(records) => {
                        let count = records.len();
                        let bytes: u64 = records.iter().map(|r| {
                            r.serialized_key_size.max(0) as u64
                                + r.serialized_value_size.max(0) as u64
                        }).sum();

                        metrics.record_batch(count as u64, bytes);

                        if count == 0 {
                            tokio::time::sleep(empty_poll_delay).await;
                            continue;
                        }

                        for record in records {
                            let value = match &record.value {
                                Some(v) => {
                                    // Try to parse as JSON, fall back to string
                                    match serde_json::from_slice::<serde_json::Value>(v) {
                                        Ok(json) => json,
                                        Err(_) => {
                                            serde_json::Value::String(
                                                String::from_utf8_lossy(v).into_owned()
                                            )
                                        }
                                    }
                                }
                                None => serde_json::Value::Null,
                            };

                            // Build position string re-using buffer
                            pos_buf.clear();
                            let _ = write!(pos_buf, "{}:{}:{}", record.topic, record.partition, record.offset);

                            let mut event = SourceEvent::record(&topic, value)
                                .with_namespace("kafka")
                                .with_position(pos_buf.as_str());

                            // Include key in metadata
                            if include_key {
                                if let Some(ref key) = record.key {
                                    event = event.with_metadata(
                                        "kafka_key",
                                        serde_json::Value::String(
                                            String::from_utf8_lossy(key).into_owned()
                                        ),
                                    );
                                }
                            }

                            // Include headers in metadata
                            if include_headers && !record.headers.is_empty() {
                                let headers_map: serde_json::Map<String, serde_json::Value> =
                                    record
                                        .headers
                                        .iter()
                                        .map(|(k, v)| {
                                            let val = match v {
                                                Some(b) => serde_json::Value::String(
                                                    String::from_utf8_lossy(b).into_owned(),
                                                ),
                                                None => serde_json::Value::Null,
                                            };
                                            (k.clone(), val)
                                        })
                                        .collect();
                                event = event.with_metadata(
                                    "kafka_headers",
                                    serde_json::Value::Object(headers_map),
                                );
                            }

                            // Add partition and offset as metadata
                            event = event
                                .with_metadata(
                                    "kafka_partition",
                                    serde_json::Value::Number(record.partition.into()),
                                )
                                .with_metadata(
                                    "kafka_offset",
                                    serde_json::Value::Number(record.offset.into()),
                                )
                                .with_metadata(
                                    "kafka_timestamp",
                                    serde_json::Value::Number(record.timestamp.into()),
                                );

                            yield Ok(event);
                        }
                    }
                    Err(e) => {
                        metrics.record_error();
                        warn!(error = %e, "Kafka poll error");
                        // Yield the error and continue — the consumer group
                        // protocol will handle rebalances automatically
                        yield Err(ConnectorError::Connection(
                            format!("Kafka poll error: {e}")
                        ).into());
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        };

        Ok(Box::pin(stream))
    }
}

// ============================================================================
// Sink Configuration
// ============================================================================

/// Configuration for the Kafka sink connector
#[derive(Debug, Clone, Serialize, Deserialize, Validate, JsonSchema)]
pub struct KafkaSinkConfig {
    /// Comma-separated list of broker addresses
    #[validate(length(min = 1, message = "At least one broker address is required"))]
    pub brokers: String,

    /// Topic to produce to
    #[validate(length(min = 1, message = "Topic name is required"))]
    pub topic: String,

    /// Acknowledgment level
    #[serde(default)]
    pub acks: AckLevel,

    /// Compression codec
    #[serde(default)]
    pub compression: CompressionCodec,

    /// Security configuration
    #[serde(default)]
    #[validate(nested)]
    pub security: KafkaSecurityConfig,

    /// Maximum batch size in bytes (krafka producer batching)
    #[serde(default = "default_batch_size_bytes")]
    pub batch_size_bytes: u32,

    /// Time to wait for additional messages before sending a batch (ms)
    #[serde(default = "default_linger_ms")]
    pub linger_ms: u64,

    /// Request timeout (ms)
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,

    /// Number of retries for failed sends
    #[serde(default = "default_retries")]
    pub retries: u32,

    /// Delay between retries (ms)
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,

    /// Whether to enable idempotent producer
    #[serde(default)]
    pub idempotent: bool,

    /// Transactional ID for exactly-once semantics (optional)
    pub transactional_id: Option<String>,

    /// JSON field to use as the message key (optional)
    pub key_field: Option<String>,

    /// Whether to include event metadata as Kafka headers
    #[serde(default)]
    pub include_headers: bool,

    /// Custom headers to add to every message
    #[serde(default)]
    pub custom_headers: HashMap<String, String>,

    /// Client ID for this producer
    pub client_id: Option<String>,
}

fn default_batch_size_bytes() -> u32 {
    16_384 // 16 KB
}
fn default_linger_ms() -> u64 {
    5
}
fn default_retries() -> u32 {
    3
}
fn default_retry_backoff_ms() -> u64 {
    100
}

// ============================================================================
// Sink Implementation
// ============================================================================

/// Kafka sink connector
///
/// Produces messages to a Kafka topic using krafka's built-in batching and compression.
pub struct KafkaSink;

impl KafkaSink {
    /// Build a krafka admin client for health checks
    async fn build_admin_client(
        config: &KafkaSinkConfig,
    ) -> Result<krafka::admin::AdminClient> {
        let mut builder = krafka::admin::AdminClient::builder()
            .bootstrap_servers(&config.brokers)
            .request_timeout(Duration::from_millis(config.request_timeout_ms));

        if let Some(ref client_id) = config.client_id {
            builder = builder.client_id(client_id);
        }

        if let Some(auth) = config.security.to_auth_config()? {
            builder = builder.auth(auth);
        }

        Ok(builder
            .build()
            .await
            .map_err(|e| ConnectorError::Connection(format!("Failed to connect to Kafka: {e}")))?)
    }

    /// Build a krafka producer
    async fn build_producer(
        config: &KafkaSinkConfig,
    ) -> Result<krafka::producer::Producer> {
        let mut builder = krafka::producer::Producer::builder()
            .bootstrap_servers(&config.brokers)
            .acks(config.acks.into())
            .compression(config.compression.into())
            .batch_size(config.batch_size_bytes as usize)
            .linger(Duration::from_millis(config.linger_ms))
            .request_timeout(Duration::from_millis(config.request_timeout_ms))
            .retries(config.retries)
            .retry_backoff(Duration::from_millis(config.retry_backoff_ms));

        if let Some(ref client_id) = config.client_id {
            builder = builder.client_id(client_id);
        }

        // Wire SASL/TLS auth to the producer (krafka v0.2+ supports auth on all clients)
        if let Some(auth) = config.security.to_auth_config()? {
            builder = builder.auth(auth);
        }

        Ok(builder
            .build()
            .await
            .map_err(|e| ConnectorError::Connection(format!("Failed to create producer: {e}")))?)
    }

    /// Extract message key from event data using the configured key field
    fn extract_key(event: &SourceEvent, key_field: Option<&str>) -> Option<Bytes> {
        key_field.and_then(|field| {
            event
                .data
                .get(field)
                .and_then(|v| match v {
                    serde_json::Value::String(s) => Some(Bytes::from(s.clone())),
                    other => serde_json::to_vec(other).ok().map(Bytes::from),
                })
        })
    }

    /// Build headers list from event metadata and custom headers
    fn build_headers(
        event: &SourceEvent,
        include_headers: bool,
        custom_headers: &HashMap<String, String>,
    ) -> Vec<(String, Vec<u8>)> {
        let capacity = custom_headers.len() + if include_headers { 5 } else { 0 };
        let mut headers = Vec::with_capacity(capacity);

        // Add custom headers first
        for (k, v) in custom_headers {
            headers.push((k.clone(), v.as_bytes().to_vec()));
        }

        // Add event metadata as headers if configured
        if include_headers {
            headers.push((
                "rivven_stream".to_string(),
                event.stream.as_bytes().to_vec(),
            ));
            headers.push((
                "rivven_event_type".to_string(),
                event.event_type.as_str().as_bytes().to_vec(),
            ));
            if let Some(ref ns) = event.namespace {
                headers.push(("rivven_namespace".to_string(), ns.as_bytes().to_vec()));
            }
            if let Some(ref pos) = event.metadata.position {
                headers.push(("rivven_position".to_string(), pos.as_bytes().to_vec()));
            }
            if let Some(ref tx) = event.metadata.transaction_id {
                headers.push((
                    "rivven_transaction_id".to_string(),
                    tx.as_bytes().to_vec(),
                ));
            }
        }

        headers
    }
}

#[async_trait]
impl Sink for KafkaSink {
    type Config = KafkaSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("kafka", env!("CARGO_PKG_VERSION"))
            .description("Apache Kafka sink connector (krafka)")
            .metadata("protocol", "kafka")
            .build()
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        if let Err(e) = config.validate() {
            return Ok(CheckResult::builder()
                .check_failed("config", format!("Invalid configuration: {e}"))
                .build());
        }

        let start = Instant::now();

        let admin = match Self::build_admin_client(config).await {
            Ok(admin) => admin,
            Err(e) => {
                return Ok(CheckResult::builder()
                    .check_failed("connectivity", format!("Failed to connect: {e}"))
                    .build());
            }
        };
        let connectivity_ms = start.elapsed().as_millis() as u64;

        // Check topic exists
        let topic_start = Instant::now();
        let topic_check = match admin.partition_count(&config.topic).await {
            Ok(Some(count)) if count > 0 => CheckDetail::passed("topic")
                .with_message(format!(
                    "Topic '{}' exists with {} partition(s)",
                    config.topic, count
                ))
                .with_duration_ms(topic_start.elapsed().as_millis() as u64),
            Ok(_) => CheckDetail::failed(
                "topic",
                format!("Topic '{}' has 0 partitions", config.topic),
            )
            .with_duration_ms(topic_start.elapsed().as_millis() as u64),
            Err(e) => CheckDetail::failed(
                "topic",
                format!("Topic '{}' not found or inaccessible: {e}", config.topic),
            )
            .with_duration_ms(topic_start.elapsed().as_millis() as u64),
        };

        Ok(CheckResult::builder()
            .check(
                CheckDetail::passed("connectivity")
                    .with_message(format!("Connected to {}", config.brokers))
                    .with_duration_ms(connectivity_ms),
            )
            .check(topic_check)
            .build())
    }

    async fn write(
        &self,
        config: &Self::Config,
        events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        use futures::StreamExt;

        let write_start = Instant::now();
        let producer = Self::build_producer(config).await?;
        let metrics = KafkaSinkMetrics::new();
        let mut result = WriteResult::new();
        let topic = &config.topic;
        let key_field = config.key_field.as_deref();
        let include_headers = config.include_headers;
        let custom_headers = &config.custom_headers;

        info!(topic = %topic, "Kafka producer started");

        let mut events = events;
        while let Some(event) = events.next().await {
            // Skip non-data events
            if !event.is_data() {
                continue;
            }

            let value_bytes = match serde_json::to_vec(&event.data) {
                Ok(bytes) => Bytes::from(bytes),
                Err(e) => {
                    metrics.record_error();
                    result.add_failure(1, format!("Serialization error: {e}"));
                    continue;
                }
            };

            let key = Self::extract_key(&event, key_field);
            let headers = Self::build_headers(&event, include_headers, custom_headers);
            let value_len = value_bytes.len() as u64;

            let send_result = if headers.is_empty() {
                producer
                    .send(topic, key.as_deref(), &value_bytes)
                    .await
            } else {
                producer
                    .send_with_headers(topic, key.as_deref(), &value_bytes, headers)
                    .await
            };

            match send_result {
                Ok(_metadata) => {
                    metrics.record_batch(1, value_len);
                    result.add_success(1, value_len);
                }
                Err(e) => {
                    metrics.record_error();
                    result.add_failure(1, format!("Send error: {e}"));
                    warn!(error = %e, "Failed to send message to Kafka");
                }
            }
        }

        // Flush remaining messages
        debug!("Flushing Kafka producer");
        if let Err(e) = producer.flush().await {
            metrics.record_error();
            warn!(error = %e, "Failed to flush producer");
        } else {
            metrics.record_flush();
        }

        // Close the producer
        producer.close().await;

        let snap = metrics.snapshot();
        info!(
            messages = snap.messages_produced,
            bytes = snap.bytes_produced,
            errors = snap.errors,
            duration_ms = write_start.elapsed().as_millis() as u64,
            "Kafka producer finished"
        );

        Ok(result)
    }
}

// ============================================================================
// Factory & Registry
// ============================================================================

/// Factory for creating Kafka source instances
pub struct KafkaSourceFactory;

impl SourceFactory for KafkaSourceFactory {
    fn spec(&self) -> ConnectorSpec {
        KafkaSource::spec()
    }

    fn create(&self) -> Box<dyn AnySource> {
        Box::new(KafkaSource)
    }
}

#[async_trait]
impl AnySource for KafkaSource {
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult> {
        let config: KafkaSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Kafka source config: {e}")))?;
        self.check(&config).await
    }

    async fn discover_raw(&self, config: &serde_yaml::Value) -> Result<Catalog> {
        let config: KafkaSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Kafka source config: {e}")))?;
        self.discover(&config).await
    }

    async fn read_raw(
        &self,
        config: &serde_yaml::Value,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        let config: KafkaSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Kafka source config: {e}")))?;
        self.read(&config, catalog, state).await
    }
}

/// Factory for creating Kafka sink instances
pub struct KafkaSinkFactory;

impl SinkFactory for KafkaSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        KafkaSink::spec()
    }

    fn create(&self) -> Box<dyn AnySink> {
        Box::new(KafkaSink)
    }
}

#[async_trait]
impl AnySink for KafkaSink {
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult> {
        let config: KafkaSinkConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Kafka sink config: {e}")))?;
        self.check(&config).await
    }

    async fn write_raw(
        &self,
        config: &serde_yaml::Value,
        events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        let config: KafkaSinkConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Kafka sink config: {e}")))?;
        self.write(&config, events).await
    }
}

/// Register Kafka sources into a source registry
pub fn register_sources(registry: &mut SourceRegistry) {
    registry.register("kafka", std::sync::Arc::new(KafkaSourceFactory));
}

/// Register Kafka sinks into a sink registry
pub fn register_sinks(registry: &mut SinkRegistry) {
    registry.register("kafka", std::sync::Arc::new(KafkaSinkFactory));
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -- Config deserialization -----------------------------------------------

    #[test]
    fn test_source_config_minimal() {
        let yaml = r#"
            brokers: "localhost:9092"
            topic: "test-topic"
        "#;
        let config: KafkaSourceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.topic, "test-topic");
        assert_eq!(config.consumer_group, "rivven-connect");
        assert_eq!(config.start_offset, StartOffset::Earliest);
        assert!(config.enable_auto_commit);
        assert_eq!(config.max_poll_records, 500);
    }

    #[test]
    fn test_source_config_full() {
        let yaml = r#"
            brokers: "broker1:9092,broker2:9092"
            topic: "events"
            consumer_group: "my-group"
            start_offset: latest
            max_poll_records: 1000
            session_timeout_ms: 45000
            heartbeat_interval_ms: 5000
            include_headers: true
            include_key: true
            enable_auto_commit: false
            client_id: "my-app"
            security:
              protocol: SASL_SSL
              sasl_mechanism: SCRAM-SHA-256
              sasl_username: "user"
              sasl_password: "pass"
        "#;
        let config: KafkaSourceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.consumer_group, "my-group");
        assert_eq!(config.start_offset, StartOffset::Latest);
        assert_eq!(config.max_poll_records, 1000);
        assert_eq!(config.session_timeout_ms, 45000);
        assert!(config.include_headers);
        assert!(config.include_key);
        assert!(!config.enable_auto_commit);
        assert_eq!(config.client_id, Some("my-app".to_string()));
        assert_eq!(config.security.protocol, SecurityProtocol::SaslSsl);
        assert_eq!(
            config.security.sasl_mechanism,
            Some(SaslMechanism::ScramSha256)
        );
    }

    #[test]
    fn test_sink_config_minimal() {
        let yaml = r#"
            brokers: "localhost:9092"
            topic: "output"
        "#;
        let config: KafkaSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.topic, "output");
        assert_eq!(config.acks, AckLevel::All);
        assert_eq!(config.compression, CompressionCodec::None);
        assert!(!config.idempotent);
        assert!(config.transactional_id.is_none());
    }

    #[test]
    fn test_sink_config_full() {
        let yaml = r#"
            brokers: "broker1:9092,broker2:9092"
            topic: "output"
            acks: leader
            compression: zstd
            batch_size_bytes: 32768
            linger_ms: 10
            retries: 5
            idempotent: true
            transactional_id: "my-tx"
            key_field: "id"
            include_headers: true
            custom_headers:
              source: "rivven"
              version: "1.0"
        "#;
        let config: KafkaSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.acks, AckLevel::Leader);
        assert_eq!(config.compression, CompressionCodec::Zstd);
        assert_eq!(config.batch_size_bytes, 32768);
        assert_eq!(config.linger_ms, 10);
        assert_eq!(config.retries, 5);
        assert!(config.idempotent);
        assert_eq!(config.transactional_id, Some("my-tx".to_string()));
        assert_eq!(config.key_field, Some("id".to_string()));
        assert!(config.include_headers);
        assert_eq!(config.custom_headers.len(), 2);
        assert_eq!(config.custom_headers.get("source"), Some(&"rivven".to_string()));
    }

    // -- Security config ------------------------------------------------------

    #[test]
    fn test_security_protocol_deserialization() {
        let cases = [
            ("PLAINTEXT", SecurityProtocol::Plaintext),
            ("SSL", SecurityProtocol::Ssl),
            ("SASL_PLAINTEXT", SecurityProtocol::SaslPlaintext),
            ("SASL_SSL", SecurityProtocol::SaslSsl),
        ];
        for (input, expected) in cases {
            let yaml = format!("protocol: {input}");
            let config: KafkaSecurityConfig = serde_yaml::from_str(&yaml).unwrap();
            assert_eq!(config.protocol, expected, "Failed for {input}");
        }
    }

    #[test]
    fn test_sasl_mechanism_deserialization() {
        let cases = [
            ("PLAIN", SaslMechanism::Plain),
            ("SCRAM-SHA-256", SaslMechanism::ScramSha256),
            ("SCRAM-SHA-512", SaslMechanism::ScramSha512),
            ("OAUTHBEARER", SaslMechanism::OAuthBearer),
            ("AWS_MSK_IAM", SaslMechanism::AwsMskIam),
        ];
        for (input, expected) in cases {
            let yaml = format!("sasl_mechanism: {input}");
            let config: KafkaSecurityConfig = serde_yaml::from_str(&yaml).unwrap();
            assert_eq!(config.sasl_mechanism, Some(expected), "Failed for {input}");
        }
    }

    #[test]
    fn test_security_config_auth_plaintext() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::Plaintext,
            ..Default::default()
        };
        assert!(config.to_auth_config().unwrap().is_none());
    }

    #[test]
    fn test_security_config_auth_sasl_plain() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::Plain),
            sasl_username: Some("user".to_string()),
            sasl_password: Some("pass".to_string()),
            ..Default::default()
        };
        assert!(config.to_auth_config().unwrap().is_some());
    }

    #[test]
    fn test_security_config_sasl_missing_username() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::Plain),
            sasl_password: Some("pass".to_string()),
            ..Default::default()
        };
        assert!(config.to_auth_config().is_err());
    }

    #[test]
    fn test_security_config_sasl_missing_password() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::Plain),
            sasl_username: Some("user".to_string()),
            ..Default::default()
        };
        assert!(config.to_auth_config().is_err());
    }

    // -- Compression codec ----------------------------------------------------

    #[test]
    fn test_compression_codec_conversion() {
        let cases = [
            (CompressionCodec::None, krafka::protocol::Compression::None),
            (CompressionCodec::Gzip, krafka::protocol::Compression::Gzip),
            (
                CompressionCodec::Snappy,
                krafka::protocol::Compression::Snappy,
            ),
            (CompressionCodec::Lz4, krafka::protocol::Compression::Lz4),
            (CompressionCodec::Zstd, krafka::protocol::Compression::Zstd),
        ];
        for (input, expected) in cases {
            let result: krafka::protocol::Compression = input.into();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_compression_deserialization() {
        let cases = ["none", "gzip", "snappy", "lz4", "zstd"];
        for name in cases {
            let yaml = format!("compression: {name}\nbrokers: x\ntopic: t");
            let config: KafkaSinkConfig = serde_yaml::from_str(&yaml).unwrap();
            assert_ne!(format!("{:?}", config.compression), "");
        }
    }

    // -- Start offset / Ack level ---------------------------------------------

    #[test]
    fn test_start_offset_conversion() {
        assert_eq!(
            krafka::consumer::AutoOffsetReset::from(StartOffset::Earliest),
            krafka::consumer::AutoOffsetReset::Earliest
        );
        assert_eq!(
            krafka::consumer::AutoOffsetReset::from(StartOffset::Latest),
            krafka::consumer::AutoOffsetReset::Latest
        );
    }

    #[test]
    fn test_ack_level_conversion() {
        assert_eq!(
            krafka::producer::Acks::from(AckLevel::None),
            krafka::producer::Acks::None
        );
        assert_eq!(
            krafka::producer::Acks::from(AckLevel::Leader),
            krafka::producer::Acks::Leader
        );
        assert_eq!(
            krafka::producer::Acks::from(AckLevel::All),
            krafka::producer::Acks::All
        );
    }

    // -- Source metrics --------------------------------------------------------

    #[test]
    fn test_source_metrics_new() {
        let metrics = KafkaSourceMetrics::new();
        let snap = metrics.snapshot();
        assert_eq!(snap.messages_consumed, 0);
        assert_eq!(snap.bytes_consumed, 0);
        assert_eq!(snap.polls, 0);
        assert_eq!(snap.empty_polls, 0);
        assert_eq!(snap.errors, 0);
        assert_eq!(snap.min_batch_size, 0); // u64::MAX => 0 in snapshot
        assert_eq!(snap.max_batch_size, 0);
    }

    #[test]
    fn test_source_metrics_record_batch() {
        let metrics = KafkaSourceMetrics::new();
        metrics.record_batch(10, 1024);
        metrics.record_batch(20, 2048);
        metrics.record_batch(5, 512);

        let snap = metrics.snapshot();
        assert_eq!(snap.messages_consumed, 35);
        assert_eq!(snap.bytes_consumed, 3584);
        assert_eq!(snap.polls, 3);
        assert_eq!(snap.empty_polls, 0);
        assert_eq!(snap.min_batch_size, 5);
        assert_eq!(snap.max_batch_size, 20);
        assert_eq!(snap.batch_count, 3);
        assert_eq!(snap.batch_size_sum, 35);
    }

    #[test]
    fn test_source_metrics_empty_poll() {
        let metrics = KafkaSourceMetrics::new();
        metrics.record_batch(0, 0);

        let snap = metrics.snapshot();
        assert_eq!(snap.polls, 1);
        assert_eq!(snap.empty_polls, 1);
        assert_eq!(snap.batch_count, 0);
    }

    #[test]
    fn test_source_metrics_avg_batch_size() {
        let snap = KafkaSourceMetricsSnapshot {
            batch_size_sum: 100,
            batch_count: 4,
            ..Default::default()
        };
        assert!((snap.avg_batch_size() - 25.0).abs() < f64::EPSILON);

        let empty = KafkaSourceMetricsSnapshot::default();
        assert!((empty.avg_batch_size() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_source_metrics_rate() {
        let snap = KafkaSourceMetricsSnapshot {
            messages_consumed: 1000,
            ..Default::default()
        };
        let rate = snap.rate(Duration::from_secs(10));
        assert!((rate - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_source_metrics_reset() {
        let metrics = KafkaSourceMetrics::new();
        metrics.record_batch(10, 100);
        metrics.record_error();
        metrics.record_commit();

        let snap = metrics.snapshot_and_reset();
        assert_eq!(snap.messages_consumed, 10);
        assert_eq!(snap.errors, 1);
        assert_eq!(snap.commits, 1);

        let after_reset = metrics.snapshot();
        assert_eq!(after_reset.messages_consumed, 0);
        assert_eq!(after_reset.errors, 0);
        assert_eq!(after_reset.commits, 0);
    }

    #[test]
    fn test_source_metrics_prometheus_format() {
        let metrics = KafkaSourceMetrics::new();
        metrics.record_batch(100, 10_000);
        let snap = metrics.snapshot();
        let prom = snap.to_prometheus_format("kafka_source");

        assert!(prom.contains("kafka_source_messages_consumed_total 100"));
        assert!(prom.contains("kafka_source_bytes_consumed_total 10000"));
        assert!(prom.contains("# TYPE kafka_source_messages_consumed_total counter"));
        assert!(prom.contains("kafka_source_batch_size_min 100"));
        assert!(prom.contains("kafka_source_batch_size_max 100"));
    }

    // -- Sink metrics ---------------------------------------------------------

    #[test]
    fn test_sink_metrics_new() {
        let metrics = KafkaSinkMetrics::new();
        let snap = metrics.snapshot();
        assert_eq!(snap.messages_produced, 0);
        assert_eq!(snap.sends, 0);
        assert_eq!(snap.errors, 0);
    }

    #[test]
    fn test_sink_metrics_record_batch() {
        let metrics = KafkaSinkMetrics::new();
        metrics.record_batch(50, 5000);
        metrics.record_batch(30, 3000);

        let snap = metrics.snapshot();
        assert_eq!(snap.messages_produced, 80);
        assert_eq!(snap.bytes_produced, 8000);
        assert_eq!(snap.sends, 2);
        assert_eq!(snap.min_batch_size, 30);
        assert_eq!(snap.max_batch_size, 50);
    }

    #[test]
    fn test_sink_metrics_prometheus_format() {
        let metrics = KafkaSinkMetrics::new();
        metrics.record_batch(200, 20_000);
        metrics.record_error();
        metrics.record_flush();
        let snap = metrics.snapshot();
        let prom = snap.to_prometheus_format("kafka_sink");

        assert!(prom.contains("kafka_sink_messages_produced_total 200"));
        assert!(prom.contains("kafka_sink_errors_total 1"));
        assert!(prom.contains("kafka_sink_flushes_total 1"));
    }

    #[test]
    fn test_sink_metrics_reset() {
        let metrics = KafkaSinkMetrics::new();
        metrics.record_batch(10, 100);
        metrics.record_error();

        let snap = metrics.snapshot_and_reset();
        assert_eq!(snap.messages_produced, 10);
        assert_eq!(snap.errors, 1);

        let after = metrics.snapshot();
        assert_eq!(after.messages_produced, 0);
        assert_eq!(after.errors, 0);
    }

    // -- Key extraction & headers --------------------------------------------

    #[test]
    fn test_extract_key_string_field() {
        let event = SourceEvent::record("t", json!({"id": "abc", "data": 42}));
        let key = KafkaSink::extract_key(&event, Some("id"));
        assert_eq!(key, Some(Bytes::from("abc")));
    }

    #[test]
    fn test_extract_key_numeric_field() {
        let event = SourceEvent::record("t", json!({"id": 123}));
        let key = KafkaSink::extract_key(&event, Some("id"));
        assert!(key.is_some());
        // Numeric field is serialized as JSON
        assert_eq!(key.unwrap(), Bytes::from("123"));
    }

    #[test]
    fn test_extract_key_missing_field() {
        let event = SourceEvent::record("t", json!({"data": 42}));
        let key = KafkaSink::extract_key(&event, Some("id"));
        assert!(key.is_none());
    }

    #[test]
    fn test_extract_key_no_field() {
        let event = SourceEvent::record("t", json!({"id": "abc"}));
        let key = KafkaSink::extract_key(&event, None);
        assert!(key.is_none());
    }

    #[test]
    fn test_build_headers_empty() {
        let event = SourceEvent::record("t", json!({}));
        let headers = KafkaSink::build_headers(&event, false, &HashMap::new());
        assert!(headers.is_empty());
    }

    #[test]
    fn test_build_headers_custom_only() {
        let event = SourceEvent::record("t", json!({}));
        let mut custom = HashMap::new();
        custom.insert("source".to_string(), "rivven".to_string());
        let headers = KafkaSink::build_headers(&event, false, &custom);
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].0, "source");
        assert_eq!(headers[0].1, b"rivven");
    }

    #[test]
    fn test_build_headers_with_metadata() {
        let event = SourceEvent::record("users", json!({}))
            .with_namespace("public")
            .with_position("offset:42");
        let headers = KafkaSink::build_headers(&event, true, &HashMap::new());
        // Convert to HashMap for easier assertion
        let map: HashMap<&str, &[u8]> = headers.iter().map(|(k, v)| (k.as_str(), v.as_slice())).collect();
        assert_eq!(map.get("rivven_stream"), Some(&b"users".as_slice()));
        assert_eq!(map.get("rivven_event_type"), Some(&b"record".as_slice()));
        assert_eq!(map.get("rivven_namespace"), Some(&b"public".as_slice()));
        assert_eq!(map.get("rivven_position"), Some(&b"offset:42".as_slice()));
    }

    // -- Serialization round-trip ---------------------------------------------

    #[test]
    fn test_source_metrics_snapshot_json_roundtrip() {
        let snap = KafkaSourceMetricsSnapshot {
            messages_consumed: 1000,
            bytes_consumed: 50_000,
            polls: 100,
            empty_polls: 5,
            errors: 2,
            commits: 10,
            commit_failures: 1,
            rebalances: 3,
            min_batch_size: 5,
            max_batch_size: 20,
            batch_size_sum: 1000,
            batch_count: 95,
        };
        let json = serde_json::to_string(&snap).unwrap();
        let deserialized: KafkaSourceMetricsSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.messages_consumed, snap.messages_consumed);
        assert_eq!(deserialized.polls, snap.polls);
        assert_eq!(deserialized.min_batch_size, snap.min_batch_size);
    }

    #[test]
    fn test_sink_metrics_snapshot_json_roundtrip() {
        let snap = KafkaSinkMetricsSnapshot {
            messages_produced: 500,
            bytes_produced: 25_000,
            sends: 50,
            errors: 1,
            flushes: 5,
            retries: 3,
            min_batch_size: 8,
            max_batch_size: 15,
            batch_size_sum: 500,
            batch_count: 50,
        };
        let json = serde_json::to_string(&snap).unwrap();
        let deserialized: KafkaSinkMetricsSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.messages_produced, snap.messages_produced);
        assert_eq!(deserialized.flushes, snap.flushes);
    }

    // -- Spec -----------------------------------------------------------------

    #[test]
    fn test_source_spec() {
        let spec = KafkaSource::spec();
        assert_eq!(spec.connector_type, "kafka");
        assert_eq!(spec.version, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn test_sink_spec() {
        let spec = KafkaSink::spec();
        assert_eq!(spec.connector_type, "kafka");
        assert_eq!(spec.version, env!("CARGO_PKG_VERSION"));
    }

    // -- Defaults -------------------------------------------------------------

    #[test]
    fn test_default_security_protocol() {
        assert_eq!(SecurityProtocol::default(), SecurityProtocol::Plaintext);
    }

    #[test]
    fn test_default_sasl_mechanism() {
        assert_eq!(SaslMechanism::default(), SaslMechanism::Plain);
    }

    #[test]
    fn test_default_compression() {
        assert_eq!(CompressionCodec::default(), CompressionCodec::None);
    }

    #[test]
    fn test_default_start_offset() {
        assert_eq!(StartOffset::default(), StartOffset::Earliest);
    }

    #[test]
    fn test_default_ack_level() {
        assert_eq!(AckLevel::default(), AckLevel::All);
    }

    // -- IsolationLevel -------------------------------------------------------

    #[test]
    fn test_default_isolation_level() {
        assert_eq!(IsolationLevel::default(), IsolationLevel::ReadUncommitted);
    }

    #[test]
    fn test_isolation_level_conversion() {
        assert_eq!(
            krafka::consumer::IsolationLevel::from(IsolationLevel::ReadUncommitted),
            krafka::consumer::IsolationLevel::ReadUncommitted
        );
        assert_eq!(
            krafka::consumer::IsolationLevel::from(IsolationLevel::ReadCommitted),
            krafka::consumer::IsolationLevel::ReadCommitted
        );
    }

    #[test]
    fn test_isolation_level_deserialization() {
        let yaml = r#"
            brokers: "localhost:9092"
            topic: "t"
            isolation_level: read_committed
        "#;
        let config: KafkaSourceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.isolation_level, IsolationLevel::ReadCommitted);
    }

    // -- Security: requires_sasl & reject_sasl_for_data_plane -----------------

    #[test]
    fn test_requires_sasl_false_for_plaintext() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::Plaintext,
            ..Default::default()
        };
        assert!(!config.requires_sasl());
    }

    #[test]
    fn test_requires_sasl_false_for_ssl() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::Ssl,
            ..Default::default()
        };
        assert!(!config.requires_sasl());
    }

    #[test]
    fn test_requires_sasl_true_for_sasl_plaintext() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslPlaintext,
            ..Default::default()
        };
        assert!(config.requires_sasl());
    }

    #[test]
    fn test_requires_sasl_true_for_sasl_ssl() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            ..Default::default()
        };
        assert!(config.requires_sasl());
    }

    #[test]
    fn test_reject_sasl_ok_for_plaintext() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::Plaintext,
            ..Default::default()
        };
        // Plaintext should produce no auth config
        assert!(config.to_auth_config().unwrap().is_none());
    }

    #[test]
    fn test_sasl_auth_config_for_sasl_plaintext() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::Plain),
            sasl_username: Some("user".to_string()),
            sasl_password: Some("pass".to_string()),
            ..Default::default()
        };
        let auth = config.to_auth_config().unwrap();
        assert!(auth.is_some(), "SASL_PLAINTEXT must produce auth config");
    }

    #[test]
    fn test_sasl_auth_config_for_sasl_ssl() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::ScramSha256),
            sasl_username: Some("user".to_string()),
            sasl_password: Some("pass".to_string()),
            ..Default::default()
        };
        let auth = config.to_auth_config().unwrap();
        assert!(auth.is_some(), "SASL_SSL must produce auth config");
    }

    // -- OAUTHBEARER ----------------------------------------------------------

    #[test]
    fn test_security_config_oauthbearer() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            oauth_token: Some("my-jwt-token".to_string()),
            ..Default::default()
        };
        let auth = config.to_auth_config().unwrap();
        assert!(auth.is_some());
    }

    #[test]
    fn test_security_config_oauthbearer_missing_token() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            ..Default::default()
        };
        assert!(config.to_auth_config().is_err());
    }

    // -- AWS MSK IAM ----------------------------------------------------------

    #[test]
    fn test_security_config_aws_msk_iam() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::AwsMskIam),
            aws_access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            aws_secret_access_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            aws_region: Some("us-east-1".to_string()),
            ..Default::default()
        };
        let auth = config.to_auth_config().unwrap();
        assert!(auth.is_some());
    }

    #[test]
    fn test_security_config_aws_msk_iam_missing_access_key() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::AwsMskIam),
            aws_secret_access_key: Some("secret".to_string()),
            aws_region: Some("us-east-1".to_string()),
            ..Default::default()
        };
        assert!(config.to_auth_config().is_err());
    }

    #[test]
    fn test_security_config_aws_msk_iam_missing_secret_key() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::AwsMskIam),
            aws_access_key_id: Some("AKID".to_string()),
            aws_region: Some("us-east-1".to_string()),
            ..Default::default()
        };
        assert!(config.to_auth_config().is_err());
    }

    #[test]
    fn test_security_config_aws_msk_iam_missing_region() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::AwsMskIam),
            aws_access_key_id: Some("AKID".to_string()),
            aws_secret_access_key: Some("secret".to_string()),
            ..Default::default()
        };
        assert!(config.to_auth_config().is_err());
    }

    // -- Derived metrics (empty_poll_rate, success_rate) -----------------------

    #[test]
    fn test_source_empty_poll_rate() {
        let snap = KafkaSourceMetricsSnapshot {
            polls: 100,
            empty_polls: 25,
            ..Default::default()
        };
        assert!((snap.empty_poll_rate() - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn test_source_empty_poll_rate_zero_polls() {
        let snap = KafkaSourceMetricsSnapshot::default();
        assert!((snap.empty_poll_rate() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_sink_success_rate() {
        let snap = KafkaSinkMetricsSnapshot {
            messages_produced: 90,
            errors: 10,
            ..Default::default()
        };
        assert!((snap.success_rate() - 0.9).abs() < f64::EPSILON);
    }

    #[test]
    fn test_sink_success_rate_no_messages() {
        let snap = KafkaSinkMetricsSnapshot::default();
        assert!((snap.success_rate() - 1.0).abs() < f64::EPSILON);
    }

    // -- Config defaults for new fields --------------------------------------

    #[test]
    fn test_source_config_default_isolation_level() {
        let yaml = r#"
            brokers: "localhost:9092"
            topic: "t"
        "#;
        let config: KafkaSourceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.isolation_level, IsolationLevel::ReadUncommitted);
    }

    #[test]
    fn test_sink_config_default_retry_backoff() {
        let yaml = r#"
            brokers: "localhost:9092"
            topic: "t"
        "#;
        let config: KafkaSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.retry_backoff_ms, 100);
    }

    #[test]
    fn test_sink_config_custom_retry_backoff() {
        let yaml = r#"
            brokers: "localhost:9092"
            topic: "t"
            retry_backoff_ms: 500
        "#;
        let config: KafkaSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.retry_backoff_ms, 500);
    }

    // -- Config validation ---------------------------------------------------

    #[test]
    fn test_source_config_validation_empty_brokers() {
        let yaml = r#"
            brokers: ""
            topic: "t"
        "#;
        let config: KafkaSourceConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_source_config_validation_empty_topic() {
        let yaml = r#"
            brokers: "localhost:9092"
            topic: ""
        "#;
        let config: KafkaSourceConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_sink_config_validation_empty_brokers() {
        let yaml = r#"
            brokers: ""
            topic: "t"
        "#;
        let config: KafkaSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_sink_config_validation_valid() {
        let yaml = r#"
            brokers: "localhost:9092"
            topic: "output"
        "#;
        let config: KafkaSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_ok());
    }

    // -- OAUTHBEARER config deserialization ------------------------------------

    #[test]
    fn test_source_config_oauthbearer() {
        let yaml = r#"
            brokers: "broker:9092"
            topic: "events"
            security:
              protocol: SASL_PLAINTEXT
              sasl_mechanism: OAUTHBEARER
              oauth_token: "my-jwt-token"
        "#;
        let config: KafkaSourceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.security.protocol, SecurityProtocol::SaslPlaintext);
        assert_eq!(config.security.sasl_mechanism, Some(SaslMechanism::OAuthBearer));
        assert_eq!(config.security.oauth_token, Some("my-jwt-token".to_string()));
    }

    // -- AWS MSK IAM config deserialization -----------------------------------

    #[test]
    fn test_source_config_msk_iam() {
        let yaml = r#"
            brokers: "b-1.msk.us-east-1.amazonaws.com:9098"
            topic: "events"
            security:
              protocol: SASL_SSL
              sasl_mechanism: AWS_MSK_IAM
              aws_access_key_id: "AKIAIOSFODNN7EXAMPLE"
              aws_secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
              aws_region: "us-east-1"
        "#;
        let config: KafkaSourceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.security.protocol, SecurityProtocol::SaslSsl);
        assert_eq!(config.security.sasl_mechanism, Some(SaslMechanism::AwsMskIam));
        assert_eq!(config.security.aws_region, Some("us-east-1".to_string()));
    }

    #[test]
    fn test_sink_config_msk_iam() {
        let yaml = r#"
            brokers: "b-1.msk.us-east-1.amazonaws.com:9098"
            topic: "output"
            security:
              protocol: SASL_SSL
              sasl_mechanism: AWS_MSK_IAM
              aws_access_key_id: "AKID"
              aws_secret_access_key: "SECRET"
              aws_region: "eu-west-1"
        "#;
        let config: KafkaSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.security.sasl_mechanism, Some(SaslMechanism::AwsMskIam));
        assert_eq!(config.security.aws_region, Some("eu-west-1".to_string()));
    }

    // -- Additional coverage --------------------------------------------------

    #[test]
    fn test_security_config_scram_sha512() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::ScramSha512),
            sasl_username: Some("user".to_string()),
            sasl_password: Some("pass".to_string()),
            ..Default::default()
        };
        let auth = config.to_auth_config().unwrap();
        assert!(auth.is_some(), "SCRAM-SHA-512 must produce auth config");
    }

    #[test]
    fn test_security_config_ssl_no_auth() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::Ssl,
            ..Default::default()
        };
        assert!(
            config.to_auth_config().unwrap().is_none(),
            "SSL protocol should not produce SASL auth config"
        );
    }

    #[test]
    fn test_security_config_sasl_default_mechanism() {
        // When no mechanism is set, defaults to PLAIN
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslPlaintext,
            sasl_username: Some("user".to_string()),
            sasl_password: Some("pass".to_string()),
            ..Default::default()
        };
        let auth = config.to_auth_config().unwrap();
        assert!(auth.is_some(), "Default PLAIN mechanism must produce auth config");
    }

    #[test]
    fn test_security_config_msk_iam_rejects_sasl_plaintext() {
        let config = KafkaSecurityConfig {
            protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::AwsMskIam),
            aws_access_key_id: Some("AKID".to_string()),
            aws_secret_access_key: Some("SECRET".to_string()),
            aws_region: Some("us-east-1".to_string()),
            ..Default::default()
        };
        let err = config.to_auth_config().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("SASL_SSL"),
            "Error should mention SASL_SSL requirement: {msg}"
        );
    }

    #[test]
    fn test_sink_config_validation_empty_topic() {
        let yaml = r#"
            brokers: "localhost:9092"
            topic: ""
        "#;
        let config: KafkaSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_build_headers_capacity_prealloc() {
        // Verify headers Vec is correctly sized when include_headers=true
        let event = SourceEvent::record("s", json!({}))
            .with_namespace("ns")
            .with_position("pos");
        let headers = KafkaSink::build_headers(&event, true, &HashMap::new());
        assert!(headers.len() >= 3, "Must include stream + event_type + namespace + position");
        assert!(headers.len() <= 5, "Max 5 headers without custom");
    }
}
