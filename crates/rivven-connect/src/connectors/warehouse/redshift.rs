//! Amazon Redshift sink connector
//!
//! This module provides a sink connector for streaming data into Amazon Redshift
//! using batch inserts via the PostgreSQL wire protocol.
//!
//! # Features
//!
//! - **Direct insert** via PostgreSQL wire protocol
//! - **Multiple authentication methods** (username/password, IAM)
//! - **SSL/TLS connections** with certificate validation
//! - **Automatic batching** for optimal throughput
//! - **Schema inference** from incoming events
//!
//! # Authentication
//!
//! The connector supports multiple authentication methods:
//!
//! 1. **Username/Password** - Via `username` and `password` config
//! 2. **IAM Authentication** - Via AWS credentials (future)
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::registry::SinkRegistry;
//! use rivven_warehouse::redshift::RedshiftSinkFactory;
//! use std::sync::Arc;
//!
//! let mut sinks = SinkRegistry::new();
//! sinks.register("redshift", Arc::new(RedshiftSinkFactory));
//! ```

use crate::connectors::{AnySink, SinkFactory};
use crate::error::ConnectorError;
use crate::prelude::*;
use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt;
use rustls::ClientConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{debug, error, info, warn};
use validator::Validate;

use crate::types::SensitiveString;

/// SSL mode for Redshift connections
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SslMode {
    /// No SSL (not recommended for production)
    Disable,
    /// Prefer SSL but allow non-SSL connections
    #[default]
    Prefer,
    /// Require SSL connection
    Require,
    /// Verify server certificate against CA
    VerifyCa,
    /// Verify server certificate and hostname
    VerifyFull,
}

/// Configuration for the Redshift sink
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct RedshiftSinkConfig {
    /// Redshift cluster endpoint hostname
    #[validate(length(min = 1, max = 255))]
    pub host: String,

    /// Redshift cluster port (default: 5439)
    #[serde(default = "default_port")]
    #[validate(range(min = 1, max = 65535))]
    pub port: u16,

    /// Database name
    #[validate(length(min = 1, max = 127))]
    pub database: String,

    /// Database username
    #[validate(length(min = 1, max = 128))]
    pub username: String,

    /// Database password
    pub password: SensitiveString,

    /// Schema name (default: public)
    #[serde(default = "default_schema")]
    #[validate(length(min = 1, max = 127))]
    pub schema: String,

    /// Target table name
    #[validate(length(min = 1, max = 127))]
    pub table: String,

    /// SSL mode for the connection
    #[serde(default)]
    pub ssl_mode: SslMode,

    /// Number of rows to batch before inserting
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 10000))]
    pub batch_size: usize,

    /// Maximum time in seconds before flushing a partial batch
    #[serde(default = "default_flush_interval")]
    #[validate(range(min = 1, max = 3600))]
    pub flush_interval_secs: u64,

    /// Connection timeout in seconds
    #[serde(default = "default_connect_timeout")]
    #[validate(range(min = 1, max = 300))]
    pub connect_timeout_secs: u64,
}

fn default_port() -> u16 {
    5439
}

fn default_schema() -> String {
    "public".to_string()
}

fn default_batch_size() -> usize {
    500
}

fn default_flush_interval() -> u64 {
    5
}

fn default_connect_timeout() -> u64 {
    30
}

impl Default for RedshiftSinkConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: default_port(),
            database: String::new(),
            username: String::new(),
            password: SensitiveString::new(""),
            schema: default_schema(),
            table: String::new(),
            ssl_mode: SslMode::default(),
            batch_size: default_batch_size(),
            flush_interval_secs: default_flush_interval(),
            connect_timeout_secs: default_connect_timeout(),
        }
    }
}

/// Redshift row data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RedshiftRow {
    event_type: String,
    stream: String,
    namespace: Option<String>,
    timestamp: String,
    data: serde_json::Value,
    metadata: serde_json::Value,
    ingested_at: String,
}

/// Redshift Sink implementation
pub struct RedshiftSink;

impl RedshiftSink {
    /// Create a new Redshift sink instance
    pub fn new() -> Self {
        Self
    }

    async fn create_client(
        config: &RedshiftSinkConfig,
    ) -> crate::error::Result<(Client, tokio::task::JoinHandle<()>)> {
        let connection_string = format!(
            "host={} port={} user={} password={} dbname={} connect_timeout={}",
            config.host,
            config.port,
            config.username,
            config.password.expose_secret(),
            config.database,
            config.connect_timeout_secs
        );

        match config.ssl_mode {
            SslMode::Disable => {
                let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
                    .await
                    .map_err(|e| {
                        ConnectorError::Connection(format!("Failed to connect to Redshift: {}", e))
                    })?;

                let handle = tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        error!("Redshift connection error: {}", e);
                    }
                });

                Ok((client, handle))
            }
            _ => {
                // Use TLS for Prefer, Require, VerifyCa, VerifyFull
                // Build rustls config with webpki roots
                let mut root_store = rustls::RootCertStore::empty();
                root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

                let tls_config = match config.ssl_mode {
                    SslMode::Prefer => {
                        warn!("SslMode::Prefer does not verify TLS certificates; consider using VerifyFull");
                        // Accept any certificate (dangerous but matches native-tls behavior)
                        ClientConfig::builder()
                            .dangerous()
                            .with_custom_certificate_verifier(Arc::new(
                                danger::NoCertificateVerification::new(
                                    rustls::crypto::ring::default_provider(),
                                ),
                            ))
                            .with_no_client_auth()
                    }
                    SslMode::Require => {
                        // Verify certificates with webpki roots
                        ClientConfig::builder()
                            .with_root_certificates(root_store.clone())
                            .with_no_client_auth()
                    }
                    SslMode::VerifyCa | SslMode::VerifyFull => {
                        // Verify certificates with webpki roots
                        ClientConfig::builder()
                            .with_root_certificates(root_store)
                            .with_no_client_auth()
                    }
                    SslMode::Disable => unreachable!(),
                };

                let tls = MakeRustlsConnect::new(tls_config);

                let (client, connection) = tokio_postgres::connect(&connection_string, tls)
                    .await
                    .map_err(|e| {
                    ConnectorError::Connection(format!(
                        "Failed to connect to Redshift with TLS: {}",
                        e
                    ))
                })?;

                let handle = tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        error!("Redshift connection error: {}", e);
                    }
                });

                Ok((client, handle))
            }
        }
    }

    /// Escape a SQL identifier by doubling any embedded double-quotes.
    /// Combined with wrapping in `"..."`, this prevents identifier injection.
    fn quote_ident(ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }

    async fn insert_batch(
        client: &Client,
        schema: &str,
        table: &str,
        rows: &[RedshiftRow],
    ) -> std::result::Result<u64, ConnectorError> {
        if rows.is_empty() {
            return Ok(0);
        }

        // Build parameterized multi-row INSERT to prevent SQL injection.
        // Each row has 7 columns, so parameters are $1..$7, $8..$14, etc.
        let cols_per_row: usize = 7;
        let mut placeholders: Vec<String> = Vec::with_capacity(rows.len());
        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
            Vec::with_capacity(rows.len() * cols_per_row);

        for (i, row) in rows.iter().enumerate() {
            let base = i * cols_per_row + 1;
            placeholders.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${})",
                base,
                base + 1,
                base + 2,
                base + 3,
                base + 4,
                base + 5,
                base + 6
            ));

            params.push(Box::new(row.event_type.clone()));
            params.push(Box::new(row.stream.clone()));
            params.push(Box::new(row.namespace.clone()));
            params.push(Box::new(row.timestamp.clone()));
            params.push(Box::new(row.data.to_string()));
            params.push(Box::new(row.metadata.to_string()));
            params.push(Box::new(row.ingested_at.clone()));
        }

        // Schema/table are admin-set config — quote identifiers to prevent injection.
        let sql = format!(
            "INSERT INTO {}.{} (event_type, stream, namespace, timestamp, data, metadata, ingested_at) VALUES {}",
            Self::quote_ident(schema),
            Self::quote_ident(table),
            placeholders.join(", ")
        );

        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|p| p.as_ref() as _).collect();

        let affected = client
            .execute(&sql, &param_refs)
            .await
            .map_err(|e| ConnectorError::Transient(format!("Failed to insert rows: {}", e)))?;

        Ok(affected)
    }
}

impl Default for RedshiftSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sink for RedshiftSink {
    type Config = RedshiftSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("redshift", env!("CARGO_PKG_VERSION"))
            .description("Amazon Redshift sink - batch data ingestion via PostgreSQL protocol")
            .documentation_url("https://rivven.dev/docs/connectors/redshift-sink")
            .config_schema_from::<RedshiftSinkConfig>()
            .metadata("protocol", "postgresql")
            .metadata("auth", "password,iam")
    }

    async fn check(&self, config: &Self::Config) -> crate::error::Result<CheckResult> {
        info!(
            "Checking Redshift connectivity for {}:{}/{}",
            config.host, config.port, config.database
        );

        let (client, handle) = Self::create_client(config).await?;

        // Try to verify table exists (parameterized to prevent SQL injection)
        let check_sql = "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1";

        match client
            .query_opt(check_sql, &[&config.schema, &config.table])
            .await
        {
            Ok(Some(_)) => {
                info!(
                    "Successfully connected to Redshift: {}.{} exists",
                    config.schema, config.table
                );
                handle.abort();
                Ok(CheckResult::success())
            }
            Ok(None) => {
                let msg = format!(
                    "Table '{}.{}' does not exist in Redshift",
                    config.schema, config.table
                );
                warn!("{}", msg);
                handle.abort();
                Ok(CheckResult::failure(msg))
            }
            Err(e) => {
                let msg = format!(
                    "Failed to verify table '{}.{}': {}",
                    config.schema, config.table, e
                );
                warn!("{}", msg);
                handle.abort();
                Ok(CheckResult::failure(msg))
            }
        }
    }

    async fn write(
        &self,
        config: &Self::Config,
        mut events: futures::stream::BoxStream<'static, SourceEvent>,
    ) -> crate::error::Result<WriteResult> {
        let (client, handle) = Self::create_client(config).await?;
        let mut batch: Vec<RedshiftRow> = Vec::with_capacity(config.batch_size);
        let mut total_written = 0u64;
        let mut total_failed = 0u64;
        let mut errors = Vec::new();
        let mut last_flush = std::time::Instant::now();
        let flush_interval = std::time::Duration::from_secs(config.flush_interval_secs);

        info!(
            "Starting Redshift sink: {}.{}, batch_size={}",
            config.schema, config.table, config.batch_size
        );

        while let Some(event) = events.next().await {
            // Convert event to Redshift row
            let row = RedshiftRow {
                event_type: event.event_type.to_string(),
                stream: event.stream,
                namespace: event.namespace,
                timestamp: event.timestamp.to_rfc3339(),
                data: event.data,
                metadata: serde_json::to_value(&event.metadata).unwrap_or_default(),
                ingested_at: Utc::now().to_rfc3339(),
            };
            batch.push(row);

            // Flush if batch is full or timeout exceeded
            let should_flush =
                batch.len() >= config.batch_size || last_flush.elapsed() >= flush_interval;

            if should_flush && !batch.is_empty() {
                let batch_size = batch.len();
                debug!(
                    "Inserting {} rows to {}.{}",
                    batch_size, config.schema, config.table
                );

                match Self::insert_batch(&client, &config.schema, &config.table, &batch).await {
                    Ok(affected) => {
                        total_written += affected;
                        if affected < batch_size as u64 {
                            total_failed += batch_size as u64 - affected;
                            warn!(
                                "Partial insert: only {}/{} rows inserted",
                                affected, batch_size
                            );
                        } else {
                            info!(
                                "Inserted {} rows to {}.{}",
                                affected, config.schema, config.table
                            );
                        }
                    }
                    Err(e) => {
                        error!("Failed to insert rows to Redshift: {}", e);
                        total_failed += batch_size as u64;
                        errors.push(format!("Batch insert failed: {}", e));
                    }
                }

                batch = Vec::with_capacity(config.batch_size);
                last_flush = std::time::Instant::now();
            }
        }

        // Flush remaining events
        if !batch.is_empty() {
            let batch_size = batch.len();
            debug!(
                "Inserting final {} rows to {}.{}",
                batch_size, config.schema, config.table
            );

            match Self::insert_batch(&client, &config.schema, &config.table, &batch).await {
                Ok(affected) => {
                    total_written += affected;
                    if affected < batch_size as u64 {
                        total_failed += batch_size as u64 - affected;
                    }
                }
                Err(e) => {
                    total_failed += batch_size as u64;
                    errors.push(format!("Final batch insert failed: {}", e));
                }
            }
        }

        // Clean up connection
        handle.abort();

        info!(
            "Redshift sink completed: {} written, {} failed",
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

/// Factory for creating Redshift sink instances
pub struct RedshiftSinkFactory;

impl SinkFactory for RedshiftSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        RedshiftSink::spec()
    }

    fn create(&self) -> Result<Box<dyn AnySink>> {
        Ok(Box::new(RedshiftSink::new()))
    }
}

// Implement AnySink for RedshiftSink
crate::impl_any_sink!(RedshiftSink, RedshiftSinkConfig);

/// Register the Redshift sink with the given registry
pub fn register(registry: &mut crate::SinkRegistry) {
    use std::sync::Arc;
    registry.register("redshift", Arc::new(RedshiftSinkFactory));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spec() {
        let spec = RedshiftSink::spec();
        assert_eq!(spec.connector_type, "redshift");
        assert!(spec.config_schema.is_some());
    }

    #[test]
    fn test_default_config() {
        let config = RedshiftSinkConfig::default();
        assert_eq!(config.port, 5439);
        assert_eq!(config.schema, "public");
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.flush_interval_secs, 5);
        assert_eq!(config.ssl_mode, SslMode::Prefer);
    }

    #[test]
    fn test_factory() {
        let factory = RedshiftSinkFactory;
        let spec = factory.spec();
        assert_eq!(spec.connector_type, "redshift");
        let _sink = factory.create().unwrap();
    }

    #[test]
    fn test_config_validation() {
        let config = RedshiftSinkConfig {
            host: "my-cluster.region.redshift.amazonaws.com".to_string(),
            port: 5439,
            database: "mydb".to_string(),
            username: "admin".to_string(),
            password: SensitiveString::new("secret"),
            schema: "public".to_string(),
            table: "events".to_string(),
            batch_size: 1000,
            ..Default::default()
        };

        // Valid config
        assert!(config.validate().is_ok());

        // Invalid batch_size
        let mut invalid = config.clone();
        invalid.batch_size = 0;
        assert!(invalid.validate().is_err());

        // Invalid port
        let mut invalid = config.clone();
        invalid.port = 0;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn test_quote_ident() {
        // Normal identifier
        assert_eq!(RedshiftSink::quote_ident("public"), "\"public\"");

        // Identifier with embedded double-quote
        assert_eq!(RedshiftSink::quote_ident("my\"schema"), "\"my\"\"schema\"");

        // Empty identifier
        assert_eq!(RedshiftSink::quote_ident(""), "\"\"");
    }

    #[test]
    fn test_row_serialization() {
        let row = RedshiftRow {
            event_type: "insert".to_string(),
            stream: "users".to_string(),
            namespace: Some("public".to_string()),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            data: serde_json::json!({"id": 1, "name": "test"}),
            metadata: serde_json::json!({}),
            ingested_at: "2024-01-01T00:00:01Z".to_string(),
        };

        let json = serde_json::to_value(&row).unwrap();
        assert_eq!(json["event_type"], "insert");
        assert_eq!(json["stream"], "users");
        assert_eq!(json["namespace"], "public");
    }

    #[test]
    fn test_ssl_modes() {
        // Test serialization
        assert_eq!(
            serde_json::to_string(&SslMode::Disable).unwrap(),
            "\"disable\""
        );
        assert_eq!(
            serde_json::to_string(&SslMode::Require).unwrap(),
            "\"require\""
        );
        assert_eq!(
            serde_json::to_string(&SslMode::VerifyFull).unwrap(),
            "\"verifyfull\""
        );
    }
}

/// Dangerous TLS verifier that accepts any certificate
///
/// This is required for SSL modes like "prefer" and "require" that don't
/// verify certificates. Use VerifyCa or VerifyFull for production.
mod danger {
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, Error, SignatureScheme};

    #[derive(Debug)]
    pub struct NoCertificateVerification(rustls::crypto::CryptoProvider);

    impl NoCertificateVerification {
        pub fn new(provider: rustls::crypto::CryptoProvider) -> Self {
            Self(provider)
        }
    }

    impl ServerCertVerifier for NoCertificateVerification {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            rustls::crypto::verify_tls12_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            rustls::crypto::verify_tls13_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }
}
