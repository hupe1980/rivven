//! Google BigQuery sink connector
//!
//! This module provides a sink connector for streaming data into Google BigQuery
//! using the insertAll API for real-time data ingestion.
//!
//! # Features
//!
//! - **Streaming inserts** via BigQuery insertAll API
//! - **Multiple authentication methods** (ADC, service account key)
//! - **Automatic batching** for optimal throughput
//! - **Schema inference** from incoming events
//!
//! # Authentication
//!
//! The connector supports multiple authentication methods:
//!
//! 1. **Application Default Credentials (ADC)** - Automatic (default)
//! 2. **Service Account Key File** - Via `credentials_file` config
//! 3. **Service Account JSON** - Via `credentials_json` config
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::registry::SinkRegistry;
//! use rivven_warehouse::bigquery::BigQuerySinkFactory;
//! use std::sync::Arc;
//!
//! let mut sinks = SinkRegistry::new();
//! sinks.register("bigquery", Arc::new(BigQuerySinkFactory));
//! ```

use crate::connectors::{AnySink, SinkFactory};
use crate::error::ConnectorError;
use crate::prelude::*;
use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt;
use gcp_bigquery_client::model::table_data_insert_all_request::TableDataInsertAllRequest;
use gcp_bigquery_client::model::table_data_insert_all_request_rows::TableDataInsertAllRequestRows;
use gcp_bigquery_client::Client;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};
use validator::Validate;

use crate::types::SensitiveString;

/// Configuration for the BigQuery sink
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct BigQuerySinkConfig {
    /// GCP project ID
    #[validate(length(min = 1, max = 255))]
    pub project_id: String,

    /// BigQuery dataset ID
    #[validate(length(min = 1, max = 1024))]
    pub dataset_id: String,

    /// BigQuery table ID
    #[validate(length(min = 1, max = 1024))]
    pub table_id: String,

    /// Path to service account credentials JSON file
    /// If not provided, uses Application Default Credentials (ADC)
    #[serde(default)]
    pub credentials_file: Option<String>,

    /// Service account credentials as JSON string
    /// Useful when credentials are stored in a secrets manager
    #[serde(default)]
    pub credentials_json: Option<SensitiveString>,

    /// Number of rows to batch before inserting
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 10000))]
    pub batch_size: usize,

    /// Maximum time in seconds before flushing a partial batch
    #[serde(default = "default_flush_interval")]
    #[validate(range(min = 1, max = 3600))]
    pub flush_interval_secs: u64,

    /// Whether to skip invalid rows during insert
    #[serde(default)]
    pub skip_invalid_rows: bool,

    /// Whether to ignore unknown values in the data
    #[serde(default = "default_ignore_unknown_values")]
    pub ignore_unknown_values: bool,

    /// Template suffix for table name (for partitioning)
    #[serde(default)]
    pub template_suffix: Option<String>,
}

fn default_batch_size() -> usize {
    500
}

fn default_flush_interval() -> u64 {
    5
}

fn default_ignore_unknown_values() -> bool {
    true
}

impl Default for BigQuerySinkConfig {
    fn default() -> Self {
        Self {
            project_id: String::new(),
            dataset_id: String::new(),
            table_id: String::new(),
            credentials_file: None,
            credentials_json: None,
            batch_size: default_batch_size(),
            flush_interval_secs: default_flush_interval(),
            skip_invalid_rows: false,
            ignore_unknown_values: default_ignore_unknown_values(),
            template_suffix: None,
        }
    }
}

/// BigQuery row data structure for insertAll API
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BigQueryRow {
    event_type: String,
    stream: String,
    namespace: Option<String>,
    timestamp: String,
    data: serde_json::Value,
    metadata: serde_json::Value,
    _ingested_at: String,
}

/// BigQuery Sink implementation
pub struct BigQuerySink;

impl BigQuerySink {
    /// Create a new BigQuery sink instance
    pub fn new() -> Self {
        Self
    }

    async fn create_client(config: &BigQuerySinkConfig) -> crate::error::Result<Client> {
        let client = if let Some(json) = &config.credentials_json {
            // Use credentials from JSON string
            let sa_key = serde_json::from_str(json.expose_secret())
                .map_err(|e| ConnectorError::Config(format!("Invalid credentials JSON: {}", e)))?;
            Client::from_service_account_key(sa_key, false)
                .await
                .map_err(|e| {
                    ConnectorError::Connection(format!(
                        "Failed to create BigQuery client from credentials: {}",
                        e
                    ))
                })?
        } else if let Some(file_path) = &config.credentials_file {
            // Use credentials from file
            let sa_key = gcp_bigquery_client::yup_oauth2::read_service_account_key(file_path)
                .await
                .map_err(|e| {
                    ConnectorError::Config(format!(
                        "Failed to read credentials from '{}': {}",
                        file_path, e
                    ))
                })?;
            Client::from_service_account_key(sa_key, false)
                .await
                .map_err(|e| {
                    ConnectorError::Connection(format!(
                        "Failed to create BigQuery client from file: {}",
                        e
                    ))
                })?
        } else {
            // Use Application Default Credentials
            Client::from_application_default_credentials()
                .await
                .map_err(|e| {
                    ConnectorError::Connection(format!(
                        "Failed to create BigQuery client with ADC: {}",
                        e
                    ))
                })?
        };

        Ok(client)
    }

    fn build_request(
        config: &BigQuerySinkConfig,
        batch: Vec<TableDataInsertAllRequestRows>,
    ) -> std::result::Result<TableDataInsertAllRequest, ConnectorError> {
        let mut request = TableDataInsertAllRequest::new();

        // Add rows to request
        request
            .add_rows(batch)
            .map_err(|e| ConnectorError::Serialization(format!("Failed to add rows: {}", e)))?;

        // Configure request options
        if config.skip_invalid_rows {
            request.skip_invalid_rows();
        }
        if config.ignore_unknown_values {
            request.ignore_unknown_values();
        }
        if let Some(suffix) = &config.template_suffix {
            request.template_suffix(suffix);
        }

        Ok(request)
    }
}

impl Default for BigQuerySink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sink for BigQuerySink {
    type Config = BigQuerySinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("bigquery", env!("CARGO_PKG_VERSION"))
            .description("Google BigQuery sink - streaming data ingestion via insertAll API")
            .documentation_url("https://rivven.dev/docs/connectors/bigquery-sink")
            .config_schema_from::<BigQuerySinkConfig>()
            .metadata("api", "insertAll")
            .metadata("auth", "adc,service-account")
    }

    async fn check(&self, config: &Self::Config) -> crate::error::Result<CheckResult> {
        info!(
            "Checking BigQuery connectivity for {}.{}.{}",
            config.project_id, config.dataset_id, config.table_id
        );

        let client = Self::create_client(config).await?;

        // Try to get table metadata to verify access
        match client
            .table()
            .get(
                &config.project_id,
                &config.dataset_id,
                &config.table_id,
                None,
            )
            .await
        {
            Ok(table) => {
                info!(
                    "Successfully connected to BigQuery table: {}.{}.{} (type: {:?})",
                    config.project_id, config.dataset_id, config.table_id, table.r#type
                );
                Ok(CheckResult::success())
            }
            Err(e) => {
                let msg = format!(
                    "Failed to access BigQuery table '{}.{}.{}': {}",
                    config.project_id, config.dataset_id, config.table_id, e
                );
                warn!("{}", msg);
                Ok(CheckResult::failure(msg))
            }
        }
    }

    async fn write(
        &self,
        config: &Self::Config,
        mut events: futures::stream::BoxStream<'static, SourceEvent>,
    ) -> crate::error::Result<WriteResult> {
        let client = Self::create_client(config).await?;
        let mut batch: Vec<TableDataInsertAllRequestRows> = Vec::with_capacity(config.batch_size);
        let mut total_written = 0u64;
        let mut total_failed = 0u64;
        let mut errors = Vec::new();
        let mut last_flush = std::time::Instant::now();
        let flush_interval = std::time::Duration::from_secs(config.flush_interval_secs);

        info!(
            "Starting BigQuery sink: {}.{}.{}, batch_size={}",
            config.project_id, config.dataset_id, config.table_id, config.batch_size
        );

        while let Some(event) = events.next().await {
            // Convert event to BigQuery row
            let row_data = BigQueryRow {
                event_type: event.event_type.to_string(),
                stream: event.stream.clone(),
                namespace: event.namespace,
                timestamp: event.timestamp.to_rfc3339(),
                data: event.data,
                metadata: serde_json::to_value(&event.metadata).unwrap_or_default(),
                _ingested_at: Utc::now().to_rfc3339(),
            };

            let row = TableDataInsertAllRequestRows {
                insert_id: Some(format!(
                    "{}_{}",
                    event.stream,
                    Utc::now().timestamp_nanos_opt().unwrap_or(0)
                )),
                json: serde_json::to_value(&row_data).unwrap_or_default(),
            };
            batch.push(row);

            // Flush if batch is full or timeout exceeded
            let should_flush =
                batch.len() >= config.batch_size || last_flush.elapsed() >= flush_interval;

            if should_flush && !batch.is_empty() {
                let batch_size = batch.len();
                debug!(
                    "Inserting {} rows to {}.{}.{}",
                    batch_size, config.project_id, config.dataset_id, config.table_id
                );

                let request = Self::build_request(config, std::mem::take(&mut batch))?;

                match client
                    .tabledata()
                    .insert_all(
                        &config.project_id,
                        &config.dataset_id,
                        &config.table_id,
                        request,
                    )
                    .await
                {
                    Ok(response) => {
                        if let Some(insert_errors) = response.insert_errors {
                            let error_count = insert_errors.len();
                            total_failed += error_count as u64;
                            total_written += (batch_size - error_count) as u64;
                            for err in insert_errors.iter().take(5) {
                                errors.push(format!(
                                    "Row {}: {:?}",
                                    err.index.unwrap_or(0),
                                    err.errors
                                ));
                            }
                            warn!("Partial insert: {}/{} rows failed", error_count, batch_size);
                        } else {
                            total_written += batch_size as u64;
                            info!(
                                "Inserted {} rows to {}.{}.{}",
                                batch_size, config.project_id, config.dataset_id, config.table_id
                            );
                        }
                    }
                    Err(e) => {
                        error!("Failed to insert rows to BigQuery: {}", e);
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
                "Inserting final {} rows to {}.{}.{}",
                batch_size, config.project_id, config.dataset_id, config.table_id
            );

            let request = Self::build_request(config, batch)?;

            match client
                .tabledata()
                .insert_all(
                    &config.project_id,
                    &config.dataset_id,
                    &config.table_id,
                    request,
                )
                .await
            {
                Ok(response) => {
                    if let Some(insert_errors) = response.insert_errors {
                        let error_count = insert_errors.len();
                        total_failed += error_count as u64;
                        total_written += (batch_size - error_count) as u64;
                    } else {
                        total_written += batch_size as u64;
                    }
                }
                Err(e) => {
                    total_failed += batch_size as u64;
                    errors.push(format!("Final batch insert failed: {}", e));
                }
            }
        }

        info!(
            "BigQuery sink completed: {} written, {} failed",
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

/// Factory for creating BigQuery sink instances
pub struct BigQuerySinkFactory;

impl SinkFactory for BigQuerySinkFactory {
    fn spec(&self) -> ConnectorSpec {
        BigQuerySink::spec()
    }

    fn create(&self) -> Result<Box<dyn AnySink>> {
        Ok(Box::new(BigQuerySink::new()))
    }
}

// Implement AnySink for BigQuerySink
crate::impl_any_sink!(BigQuerySink, BigQuerySinkConfig);

/// Register the BigQuery sink with the given registry
pub fn register(registry: &mut crate::SinkRegistry) {
    use std::sync::Arc;
    registry.register("bigquery", Arc::new(BigQuerySinkFactory));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spec() {
        let spec = BigQuerySink::spec();
        assert_eq!(spec.connector_type, "bigquery");
        assert!(spec.config_schema.is_some());
    }

    #[test]
    fn test_default_config() {
        let config = BigQuerySinkConfig::default();
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.flush_interval_secs, 5);
        assert!(!config.skip_invalid_rows);
        assert!(config.ignore_unknown_values);
    }

    #[test]
    fn test_factory() {
        let factory = BigQuerySinkFactory;
        let spec = factory.spec();
        assert_eq!(spec.connector_type, "bigquery");
        let _sink = factory.create().unwrap();
    }

    #[test]
    fn test_config_validation() {
        let config = BigQuerySinkConfig {
            project_id: "my-project".to_string(),
            dataset_id: "my_dataset".to_string(),
            table_id: "my_table".to_string(),
            batch_size: 1000,
            ..Default::default()
        };

        // Valid config
        assert!(config.validate().is_ok());

        // Invalid batch_size
        let mut invalid = config.clone();
        invalid.batch_size = 0;
        assert!(invalid.validate().is_err());

        // Invalid batch_size (too large)
        let mut invalid = config.clone();
        invalid.batch_size = 20000;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn test_row_serialization() {
        let row = BigQueryRow {
            event_type: "insert".to_string(),
            stream: "users".to_string(),
            namespace: Some("public".to_string()),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            data: serde_json::json!({"id": 1, "name": "test"}),
            metadata: serde_json::json!({}),
            _ingested_at: "2024-01-01T00:00:01Z".to_string(),
        };

        let json = serde_json::to_value(&row).unwrap();
        assert_eq!(json["event_type"], "insert");
        assert_eq!(json["stream"], "users");
        assert_eq!(json["namespace"], "public");
    }
}
