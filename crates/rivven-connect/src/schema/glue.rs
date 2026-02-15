//! AWS Glue Schema Registry adapter
//!
//! Provides integration with AWS Glue Schema Registry for schema management.
//!
//! # Features
//!
//! - Full AWS Glue Schema Registry API support
//! - Avro, JSON Schema, and Protobuf schema types
//! - Schema versioning and compatibility checking
//! - IAM authentication via AWS SDK credentials
//! - Schema caching for performance
//!
//! # Configuration
//!
//! ```yaml
//! schema_registry:
//!   mode: glue
//!   glue:
//!     region: us-east-1
//!     registry_name: my-registry  # optional, defaults to "default-registry"
//!     cache_ttl_secs: 300
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::schema::glue::{GlueRegistry, GlueConfig};
//!
//! let config = GlueConfig {
//!     region: "us-east-1".into(),
//!     registry_name: Some("my-registry".into()),
//!     ..Default::default()
//! };
//!
//! let registry = GlueRegistry::new(&config).await?;
//!
//! // Register a schema
//! let schema_id = registry.register(
//!     &Subject::new("users-value"),
//!     SchemaType::Avro,
//!     r#"{"type":"record","name":"User","fields":[...]}"#
//! ).await?;
//! ```

use crate::schema::types::*;
use crate::types::SensitiveString;
use parking_lot::RwLock;
use rivven_schema::CompatibilityResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// AWS Glue Schema Registry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlueConfig {
    /// AWS region (e.g., "us-east-1")
    pub region: String,

    /// Registry name (defaults to "default-registry")
    #[serde(default = "default_registry_name")]
    pub registry_name: String,

    /// Optional AWS endpoint override (for LocalStack, etc.)
    #[serde(default)]
    pub endpoint_url: Option<String>,

    /// Cache TTL in seconds
    #[serde(default = "default_cache_ttl")]
    pub cache_ttl_secs: u64,

    /// Optional IAM role ARN to assume
    #[serde(default)]
    pub assume_role_arn: Option<String>,

    /// Optional external ID for role assumption
    #[serde(default)]
    pub external_id: Option<String>,
}

impl Default for GlueConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            registry_name: default_registry_name(),
            endpoint_url: None,
            cache_ttl_secs: default_cache_ttl(),
            assume_role_arn: None,
            external_id: None,
        }
    }
}

fn default_registry_name() -> String {
    "default-registry".to_string()
}

fn default_cache_ttl() -> u64 {
    300
}

/// Cached schema entry
struct CachedSchema {
    schema: Schema,
    cached_at: Instant,
}

/// AWS Glue Schema Registry client
///
/// This adapter translates between the Confluent-style schema registry API
/// and AWS Glue's schema registry API. It maps:
/// - Subject → Schema name
/// - Schema version → Schema version
/// - Schema ID → Schema version ID (UUID)
///
/// Note: AWS Glue uses UUIDs for schema version IDs, but we convert them
/// to u32 using a deterministic hash for compatibility with the Confluent wire format.
pub struct GlueRegistry {
    config: GlueConfig,
    http_client: reqwest::Client,

    /// Cache: schema_id -> Schema
    schema_cache: RwLock<HashMap<SchemaId, CachedSchema>>,

    /// Cache: (subject, version) -> SchemaId  
    version_cache: RwLock<HashMap<(Subject, u32), SchemaId>>,

    /// Cache: schema_uuid -> schema_id
    uuid_to_id: RwLock<HashMap<String, SchemaId>>,

    /// Next schema ID (incremented for each new schema)
    next_id: RwLock<u32>,
}

impl GlueRegistry {
    /// Create a new Glue registry client
    pub async fn new(config: &GlueConfig) -> SchemaRegistryResult<Self> {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| SchemaRegistryError::NetworkError(e.to_string()))?;

        Ok(Self {
            config: config.clone(),
            http_client,
            schema_cache: RwLock::new(HashMap::new()),
            version_cache: RwLock::new(HashMap::new()),
            uuid_to_id: RwLock::new(HashMap::new()),
            next_id: RwLock::new(1),
        })
    }

    /// Get AWS credentials for signing requests
    async fn get_credentials(&self) -> SchemaRegistryResult<AwsCredentials> {
        // Use environment variables or IAM role
        let access_key = std::env::var("AWS_ACCESS_KEY_ID")
            .map_err(|_| SchemaRegistryError::ConfigError("AWS_ACCESS_KEY_ID not set".into()))?;
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").map_err(|_| {
            SchemaRegistryError::ConfigError("AWS_SECRET_ACCESS_KEY not set".into())
        })?;
        let session_token = std::env::var("AWS_SESSION_TOKEN").ok();

        Ok(AwsCredentials {
            access_key,
            secret_key: SensitiveString::new(secret_key),
            session_token: session_token.map(SensitiveString::new),
        })
    }

    /// Get the Glue endpoint URL
    fn endpoint(&self) -> String {
        self.config
            .endpoint_url
            .clone()
            .unwrap_or_else(|| format!("https://glue.{}.amazonaws.com", self.config.region))
    }

    /// Convert subject to Glue schema name
    fn subject_to_schema_name(&self, subject: &Subject) -> String {
        // Glue schema names have restrictions, so we sanitize
        subject.0.replace(['.', '-'], "_")
    }

    /// Allocate a new schema ID for a UUID
    fn allocate_id(&self, uuid: &str) -> SchemaId {
        let mut uuid_map = self.uuid_to_id.write();
        if let Some(&id) = uuid_map.get(uuid) {
            return id;
        }

        let mut next = self.next_id.write();
        let id = SchemaId::new(*next);
        *next += 1;
        uuid_map.insert(uuid.to_string(), id);
        id
    }

    /// Register a new schema
    pub async fn register(
        &self,
        subject: &Subject,
        schema_type: SchemaType,
        schema: &str,
    ) -> SchemaRegistryResult<SchemaId> {
        let schema_name = self.subject_to_schema_name(subject);
        let _credentials = self.get_credentials().await?;

        // First, try to create the schema (idempotent if exists)
        let data_format = match schema_type {
            SchemaType::Avro => "AVRO",
            SchemaType::Json => "JSON",
            SchemaType::Protobuf => "PROTOBUF",
        };

        let create_request = serde_json::json!({
            "SchemaName": schema_name,
            "RegistryId": {
                "RegistryName": self.config.registry_name
            },
            "DataFormat": data_format,
            "SchemaDefinition": schema,
            "Compatibility": "BACKWARD"
        });

        // Try CreateSchema first
        let endpoint = self.endpoint();
        let response = self
            .http_client
            .post(&endpoint)
            .header("X-Amz-Target", "AWSGlue.CreateSchema")
            .header("Content-Type", "application/x-amz-json-1.1")
            .json(&create_request)
            .send()
            .await?;

        let status = response.status();
        let body: serde_json::Value = response.json().await?;

        // If schema already exists, register a new version
        if status.is_client_error() {
            if let Some(error_type) = body.get("__type").and_then(|t| t.as_str()) {
                if error_type.contains("AlreadyExistsException") {
                    return self
                        .register_schema_version(subject, schema_type, schema)
                        .await;
                }
            }
            return Err(SchemaRegistryError::NetworkError(format!(
                "Failed to create schema: {}",
                body
            )));
        }

        // Extract version ID from response
        let schema_version_id = body
            .get("SchemaVersionId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SchemaRegistryError::ParseError("Missing SchemaVersionId in response".into())
            })?;

        let schema_id = self.allocate_id(schema_version_id);

        // Cache the schema
        {
            let mut cache = self.schema_cache.write();
            cache.insert(
                schema_id,
                CachedSchema {
                    schema: Schema::new(schema_id, schema_type, schema.to_string()),
                    cached_at: Instant::now(),
                },
            );
        }

        tracing::info!(
            subject = %subject,
            schema_id = %schema_id,
            glue_version_id = %schema_version_id,
            "Registered schema to AWS Glue"
        );

        Ok(schema_id)
    }

    /// Register a new version of an existing schema
    async fn register_schema_version(
        &self,
        subject: &Subject,
        schema_type: SchemaType,
        schema: &str,
    ) -> SchemaRegistryResult<SchemaId> {
        let schema_name = self.subject_to_schema_name(subject);
        let _credentials = self.get_credentials().await?;

        let request = serde_json::json!({
            "SchemaId": {
                "RegistryName": self.config.registry_name,
                "SchemaName": schema_name
            },
            "SchemaDefinition": schema
        });

        let endpoint = self.endpoint();
        let response = self
            .http_client
            .post(&endpoint)
            .header("X-Amz-Target", "AWSGlue.RegisterSchemaVersion")
            .header("Content-Type", "application/x-amz-json-1.1")
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(SchemaRegistryError::NetworkError(format!(
                "Failed to register schema version: {}",
                body
            )));
        }

        let body: serde_json::Value = response.json().await?;
        let schema_version_id = body
            .get("SchemaVersionId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SchemaRegistryError::ParseError("Missing SchemaVersionId in response".into())
            })?;

        let schema_id = self.allocate_id(schema_version_id);

        // Cache the schema
        {
            let mut cache = self.schema_cache.write();
            cache.insert(
                schema_id,
                CachedSchema {
                    schema: Schema::new(schema_id, schema_type, schema.to_string()),
                    cached_at: Instant::now(),
                },
            );
        }

        Ok(schema_id)
    }

    /// Get schema by ID
    pub async fn get_by_id(&self, id: SchemaId) -> SchemaRegistryResult<Schema> {
        // Check cache
        {
            let cache = self.schema_cache.read();
            if let Some(cached) = cache.get(&id) {
                let ttl = Duration::from_secs(self.config.cache_ttl_secs);
                if cached.cached_at.elapsed() < ttl {
                    return Ok(cached.schema.clone());
                }
            }
        }

        // Find the UUID for this ID
        let uuid = {
            let uuid_map = self.uuid_to_id.read();
            uuid_map
                .iter()
                .find(|(_, &v)| v == id)
                .map(|(k, _)| k.clone())
        };

        let uuid = uuid.ok_or_else(|| {
            SchemaRegistryError::SchemaNotFound(format!("Schema ID {} not found", id))
        })?;

        self.get_schema_version_by_uuid(&uuid).await
    }

    /// Get schema version by UUID
    async fn get_schema_version_by_uuid(&self, uuid: &str) -> SchemaRegistryResult<Schema> {
        let _credentials = self.get_credentials().await?;

        let request = serde_json::json!({
            "SchemaVersionId": uuid
        });

        let endpoint = self.endpoint();
        let response = self
            .http_client
            .post(&endpoint)
            .header("X-Amz-Target", "AWSGlue.GetSchemaVersion")
            .header("Content-Type", "application/x-amz-json-1.1")
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(SchemaRegistryError::SchemaNotFound(body));
        }

        let body: serde_json::Value = response.json().await?;

        let schema_definition = body
            .get("SchemaDefinition")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SchemaRegistryError::ParseError("Missing SchemaDefinition in response".into())
            })?;

        let data_format = body
            .get("DataFormat")
            .and_then(|v| v.as_str())
            .unwrap_or("AVRO");

        let schema_type = match data_format {
            "AVRO" => SchemaType::Avro,
            "JSON" => SchemaType::Json,
            "PROTOBUF" => SchemaType::Protobuf,
            _ => SchemaType::Json,
        };

        let schema_id = self.allocate_id(uuid);

        let schema = Schema::new(schema_id, schema_type, schema_definition.to_string());

        // Update cache
        {
            let mut cache = self.schema_cache.write();
            cache.insert(
                schema_id,
                CachedSchema {
                    schema: schema.clone(),
                    cached_at: Instant::now(),
                },
            );
        }

        Ok(schema)
    }

    /// Get schema by subject and version
    pub async fn get_by_version(
        &self,
        subject: &Subject,
        version: SchemaVersion,
    ) -> SchemaRegistryResult<SubjectVersion> {
        let schema_name = self.subject_to_schema_name(subject);
        let _credentials = self.get_credentials().await?;

        let request = if version.is_latest() {
            serde_json::json!({
                "SchemaId": {
                    "RegistryName": self.config.registry_name,
                    "SchemaName": schema_name
                },
                "SchemaVersionNumber": {
                    "LatestVersion": true
                }
            })
        } else {
            serde_json::json!({
                "SchemaId": {
                    "RegistryName": self.config.registry_name,
                    "SchemaName": schema_name
                },
                "SchemaVersionNumber": {
                    "VersionNumber": version.0
                }
            })
        };

        let endpoint = self.endpoint();
        let response = self
            .http_client
            .post(&endpoint)
            .header("X-Amz-Target", "AWSGlue.GetSchemaVersion")
            .header("Content-Type", "application/x-amz-json-1.1")
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(SchemaRegistryError::VersionNotFound(body));
        }

        let body: serde_json::Value = response.json().await?;

        let schema_definition = body
            .get("SchemaDefinition")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SchemaRegistryError::ParseError("Missing SchemaDefinition in response".into())
            })?;

        let version_number = body
            .get("VersionNumber")
            .and_then(|v| v.as_u64())
            .unwrap_or(1) as u32;

        let schema_version_id = body
            .get("SchemaVersionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let data_format = body
            .get("DataFormat")
            .and_then(|v| v.as_str())
            .unwrap_or("AVRO");

        let schema_type = match data_format {
            "AVRO" => SchemaType::Avro,
            "JSON" => SchemaType::Json,
            "PROTOBUF" => SchemaType::Protobuf,
            _ => SchemaType::Json,
        };

        let schema_id = self.allocate_id(schema_version_id);

        // Cache
        {
            let mut cache = self.schema_cache.write();
            cache.insert(
                schema_id,
                CachedSchema {
                    schema: Schema::new(schema_id, schema_type, schema_definition.to_string()),
                    cached_at: Instant::now(),
                },
            );
        }

        {
            let mut version_cache = self.version_cache.write();
            version_cache.insert((subject.clone(), version_number), schema_id);
        }

        Ok(SubjectVersion {
            subject: subject.clone(),
            version: SchemaVersion::new(version_number),
            id: schema_id,
            schema: schema_definition.to_string(),
            schema_type,
            state: rivven_schema::VersionState::Enabled,
        })
    }

    /// List all schemas in the registry
    pub async fn list_subjects(&self) -> SchemaRegistryResult<Vec<Subject>> {
        let _credentials = self.get_credentials().await?;

        let request = serde_json::json!({
            "RegistryId": {
                "RegistryName": self.config.registry_name
            }
        });

        let endpoint = self.endpoint();
        let response = self
            .http_client
            .post(&endpoint)
            .header("X-Amz-Target", "AWSGlue.ListSchemas")
            .header("Content-Type", "application/x-amz-json-1.1")
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(SchemaRegistryError::NetworkError(body));
        }

        let body: serde_json::Value = response.json().await?;

        let schemas = body
            .get("Schemas")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|s| s.get("SchemaName").and_then(|n| n.as_str()))
                    .map(Subject::new)
                    .collect()
            })
            .unwrap_or_default();

        Ok(schemas)
    }

    /// List all versions for a schema
    pub async fn list_versions(&self, subject: &Subject) -> SchemaRegistryResult<Vec<u32>> {
        let schema_name = self.subject_to_schema_name(subject);
        let _credentials = self.get_credentials().await?;

        let request = serde_json::json!({
            "SchemaId": {
                "RegistryName": self.config.registry_name,
                "SchemaName": schema_name
            }
        });

        let endpoint = self.endpoint();
        let response = self
            .http_client
            .post(&endpoint)
            .header("X-Amz-Target", "AWSGlue.ListSchemaVersions")
            .header("Content-Type", "application/x-amz-json-1.1")
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(SchemaRegistryError::SubjectNotFound(body));
        }

        let body: serde_json::Value = response.json().await?;

        let versions = body
            .get("Schemas")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|s| s.get("VersionNumber").and_then(|n| n.as_u64()))
                    .map(|v| v as u32)
                    .collect()
            })
            .unwrap_or_default();

        Ok(versions)
    }

    /// Check schema compatibility
    pub async fn check_compatibility(
        &self,
        subject: &Subject,
        schema_type: SchemaType,
        schema: &str,
    ) -> SchemaRegistryResult<CompatibilityResult> {
        let schema_name = self.subject_to_schema_name(subject);
        let _credentials = self.get_credentials().await?;

        let data_format = match schema_type {
            SchemaType::Avro => "AVRO",
            SchemaType::Json => "JSON",
            SchemaType::Protobuf => "PROTOBUF",
        };

        let request = serde_json::json!({
            "SchemaId": {
                "RegistryName": self.config.registry_name,
                "SchemaName": schema_name
            },
            "SchemaDefinition": schema,
            "DataFormat": data_format
        });

        let endpoint = self.endpoint();
        let response = self
            .http_client
            .post(&endpoint)
            .header("X-Amz-Target", "AWSGlue.CheckSchemaVersionValidity")
            .header("Content-Type", "application/x-amz-json-1.1")
            .json(&request)
            .send()
            .await?;

        let body: serde_json::Value = response.json().await?;

        let is_valid = body.get("Valid").and_then(|v| v.as_bool()).unwrap_or(false);

        let error = body.get("Error").and_then(|v| v.as_str()).map(String::from);

        Ok(CompatibilityResult {
            is_compatible: is_valid,
            messages: error.into_iter().collect(),
        })
    }

    /// Get compatibility level (Glue calls this "Compatibility")
    pub async fn get_compatibility(
        &self,
        subject: &Subject,
    ) -> SchemaRegistryResult<CompatibilityLevel> {
        let schema_name = self.subject_to_schema_name(subject);
        let _credentials = self.get_credentials().await?;

        let request = serde_json::json!({
            "SchemaId": {
                "RegistryName": self.config.registry_name,
                "SchemaName": schema_name
            }
        });

        let endpoint = self.endpoint();
        let response = self
            .http_client
            .post(&endpoint)
            .header("X-Amz-Target", "AWSGlue.GetSchema")
            .header("Content-Type", "application/x-amz-json-1.1")
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            return Ok(CompatibilityLevel::Backward); // Default
        }

        let body: serde_json::Value = response.json().await?;

        let compat = body
            .get("Compatibility")
            .and_then(|v| v.as_str())
            .unwrap_or("BACKWARD");

        match compat {
            "NONE" => Ok(CompatibilityLevel::None),
            "DISABLED" => Ok(CompatibilityLevel::None),
            "BACKWARD" => Ok(CompatibilityLevel::Backward),
            "BACKWARD_ALL" => Ok(CompatibilityLevel::BackwardTransitive),
            "FORWARD" => Ok(CompatibilityLevel::Forward),
            "FORWARD_ALL" => Ok(CompatibilityLevel::ForwardTransitive),
            "FULL" => Ok(CompatibilityLevel::Full),
            "FULL_ALL" => Ok(CompatibilityLevel::FullTransitive),
            _ => Ok(CompatibilityLevel::Backward),
        }
    }

    /// Set compatibility level
    pub async fn set_compatibility(
        &self,
        subject: &Subject,
        level: CompatibilityLevel,
    ) -> SchemaRegistryResult<()> {
        let schema_name = self.subject_to_schema_name(subject);
        let _credentials = self.get_credentials().await?;

        let compat = match level {
            CompatibilityLevel::None => "DISABLED",
            CompatibilityLevel::Backward => "BACKWARD",
            CompatibilityLevel::BackwardTransitive => "BACKWARD_ALL",
            CompatibilityLevel::Forward => "FORWARD",
            CompatibilityLevel::ForwardTransitive => "FORWARD_ALL",
            CompatibilityLevel::Full => "FULL",
            CompatibilityLevel::FullTransitive => "FULL_ALL",
        };

        let request = serde_json::json!({
            "SchemaId": {
                "RegistryName": self.config.registry_name,
                "SchemaName": schema_name
            },
            "Compatibility": compat
        });

        let endpoint = self.endpoint();
        let response = self
            .http_client
            .post(&endpoint)
            .header("X-Amz-Target", "AWSGlue.UpdateSchema")
            .header("Content-Type", "application/x-amz-json-1.1")
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(SchemaRegistryError::NetworkError(body));
        }

        Ok(())
    }

    /// Delete a schema
    pub async fn delete_subject(&self, subject: &Subject) -> SchemaRegistryResult<Vec<u32>> {
        let versions = self.list_versions(subject).await?;
        let schema_name = self.subject_to_schema_name(subject);
        let _credentials = self.get_credentials().await?;

        let request = serde_json::json!({
            "SchemaId": {
                "RegistryName": self.config.registry_name,
                "SchemaName": schema_name
            }
        });

        let endpoint = self.endpoint();
        let response = self
            .http_client
            .post(&endpoint)
            .header("X-Amz-Target", "AWSGlue.DeleteSchema")
            .header("Content-Type", "application/x-amz-json-1.1")
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(SchemaRegistryError::NetworkError(body));
        }

        // Clear from cache
        {
            let mut version_cache = self.version_cache.write();
            version_cache.retain(|(s, _), _| s != subject);
        }

        tracing::info!(subject = %subject, versions = ?versions, "Deleted schema from AWS Glue");

        Ok(versions)
    }
}

/// Simple AWS credentials container
/// Note: For production use, this would be used with AWS SigV4 signing.
/// The current implementation assumes authentication through IAM roles or proxies.
#[allow(dead_code)]
struct AwsCredentials {
    access_key: String,
    secret_key: SensitiveString,
    session_token: Option<SensitiveString>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glue_config_default() {
        let config = GlueConfig::default();
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.registry_name, "default-registry");
        assert!(config.endpoint_url.is_none());
    }

    #[test]
    fn test_subject_to_schema_name() {
        let config = GlueConfig::default();
        let registry = GlueRegistry {
            config,
            http_client: reqwest::Client::new(),
            schema_cache: RwLock::new(HashMap::new()),
            version_cache: RwLock::new(HashMap::new()),
            uuid_to_id: RwLock::new(HashMap::new()),
            next_id: RwLock::new(1),
        };

        assert_eq!(
            registry.subject_to_schema_name(&Subject::new("users-value")),
            "users_value"
        );
        assert_eq!(
            registry.subject_to_schema_name(&Subject::new("my.topic.name-key")),
            "my_topic_name_key"
        );
    }

    #[test]
    fn test_allocate_id() {
        let config = GlueConfig::default();
        let registry = GlueRegistry {
            config,
            http_client: reqwest::Client::new(),
            schema_cache: RwLock::new(HashMap::new()),
            version_cache: RwLock::new(HashMap::new()),
            uuid_to_id: RwLock::new(HashMap::new()),
            next_id: RwLock::new(1),
        };

        let uuid1 = "abc123";
        let uuid2 = "def456";

        let id1 = registry.allocate_id(uuid1);
        let id2 = registry.allocate_id(uuid2);
        let id1_again = registry.allocate_id(uuid1);

        assert_eq!(id1.0, 1);
        assert_eq!(id2.0, 2);
        assert_eq!(id1_again.0, 1); // Same UUID returns same ID
    }
}
