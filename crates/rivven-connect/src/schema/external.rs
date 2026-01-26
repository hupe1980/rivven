//! External Schema Registry client (Confluent-compatible)
//!
//! Connects to external schema registries that implement the Confluent API:
//! - Confluent Schema Registry
//! - Apicurio Registry
//! - AWS Glue Schema Registry (with adapter)

use crate::schema::compatibility::CompatibilityResult;
use crate::schema::types::*;
use parking_lot::RwLock;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Configuration for external schema registry
pub type ExternalRegistryConfig = ExternalConfig;

/// Request body for registering a schema
#[derive(Debug, Serialize)]
struct RegisterSchemaRequest {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    references: Vec<SchemaReferenceRequest>,
}

#[derive(Debug, Serialize)]
struct SchemaReferenceRequest {
    name: String,
    subject: String,
    version: u32,
}

/// Response from schema registration
#[derive(Debug, Deserialize)]
struct RegisterSchemaResponse {
    id: u32,
}

/// Response from getting a schema by ID
#[derive(Debug, Deserialize)]
struct GetSchemaResponse {
    schema: String,
    #[serde(rename = "schemaType", default)]
    schema_type: Option<String>,
    #[serde(default)]
    references: Vec<SchemaReferenceResponse>,
}

#[derive(Debug, Deserialize)]
struct SchemaReferenceResponse {
    name: String,
    subject: String,
    version: u32,
}

/// Response from getting a subject version
#[derive(Debug, Deserialize)]
struct SubjectVersionResponse {
    subject: String,
    version: u32,
    id: u32,
    schema: String,
    #[serde(rename = "schemaType", default)]
    schema_type: Option<String>,
}

/// Response from compatibility check
#[derive(Debug, Deserialize)]
struct CompatibilityCheckResponse {
    is_compatible: bool,
    #[serde(default)]
    messages: Vec<String>,
}

/// Config response
#[derive(Debug, Deserialize, Serialize)]
struct ConfigResponse {
    #[serde(rename = "compatibilityLevel")]
    compatibility_level: String,
}

/// External Schema Registry client
pub struct ExternalRegistry {
    /// HTTP client
    client: Client,
    /// Base URL
    base_url: String,
    /// Cache: schema_id -> Schema
    schema_cache: RwLock<HashMap<SchemaId, Schema>>,
    /// Cache: (subject, version) -> SchemaId
    version_cache: RwLock<HashMap<(Subject, u32), SchemaId>>,
}

impl ExternalRegistry {
    /// Create a new external registry client
    pub fn new(config: &ExternalRegistryConfig) -> SchemaRegistryResult<Self> {
        let mut client_builder =
            Client::builder().timeout(Duration::from_secs(config.timeout_secs));

        // Add basic auth if configured
        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            use base64::Engine;
            use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};

            let credentials = format!("{}:{}", username, password);
            let encoded = base64::engine::general_purpose::STANDARD.encode(credentials);
            let auth_value = format!("Basic {}", encoded);

            let mut headers = HeaderMap::new();
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&auth_value)
                    .map_err(|e| SchemaRegistryError::ConfigError(e.to_string()))?,
            );

            client_builder = client_builder.default_headers(headers);
        }

        let client = client_builder
            .build()
            .map_err(|e| SchemaRegistryError::NetworkError(e.to_string()))?;

        let base_url = config.url.trim_end_matches('/').to_string();

        Ok(Self {
            client,
            base_url,
            schema_cache: RwLock::new(HashMap::new()),
            version_cache: RwLock::new(HashMap::new()),
        })
    }

    /// Register a new schema
    pub async fn register(
        &self,
        subject: &Subject,
        schema_type: SchemaType,
        schema: &str,
    ) -> SchemaRegistryResult<SchemaId> {
        let url = format!("{}/subjects/{}/versions", self.base_url, subject.0);

        let request = RegisterSchemaRequest {
            schema: schema.to_string(),
            schema_type: schema_type.to_string(),
            references: Vec::new(),
        };

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.parse_error(status, &body));
        }

        let result: RegisterSchemaResponse = response.json().await?;
        let schema_id = SchemaId::new(result.id);

        // Cache the schema
        {
            let mut cache = self.schema_cache.write();
            cache.insert(
                schema_id,
                Schema::new(schema_id, schema_type, schema.to_string()),
            );
        }

        tracing::info!(
            subject = %subject,
            schema_id = %schema_id,
            "Registered schema to external registry"
        );

        Ok(schema_id)
    }

    /// Get schema by ID
    pub async fn get_by_id(&self, id: SchemaId) -> SchemaRegistryResult<Schema> {
        // Check cache first
        {
            let cache = self.schema_cache.read();
            if let Some(schema) = cache.get(&id) {
                return Ok(schema.clone());
            }
        }

        let url = format!("{}/schemas/ids/{}", self.base_url, id.0);

        let response = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json")
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.parse_error(status, &body));
        }

        let result: GetSchemaResponse = response.json().await?;

        let schema_type = result
            .schema_type
            .as_deref()
            .unwrap_or("JSON")
            .parse()
            .unwrap_or(SchemaType::Json);

        let schema = Schema {
            id,
            schema_type,
            schema: result.schema,
            references: result
                .references
                .into_iter()
                .map(|r| SchemaReference {
                    name: r.name,
                    subject: r.subject,
                    version: r.version,
                })
                .collect(),
        };

        // Cache the result
        {
            let mut cache = self.schema_cache.write();
            cache.insert(id, schema.clone());
        }

        Ok(schema)
    }

    /// Get schema by subject and version
    pub async fn get_by_version(
        &self,
        subject: &Subject,
        version: SchemaVersion,
    ) -> SchemaRegistryResult<SubjectVersion> {
        let version_str = if version.is_latest() {
            "latest".to_string()
        } else {
            version.0.to_string()
        };

        let url = format!(
            "{}/subjects/{}/versions/{}",
            self.base_url, subject.0, version_str
        );

        let response = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json")
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.parse_error(status, &body));
        }

        let result: SubjectVersionResponse = response.json().await?;

        let schema_type = result
            .schema_type
            .as_deref()
            .unwrap_or("JSON")
            .parse()
            .unwrap_or(SchemaType::Json);

        // Cache the schema
        let schema_id = SchemaId::new(result.id);
        {
            let mut schema_cache = self.schema_cache.write();
            schema_cache.insert(
                schema_id,
                Schema::new(schema_id, schema_type, result.schema.clone()),
            );
        }

        {
            let mut version_cache = self.version_cache.write();
            version_cache.insert((subject.clone(), result.version), schema_id);
        }

        Ok(SubjectVersion {
            subject: Subject::new(result.subject),
            version: SchemaVersion::new(result.version),
            id: schema_id,
            schema: result.schema,
            schema_type,
        })
    }

    /// List all subjects
    pub async fn list_subjects(&self) -> SchemaRegistryResult<Vec<Subject>> {
        let url = format!("{}/subjects", self.base_url);

        let response = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json")
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.parse_error(status, &body));
        }

        let subjects: Vec<String> = response.json().await?;
        Ok(subjects.into_iter().map(Subject::new).collect())
    }

    /// List all versions for a subject
    pub async fn list_versions(&self, subject: &Subject) -> SchemaRegistryResult<Vec<u32>> {
        let url = format!("{}/subjects/{}/versions", self.base_url, subject.0);

        let response = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json")
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.parse_error(status, &body));
        }

        let versions: Vec<u32> = response.json().await?;
        Ok(versions)
    }

    /// Delete a subject
    pub async fn delete_subject(&self, subject: &Subject) -> SchemaRegistryResult<Vec<u32>> {
        let url = format!("{}/subjects/{}", self.base_url, subject.0);

        let response = self
            .client
            .delete(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json")
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.parse_error(status, &body));
        }

        let versions: Vec<u32> = response.json().await?;

        // Clear from cache
        {
            let mut version_cache = self.version_cache.write();
            version_cache.retain(|(s, _), _| s != subject);
        }

        tracing::info!(subject = %subject, versions = ?versions, "Deleted subject from external registry");

        Ok(versions)
    }

    /// Get compatibility level for a subject
    pub async fn get_compatibility(
        &self,
        subject: &Subject,
    ) -> SchemaRegistryResult<CompatibilityLevel> {
        let url = format!("{}/config/{}", self.base_url, subject.0);

        let response = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json")
            .send()
            .await?;

        if response.status() == StatusCode::NOT_FOUND {
            // Return global config if subject-specific not found
            return self.get_global_compatibility().await;
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.parse_error(status, &body));
        }

        let config: ConfigResponse = response.json().await?;
        self.parse_compatibility_level(&config.compatibility_level)
    }

    /// Get global compatibility level
    pub async fn get_global_compatibility(&self) -> SchemaRegistryResult<CompatibilityLevel> {
        let url = format!("{}/config", self.base_url);

        let response = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json")
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.parse_error(status, &body));
        }

        let config: ConfigResponse = response.json().await?;
        self.parse_compatibility_level(&config.compatibility_level)
    }

    /// Set compatibility level for a subject
    pub async fn set_compatibility(
        &self,
        subject: &Subject,
        level: CompatibilityLevel,
    ) -> SchemaRegistryResult<()> {
        let url = format!("{}/config/{}", self.base_url, subject.0);

        let body = ConfigResponse {
            compatibility_level: level.to_string(),
        };

        let response = self
            .client
            .put(&url)
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.parse_error(status, &body));
        }

        Ok(())
    }

    /// Check compatibility of a new schema
    pub async fn check_compatibility(
        &self,
        subject: &Subject,
        schema_type: SchemaType,
        schema: &str,
        version: Option<SchemaVersion>,
    ) -> SchemaRegistryResult<CompatibilityResult> {
        let version_str = version
            .map(|v| {
                if v.is_latest() {
                    "latest".to_string()
                } else {
                    v.0.to_string()
                }
            })
            .unwrap_or_else(|| "latest".to_string());

        let url = format!(
            "{}/compatibility/subjects/{}/versions/{}",
            self.base_url, subject.0, version_str
        );

        let request = RegisterSchemaRequest {
            schema: schema.to_string(),
            schema_type: schema_type.to_string(),
            references: Vec::new(),
        };

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .json(&request)
            .send()
            .await?;

        // 404 means no existing schema, so it's compatible
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(CompatibilityResult {
                is_compatible: true,
                messages: Vec::new(),
            });
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.parse_error(status, &body));
        }

        let result: CompatibilityCheckResponse = response.json().await?;

        Ok(CompatibilityResult {
            is_compatible: result.is_compatible,
            messages: result.messages,
        })
    }

    // Private helpers

    fn parse_error(&self, status: StatusCode, body: &str) -> SchemaRegistryError {
        // Try to parse as JSON error
        #[derive(Deserialize)]
        struct ErrorResponse {
            error_code: Option<u32>,
            message: Option<String>,
        }

        if let Ok(err) = serde_json::from_str::<ErrorResponse>(body) {
            let msg = err.message.unwrap_or_else(|| body.to_string());
            return match err.error_code {
                Some(40401) => SchemaRegistryError::SubjectNotFound(msg),
                Some(40402) => SchemaRegistryError::VersionNotFound(msg),
                Some(40403) => SchemaRegistryError::SchemaNotFound(msg),
                Some(409) | Some(42201) => SchemaRegistryError::IncompatibleSchema(msg),
                _ => SchemaRegistryError::NetworkError(format!("{}: {}", status, msg)),
            };
        }

        SchemaRegistryError::NetworkError(format!("{}: {}", status, body))
    }

    fn parse_compatibility_level(&self, level: &str) -> SchemaRegistryResult<CompatibilityLevel> {
        match level.to_uppercase().as_str() {
            "NONE" => Ok(CompatibilityLevel::None),
            "BACKWARD" => Ok(CompatibilityLevel::Backward),
            "BACKWARD_TRANSITIVE" => Ok(CompatibilityLevel::BackwardTransitive),
            "FORWARD" => Ok(CompatibilityLevel::Forward),
            "FORWARD_TRANSITIVE" => Ok(CompatibilityLevel::ForwardTransitive),
            "FULL" => Ok(CompatibilityLevel::Full),
            "FULL_TRANSITIVE" => Ok(CompatibilityLevel::FullTransitive),
            _ => Err(SchemaRegistryError::ConfigError(format!(
                "Unknown compatibility level: {}",
                level
            ))),
        }
    }
}
