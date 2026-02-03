//! External Schema Registry client (Confluent-compatible)
//!
//! Connects to external schema registries that implement the Confluent API:
//! - Confluent Cloud (OAuth2)
//! - Confluent Platform (Basic Auth, mTLS)
//! - rivven-schema (Basic Auth, Bearer Token, JWT/OIDC, API Keys)
//! - Apicurio Registry
//! - Enterprise registries (mTLS, API Keys)
//!
//! # Authentication
//!
//! Supports best-in-class authentication methods:
//! - **OAuth2/OIDC** - Client credentials flow with automatic token refresh
//! - **mTLS** - Mutual TLS for zero-trust environments
//! - **API Keys** - Simple header-based authentication
//! - **Bearer Tokens** - Static or provider-issued tokens
//! - **Basic Auth** - Username/password (legacy fallback)
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::schema::{ExternalRegistry, ExternalConfig, RegistryAuth};
//!
//! // Confluent Cloud with OAuth2
//! let config = ExternalConfig {
//!     url: "https://psrc-xxxxx.us-east-2.aws.confluent.cloud".into(),
//!     auth: RegistryAuth::OAuth2 {
//!         client_id: "API_KEY".into(),
//!         client_secret: "API_SECRET".into(),
//!         token_url: "https://confluent.cloud/oauth/token".into(),
//!         scope: Some("registry:read registry:write".into()),
//!     },
//!     ..Default::default()
//! };
//!
//! let registry = ExternalRegistry::new(&config)?;
//! ```

use crate::schema::types::*;
use parking_lot::RwLock;
use reqwest::{Client, StatusCode};
use rivven_schema::CompatibilityResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// User-Agent string for identifying rivven clients
const USER_AGENT: &str = concat!("rivven-connect/", env!("CARGO_PKG_VERSION"));

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

/// OAuth2 token response
#[derive(Debug, Deserialize)]
struct OAuth2TokenResponse {
    access_token: String,
    /// Token type (usually "Bearer")
    #[serde(default)]
    #[allow(dead_code)]
    token_type: String,
    /// Token lifetime in seconds
    #[serde(default)]
    expires_in: u64,
}

/// Cached OAuth2 token with expiry tracking
struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

impl CachedToken {
    fn is_expired(&self) -> bool {
        // Refresh 30 seconds before actual expiry
        Instant::now() >= self.expires_at - Duration::from_secs(30)
    }
}

/// Cached schema entry with TTL
struct CachedSchema {
    schema: Schema,
    cached_at: Instant,
}

impl CachedSchema {
    fn new(schema: Schema) -> Self {
        Self {
            schema,
            cached_at: Instant::now(),
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        ttl.as_secs() > 0 && Instant::now() >= self.cached_at + ttl
    }
}

/// OAuth2 token manager for automatic token refresh
struct OAuth2TokenManager {
    client_id: String,
    client_secret: String,
    token_url: String,
    scope: Option<String>,
    cached_token: RwLock<Option<CachedToken>>,
    http_client: Client,
}

impl OAuth2TokenManager {
    /// Invalidate the cached token (e.g., on 401 response)
    fn invalidate(&self) {
        let mut cached = self.cached_token.write();
        *cached = None;
        tracing::debug!("OAuth2 token invalidated");
    }
}

impl OAuth2TokenManager {
    fn new(
        client_id: String,
        client_secret: String,
        token_url: String,
        scope: Option<String>,
        http_client: Client,
    ) -> Self {
        Self {
            client_id,
            client_secret,
            token_url,
            scope,
            cached_token: RwLock::new(None),
            http_client,
        }
    }

    async fn get_token(&self) -> SchemaRegistryResult<String> {
        // Check if we have a valid cached token
        {
            let cached = self.cached_token.read();
            if let Some(token) = cached.as_ref() {
                if !token.is_expired() {
                    return Ok(token.access_token.clone());
                }
            }
        }

        // Fetch a new token
        let token = self.fetch_token().await?;
        Ok(token)
    }

    async fn fetch_token(&self) -> SchemaRegistryResult<String> {
        let mut form = vec![
            ("grant_type", "client_credentials"),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
        ];

        let scope_str;
        if let Some(scope) = &self.scope {
            scope_str = scope.clone();
            form.push(("scope", &scope_str));
        }

        let response = self
            .http_client
            .post(&self.token_url)
            .form(&form)
            .send()
            .await
            .map_err(|e| {
                SchemaRegistryError::NetworkError(format!("OAuth2 token request failed: {}", e))
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(SchemaRegistryError::ConfigError(format!(
                "OAuth2 token request failed ({}): {}",
                status, body
            )));
        }

        let token_response: OAuth2TokenResponse = response.json().await.map_err(|e| {
            SchemaRegistryError::ConfigError(format!("Invalid OAuth2 response: {}", e))
        })?;

        // Cache the token
        let expires_at = Instant::now() + Duration::from_secs(token_response.expires_in.max(60));
        {
            let mut cached = self.cached_token.write();
            *cached = Some(CachedToken {
                access_token: token_response.access_token.clone(),
                expires_at,
            });
        }

        tracing::debug!(
            expires_in = token_response.expires_in,
            "OAuth2 token refreshed"
        );

        Ok(token_response.access_token)
    }
}

/// External Schema Registry client
///
/// Enterprise-grade client with support for multiple authentication methods:
/// - OAuth2/OIDC with automatic token refresh
/// - mTLS (mutual TLS)
/// - API Keys
/// - Bearer Tokens
/// - Basic Auth
///
/// # Features
///
/// - **Automatic retry** with exponential backoff for transient failures
/// - **Token refresh** for OAuth2 with automatic invalidation on 401
/// - **Caching** with configurable TTL for schemas
/// - **URL encoding** for subject names with special characters
/// - **Schema references** for complex schema compositions
pub struct ExternalRegistry {
    /// HTTP client
    client: Client,
    /// Base URL
    base_url: String,
    /// Authentication method
    auth: RegistryAuth,
    /// OAuth2 token manager (if using OAuth2)
    oauth2_manager: Option<Arc<OAuth2TokenManager>>,
    /// Cache: schema_id -> CachedSchema (with TTL)
    schema_cache: RwLock<HashMap<SchemaId, CachedSchema>>,
    /// Cache: (subject, version) -> SchemaId
    version_cache: RwLock<HashMap<(Subject, u32), SchemaId>>,
    /// Max retries for transient failures
    max_retries: u32,
    /// Retry backoff base
    retry_backoff: Duration,
    /// Cache TTL
    cache_ttl: Duration,
}

impl ExternalRegistry {
    /// Create a new external registry client with comprehensive auth support
    pub fn new(config: &ExternalRegistryConfig) -> SchemaRegistryResult<Self> {
        use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, USER_AGENT as UA_HEADER};

        let mut client_builder = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .user_agent(USER_AGENT)
            .pool_max_idle_per_host(10); // Connection pooling

        // Set User-Agent header
        let mut default_headers = HeaderMap::new();
        default_headers.insert(UA_HEADER, HeaderValue::from_static(USER_AGENT));

        let mut oauth2_manager = None;

        // Configure TLS if needed
        if config.tls.insecure_skip_verify {
            tracing::warn!("TLS certificate verification is disabled - this is insecure!");
            client_builder = client_builder.danger_accept_invalid_certs(true);
        }

        // Configure authentication
        match &config.auth {
            RegistryAuth::None => {
                // No authentication
            }

            RegistryAuth::Basic { username, password } => {
                use base64::Engine;
                let credentials = format!("{}:{}", username, password);
                let encoded = base64::engine::general_purpose::STANDARD.encode(credentials);
                let auth_value = format!("Basic {}", encoded);
                default_headers.insert(
                    AUTHORIZATION,
                    HeaderValue::from_str(&auth_value)
                        .map_err(|e| SchemaRegistryError::ConfigError(e.to_string()))?,
                );
                tracing::debug!("Using Basic Auth");
            }

            RegistryAuth::Bearer { token } => {
                let auth_value = format!("Bearer {}", token);
                default_headers.insert(
                    AUTHORIZATION,
                    HeaderValue::from_str(&auth_value)
                        .map_err(|e| SchemaRegistryError::ConfigError(e.to_string()))?,
                );
                tracing::debug!("Using Bearer Token");
            }

            RegistryAuth::ApiKey { key, header_name } => {
                default_headers.insert(
                    reqwest::header::HeaderName::from_bytes(header_name.as_bytes()).map_err(
                        |e| SchemaRegistryError::ConfigError(format!("Invalid header name: {}", e)),
                    )?,
                    HeaderValue::from_str(key)
                        .map_err(|e| SchemaRegistryError::ConfigError(e.to_string()))?,
                );
                tracing::debug!(header = %header_name, "Using API Key");
            }

            RegistryAuth::OAuth2 {
                client_id,
                client_secret,
                token_url,
                scope,
            } => {
                // OAuth2 tokens are managed dynamically
                // Build a separate client for token requests
                let token_client = Client::builder()
                    .timeout(Duration::from_secs(30))
                    .build()
                    .map_err(|e| SchemaRegistryError::ConfigError(e.to_string()))?;

                oauth2_manager = Some(Arc::new(OAuth2TokenManager::new(
                    client_id.clone(),
                    client_secret.clone(),
                    token_url.clone(),
                    scope.clone(),
                    token_client,
                )));
                tracing::debug!("Using OAuth2 Client Credentials");
            }

            RegistryAuth::MTLS {
                client_cert_path,
                client_key_path,
                ca_cert_path,
            } => {
                // Load client certificate and key
                let cert_pem = std::fs::read(client_cert_path).map_err(|e| {
                    SchemaRegistryError::ConfigError(format!(
                        "Failed to read client cert {}: {}",
                        client_cert_path, e
                    ))
                })?;

                let key_pem = std::fs::read(client_key_path).map_err(|e| {
                    SchemaRegistryError::ConfigError(format!(
                        "Failed to read client key {}: {}",
                        client_key_path, e
                    ))
                })?;

                let identity =
                    reqwest::Identity::from_pem(&[cert_pem, key_pem].concat()).map_err(|e| {
                        SchemaRegistryError::ConfigError(format!("Invalid client identity: {}", e))
                    })?;

                client_builder = client_builder.identity(identity);

                // Load CA certificate if provided
                if let Some(ca_path) = ca_cert_path {
                    let ca_pem = std::fs::read(ca_path).map_err(|e| {
                        SchemaRegistryError::ConfigError(format!(
                            "Failed to read CA cert {}: {}",
                            ca_path, e
                        ))
                    })?;
                    let ca_cert = reqwest::Certificate::from_pem(&ca_pem).map_err(|e| {
                        SchemaRegistryError::ConfigError(format!("Invalid CA cert: {}", e))
                    })?;
                    client_builder = client_builder.add_root_certificate(ca_cert);
                }

                tracing::debug!("Using mTLS authentication");
            }
        }

        // Also load CA cert from TLS config if provided (separate from mTLS)
        if let Some(ca_path) = &config.tls.ca_cert_path {
            if !matches!(config.auth, RegistryAuth::MTLS { .. }) {
                let ca_pem = std::fs::read(ca_path).map_err(|e| {
                    SchemaRegistryError::ConfigError(format!(
                        "Failed to read CA cert {}: {}",
                        ca_path, e
                    ))
                })?;
                let ca_cert = reqwest::Certificate::from_pem(&ca_pem).map_err(|e| {
                    SchemaRegistryError::ConfigError(format!("Invalid CA cert: {}", e))
                })?;
                client_builder = client_builder.add_root_certificate(ca_cert);
            }
        }

        if !default_headers.is_empty() {
            client_builder = client_builder.default_headers(default_headers);
        }

        let client = client_builder
            .build()
            .map_err(|e| SchemaRegistryError::NetworkError(e.to_string()))?;

        let base_url = config.url.trim_end_matches('/').to_string();

        Ok(Self {
            client,
            base_url,
            auth: config.auth.clone(),
            oauth2_manager,
            schema_cache: RwLock::new(HashMap::new()),
            version_cache: RwLock::new(HashMap::new()),
            max_retries: config.max_retries,
            retry_backoff: Duration::from_millis(config.retry_backoff_ms),
            cache_ttl: Duration::from_secs(config.cache_ttl_secs),
        })
    }

    /// URL-encode a subject name for use in API paths
    fn encode_subject(subject: &Subject) -> String {
        // URL-encode special characters in subject names
        // Subjects can contain: letters, digits, ., _, -
        // But also potentially: / (for TopicNameStrategy with namespaces)
        urlencoding::encode(&subject.0).into_owned()
    }

    /// Get authorization header value (handles OAuth2 token refresh)
    async fn get_auth_header(&self) -> SchemaRegistryResult<Option<String>> {
        match &self.auth {
            RegistryAuth::OAuth2 { .. } => {
                if let Some(manager) = &self.oauth2_manager {
                    let token = manager.get_token().await?;
                    Ok(Some(format!("Bearer {}", token)))
                } else {
                    Ok(None)
                }
            }
            // Other auth methods use default headers set at client creation
            _ => Ok(None),
        }
    }

    /// Execute request with retry logic and OAuth2 token refresh
    async fn execute_request(
        &self,
        mut request: reqwest::RequestBuilder,
    ) -> SchemaRegistryResult<reqwest::Response> {
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                let backoff = self.retry_backoff * (1 << (attempt - 1).min(4));
                tokio::time::sleep(backoff).await;
                tracing::debug!(attempt, "Retrying request");
            }

            // Add OAuth2 token if needed
            if let Some(auth_header) = self.get_auth_header().await? {
                request = request.header(reqwest::header::AUTHORIZATION, &auth_header);
            }

            // Clone the request for potential retry
            let req = match request.try_clone() {
                Some(r) => r,
                None => {
                    // Request body was consumed, can't retry
                    return request
                        .send()
                        .await
                        .map_err(|e| SchemaRegistryError::NetworkError(e.to_string()));
                }
            };

            match req.send().await {
                Ok(response) => {
                    // Check for retryable status codes
                    let status = response.status();

                    // Handle 401 Unauthorized - invalidate OAuth2 token and retry
                    if status == StatusCode::UNAUTHORIZED {
                        if let Some(manager) = &self.oauth2_manager {
                            manager.invalidate();
                            if attempt < self.max_retries {
                                tracing::debug!(
                                    "Received 401, invalidating OAuth2 token and retrying"
                                );
                                last_error = Some(SchemaRegistryError::NetworkError(
                                    "Authentication failed, retrying with fresh token".to_string(),
                                ));
                                continue;
                            }
                        }
                    }

                    if status == StatusCode::TOO_MANY_REQUESTS
                        || status == StatusCode::SERVICE_UNAVAILABLE
                        || status == StatusCode::GATEWAY_TIMEOUT
                    {
                        last_error = Some(SchemaRegistryError::NetworkError(format!(
                            "Server returned {}",
                            status
                        )));
                        continue;
                    }
                    return Ok(response);
                }
                Err(e) => {
                    if e.is_timeout() || e.is_connect() {
                        last_error = Some(SchemaRegistryError::NetworkError(e.to_string()));
                        continue;
                    }
                    return Err(SchemaRegistryError::NetworkError(e.to_string()));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            SchemaRegistryError::NetworkError("Max retries exceeded".to_string())
        }))
    }

    /// Register a new schema
    ///
    /// Registers a schema under the given subject. If the schema already exists,
    /// returns the existing schema ID (idempotent).
    ///
    /// # Arguments
    ///
    /// * `subject` - The subject name (typically `{topic}-key` or `{topic}-value`)
    /// * `schema_type` - The schema format (Avro, Protobuf, Json)
    /// * `schema` - The schema definition string
    pub async fn register(
        &self,
        subject: &Subject,
        schema_type: SchemaType,
        schema: &str,
    ) -> SchemaRegistryResult<SchemaId> {
        self.register_with_references(subject, schema_type, schema, vec![], false)
            .await
    }

    /// Register a new schema with references and optional normalization
    ///
    /// # Arguments
    ///
    /// * `subject` - The subject name
    /// * `schema_type` - The schema format
    /// * `schema` - The schema definition string
    /// * `references` - References to other schemas (for composition)
    /// * `normalize` - Whether to normalize the schema before storing
    pub async fn register_with_references(
        &self,
        subject: &Subject,
        schema_type: SchemaType,
        schema: &str,
        references: Vec<SchemaReference>,
        normalize: bool,
    ) -> SchemaRegistryResult<SchemaId> {
        let encoded_subject = Self::encode_subject(subject);
        let mut url = format!("{}/subjects/{}/versions", self.base_url, encoded_subject);
        if normalize {
            url.push_str("?normalize=true");
        }

        let request = RegisterSchemaRequest {
            schema: schema.to_string(),
            schema_type: schema_type.to_string(),
            references: references
                .iter()
                .map(|r| SchemaReferenceRequest {
                    name: r.name.clone(),
                    subject: r.subject.clone(),
                    version: r.version,
                })
                .collect(),
        };

        let request_builder = self
            .client
            .post(&url)
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .json(&request);

        let response = self.execute_request(request_builder).await?;

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
                CachedSchema::new(Schema::new(schema_id, schema_type, schema.to_string())),
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
    ///
    /// Returns the schema definition for the given global schema ID.
    /// Results are cached with configurable TTL.
    pub async fn get_by_id(&self, id: SchemaId) -> SchemaRegistryResult<Schema> {
        // Check cache first (with TTL)
        {
            let cache = self.schema_cache.read();
            if let Some(cached) = cache.get(&id) {
                if !cached.is_expired(self.cache_ttl) {
                    return Ok(cached.schema.clone());
                }
            }
        }

        let url = format!("{}/schemas/ids/{}", self.base_url, id.0);

        let request_builder = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json");

        let response = self.execute_request(request_builder).await?;

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
            fingerprint: None,
            references: result
                .references
                .into_iter()
                .map(|r| SchemaReference {
                    name: r.name,
                    subject: r.subject,
                    version: r.version,
                })
                .collect(),
            metadata: None,
        };

        // Cache the result
        {
            let mut cache = self.schema_cache.write();
            cache.insert(id, CachedSchema::new(schema.clone()));
        }

        Ok(schema)
    }

    /// Get schema by subject and version
    ///
    /// Returns the schema registered under the subject at the specified version.
    /// Use `SchemaVersion::latest()` to get the most recent version.
    ///
    /// Returns the schema registered under the subject at the specified version.
    /// Use `SchemaVersion::latest()` to get the most recent version.
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

        let encoded_subject = Self::encode_subject(subject);
        let url = format!(
            "{}/subjects/{}/versions/{}",
            self.base_url, encoded_subject, version_str
        );

        let request_builder = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json");

        let response = self.execute_request(request_builder).await?;

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
                CachedSchema::new(Schema::new(schema_id, schema_type, result.schema.clone())),
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
            state: rivven_schema::VersionState::Enabled,
        })
    }

    /// List all subjects
    pub async fn list_subjects(&self) -> SchemaRegistryResult<Vec<Subject>> {
        let url = format!("{}/subjects", self.base_url);

        let request_builder = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json");

        let response = self.execute_request(request_builder).await?;

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
        let encoded_subject = Self::encode_subject(subject);
        let url = format!("{}/subjects/{}/versions", self.base_url, encoded_subject);

        let request_builder = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json");

        let response = self.execute_request(request_builder).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.parse_error(status, &body));
        }

        let versions: Vec<u32> = response.json().await?;
        Ok(versions)
    }

    /// Delete a subject (soft delete)
    ///
    /// Soft deletes all versions of a subject. The subject can be recovered
    /// unless permanently deleted.
    pub async fn delete_subject(&self, subject: &Subject) -> SchemaRegistryResult<Vec<u32>> {
        self.delete_subject_with_options(subject, false).await
    }

    /// Delete a subject with options
    ///
    /// # Arguments
    ///
    /// * `subject` - The subject to delete
    /// * `permanent` - If true, permanently deletes the subject (cannot be recovered)
    pub async fn delete_subject_with_options(
        &self,
        subject: &Subject,
        permanent: bool,
    ) -> SchemaRegistryResult<Vec<u32>> {
        let encoded_subject = Self::encode_subject(subject);
        let mut url = format!("{}/subjects/{}", self.base_url, encoded_subject);
        if permanent {
            url.push_str("?permanent=true");
        }

        let request_builder = self
            .client
            .delete(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json");

        let response = self.execute_request(request_builder).await?;

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

        tracing::info!(subject = %subject, versions = ?versions, permanent, "Deleted subject from external registry");

        Ok(versions)
    }

    /// Delete a specific version of a subject (soft delete)
    pub async fn delete_version(
        &self,
        subject: &Subject,
        version: SchemaVersion,
    ) -> SchemaRegistryResult<u32> {
        self.delete_version_with_options(subject, version, false)
            .await
    }

    /// Delete a specific version with options
    ///
    /// # Arguments
    ///
    /// * `subject` - The subject containing the version
    /// * `version` - The version to delete
    /// * `permanent` - If true, permanently deletes the version
    pub async fn delete_version_with_options(
        &self,
        subject: &Subject,
        version: SchemaVersion,
        permanent: bool,
    ) -> SchemaRegistryResult<u32> {
        let encoded_subject = Self::encode_subject(subject);
        let mut url = format!(
            "{}/subjects/{}/versions/{}",
            self.base_url, encoded_subject, version.0
        );
        if permanent {
            url.push_str("?permanent=true");
        }

        let request_builder = self
            .client
            .delete(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json");

        let response = self.execute_request(request_builder).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.parse_error(status, &body));
        }

        let deleted_version: u32 = response.json().await?;

        // Clear from cache
        {
            let mut version_cache = self.version_cache.write();
            version_cache.remove(&(subject.clone(), version.0));
        }

        tracing::info!(
            subject = %subject,
            version = %version,
            permanent,
            "Deleted version from external registry"
        );

        Ok(deleted_version)
    }

    /// Get compatibility level for a subject
    ///
    /// Returns the subject-specific compatibility level, or falls back to
    /// the global compatibility level if not set.
    pub async fn get_compatibility(
        &self,
        subject: &Subject,
    ) -> SchemaRegistryResult<CompatibilityLevel> {
        let encoded_subject = Self::encode_subject(subject);
        let url = format!("{}/config/{}", self.base_url, encoded_subject);

        let request_builder = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json");

        let response = self.execute_request(request_builder).await?;

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

        let request_builder = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json");

        let response = self.execute_request(request_builder).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.parse_error(status, &body));
        }

        let config: ConfigResponse = response.json().await?;
        self.parse_compatibility_level(&config.compatibility_level)
    }

    /// Set compatibility level for a subject
    ///
    /// Sets the compatibility level for a specific subject. This overrides
    /// the global compatibility level for this subject.
    pub async fn set_compatibility(
        &self,
        subject: &Subject,
        level: CompatibilityLevel,
    ) -> SchemaRegistryResult<()> {
        let encoded_subject = Self::encode_subject(subject);
        let url = format!("{}/config/{}", self.base_url, encoded_subject);

        let body = ConfigResponse {
            compatibility_level: level.to_string(),
        };

        let request_builder = self
            .client
            .put(&url)
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .json(&body);

        let response = self.execute_request(request_builder).await?;

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

        let encoded_subject = Self::encode_subject(subject);
        let url = format!(
            "{}/compatibility/subjects/{}/versions/{}",
            self.base_url, encoded_subject, version_str
        );

        let request = RegisterSchemaRequest {
            schema: schema.to_string(),
            schema_type: schema_type.to_string(),
            references: Vec::new(),
        };

        let request_builder = self
            .client
            .post(&url)
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .json(&request);

        let response = self.execute_request(request_builder).await?;

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

    /// Check if the schema registry is healthy
    ///
    /// Performs a simple health check by fetching the root endpoint.
    pub async fn health_check(&self) -> SchemaRegistryResult<bool> {
        let url = format!("{}/", self.base_url);

        let request_builder = self.client.get(&url).header("Accept", "application/json");

        match self.execute_request(request_builder).await {
            Ok(response) => Ok(response.status().is_success()),
            Err(_) => Ok(false),
        }
    }

    /// Get the registry mode (READWRITE, READONLY, or IMPORT)
    pub async fn get_mode(&self) -> SchemaRegistryResult<String> {
        let url = format!("{}/mode", self.base_url);

        let request_builder = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json");

        let response = self.execute_request(request_builder).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.parse_error(status, &body));
        }

        #[derive(Deserialize)]
        struct ModeResponse {
            mode: String,
        }

        let result: ModeResponse = response.json().await?;
        Ok(result.mode)
    }

    /// Get the mode for a specific subject
    pub async fn get_subject_mode(&self, subject: &Subject) -> SchemaRegistryResult<String> {
        let encoded_subject = Self::encode_subject(subject);
        let url = format!("{}/mode/{}", self.base_url, encoded_subject);

        let request_builder = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json");

        let response = self.execute_request(request_builder).await?;

        // If not found, return global mode
        if response.status() == StatusCode::NOT_FOUND {
            return self.get_mode().await;
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.parse_error(status, &body));
        }

        #[derive(Deserialize)]
        struct ModeResponse {
            mode: String,
        }

        let result: ModeResponse = response.json().await?;
        Ok(result.mode)
    }

    /// Look up schema ID by schema definition
    ///
    /// Returns the schema ID if the schema is already registered under the subject.
    /// Useful for checking if a schema already exists without registering it.
    pub async fn lookup_schema(
        &self,
        subject: &Subject,
        schema_type: SchemaType,
        schema: &str,
    ) -> SchemaRegistryResult<Option<SchemaId>> {
        let encoded_subject = Self::encode_subject(subject);
        let url = format!("{}/subjects/{}", self.base_url, encoded_subject);

        let request = RegisterSchemaRequest {
            schema: schema.to_string(),
            schema_type: schema_type.to_string(),
            references: Vec::new(),
        };

        let request_builder = self
            .client
            .post(&url)
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .json(&request);

        let response = self.execute_request(request_builder).await?;

        // 404 means schema not found
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.parse_error(status, &body));
        }

        #[derive(Deserialize)]
        struct LookupResponse {
            id: u32,
        }

        let result: LookupResponse = response.json().await?;
        Ok(Some(SchemaId::new(result.id)))
    }

    /// Get all schemas (types) registered under a subject
    ///
    /// Returns schema IDs for all versions of a subject. Useful for bulk operations.
    pub async fn get_all_schema_ids(
        &self,
        subject: &Subject,
    ) -> SchemaRegistryResult<Vec<SchemaId>> {
        let versions = self.list_versions(subject).await?;
        let mut schema_ids = Vec::with_capacity(versions.len());

        for version in versions {
            let sv = self
                .get_by_version(subject, SchemaVersion::new(version))
                .await?;
            schema_ids.push(sv.id);
        }

        Ok(schema_ids)
    }

    /// Clear the local schema cache
    ///
    /// Useful when you need to force fresh fetches from the registry.
    pub fn clear_cache(&self) {
        {
            let mut schema_cache = self.schema_cache.write();
            schema_cache.clear();
        }
        {
            let mut version_cache = self.version_cache.write();
            version_cache.clear();
        }
        tracing::debug!("Schema cache cleared");
    }

    /// Get cache statistics
    ///
    /// Returns (schema_count, version_count) currently in cache.
    pub fn cache_stats(&self) -> (usize, usize) {
        let schema_count = self.schema_cache.read().len();
        let version_count = self.version_cache.read().len();
        (schema_count, version_count)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subject_encoding() {
        // Simple subject
        let simple = Subject::new("my-topic");
        let encoded = ExternalRegistry::encode_subject(&simple);
        assert_eq!(encoded, "my-topic");

        // Subject with special characters
        let special = Subject::new("my/topic:key");
        let encoded = ExternalRegistry::encode_subject(&special);
        assert_eq!(encoded, "my%2Ftopic%3Akey");

        // Subject with spaces
        let spaced = Subject::new("my topic");
        let encoded = ExternalRegistry::encode_subject(&spaced);
        assert_eq!(encoded, "my%20topic");
    }

    #[test]
    fn test_cached_schema_expiry() {
        use rivven_schema::{SchemaId, SchemaType};
        let schema = Schema::new(
            SchemaId::new(1),
            SchemaType::Avro,
            r#"{"type":"record","name":"Test","fields":[]}"#.to_string(),
        );
        let cached = CachedSchema::new(schema);

        // Just cached, should not be expired
        assert!(!cached.is_expired(Duration::from_secs(300)));

        // With zero TTL, always expired
        assert!(!cached.is_expired(Duration::from_secs(0))); // zero TTL means no expiry
    }

    #[test]
    fn test_cached_token_expiry() {
        let token = CachedToken {
            access_token: "test_token".to_string(),
            expires_at: std::time::Instant::now() + Duration::from_secs(60),
        };

        // Should not be expired yet (has 60 seconds left)
        assert!(!token.is_expired());

        let expired_token = CachedToken {
            access_token: "test_token".to_string(),
            expires_at: std::time::Instant::now() - Duration::from_secs(1),
        };

        // Should be expired (was 1 second ago)
        assert!(expired_token.is_expired());
    }

    #[test]
    fn test_external_registry_config_default() {
        let config = ExternalRegistryConfig::default();

        assert_eq!(config.url, "http://localhost:8081");
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.cache_ttl_secs, 300);
    }

    #[test]
    fn test_registry_auth_variants() {
        // Test Basic Auth
        let basic = RegistryAuth::Basic {
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        match basic {
            RegistryAuth::Basic { username, password } => {
                assert_eq!(username, "user");
                assert_eq!(password, "pass");
            }
            _ => panic!("Expected Basic auth"),
        }

        // Test Bearer Token
        let bearer = RegistryAuth::Bearer {
            token: "my-token".to_string(),
        };
        match bearer {
            RegistryAuth::Bearer { token } => {
                assert_eq!(token, "my-token");
            }
            _ => panic!("Expected Bearer auth"),
        }

        // Test API Key
        let api_key = RegistryAuth::ApiKey {
            key: "api-key".to_string(),
            header_name: "X-API-Key".to_string(),
        };
        match api_key {
            RegistryAuth::ApiKey { key, header_name } => {
                assert_eq!(key, "api-key");
                assert_eq!(header_name, "X-API-Key");
            }
            _ => panic!("Expected ApiKey auth"),
        }

        // Test OAuth2
        let oauth = RegistryAuth::OAuth2 {
            client_id: "client".to_string(),
            client_secret: "secret".to_string(),
            token_url: "https://auth.example.com/token".to_string(),
            scope: Some("registry".to_string()),
        };
        match oauth {
            RegistryAuth::OAuth2 {
                client_id,
                client_secret,
                token_url,
                scope,
            } => {
                assert_eq!(client_id, "client");
                assert_eq!(client_secret, "secret");
                assert_eq!(token_url, "https://auth.example.com/token");
                assert_eq!(scope, Some("registry".to_string()));
            }
            _ => panic!("Expected OAuth2 auth"),
        }
    }

    #[test]
    fn test_parse_compatibility_level() {
        let config = ExternalRegistryConfig::default();
        let registry = ExternalRegistry::new(&config).unwrap();

        // Test all compatibility levels
        assert!(matches!(
            registry.parse_compatibility_level("NONE"),
            Ok(CompatibilityLevel::None)
        ));
        assert!(matches!(
            registry.parse_compatibility_level("BACKWARD"),
            Ok(CompatibilityLevel::Backward)
        ));
        assert!(matches!(
            registry.parse_compatibility_level("BACKWARD_TRANSITIVE"),
            Ok(CompatibilityLevel::BackwardTransitive)
        ));
        assert!(matches!(
            registry.parse_compatibility_level("FORWARD"),
            Ok(CompatibilityLevel::Forward)
        ));
        assert!(matches!(
            registry.parse_compatibility_level("FORWARD_TRANSITIVE"),
            Ok(CompatibilityLevel::ForwardTransitive)
        ));
        assert!(matches!(
            registry.parse_compatibility_level("FULL"),
            Ok(CompatibilityLevel::Full)
        ));
        assert!(matches!(
            registry.parse_compatibility_level("FULL_TRANSITIVE"),
            Ok(CompatibilityLevel::FullTransitive)
        ));

        // Test case insensitivity
        assert!(matches!(
            registry.parse_compatibility_level("backward"),
            Ok(CompatibilityLevel::Backward)
        ));

        // Test unknown level
        assert!(registry.parse_compatibility_level("UNKNOWN").is_err());
    }

    #[test]
    fn test_cache_stats() {
        let config = ExternalRegistryConfig::default();
        let registry = ExternalRegistry::new(&config).unwrap();

        // Initially cache should be empty
        let (schema_count, version_count) = registry.cache_stats();
        assert_eq!(schema_count, 0);
        assert_eq!(version_count, 0);
    }

    #[test]
    fn test_clear_cache() {
        use rivven_schema::{SchemaId, SchemaType};

        let config = ExternalRegistryConfig::default();
        let registry = ExternalRegistry::new(&config).unwrap();

        // Manually insert something into cache for testing
        {
            let mut cache = registry.schema_cache.write();
            let schema = Schema::new(
                SchemaId::new(1),
                SchemaType::Avro,
                r#"{"type":"record","name":"Test","fields":[]}"#.to_string(),
            );
            cache.insert(SchemaId::new(1), CachedSchema::new(schema));
        }
        {
            let mut cache = registry.version_cache.write();
            let subject = Subject::new("test-topic");
            cache.insert((subject, 1), SchemaId::new(1));
        }

        // Verify cache has items
        let (schema_count, version_count) = registry.cache_stats();
        assert_eq!(schema_count, 1);
        assert_eq!(version_count, 1);

        // Clear cache
        registry.clear_cache();

        // Verify cache is empty
        let (schema_count, version_count) = registry.cache_stats();
        assert_eq!(schema_count, 0);
        assert_eq!(version_count, 0);
    }

    #[test]
    fn test_schema_type_to_string() {
        assert_eq!(SchemaType::Avro.to_string(), "AVRO");
        assert_eq!(SchemaType::Json.to_string(), "JSON");
        assert_eq!(SchemaType::Protobuf.to_string(), "PROTOBUF");
    }

    #[tokio::test]
    async fn test_external_registry_creation() {
        let config = ExternalRegistryConfig::default();
        let registry = ExternalRegistry::new(&config);
        assert!(registry.is_ok());
    }

    #[tokio::test]
    async fn test_external_registry_with_basic_auth() {
        let config =
            ExternalRegistryConfig::with_basic_auth("http://localhost:8081", "user", "pass");
        let registry = ExternalRegistry::new(&config);
        assert!(registry.is_ok());
    }

    #[tokio::test]
    async fn test_external_registry_with_bearer_auth() {
        let config = ExternalConfig {
            url: "http://localhost:8081".to_string(),
            auth: RegistryAuth::Bearer {
                token: "my-token".to_string(),
            },
            ..Default::default()
        };
        let registry = ExternalRegistry::new(&config);
        assert!(registry.is_ok());
    }

    #[tokio::test]
    async fn test_external_registry_with_oauth2() {
        let config = ExternalRegistryConfig::with_oauth2(
            "http://localhost:8081",
            "client",
            "secret",
            "https://auth.example.com/token",
        );
        let registry = ExternalRegistry::new(&config);
        assert!(registry.is_ok());
    }

    #[tokio::test]
    async fn test_external_registry_with_api_key() {
        let config = ExternalConfig {
            url: "http://localhost:8081".to_string(),
            auth: RegistryAuth::ApiKey {
                key: "my-api-key".to_string(),
                header_name: "X-API-Key".to_string(),
            },
            ..Default::default()
        };
        let registry = ExternalRegistry::new(&config);
        assert!(registry.is_ok());
    }

    // Integration tests would require a running schema registry
    // These can be added with #[ignore] for CI or with testcontainers
}
