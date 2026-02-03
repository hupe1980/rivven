//! HTTP Server for Schema Registry
//!
//! Provides a Confluent-compatible REST API with optional authentication.
//!
//! ## Authentication
//!
//! When the `auth` feature is enabled, the server supports:
//! - HTTP Basic Authentication
//! - Bearer Token authentication (OIDC/service accounts)
//!
//! See [`crate::auth`] for configuration options.

use crate::compatibility::CompatibilityLevel;
use crate::error::SchemaError;
use crate::registry::SchemaRegistry;
use crate::types::{
    SchemaContext, SchemaId, SchemaType, SchemaVersion, Subject, ValidationLevel, ValidationRule,
    ValidationRuleType, VersionState,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{delete, get, post, put},
    Json, Router,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::info;

#[cfg(feature = "auth")]
use crate::auth::{auth_middleware, AuthConfig, ServerAuthState};
#[cfg(feature = "auth")]
use axum::middleware;
#[cfg(feature = "auth")]
use rivven_core::AuthManager;

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    /// Authentication configuration (requires `auth` feature)
    #[cfg(feature = "auth")]
    pub auth: Option<AuthConfig>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8081,
            #[cfg(feature = "auth")]
            auth: None,
        }
    }
}

impl ServerConfig {
    /// Enable authentication with default config
    #[cfg(feature = "auth")]
    pub fn with_auth(mut self, auth_config: AuthConfig) -> Self {
        self.auth = Some(auth_config);
        self
    }
}

/// Registry mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RegistryMode {
    #[default]
    ReadWrite,
    ReadOnly,
    Import,
}

impl std::fmt::Display for RegistryMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryMode::ReadWrite => write!(f, "READWRITE"),
            RegistryMode::ReadOnly => write!(f, "READONLY"),
            RegistryMode::Import => write!(f, "IMPORT"),
        }
    }
}

/// Shared server state
pub struct ServerState {
    pub registry: SchemaRegistry,
    pub mode: RwLock<RegistryMode>,
    pub default_compatibility: RwLock<CompatibilityLevel>,
}

/// Schema Registry HTTP Server
pub struct SchemaServer {
    state: Arc<ServerState>,
    #[allow(dead_code)] // Used for auth config when feature is enabled
    config: ServerConfig,
    #[cfg(all(feature = "auth", not(feature = "cedar")))]
    auth_manager: Option<Arc<AuthManager>>,
    #[cfg(feature = "cedar")]
    auth_manager: Option<Arc<AuthManager>>,
    #[cfg(feature = "cedar")]
    cedar_authorizer: Option<Arc<rivven_core::CedarAuthorizer>>,
}

impl SchemaServer {
    /// Create a new schema server
    pub fn new(registry: SchemaRegistry, config: ServerConfig) -> Self {
        let default_compat = registry.get_default_compatibility();
        Self {
            state: Arc::new(ServerState {
                registry,
                mode: RwLock::new(RegistryMode::default()),
                default_compatibility: RwLock::new(default_compat),
            }),
            config,
            #[cfg(feature = "auth")]
            auth_manager: None,
            #[cfg(feature = "cedar")]
            cedar_authorizer: None,
        }
    }

    /// Create a new schema server with authentication
    #[cfg(all(feature = "auth", not(feature = "cedar")))]
    pub fn with_auth(
        registry: SchemaRegistry,
        config: ServerConfig,
        auth_manager: Arc<AuthManager>,
    ) -> Self {
        let default_compat = registry.get_default_compatibility();
        Self {
            state: Arc::new(ServerState {
                registry,
                mode: RwLock::new(RegistryMode::default()),
                default_compatibility: RwLock::new(default_compat),
            }),
            config,
            auth_manager: Some(auth_manager),
        }
    }

    /// Create a new schema server with authentication (simple RBAC)
    #[cfg(feature = "cedar")]
    pub fn with_auth(
        registry: SchemaRegistry,
        config: ServerConfig,
        auth_manager: Arc<AuthManager>,
    ) -> Self {
        let default_compat = registry.get_default_compatibility();
        Self {
            state: Arc::new(ServerState {
                registry,
                mode: RwLock::new(RegistryMode::default()),
                default_compatibility: RwLock::new(default_compat),
            }),
            config,
            auth_manager: Some(auth_manager),
            cedar_authorizer: None,
        }
    }

    /// Create a new schema server with Cedar policy-based authorization
    #[cfg(feature = "cedar")]
    pub fn with_cedar(
        registry: SchemaRegistry,
        config: ServerConfig,
        auth_manager: Arc<AuthManager>,
        cedar_authorizer: Arc<rivven_core::CedarAuthorizer>,
    ) -> Self {
        let default_compat = registry.get_default_compatibility();
        Self {
            state: Arc::new(ServerState {
                registry,
                mode: RwLock::new(RegistryMode::default()),
                default_compatibility: RwLock::new(default_compat),
            }),
            config,
            auth_manager: Some(auth_manager),
            cedar_authorizer: Some(cedar_authorizer),
        }
    }

    /// Build the Axum router
    pub fn router(&self) -> Router {
        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any);

        let base_router = Router::new()
            // Root / health
            .route("/", get(root_handler))
            .route("/health", get(health_handler))
            .route("/health/live", get(liveness_handler))
            .route("/health/ready", get(readiness_handler))
            // Schemas
            .route("/schemas/ids/:id", get(get_schema_by_id))
            .route("/schemas/ids/:id/versions", get(get_schema_versions))
            // Subjects
            .route("/subjects", get(list_subjects))
            .route("/subjects/:subject", delete(delete_subject))
            .route("/subjects/:subject/versions", get(list_subject_versions))
            .route("/subjects/:subject/versions", post(register_schema))
            .route(
                "/subjects/:subject/versions/:version",
                get(get_subject_version),
            )
            .route(
                "/subjects/:subject/versions/:version",
                delete(delete_version),
            )
            // Subject recovery (undelete)
            .route("/subjects/deleted", get(list_deleted_subjects))
            .route("/subjects/:subject/undelete", post(undelete_subject))
            // Version state management
            .route(
                "/subjects/:subject/versions/:version/state",
                get(get_version_state),
            )
            .route(
                "/subjects/:subject/versions/:version/state",
                put(set_version_state),
            )
            .route(
                "/subjects/:subject/versions/:version/deprecate",
                post(deprecate_version),
            )
            .route(
                "/subjects/:subject/versions/:version/disable",
                post(disable_version),
            )
            .route(
                "/subjects/:subject/versions/:version/enable",
                post(enable_version),
            )
            // Schema references
            .route(
                "/subjects/:subject/versions/:version/referencedby",
                get(get_referenced_by),
            )
            // Schema validation (without registering)
            .route("/subjects/:subject/validate", post(validate_schema))
            // Compatibility
            .route(
                "/compatibility/subjects/:subject/versions/:version",
                post(check_compatibility),
            )
            // Config
            .route("/config", get(get_global_config))
            .route("/config", put(update_global_config))
            .route("/config/:subject", get(get_subject_config))
            .route("/config/:subject", put(update_subject_config))
            // Validation rules
            .route("/config/validation/rules", get(list_validation_rules))
            .route("/config/validation/rules", post(add_validation_rule))
            .route(
                "/config/validation/rules/:name",
                delete(delete_validation_rule),
            )
            // Mode
            .route("/mode", get(get_mode))
            .route("/mode", put(update_mode))
            // Contexts (multi-tenant)
            .route("/contexts", get(list_contexts))
            .route("/contexts", post(create_context))
            .route("/contexts/:context", get(get_context))
            .route("/contexts/:context", delete(delete_context))
            .route("/contexts/:context/subjects", get(list_context_subjects))
            // Statistics
            .route("/stats", get(get_stats))
            .with_state(self.state.clone())
            .layer(cors)
            .layer(TraceLayer::new_for_http());

        // Add authentication middleware if configured (simple RBAC)
        #[cfg(all(feature = "auth", not(feature = "cedar")))]
        let base_router = if let Some(auth_manager) = &self.auth_manager {
            let auth_config = self.config.auth.clone().unwrap_or_default();
            let auth_state = Arc::new(ServerAuthState {
                auth_manager: auth_manager.clone(),
                config: auth_config,
            });
            base_router.layer(middleware::from_fn_with_state(auth_state, auth_middleware))
        } else {
            base_router
        };

        // Add authentication middleware if configured (with Cedar support)
        #[cfg(feature = "cedar")]
        let base_router = if let Some(auth_manager) = &self.auth_manager {
            let auth_config = self.config.auth.clone().unwrap_or_default();
            let auth_state = Arc::new(ServerAuthState {
                auth_manager: auth_manager.clone(),
                cedar_authorizer: self.cedar_authorizer.clone(),
                config: auth_config,
            });
            base_router.layer(middleware::from_fn_with_state(auth_state, auth_middleware))
        } else {
            base_router
        };

        base_router
    }

    /// Run the server
    pub async fn run(self, addr: SocketAddr) -> anyhow::Result<()> {
        let router = self.router();
        let listener = tokio::net::TcpListener::bind(addr).await?;
        info!("Schema Registry server listening on {}", addr);
        axum::serve(listener, router).await?;
        Ok(())
    }
}

// ============================================================================
// Helper for converting SchemaError to HTTP response
// ============================================================================

fn schema_error_response(e: SchemaError) -> (StatusCode, Json<ErrorResponse>) {
    let status = match e.http_status() {
        404 => StatusCode::NOT_FOUND,
        409 => StatusCode::CONFLICT,
        422 => StatusCode::UNPROCESSABLE_ENTITY,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (
        status,
        Json(ErrorResponse {
            error_code: e.error_code(),
            message: e.to_string(),
        }),
    )
}

// ============================================================================
// Request/Response Types
// ============================================================================

#[derive(Serialize)]
struct RootResponse {
    version: &'static str,
    commit: &'static str,
}

#[derive(Deserialize)]
struct RegisterSchemaRequest {
    schema: String,
    #[serde(rename = "schemaType", default)]
    schema_type: Option<String>,
    /// Schema references for complex types (Avro named types, Protobuf imports, JSON Schema $refs)
    #[serde(default)]
    references: Vec<SchemaReference>,
}

#[derive(Deserialize, Serialize)]
struct SchemaReference {
    name: String,
    subject: String,
    version: u32,
}

#[derive(Serialize)]
struct RegisterSchemaResponse {
    id: u32,
}

#[derive(Serialize)]
struct SchemaResponse {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    references: Vec<SchemaReference>,
}

#[derive(Serialize)]
struct SubjectVersionResponse {
    subject: String,
    version: u32,
    id: u32,
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: String,
}

#[derive(Deserialize)]
struct CompatibilityRequest {
    schema: String,
    #[serde(rename = "schemaType", default)]
    schema_type: Option<String>,
}

#[derive(Serialize)]
struct CompatibilityResponse {
    is_compatible: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    messages: Vec<String>,
}

#[derive(Serialize)]
struct ConfigResponse {
    #[serde(rename = "compatibilityLevel")]
    compatibility_level: String,
}

#[derive(Deserialize)]
struct ConfigRequest {
    compatibility: String,
}

#[derive(Serialize)]
struct ModeResponse {
    mode: String,
}

#[derive(Deserialize)]
struct ModeRequest {
    mode: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error_code: u32,
    message: String,
}

#[derive(Deserialize)]
struct QueryParams {
    #[serde(default)]
    permanent: bool,
}

// ============================================================================
// Handlers
// ============================================================================

async fn root_handler() -> Json<RootResponse> {
    Json(RootResponse {
        version: env!("CARGO_PKG_VERSION"),
        commit: "unknown",
    })
}

/// Health check endpoint - returns basic health status
#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
}

async fn health_handler() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy",
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// Kubernetes liveness probe - is the service alive?
async fn liveness_handler() -> StatusCode {
    StatusCode::OK
}

/// Kubernetes readiness probe - can the service handle requests?
async fn readiness_handler(
    State(state): State<Arc<ServerState>>,
) -> Result<StatusCode, StatusCode> {
    // Check if we can access the registry (basic health check)
    match state.registry.list_subjects().await {
        Ok(_) => Ok(StatusCode::OK),
        Err(_) => Err(StatusCode::SERVICE_UNAVAILABLE),
    }
}

async fn get_schema_by_id(
    State(state): State<Arc<ServerState>>,
    Path(id): Path<u32>,
) -> Result<Json<SchemaResponse>, (StatusCode, Json<ErrorResponse>)> {
    state
        .registry
        .get_by_id(SchemaId::new(id))
        .await
        .map(|schema| {
            Json(SchemaResponse {
                schema: schema.schema,
                schema_type: schema.schema_type.to_string(),
                references: schema
                    .references
                    .into_iter()
                    .map(|r| SchemaReference {
                        name: r.name,
                        subject: r.subject,
                        version: r.version,
                    })
                    .collect(),
            })
        })
        .map_err(schema_error_response)
}

async fn get_schema_versions(
    State(state): State<Arc<ServerState>>,
    Path(id): Path<u32>,
) -> Result<Json<Vec<SubjectVersionResponse>>, (StatusCode, Json<ErrorResponse>)> {
    // Find all subjects that use this schema ID
    let subjects = state
        .registry
        .list_subjects()
        .await
        .map_err(schema_error_response)?;
    let mut results = Vec::new();

    for subject in subjects {
        let versions = state
            .registry
            .list_versions(subject.as_str())
            .await
            .map_err(schema_error_response)?;
        for ver in versions {
            if let Ok(sv) = state
                .registry
                .get_by_version(subject.as_str(), SchemaVersion::new(ver))
                .await
            {
                if sv.id.0 == id {
                    results.push(SubjectVersionResponse {
                        subject: sv.subject.0,
                        version: sv.version.0,
                        id: sv.id.0,
                        schema: sv.schema,
                        schema_type: sv.schema_type.to_string(),
                    });
                }
            }
        }
    }

    Ok(Json(results))
}

async fn list_subjects(
    State(state): State<Arc<ServerState>>,
) -> Result<Json<Vec<String>>, (StatusCode, Json<ErrorResponse>)> {
    state
        .registry
        .list_subjects()
        .await
        .map(|subjects| Json(subjects.into_iter().map(|s| s.0).collect()))
        .map_err(schema_error_response)
}

async fn delete_subject(
    State(state): State<Arc<ServerState>>,
    Path(subject): Path<String>,
    Query(params): Query<QueryParams>,
) -> Result<Json<Vec<u32>>, (StatusCode, Json<ErrorResponse>)> {
    // Check mode
    if *state.mode.read() == RegistryMode::ReadOnly {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error_code: 40301,
                message: "Registry is in READONLY mode".to_string(),
            }),
        ));
    }

    state
        .registry
        .delete_subject(subject.as_str(), params.permanent)
        .await
        .map(Json)
        .map_err(schema_error_response)
}

/// List soft-deleted subjects that can be recovered
async fn list_deleted_subjects(
    State(state): State<Arc<ServerState>>,
) -> Result<Json<Vec<String>>, (StatusCode, Json<ErrorResponse>)> {
    state
        .registry
        .list_deleted_subjects()
        .await
        .map(|subjects| Json(subjects.into_iter().map(|s| s.0).collect()))
        .map_err(schema_error_response)
}

/// Undelete a soft-deleted subject
async fn undelete_subject(
    State(state): State<Arc<ServerState>>,
    Path(subject): Path<String>,
) -> Result<Json<Vec<u32>>, (StatusCode, Json<ErrorResponse>)> {
    // Check mode
    if *state.mode.read() == RegistryMode::ReadOnly {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error_code: 40301,
                message: "Registry is in READONLY mode".to_string(),
            }),
        ));
    }

    state
        .registry
        .undelete_subject(subject.as_str())
        .await
        .map(Json)
        .map_err(schema_error_response)
}

async fn list_subject_versions(
    State(state): State<Arc<ServerState>>,
    Path(subject): Path<String>,
) -> Result<Json<Vec<u32>>, (StatusCode, Json<ErrorResponse>)> {
    state
        .registry
        .list_versions(subject.as_str())
        .await
        .map(Json)
        .map_err(schema_error_response)
}

async fn register_schema(
    State(state): State<Arc<ServerState>>,
    Path(subject): Path<String>,
    Json(req): Json<RegisterSchemaRequest>,
) -> Result<Json<RegisterSchemaResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Check mode
    if *state.mode.read() == RegistryMode::ReadOnly {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error_code: 40301,
                message: "Registry is in READONLY mode".to_string(),
            }),
        ));
    }

    let schema_type = req
        .schema_type
        .as_deref()
        .unwrap_or("AVRO")
        .parse::<SchemaType>()
        .unwrap_or(SchemaType::Avro);

    // Convert API references to internal type
    let references: Vec<crate::types::SchemaReference> = req
        .references
        .into_iter()
        .map(|r| crate::types::SchemaReference {
            name: r.name,
            subject: r.subject,
            version: r.version,
        })
        .collect();

    state
        .registry
        .register_with_references(subject.as_str(), schema_type, &req.schema, references)
        .await
        .map(|id| Json(RegisterSchemaResponse { id: id.0 }))
        .map_err(schema_error_response)
}

async fn get_subject_version(
    State(state): State<Arc<ServerState>>,
    Path((subject, version)): Path<(String, String)>,
) -> Result<Json<SubjectVersionResponse>, (StatusCode, Json<ErrorResponse>)> {
    let version = if version == "latest" {
        state
            .registry
            .get_latest(subject.as_str())
            .await
            .map_err(schema_error_response)?
            .version
    } else {
        SchemaVersion::new(version.parse().unwrap_or(0))
    };

    state
        .registry
        .get_by_version(subject.as_str(), version)
        .await
        .map(|sv| {
            Json(SubjectVersionResponse {
                subject: sv.subject.0,
                version: sv.version.0,
                id: sv.id.0,
                schema: sv.schema,
                schema_type: sv.schema_type.to_string(),
            })
        })
        .map_err(schema_error_response)
}

async fn delete_version(
    State(state): State<Arc<ServerState>>,
    Path((subject, version)): Path<(String, u32)>,
    Query(params): Query<QueryParams>,
) -> Result<Json<u32>, (StatusCode, Json<ErrorResponse>)> {
    // Check mode
    if *state.mode.read() == RegistryMode::ReadOnly {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error_code: 40301,
                message: "Registry is in READONLY mode".to_string(),
            }),
        ));
    }

    state
        .registry
        .delete_version(
            subject.as_str(),
            SchemaVersion::new(version),
            params.permanent,
        )
        .await
        .map(|_| Json(version))
        .map_err(schema_error_response)
}

/// Get all schema IDs that reference a given subject/version
async fn get_referenced_by(
    State(state): State<Arc<ServerState>>,
    Path((subject, version)): Path<(String, String)>,
) -> Result<Json<Vec<u32>>, (StatusCode, Json<ErrorResponse>)> {
    let version = if version == "latest" {
        // Get latest version number
        let versions = state
            .registry
            .list_versions(subject.as_str())
            .await
            .map_err(schema_error_response)?;
        versions.into_iter().max().unwrap_or(1)
    } else {
        version.parse().unwrap_or(1)
    };

    state
        .registry
        .get_schemas_referencing(subject.as_str(), SchemaVersion::new(version))
        .await
        .map(|ids| Json(ids.into_iter().map(|id| id.0).collect()))
        .map_err(schema_error_response)
}

async fn check_compatibility(
    State(state): State<Arc<ServerState>>,
    Path((subject, version)): Path<(String, String)>,
    Json(req): Json<CompatibilityRequest>,
) -> Result<Json<CompatibilityResponse>, (StatusCode, Json<ErrorResponse>)> {
    let schema_type = req
        .schema_type
        .as_deref()
        .unwrap_or("AVRO")
        .parse::<SchemaType>()
        .unwrap_or(SchemaType::Avro);

    let version = if version == "latest" {
        None
    } else {
        Some(SchemaVersion::new(version.parse().unwrap_or(0)))
    };

    state
        .registry
        .check_compatibility(subject.as_str(), schema_type, &req.schema, version)
        .await
        .map(|result| {
            Json(CompatibilityResponse {
                is_compatible: result.is_compatible,
                messages: result.messages,
            })
        })
        .map_err(schema_error_response)
}

async fn get_global_config(State(state): State<Arc<ServerState>>) -> Json<ConfigResponse> {
    Json(ConfigResponse {
        compatibility_level: state.default_compatibility.read().to_string(),
    })
}

async fn update_global_config(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<ConfigRequest>,
) -> Result<Json<ConfigResponse>, (StatusCode, Json<ErrorResponse>)> {
    let level: CompatibilityLevel = req.compatibility.parse().map_err(|_| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(ErrorResponse {
                error_code: 42203,
                message: format!("Invalid compatibility level: {}", req.compatibility),
            }),
        )
    })?;

    *state.default_compatibility.write() = level;

    Ok(Json(ConfigResponse {
        compatibility_level: level.to_string(),
    }))
}

async fn get_subject_config(
    State(state): State<Arc<ServerState>>,
    Path(subject): Path<String>,
) -> Json<ConfigResponse> {
    let level = state
        .registry
        .get_subject_compatibility(&Subject::new(subject));
    Json(ConfigResponse {
        compatibility_level: level.to_string(),
    })
}

async fn update_subject_config(
    State(state): State<Arc<ServerState>>,
    Path(subject): Path<String>,
    Json(req): Json<ConfigRequest>,
) -> Result<Json<ConfigResponse>, (StatusCode, Json<ErrorResponse>)> {
    let level: CompatibilityLevel = req.compatibility.parse().map_err(|_| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(ErrorResponse {
                error_code: 42203,
                message: format!("Invalid compatibility level: {}", req.compatibility),
            }),
        )
    })?;

    state
        .registry
        .set_subject_compatibility(subject.as_str(), level);

    Ok(Json(ConfigResponse {
        compatibility_level: level.to_string(),
    }))
}

async fn get_mode(State(state): State<Arc<ServerState>>) -> Json<ModeResponse> {
    Json(ModeResponse {
        mode: state.mode.read().to_string(),
    })
}

async fn update_mode(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<ModeRequest>,
) -> Result<Json<ModeResponse>, (StatusCode, Json<ErrorResponse>)> {
    let mode = match req.mode.to_uppercase().as_str() {
        "READWRITE" => RegistryMode::ReadWrite,
        "READONLY" => RegistryMode::ReadOnly,
        "IMPORT" => RegistryMode::Import,
        _ => {
            return Err((
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(ErrorResponse {
                    error_code: 42204,
                    message: format!("Invalid mode: {}", req.mode),
                }),
            ))
        }
    };

    *state.mode.write() = mode;

    Ok(Json(ModeResponse {
        mode: mode.to_string(),
    }))
}

// ============================================================================
// Version State Handlers
// ============================================================================

#[derive(Serialize)]
struct VersionStateResponse {
    state: String,
}

#[derive(Deserialize)]
struct VersionStateRequest {
    state: String,
}

async fn get_version_state(
    State(state): State<Arc<ServerState>>,
    Path((subject, version)): Path<(String, u32)>,
) -> Result<Json<VersionStateResponse>, (StatusCode, Json<ErrorResponse>)> {
    state
        .registry
        .get_version_state(subject.as_str(), SchemaVersion::new(version))
        .await
        .map(|s| {
            Json(VersionStateResponse {
                state: s.to_string(),
            })
        })
        .map_err(schema_error_response)
}

async fn set_version_state(
    State(state): State<Arc<ServerState>>,
    Path((subject, version)): Path<(String, u32)>,
    Json(req): Json<VersionStateRequest>,
) -> Result<Json<VersionStateResponse>, (StatusCode, Json<ErrorResponse>)> {
    let version_state: VersionState = req.state.parse().map_err(|_| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(ErrorResponse {
                error_code: 42205,
                message: format!("Invalid version state: {}", req.state),
            }),
        )
    })?;

    state
        .registry
        .set_version_state(subject.as_str(), SchemaVersion::new(version), version_state)
        .await
        .map_err(schema_error_response)?;

    Ok(Json(VersionStateResponse {
        state: version_state.to_string(),
    }))
}

async fn deprecate_version(
    State(state): State<Arc<ServerState>>,
    Path((subject, version)): Path<(String, u32)>,
) -> Result<Json<VersionStateResponse>, (StatusCode, Json<ErrorResponse>)> {
    state
        .registry
        .deprecate_version(subject.as_str(), SchemaVersion::new(version))
        .await
        .map_err(schema_error_response)?;

    Ok(Json(VersionStateResponse {
        state: "DEPRECATED".to_string(),
    }))
}

async fn disable_version(
    State(state): State<Arc<ServerState>>,
    Path((subject, version)): Path<(String, u32)>,
) -> Result<Json<VersionStateResponse>, (StatusCode, Json<ErrorResponse>)> {
    state
        .registry
        .disable_version(subject.as_str(), SchemaVersion::new(version))
        .await
        .map_err(schema_error_response)?;

    Ok(Json(VersionStateResponse {
        state: "DISABLED".to_string(),
    }))
}

async fn enable_version(
    State(state): State<Arc<ServerState>>,
    Path((subject, version)): Path<(String, u32)>,
) -> Result<Json<VersionStateResponse>, (StatusCode, Json<ErrorResponse>)> {
    state
        .registry
        .enable_version(subject.as_str(), SchemaVersion::new(version))
        .await
        .map_err(schema_error_response)?;

    Ok(Json(VersionStateResponse {
        state: "ENABLED".to_string(),
    }))
}

// ============================================================================
// Validation Handlers
// ============================================================================

#[derive(Deserialize)]
struct ValidateSchemaRequest {
    schema: String,
    #[serde(rename = "schemaType", default)]
    schema_type: Option<String>,
}

#[derive(Serialize)]
struct ValidationResponse {
    is_valid: bool,
    errors: Vec<String>,
    warnings: Vec<String>,
}

async fn validate_schema(
    State(state): State<Arc<ServerState>>,
    Path(subject): Path<String>,
    Json(req): Json<ValidateSchemaRequest>,
) -> Result<Json<ValidationResponse>, (StatusCode, Json<ErrorResponse>)> {
    let schema_type = req
        .schema_type
        .as_deref()
        .unwrap_or("AVRO")
        .parse::<SchemaType>()
        .unwrap_or(SchemaType::Avro);

    state
        .registry
        .validate_schema(schema_type, &subject, &req.schema)
        .map(|report| {
            Json(ValidationResponse {
                is_valid: report.is_valid(),
                errors: report.error_messages(),
                warnings: report.warning_messages(),
            })
        })
        .map_err(schema_error_response)
}

#[derive(Serialize)]
struct ValidationRuleResponse {
    name: String,
    rule_type: String,
    config: String,
    level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    active: bool,
}

#[derive(Deserialize)]
struct AddValidationRuleRequest {
    name: String,
    rule_type: String,
    config: String,
    #[serde(default = "default_level")]
    level: String,
    description: Option<String>,
    #[serde(default)]
    subjects: Vec<String>,
    #[serde(default)]
    schema_types: Vec<String>,
}

fn default_level() -> String {
    "ERROR".to_string()
}

async fn list_validation_rules(
    State(state): State<Arc<ServerState>>,
) -> Json<Vec<ValidationRuleResponse>> {
    let rules = state.registry.validation_engine().read().list_rules();
    Json(
        rules
            .into_iter()
            .map(|r| ValidationRuleResponse {
                name: r.name().to_string(),
                rule_type: format!("{:?}", r.rule_type()),
                config: r.config().to_string(),
                level: format!("{:?}", r.level()),
                description: r.description().map(|s| s.to_string()),
                active: r.is_active(),
            })
            .collect(),
    )
}

async fn add_validation_rule(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<AddValidationRuleRequest>,
) -> Result<Json<ValidationRuleResponse>, (StatusCode, Json<ErrorResponse>)> {
    let rule_type: ValidationRuleType = req.rule_type.parse().map_err(|_| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(ErrorResponse {
                error_code: 42206,
                message: format!("Invalid rule type: {}", req.rule_type),
            }),
        )
    })?;

    let level: ValidationLevel = req.level.parse().map_err(|_| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(ErrorResponse {
                error_code: 42207,
                message: format!("Invalid validation level: {}", req.level),
            }),
        )
    })?;

    let mut rule = ValidationRule::new(&req.name, rule_type, &req.config).with_level(level);

    if let Some(desc) = &req.description {
        rule = rule.with_description(desc);
    }

    if !req.subjects.is_empty() {
        rule = rule.for_subjects(req.subjects);
    }

    if !req.schema_types.is_empty() {
        let schema_types: Vec<SchemaType> = req
            .schema_types
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        rule = rule.for_schema_types(schema_types);
    }

    state.registry.add_validation_rule(rule.clone());

    Ok(Json(ValidationRuleResponse {
        name: rule.name().to_string(),
        rule_type: format!("{:?}", rule.rule_type()),
        config: rule.config().to_string(),
        level: format!("{:?}", rule.level()),
        description: rule.description().map(|s| s.to_string()),
        active: rule.is_active(),
    }))
}

async fn delete_validation_rule(
    State(state): State<Arc<ServerState>>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let removed = state
        .registry
        .validation_engine()
        .write()
        .remove_rule(&name);
    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error_code: 40404,
                message: format!("Validation rule not found: {}", name),
            }),
        ))
    }
}

// ============================================================================
// Context Handlers (Multi-tenant)
// ============================================================================

#[derive(Serialize)]
struct ContextResponse {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    active: bool,
}

#[derive(Deserialize)]
struct CreateContextRequest {
    name: String,
    description: Option<String>,
}

async fn list_contexts(State(state): State<Arc<ServerState>>) -> Json<Vec<ContextResponse>> {
    let contexts = state.registry.list_contexts();
    Json(
        contexts
            .into_iter()
            .map(|c| ContextResponse {
                name: c.name().to_string(),
                description: c.description().map(|s| s.to_string()),
                active: c.is_active(),
            })
            .collect(),
    )
}

async fn create_context(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<CreateContextRequest>,
) -> Result<Json<ContextResponse>, (StatusCode, Json<ErrorResponse>)> {
    let mut context = SchemaContext::new(&req.name);
    if let Some(desc) = &req.description {
        context = context.with_description(desc);
    }

    state
        .registry
        .create_context(context.clone())
        .map_err(schema_error_response)?;

    Ok(Json(ContextResponse {
        name: context.name().to_string(),
        description: context.description().map(|s| s.to_string()),
        active: context.is_active(),
    }))
}

async fn get_context(
    State(state): State<Arc<ServerState>>,
    Path(context_name): Path<String>,
) -> Result<Json<ContextResponse>, (StatusCode, Json<ErrorResponse>)> {
    state
        .registry
        .get_context(&context_name)
        .map(|c| {
            Json(ContextResponse {
                name: c.name().to_string(),
                description: c.description().map(|s| s.to_string()),
                active: c.is_active(),
            })
        })
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error_code: 40405,
                    message: format!("Context not found: {}", context_name),
                }),
            )
        })
}

async fn delete_context(
    State(state): State<Arc<ServerState>>,
    Path(context_name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    state
        .registry
        .delete_context(&context_name)
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(schema_error_response)
}

async fn list_context_subjects(
    State(state): State<Arc<ServerState>>,
    Path(context_name): Path<String>,
) -> Json<Vec<String>> {
    Json(state.registry.list_subjects_in_context(&context_name))
}

// ============================================================================
// Statistics Handler
// ============================================================================

#[derive(Serialize)]
struct StatsResponse {
    schema_count: usize,
    subject_count: usize,
    context_count: usize,
    cache_size: usize,
}

async fn get_stats(State(state): State<Arc<ServerState>>) -> Json<StatsResponse> {
    let stats = state.registry.stats().await;
    Json(StatsResponse {
        schema_count: stats.schema_count,
        subject_count: stats.subject_count,
        context_count: stats.context_count,
        cache_size: stats.cache_size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RegistryConfig;
    use axum::body::Body;
    use axum::http::Request;
    use tower::util::ServiceExt;

    async fn create_test_app() -> Router {
        let config = RegistryConfig::memory();
        let registry = SchemaRegistry::new(config).await.unwrap();
        let server = SchemaServer::new(registry, ServerConfig::default());
        server.router()
    }

    #[tokio::test]
    async fn test_root_endpoint() {
        let app = create_test_app().await;

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_list_subjects_empty() {
        let app = create_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/subjects")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_config() {
        let app = create_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_mode() {
        let app = create_test_app().await;

        let response = app
            .oneshot(Request::builder().uri("/mode").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let app = create_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_liveness_endpoint() {
        let app = create_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health/live")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_readiness_endpoint() {
        let app = create_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_stats_endpoint() {
        let app = create_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_list_contexts_empty() {
        let app = create_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/contexts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_list_validation_rules_empty() {
        let app = create_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/config/validation/rules")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
