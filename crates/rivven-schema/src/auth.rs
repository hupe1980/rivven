//! Authentication & Authorization middleware for Schema Registry
//!
//! Provides enterprise-grade authentication for the Confluent-compatible REST API.
//! Supports multiple authentication methods and authorization modes.
//!
//! ## Authentication Methods
//!
//! 1. **OAuth2/JWT** (jwt feature) - OIDC tokens with claim-based identity
//! 2. **API Key** - Header-based authentication (X-API-Key or custom)
//! 3. **Bearer Token** - Session ID or static token
//! 4. **Basic Auth** - Username/password (legacy fallback)
//!
//! ## Authorization Modes
//!
//! 1. **Simple RBAC** (auth feature) - Permission-based using rivven-core AuthManager
//! 2. **Cedar Policies** (cedar feature) - Fine-grained policy-as-code authorization
//!
//! ## Permissions
//!
//! Permissions are checked at the subject level:
//! - `describe` - Get schemas, list subjects, check compatibility
//! - `create` - Register schemas
//! - `alter` - Update config
//! - `delete` - Delete versions, delete subjects
//!
//! ## Configuration Examples
//!
//! ```rust,ignore
//! use rivven_schema::auth::{AuthConfig, AuthMethod};
//!
//! // OAuth2/OIDC with JWT validation
//! let config = AuthConfig::required()
//!     .with_jwt_validation(JwtConfig {
//!         issuer: "https://auth.example.com".into(),
//!         audience: Some("rivven-schema".into()),
//!         jwks_url: Some("https://auth.example.com/.well-known/jwks.json".into()),
//!         ..Default::default()
//!     });
//!
//! // API Key authentication
//! let config = AuthConfig::required()
//!     .with_api_keys(vec![("admin-key", "admin"), ("readonly-key", "viewer")]);
//!
//! // Multiple methods (evaluated in order)
//! let config = AuthConfig::required()
//!     .with_jwt_validation(jwt_config)
//!     .with_api_keys(api_keys)
//!     .with_basic_auth(true); // Fallback
//! ```
//!
//! ## Cedar Policy Example
//!
//! ```cedar,ignore
//! // Allow schema admins full access to schemas
//! permit(
//!   principal in Rivven::Group::"schema-admins",
//!   action,
//!   resource is Rivven::Schema
//! );
//!
//! // Allow producers to register schemas matching their topic patterns
//! permit(
//!   principal,
//!   action == Rivven::Action::"create",
//!   resource is Rivven::Schema
//! ) when {
//!   resource.name.startsWith(principal.team + "-")
//! };
//! ```

use axum::{body::Body, extract::State, http::Request, middleware::Next, response::Response};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use subtle::ConstantTimeEq;

#[cfg(feature = "auth")]
use axum::{
    http::{header::AUTHORIZATION, StatusCode},
    response::IntoResponse,
    Json,
};
#[cfg(feature = "auth")]
use base64::Engine;
#[cfg(feature = "auth")]
use tracing::{debug, warn};

#[cfg(all(feature = "auth", not(feature = "cedar")))]
use rivven_core::{AuthManager, AuthSession, Permission, ResourceType};

#[cfg(feature = "cedar")]
use rivven_core::{AuthManager, AuthSession, Permission};

// Cedar authorization support
#[cfg(feature = "cedar")]
use rivven_core::{AuthzContext, CedarAuthorizer, RivvenAction, RivvenResource};

// JWT validation support
#[cfg(feature = "jwt")]
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};

/// JWT/OIDC Configuration
#[derive(Clone, Default)]
pub struct JwtConfig {
    /// Expected token issuer (iss claim)
    pub issuer: Option<String>,
    /// Expected audience (aud claim)
    pub audience: Option<String>,
    /// JWKS URL for key discovery (not implemented yet - uses secret)
    pub jwks_url: Option<String>,
    /// Secret key for HS256 validation (for testing/simple setups)
    pub secret: Option<String>,
    /// RSA public key in PEM format for RS256 validation
    pub rsa_public_key: Option<String>,
    /// Claim to use as principal name (default: "sub")
    pub principal_claim: String,
    /// Claim to use for roles/groups (default: "groups")
    pub roles_claim: String,
}

impl std::fmt::Debug for JwtConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtConfig")
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .field("jwks_url", &self.jwks_url)
            .field("secret", &self.secret.as_ref().map(|_| "[REDACTED]"))
            .field(
                "rsa_public_key",
                &self.rsa_public_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field("principal_claim", &self.principal_claim)
            .field("roles_claim", &self.roles_claim)
            .finish()
    }
}

impl JwtConfig {
    /// Create config with issuer validation
    pub fn with_issuer(issuer: impl Into<String>) -> Self {
        Self {
            issuer: Some(issuer.into()),
            principal_claim: "sub".to_string(),
            roles_claim: "groups".to_string(),
            ..Default::default()
        }
    }

    /// Set expected audience
    pub fn with_audience(mut self, audience: impl Into<String>) -> Self {
        self.audience = Some(audience.into());
        self
    }

    /// Set HS256 secret for validation
    pub fn with_secret(mut self, secret: impl Into<String>) -> Self {
        self.secret = Some(secret.into());
        self
    }

    /// Set RSA public key for RS256 validation
    pub fn with_rsa_public_key(mut self, key: impl Into<String>) -> Self {
        self.rsa_public_key = Some(key.into());
        self
    }
}

/// API Key entry
#[derive(Debug, Clone)]
pub struct ApiKeyEntry {
    /// The API key value
    pub key: String,
    /// Principal/user name this key maps to
    pub principal: String,
    /// Roles/groups for this key
    pub roles: Vec<String>,
}

impl ApiKeyEntry {
    pub fn new(key: impl Into<String>, principal: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            principal: principal.into(),
            roles: Vec::new(),
        }
    }

    pub fn with_roles(mut self, roles: Vec<String>) -> Self {
        self.roles = roles;
        self
    }
}

/// Authentication configuration
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Whether authentication is required
    pub require_auth: bool,
    /// Realm for Basic Auth challenge
    pub realm: String,
    /// Allow anonymous read access
    pub allow_anonymous_read: bool,
    /// Enable Basic Auth (username:password)
    pub enable_basic_auth: bool,
    /// Enable Bearer Token (session ID lookup)
    pub enable_bearer_token: bool,
    /// JWT/OIDC configuration (requires jwt feature)
    #[cfg(feature = "jwt")]
    pub jwt_config: Option<JwtConfig>,
    /// API Keys (key -> ApiKeyEntry mapping)
    pub api_keys: HashMap<String, ApiKeyEntry>,
    /// Custom API key header name (default: X-API-Key)
    pub api_key_header: String,
    /// Use Cedar for authorization (when cedar feature enabled)
    #[cfg(feature = "cedar")]
    pub use_cedar: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            require_auth: false,
            realm: "Rivven Schema Registry".to_string(),
            allow_anonymous_read: true,
            enable_basic_auth: true,
            enable_bearer_token: true,
            #[cfg(feature = "jwt")]
            jwt_config: None,
            api_keys: HashMap::new(),
            api_key_header: "X-API-Key".to_string(),
            #[cfg(feature = "cedar")]
            use_cedar: false,
        }
    }
}

impl AuthConfig {
    /// Create config requiring authentication
    pub fn required() -> Self {
        Self {
            require_auth: true,
            realm: "Rivven Schema Registry".to_string(),
            allow_anonymous_read: false,
            enable_basic_auth: true,
            enable_bearer_token: true,
            #[cfg(feature = "jwt")]
            jwt_config: None,
            api_keys: HashMap::new(),
            api_key_header: "X-API-Key".to_string(),
            #[cfg(feature = "cedar")]
            use_cedar: false,
        }
    }

    /// Create config with anonymous read access
    pub fn with_anonymous_read(mut self, allow: bool) -> Self {
        self.allow_anonymous_read = allow;
        self
    }

    /// Enable/disable Basic Auth
    pub fn with_basic_auth(mut self, enable: bool) -> Self {
        self.enable_basic_auth = enable;
        self
    }

    /// Enable/disable Bearer Token (session lookup)
    pub fn with_bearer_token(mut self, enable: bool) -> Self {
        self.enable_bearer_token = enable;
        self
    }

    /// Configure JWT/OIDC validation (requires jwt feature)
    #[cfg(feature = "jwt")]
    pub fn with_jwt(mut self, config: JwtConfig) -> Self {
        self.jwt_config = Some(config);
        self
    }

    /// Add API keys for authentication
    pub fn with_api_keys(mut self, keys: Vec<ApiKeyEntry>) -> Self {
        for entry in keys {
            self.api_keys.insert(entry.key.clone(), entry);
        }
        self
    }

    /// Add a single API key
    pub fn add_api_key(mut self, key: impl Into<String>, principal: impl Into<String>) -> Self {
        let entry = ApiKeyEntry::new(key, principal);
        self.api_keys.insert(entry.key.clone(), entry);
        self
    }

    /// Set custom API key header name
    pub fn with_api_key_header(mut self, header: impl Into<String>) -> Self {
        self.api_key_header = header.into();
        self
    }

    /// Enable Cedar-based authorization
    #[cfg(feature = "cedar")]
    pub fn with_cedar(mut self) -> Self {
        self.use_cedar = true;
        self
    }
}

/// Authentication state for extracting in handlers
#[derive(Debug, Clone)]
pub struct AuthState {
    #[cfg(feature = "auth")]
    pub session: Option<AuthSession>,
    #[cfg(not(feature = "auth"))]
    pub session: Option<()>,
    pub authenticated: bool,
}

impl AuthState {
    /// Anonymous/unauthenticated state
    pub fn anonymous() -> Self {
        Self {
            session: None,
            authenticated: false,
        }
    }

    #[cfg(feature = "auth")]
    /// Authenticated state with session
    pub fn authenticated(session: AuthSession) -> Self {
        Self {
            session: Some(session),
            authenticated: true,
        }
    }

    /// Get the principal name if authenticated
    pub fn principal(&self) -> Option<&str> {
        #[cfg(feature = "auth")]
        {
            self.session.as_ref().map(|s| s.principal_name.as_str())
        }
        #[cfg(not(feature = "auth"))]
        {
            None
        }
    }
}

/// Shared authentication state for the server
#[cfg(all(feature = "auth", not(feature = "cedar")))]
pub struct ServerAuthState {
    pub auth_manager: Arc<AuthManager>,
    pub config: AuthConfig,
}

/// Shared authentication state for the server (with Cedar support)
#[cfg(feature = "cedar")]
pub struct ServerAuthState {
    pub auth_manager: Arc<AuthManager>,
    pub cedar_authorizer: Option<Arc<CedarAuthorizer>>,
    pub config: AuthConfig,
}

#[cfg(not(feature = "auth"))]
pub struct ServerAuthState {
    pub config: AuthConfig,
}

#[cfg(feature = "cedar")]
impl ServerAuthState {
    /// Create server auth state with Cedar authorizer
    pub fn with_cedar(
        auth_manager: Arc<AuthManager>,
        cedar_authorizer: Arc<CedarAuthorizer>,
        config: AuthConfig,
    ) -> Self {
        Self {
            auth_manager,
            cedar_authorizer: Some(cedar_authorizer),
            config,
        }
    }
}

/// Error response for auth failures
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthErrorResponse {
    pub error_code: i32,
    pub message: String,
}

/// Authentication middleware
///
/// Evaluates authentication methods in order:
/// 1. API Key (X-API-Key header or custom)
/// 2. JWT/OIDC (Bearer token with JWT validation)
/// 3. Bearer Token (session ID lookup)
/// 4. Basic Auth (username:password)
#[cfg(feature = "auth")]
pub async fn auth_middleware(
    State(auth_state): State<Arc<ServerAuthState>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let config = &auth_state.config;

    // Try API Key first (custom header)
    if !config.api_keys.is_empty() {
        if let Some(api_key) = request
            .headers()
            .get(&config.api_key_header)
            .and_then(|v| v.to_str().ok())
        {
            match validate_api_key(api_key, config, &auth_state.auth_manager) {
                Ok(auth) => {
                    debug!("API Key authentication successful");
                    request.extensions_mut().insert(auth);
                    return next.run(request).await;
                }
                Err(e) => {
                    warn!("API Key authentication failed: {}", e.message);
                    return unauthorized_response(&config.realm, e);
                }
            }
        }
    }

    // Try Authorization header
    let auth_header = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let result = match auth_header {
        Some(header) if header.starts_with("Bearer ") => {
            // Try JWT validation first (if configured)
            #[cfg(feature = "jwt")]
            if let Some(jwt_config) = &config.jwt_config {
                match validate_jwt_token(header, jwt_config, &auth_state.auth_manager) {
                    Ok(auth) => {
                        return {
                            request.extensions_mut().insert(auth);
                            next.run(request).await
                        }
                    }
                    Err(e) => {
                        debug!(
                            "JWT validation failed, trying session lookup: {}",
                            e.message
                        );
                        // Fall through to session lookup
                    }
                }
            }

            // Try session ID lookup
            if config.enable_bearer_token {
                parse_bearer_token(header, &auth_state.auth_manager).await
            } else {
                Err(AuthErrorResponse {
                    error_code: 40101,
                    message: "Bearer token authentication is disabled".to_string(),
                })
            }
        }
        Some(header) if header.starts_with("Basic ") => {
            if config.enable_basic_auth {
                parse_basic_auth(header, &auth_state.auth_manager).await
            } else {
                Err(AuthErrorResponse {
                    error_code: 40101,
                    message: "Basic authentication is disabled".to_string(),
                })
            }
        }
        Some(_) => Err(AuthErrorResponse {
            error_code: 40101,
            message: "Invalid Authorization header format. Supported: Basic, Bearer".to_string(),
        }),
        None => {
            if config.require_auth {
                if config.allow_anonymous_read && is_read_request(&request) {
                    Ok(AuthState::anonymous())
                } else {
                    Err(AuthErrorResponse {
                        error_code: 40101,
                        message: "Authentication required".to_string(),
                    })
                }
            } else {
                Ok(AuthState::anonymous())
            }
        }
    };

    match result {
        Ok(auth) => {
            request.extensions_mut().insert(auth);
            next.run(request).await
        }
        Err(e) => unauthorized_response(&config.realm, e),
    }
}

/// Build unauthorized response with WWW-Authenticate header
#[cfg(feature = "auth")]
fn unauthorized_response(realm: &str, error: AuthErrorResponse) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(
            "WWW-Authenticate",
            format!("Basic realm=\"{}\", Bearer", realm).as_str(),
        )],
        Json(error),
    )
        .into_response()
}

/// Authentication middleware when auth feature is disabled (passthrough)
#[cfg(not(feature = "auth"))]
pub async fn auth_middleware(
    State(_auth_state): State<Arc<ServerAuthState>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    // Without auth feature, always allow anonymous
    request.extensions_mut().insert(AuthState::anonymous());
    next.run(request).await
}

/// Validate API Key (timing-safe).
///
/// Standard HashMap::get short-circuits on key mismatch, creating
/// a timing side-channel that enables brute-force key enumeration. Instead,
/// we iterate all stored keys and use constant-time byte comparison so that
/// the response time does not leak which keys exist.
#[cfg(feature = "auth")]
fn validate_api_key(
    api_key: &str,
    config: &AuthConfig,
    auth_manager: &AuthManager,
) -> Result<AuthState, AuthErrorResponse> {
    // Constant-time scan: check every key even after a match is found
    let api_key_bytes = api_key.as_bytes();
    let mut matched_entry = None;

    for (stored_key, entry) in &config.api_keys {
        let stored_bytes = stored_key.as_bytes();
        // Use subtle::ConstantTimeEq for immune-to-optimization comparison
        if stored_bytes.len() == api_key_bytes.len()
            && bool::from(stored_bytes.ct_eq(api_key_bytes))
        {
            matched_entry = Some(entry);
        }
        // Continue scanning even after match to prevent timing leak
    }

    if let Some(entry) = matched_entry {
        debug!("API Key validated for principal: {}", entry.principal);

        if let Some(session) = auth_manager.get_session_by_principal(&entry.principal) {
            Ok(AuthState::authenticated(session))
        } else {
            let session = auth_manager.create_api_key_session(&entry.principal, &entry.roles);
            Ok(AuthState::authenticated(session))
        }
    } else {
        Err(AuthErrorResponse {
            error_code: 40101,
            message: "Invalid API key".to_string(),
        })
    }
}

/// JWT Claims structure
///
/// Note: Some fields (iss, aud, exp) are validated internally by jsonwebtoken
/// during token verification but not accessed directly in our code.
#[cfg(feature = "jwt")]
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct JwtClaims {
    /// Subject (user identifier)
    sub: Option<String>,
    /// Issuer (validated by jsonwebtoken)
    iss: Option<String>,
    /// Audience (validated by jsonwebtoken)
    aud: Option<serde_json::Value>,
    /// Expiration time (validated by jsonwebtoken)
    exp: Option<u64>,
    /// Groups/roles claim
    groups: Option<Vec<String>>,
    /// Alternative roles claim
    roles: Option<Vec<String>>,
    /// Email (alternative principal)
    email: Option<String>,
    /// Preferred username
    preferred_username: Option<String>,
}

/// Validate JWT/OIDC token
#[cfg(feature = "jwt")]
fn validate_jwt_token(
    header: &str,
    jwt_config: &JwtConfig,
    auth_manager: &AuthManager,
) -> Result<AuthState, AuthErrorResponse> {
    let token = header.trim_start_matches("Bearer ");

    // Determine decoding key
    let decoding_key = if let Some(secret) = &jwt_config.secret {
        DecodingKey::from_secret(secret.as_bytes())
    } else if let Some(rsa_key) = &jwt_config.rsa_public_key {
        DecodingKey::from_rsa_pem(rsa_key.as_bytes()).map_err(|e| AuthErrorResponse {
            error_code: 40101,
            message: format!("Invalid RSA public key: {}", e),
        })?
    } else {
        return Err(AuthErrorResponse {
            error_code: 50001,
            message: "JWT validation not configured (no secret or public key)".to_string(),
        });
    };

    // Configure validation
    let mut validation = Validation::default();

    // Set algorithm based on key type
    if jwt_config.rsa_public_key.is_some() {
        validation.algorithms = vec![Algorithm::RS256, Algorithm::RS384, Algorithm::RS512];
    } else {
        validation.algorithms = vec![Algorithm::HS256, Algorithm::HS384, Algorithm::HS512];
    }

    // Set issuer validation
    if let Some(issuer) = &jwt_config.issuer {
        validation.set_issuer(&[issuer]);
    }

    // Set audience validation
    if let Some(audience) = &jwt_config.audience {
        validation.set_audience(&[audience]);
    }

    // Decode and validate token
    let token_data =
        decode::<JwtClaims>(token, &decoding_key, &validation).map_err(|e| AuthErrorResponse {
            error_code: 40101,
            message: format!("JWT validation failed: {}", e),
        })?;

    let claims = token_data.claims;

    // Extract principal from configured claim
    let principal = match jwt_config.principal_claim.as_str() {
        "sub" => claims.sub.clone(),
        "email" => claims.email.clone(),
        "preferred_username" => claims.preferred_username.clone(),
        _ => claims.sub.clone(),
    }
    .ok_or_else(|| AuthErrorResponse {
        error_code: 40101,
        message: format!("JWT missing required claim: {}", jwt_config.principal_claim),
    })?;

    // Extract roles from configured claim
    let roles = match jwt_config.roles_claim.as_str() {
        "groups" => claims.groups.unwrap_or_default(),
        "roles" => claims.roles.unwrap_or_default(),
        _ => claims.groups.or(claims.roles).unwrap_or_default(),
    };

    debug!(
        "JWT validated for principal: {} with roles: {:?}",
        principal, roles
    );

    // Create session for this JWT user
    let session = auth_manager.create_jwt_session(&principal, &roles);
    Ok(AuthState::authenticated(session))
}

/// Parse Basic Auth header
#[cfg(feature = "auth")]
async fn parse_basic_auth(
    header: &str,
    auth_manager: &AuthManager,
) -> Result<AuthState, AuthErrorResponse> {
    let encoded = header.trim_start_matches("Basic ");
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| AuthErrorResponse {
            error_code: 40101,
            message: "Invalid Basic auth encoding".to_string(),
        })?;

    let credentials = String::from_utf8(decoded).map_err(|_| AuthErrorResponse {
        error_code: 40101,
        message: "Invalid credentials encoding".to_string(),
    })?;

    let (username, password) = credentials.split_once(':').ok_or(AuthErrorResponse {
        error_code: 40101,
        message: "Invalid Basic auth format".to_string(),
    })?;

    debug!("Authenticating user: {}", username);

    // Note: We pass "http" as client_ip since we don't have access to it here
    // In production, extract from X-Forwarded-For or connection info
    match auth_manager.authenticate(username, password, "http") {
        Ok(session) => {
            debug!("Authentication successful for: {}", username);
            Ok(AuthState::authenticated(session))
        }
        Err(e) => {
            warn!("Authentication failed for {}: {:?}", username, e);
            Err(AuthErrorResponse {
                error_code: 40101,
                message: "Invalid credentials".to_string(),
            })
        }
    }
}

/// Parse Bearer token header
/// Bearer tokens are treated as session IDs
#[cfg(feature = "auth")]
async fn parse_bearer_token(
    header: &str,
    auth_manager: &AuthManager,
) -> Result<AuthState, AuthErrorResponse> {
    let token = header.trim_start_matches("Bearer ");

    // Bearer token is treated as session ID
    match auth_manager.get_session(token) {
        Some(session) => {
            debug!(
                "Token validation successful for: {}",
                session.principal_name
            );
            Ok(AuthState::authenticated(session))
        }
        None => {
            warn!("Token validation failed: invalid or expired session");
            Err(AuthErrorResponse {
                error_code: 40101,
                message: "Invalid or expired token".to_string(),
            })
        }
    }
}

/// Check if request is a read-only operation
#[cfg(feature = "auth")]
fn is_read_request(request: &Request<Body>) -> bool {
    matches!(request.method().as_str(), "GET" | "HEAD" | "OPTIONS")
}

/// Permission required for schema operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaPermission {
    /// Read schemas, list subjects, check compatibility
    Describe,
    /// Register new schemas
    Create,
    /// Update configurations
    Alter,
    /// Delete schemas or subjects
    Delete,
}

impl SchemaPermission {
    /// Get the corresponding rivven-core Permission
    #[cfg(feature = "auth")]
    pub fn to_core_permission(self) -> Permission {
        match self {
            SchemaPermission::Describe => Permission::Describe,
            SchemaPermission::Create => Permission::Create,
            SchemaPermission::Alter => Permission::Alter,
            SchemaPermission::Delete => Permission::Delete,
        }
    }

    /// Get the corresponding Cedar action
    #[cfg(feature = "cedar")]
    pub fn to_cedar_action(self) -> RivvenAction {
        match self {
            SchemaPermission::Describe => RivvenAction::Describe,
            SchemaPermission::Create => RivvenAction::Create,
            SchemaPermission::Alter => RivvenAction::Alter,
            SchemaPermission::Delete => RivvenAction::Delete,
        }
    }
}

/// Check if the current session has permission on a subject (Simple RBAC)
#[cfg(all(feature = "auth", not(feature = "cedar")))]
pub fn check_subject_permission(
    auth_state: &AuthState,
    subject: &str,
    permission: SchemaPermission,
) -> Result<(), AuthErrorResponse> {
    // Anonymous access for read-only when configured
    if !auth_state.authenticated {
        if matches!(permission, SchemaPermission::Describe) {
            return Ok(());
        }
        return Err(AuthErrorResponse {
            error_code: 40301,
            message: "Authentication required for write operations".to_string(),
        });
    }

    if let Some(session) = &auth_state.session {
        let resource = ResourceType::Schema(subject.to_string());
        let perm = permission.to_core_permission();

        // Use session's has_permission method instead of AuthManager
        if session.has_permission(&resource, &perm) {
            Ok(())
        } else {
            Err(AuthErrorResponse {
                error_code: 40301,
                message: format!(
                    "Access denied: {} lacks {:?} permission on subject '{}'",
                    session.principal_name, permission, subject
                ),
            })
        }
    } else {
        Err(AuthErrorResponse {
            error_code: 40101,
            message: "No valid session".to_string(),
        })
    }
}

/// Check if the current session has permission on a subject (Cedar policy-based)
#[cfg(feature = "cedar")]
pub fn check_subject_permission_cedar(
    auth_state: &AuthState,
    subject: &str,
    permission: SchemaPermission,
    authorizer: &CedarAuthorizer,
    context: Option<AuthzContext>,
) -> Result<(), AuthErrorResponse> {
    // Anonymous access for read-only when configured
    if !auth_state.authenticated {
        if matches!(permission, SchemaPermission::Describe) {
            return Ok(());
        }
        return Err(AuthErrorResponse {
            error_code: 40301,
            message: "Authentication required for write operations".to_string(),
        });
    }

    let principal = auth_state.principal().ok_or_else(|| AuthErrorResponse {
        error_code: 40101,
        message: "No valid session".to_string(),
    })?;

    let action = permission.to_cedar_action();
    let resource = RivvenResource::Schema(subject.to_string());
    let ctx = context.unwrap_or_default();

    match authorizer.authorize(principal, action, &resource, &ctx) {
        Ok(()) => Ok(()),
        Err(e) => {
            warn!("Cedar authorization denied: {:?}", e);
            Err(AuthErrorResponse {
                error_code: 40301,
                message: format!(
                    "Access denied: {} cannot {:?} on subject '{}'",
                    principal, permission, subject
                ),
            })
        }
    }
}

/// Check permission without auth feature (always succeeds)
#[cfg(not(feature = "auth"))]
pub fn check_subject_permission(
    _auth_state: &AuthState,
    _subject: &str,
    _permission: SchemaPermission,
) -> Result<(), AuthErrorResponse> {
    Ok(())
}

#[cfg(all(test, feature = "auth"))]
mod tests {
    use super::*;

    #[test]
    fn test_auth_config_default() {
        let config = AuthConfig::default();
        assert!(!config.require_auth);
        assert!(config.allow_anonymous_read);
    }

    #[test]
    fn test_auth_config_required() {
        let config = AuthConfig::required();
        assert!(config.require_auth);
        assert!(!config.allow_anonymous_read);
    }

    #[test]
    fn test_auth_state_anonymous() {
        let state = AuthState::anonymous();
        assert!(!state.authenticated);
        assert!(state.principal().is_none());
    }

    #[test]
    fn test_schema_permission_to_core() {
        assert!(matches!(
            SchemaPermission::Describe.to_core_permission(),
            Permission::Describe
        ));
        assert!(matches!(
            SchemaPermission::Create.to_core_permission(),
            Permission::Create
        ));
        assert!(matches!(
            SchemaPermission::Alter.to_core_permission(),
            Permission::Alter
        ));
        assert!(matches!(
            SchemaPermission::Delete.to_core_permission(),
            Permission::Delete
        ));
    }
}

#[cfg(all(test, feature = "cedar"))]
mod cedar_tests {
    use super::*;

    #[test]
    fn test_auth_config_with_cedar() {
        let config = AuthConfig::required().with_cedar();
        assert!(config.require_auth);
        assert!(config.use_cedar);
    }

    #[test]
    fn test_schema_permission_to_cedar_action() {
        assert_eq!(
            SchemaPermission::Describe.to_cedar_action(),
            RivvenAction::Describe
        );
        assert_eq!(
            SchemaPermission::Create.to_cedar_action(),
            RivvenAction::Create
        );
        assert_eq!(
            SchemaPermission::Alter.to_cedar_action(),
            RivvenAction::Alter
        );
        assert_eq!(
            SchemaPermission::Delete.to_cedar_action(),
            RivvenAction::Delete
        );
    }

    #[test]
    fn test_cedar_authorization() {
        // Create authorizer without schema validation for testing
        let authorizer = CedarAuthorizer::new_without_schema();

        // Add admin policy for schemas
        authorizer
            .add_policy(
                "schema-admin",
                r#"
permit(
  principal in Rivven::Group::"schema-admins",
  action,
  resource is Rivven::Schema
);
"#,
            )
            .unwrap();

        // Add group and user
        authorizer.add_group("schema-admins", &[]).unwrap();
        authorizer
            .add_user(
                "alice",
                Some("alice@example.com"),
                &["admin"],
                &["schema-admins"],
                false,
            )
            .unwrap();

        // Add schema entity
        authorizer.add_schema("user-events-value", 1).unwrap();

        // Create authenticated state
        let _auth_state = AuthState {
            session: None, // Cedar doesn't need the session, uses principal directly
            authenticated: true,
        };

        // Test authorization with manual principal extraction
        // In real code, auth_state.principal() would return the authenticated user
        let ctx = AuthzContext::new().with_ip("127.0.0.1");
        let result = authorizer.authorize(
            "alice",
            RivvenAction::Create,
            &RivvenResource::Schema("user-events-value".to_string()),
            &ctx,
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_cedar_authorization_denied() {
        let authorizer = CedarAuthorizer::new_without_schema();

        // Only admins can alter schemas
        authorizer
            .add_policy(
                "only-admins-alter",
                r#"
permit(
  principal in Rivven::Group::"admins",
  action == Rivven::Action::"alter",
  resource is Rivven::Schema
);
"#,
            )
            .unwrap();

        // Add a non-admin user
        authorizer
            .add_user("bob", Some("bob@example.com"), &["user"], &[], false)
            .unwrap();

        authorizer.add_schema("config-value", 1).unwrap();

        let ctx = AuthzContext::new();
        let result = authorizer.authorize(
            "bob",
            RivvenAction::Alter,
            &RivvenResource::Schema("config-value".to_string()),
            &ctx,
        );

        assert!(result.is_err());
    }
}
