//! OpenID Connect (OIDC) Authentication for Rivven
//!
//! This module provides OIDC-based authentication, allowing Rivven to integrate
//! with identity providers like Keycloak, Auth0, Okta, Azure AD, and more.
//!
//! ## Features
//!
//! - **JWT validation** with JWKS (JSON Web Key Set) support
//! - **Claims mapping** to Rivven principals and groups
//! - **Multiple provider** support with per-provider configuration
//! - **Token refresh** handling
//! - **Custom claims** extraction for fine-grained authorization
//!
//! ## Example Configuration
//!
//! ```yaml
//! authentication:
//!   oidc:
//!     providers:
//!       - name: keycloak
//!         issuer: https://auth.example.com/realms/rivven
//!         audience: rivven-api
//!         username_claim: preferred_username
//!         groups_claim: groups
//!         roles_claim: realm_access.roles
//!
//!       - name: azure-ad
//!         issuer: https://login.microsoftonline.com/{tenant-id}/v2.0
//!         audience: api://rivven
//!         username_claim: upn
//!         groups_claim: groups
//! ```
//!
//! ## Flow
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────────────┐
//! │                        OIDC AUTHENTICATION FLOW                          │
//! ├──────────────────────────────────────────────────────────────────────────┤
//! │                                                                          │
//! │  1. Client obtains JWT from Identity Provider                            │
//! │     ┌─────────┐         ┌─────────────────┐                              │
//! │     │ Client  │ ──────► │ Identity        │                              │
//! │     │         │ ◄────── │ Provider (IdP)  │  ← Returns JWT               │
//! │     └─────────┘         └─────────────────┘                              │
//! │                                                                          │
//! │  2. Client sends JWT to Rivven                                           │
//! │     ┌─────────┐         ┌─────────────────┐                              │
//! │     │ Client  │ ──JWT──►│ Rivven Server   │                              │
//! │     │         │         │                 │                              │
//! │     └─────────┘         └─────────────────┘                              │
//! │                                 │                                         │
//! │  3. Rivven validates JWT         │                                         │
//! │                                 ▼                                         │
//! │     ┌─────────────────────────────────────────────────────────────┐       │
//! │     │ a) Fetch JWKS from IdP (cached)                             │       │
//! │     │ b) Verify signature with public key                         │       │
//! │     │ c) Validate claims (iss, aud, exp, nbf)                     │       │
//! │     │ d) Extract username, groups, roles                          │       │
//! │     │ e) Create Rivven Principal                                  │       │
//! │     └─────────────────────────────────────────────────────────────┘       │
//! │                                                                          │
//! └──────────────────────────────────────────────────────────────────────────┘
//! ```

#[cfg(feature = "oidc")]
mod oidc_impl {
    use jsonwebtoken::{
        decode, decode_header, jwk::JwkSet, Algorithm, DecodingKey, TokenData, Validation,
    };
    use parking_lot::RwLock;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::time::{Duration, Instant};
    use thiserror::Error;
    use tracing::{debug, info};

    // ============================================================================
    // Error Types
    // ============================================================================

    #[derive(Error, Debug)]
    pub enum OidcError {
        #[error("Provider not found: {0}")]
        ProviderNotFound(String),

        #[error("Invalid token: {0}")]
        InvalidToken(String),

        #[error("Token expired")]
        TokenExpired,

        #[error("Invalid issuer: expected {expected}, got {actual}")]
        InvalidIssuer { expected: String, actual: String },

        #[error("Invalid audience: {0}")]
        InvalidAudience(String),

        #[error("Missing claim: {0}")]
        MissingClaim(String),

        #[error("JWKS fetch error: {0}")]
        JwksFetch(String),

        #[error("Key not found for kid: {0}")]
        KeyNotFound(String),

        #[error("Configuration error: {0}")]
        Configuration(String),

        #[error("HTTP error: {0}")]
        Http(String),

        #[error("JSON error: {0}")]
        Json(String),
    }

    pub type OidcResult<T> = Result<T, OidcError>;

    // ============================================================================
    // Configuration
    // ============================================================================

    /// OIDC provider configuration
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct OidcProviderConfig {
        /// Unique name for this provider
        pub name: String,

        /// OIDC issuer URL (e.g., `https://auth.example.com/realms/rivven`)
        pub issuer: String,

        /// Expected audience (client_id or API identifier)
        pub audience: String,

        /// Claim to extract username from (default: "sub")
        #[serde(default = "default_username_claim")]
        pub username_claim: String,

        /// Claim to extract groups from (optional)
        pub groups_claim: Option<String>,

        /// Claim to extract roles from (optional)
        pub roles_claim: Option<String>,

        /// Additional required claims
        #[serde(default)]
        pub required_claims: Vec<String>,

        /// Clock skew tolerance in seconds
        #[serde(default = "default_clock_skew")]
        pub clock_skew_seconds: u64,

        /// JWKS cache TTL in seconds
        #[serde(default = "default_jwks_cache_ttl")]
        pub jwks_cache_ttl_seconds: u64,

        /// Allowed algorithms (default: RS256)
        #[serde(default = "default_algorithms")]
        pub algorithms: Vec<String>,

        /// Custom JWKS endpoint (if different from standard discovery)
        pub jwks_uri: Option<String>,

        /// Whether this provider is enabled
        #[serde(default = "default_enabled")]
        pub enabled: bool,
    }

    fn default_username_claim() -> String {
        "sub".to_string()
    }

    fn default_clock_skew() -> u64 {
        60
    }

    fn default_jwks_cache_ttl() -> u64 {
        3600
    }

    fn default_algorithms() -> Vec<String> {
        vec!["RS256".to_string()]
    }

    fn default_enabled() -> bool {
        true
    }

    impl OidcProviderConfig {
        pub fn new(
            name: impl Into<String>,
            issuer: impl Into<String>,
            audience: impl Into<String>,
        ) -> Self {
            Self {
                name: name.into(),
                issuer: issuer.into(),
                audience: audience.into(),
                username_claim: default_username_claim(),
                groups_claim: None,
                roles_claim: None,
                required_claims: vec![],
                clock_skew_seconds: default_clock_skew(),
                jwks_cache_ttl_seconds: default_jwks_cache_ttl(),
                algorithms: default_algorithms(),
                jwks_uri: None,
                enabled: true,
            }
        }

        pub fn with_groups_claim(mut self, claim: impl Into<String>) -> Self {
            self.groups_claim = Some(claim.into());
            self
        }

        pub fn with_roles_claim(mut self, claim: impl Into<String>) -> Self {
            self.roles_claim = Some(claim.into());
            self
        }

        pub fn with_username_claim(mut self, claim: impl Into<String>) -> Self {
            self.username_claim = claim.into();
            self
        }

        pub fn jwks_url(&self) -> String {
            self.jwks_uri.clone().unwrap_or_else(|| {
                format!(
                    "{}/.well-known/jwks.json",
                    self.issuer.trim_end_matches('/')
                )
            })
        }
    }

    // ============================================================================
    // Claims
    // ============================================================================

    /// Standard JWT claims
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct StandardClaims {
        /// Subject (user identifier)
        pub sub: String,

        /// Issuer
        pub iss: String,

        /// Audience
        #[serde(default)]
        pub aud: ClaimValue,

        /// Expiration time
        pub exp: u64,

        /// Issued at time
        #[serde(default)]
        pub iat: Option<u64>,

        /// Not before time
        #[serde(default)]
        pub nbf: Option<u64>,

        /// JWT ID
        #[serde(default)]
        pub jti: Option<String>,

        /// Additional claims
        #[serde(flatten)]
        pub extra: HashMap<String, serde_json::Value>,
    }

    /// Claims can be string or array
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(untagged)]
    pub enum ClaimValue {
        Single(String),
        Multiple(Vec<String>),
    }

    impl Default for ClaimValue {
        fn default() -> Self {
            ClaimValue::Multiple(vec![])
        }
    }

    impl ClaimValue {
        pub fn contains(&self, value: &str) -> bool {
            match self {
                ClaimValue::Single(s) => s == value,
                ClaimValue::Multiple(v) => v.iter().any(|s| s == value),
            }
        }
    }

    // ============================================================================
    // Validated Identity
    // ============================================================================

    /// Identity extracted from validated JWT
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct OidcIdentity {
        /// Username extracted from configured claim
        pub username: String,

        /// Subject from token
        pub subject: String,

        /// Issuer (provider) that issued the token
        pub issuer: String,

        /// Provider name that validated this token
        pub provider: String,

        /// Groups extracted from token (if configured)
        pub groups: Vec<String>,

        /// Roles extracted from token (if configured)
        pub roles: Vec<String>,

        /// Email (if present)
        pub email: Option<String>,

        /// Full name (if present)
        pub name: Option<String>,

        /// Token expiration time
        pub expires_at: u64,

        /// All claims from the token
        pub claims: HashMap<String, serde_json::Value>,
    }

    impl OidcIdentity {
        /// Check if identity has a specific group
        pub fn has_group(&self, group: &str) -> bool {
            self.groups.iter().any(|g| g == group)
        }

        /// Check if identity has a specific role
        pub fn has_role(&self, role: &str) -> bool {
            self.roles.iter().any(|r| r == role)
        }

        /// Check if identity has any of the specified groups
        pub fn has_any_group(&self, groups: &[&str]) -> bool {
            groups.iter().any(|g| self.has_group(g))
        }

        /// Check if identity has all of the specified groups
        pub fn has_all_groups(&self, groups: &[&str]) -> bool {
            groups.iter().all(|g| self.has_group(g))
        }
    }

    // ============================================================================
    // JWKS Cache
    // ============================================================================

    #[derive(Debug)]
    struct CachedJwks {
        jwks: JwkSet,
        fetched_at: Instant,
        ttl: Duration,
    }

    impl CachedJwks {
        fn is_expired(&self) -> bool {
            self.fetched_at.elapsed() > self.ttl
        }
    }

    // ============================================================================
    // OIDC Authenticator
    // ============================================================================

    /// OIDC authenticator supporting multiple providers
    pub struct OidcAuthenticator {
        /// Configured providers
        providers: HashMap<String, OidcProviderConfig>,

        /// JWKS cache per provider
        jwks_cache: RwLock<HashMap<String, CachedJwks>>,

        /// HTTP client for JWKS fetching
        http_client: reqwest::Client,
    }

    impl OidcAuthenticator {
        /// Create a new authenticator with no providers (F-117 fix: returns Result)
        pub fn new() -> OidcResult<Self> {
            let http_client = reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|e| {
                    OidcError::Configuration(format!("Failed to create OIDC HTTP client: {}", e))
                })?;

            Ok(Self {
                providers: HashMap::new(),
                jwks_cache: RwLock::new(HashMap::new()),
                http_client,
            })
        }

        /// Add a provider
        pub fn add_provider(&mut self, config: OidcProviderConfig) {
            if config.enabled {
                info!("Added OIDC provider: {} ({})", config.name, config.issuer);
                self.providers.insert(config.name.clone(), config);
            }
        }

        /// Get list of provider names
        pub fn provider_names(&self) -> Vec<&str> {
            self.providers.keys().map(|s| s.as_str()).collect()
        }

        /// Validate a JWT token and extract identity
        ///
        /// Tries each provider in order until one succeeds
        pub async fn validate_token(&self, token: &str) -> OidcResult<OidcIdentity> {
            // Decode header to get issuer hint
            let header = decode_header(token)
                .map_err(|e| OidcError::InvalidToken(format!("Invalid header: {}", e)))?;

            // Try to find provider by issuer in token (peek at claims)
            let insecure_claims = self.peek_claims(token)?;

            // Find matching provider by issuer
            let provider = self
                .providers
                .values()
                .find(|p| p.issuer == insecure_claims.iss)
                .ok_or_else(|| OidcError::ProviderNotFound(insecure_claims.iss.clone()))?;

            // Validate with the matching provider
            self.validate_with_provider(token, provider, &header.kid)
                .await
        }

        /// Validate a JWT token with a specific provider
        pub async fn validate_with_provider_name(
            &self,
            token: &str,
            provider_name: &str,
        ) -> OidcResult<OidcIdentity> {
            let provider = self
                .providers
                .get(provider_name)
                .ok_or_else(|| OidcError::ProviderNotFound(provider_name.to_string()))?;

            let header = decode_header(token)
                .map_err(|e| OidcError::InvalidToken(format!("Invalid header: {}", e)))?;

            self.validate_with_provider(token, provider, &header.kid)
                .await
        }

        /// Peek at claims without validation (for issuer discovery)
        fn peek_claims(&self, token: &str) -> OidcResult<StandardClaims> {
            let mut validation = Validation::default();
            validation.insecure_disable_signature_validation();
            validation.validate_exp = false;
            validation.validate_aud = false;

            let data: TokenData<StandardClaims> =
                decode(token, &DecodingKey::from_secret(&[]), &validation)
                    .map_err(|e| OidcError::InvalidToken(format!("Cannot parse token: {}", e)))?;

            Ok(data.claims)
        }

        /// Validate with a specific provider
        async fn validate_with_provider(
            &self,
            token: &str,
            provider: &OidcProviderConfig,
            kid: &Option<String>,
        ) -> OidcResult<OidcIdentity> {
            // Get JWKS (from cache or fetch)
            let jwks = self.get_jwks(provider).await?;

            // Find the key
            let key = if let Some(kid) = kid {
                jwks.find(kid)
                    .ok_or_else(|| OidcError::KeyNotFound(kid.clone()))?
            } else {
                // Use first key if no kid
                jwks.keys
                    .first()
                    .ok_or_else(|| OidcError::KeyNotFound("no keys in JWKS".to_string()))?
            };

            // Build decoding key
            let decoding_key = DecodingKey::from_jwk(key)
                .map_err(|e| OidcError::InvalidToken(format!("Invalid JWK: {}", e)))?;

            // Build validation
            // Parse configured algorithms (default to RS256)
            let algorithms: Vec<Algorithm> = provider
                .algorithms
                .iter()
                .filter_map(|a| match a.as_str() {
                    "RS256" => Some(Algorithm::RS256),
                    "RS384" => Some(Algorithm::RS384),
                    "RS512" => Some(Algorithm::RS512),
                    "ES256" => Some(Algorithm::ES256),
                    "ES384" => Some(Algorithm::ES384),
                    "PS256" => Some(Algorithm::PS256),
                    "PS384" => Some(Algorithm::PS384),
                    "PS512" => Some(Algorithm::PS512),
                    _ => None,
                })
                .collect();
            let primary_alg = algorithms.first().copied().unwrap_or(Algorithm::RS256);
            let mut validation = Validation::new(primary_alg);
            if algorithms.len() > 1 {
                validation.algorithms = algorithms;
            }
            validation.set_issuer(&[&provider.issuer]);
            validation.set_audience(&[&provider.audience]);
            validation.leeway = provider.clock_skew_seconds;

            // Decode and validate
            let data: TokenData<StandardClaims> = decode(token, &decoding_key, &validation)
                .map_err(|e| match e.kind() {
                    jsonwebtoken::errors::ErrorKind::ExpiredSignature => OidcError::TokenExpired,
                    jsonwebtoken::errors::ErrorKind::InvalidIssuer => OidcError::InvalidIssuer {
                        expected: provider.issuer.clone(),
                        actual: "unknown".to_string(),
                    },
                    jsonwebtoken::errors::ErrorKind::InvalidAudience => {
                        OidcError::InvalidAudience(provider.audience.clone())
                    }
                    _ => OidcError::InvalidToken(format!("{}", e)),
                })?;

            let claims = data.claims;

            // Extract username
            let username = self.extract_claim(&claims, &provider.username_claim)?;

            // Extract groups
            let groups = provider
                .groups_claim
                .as_ref()
                .and_then(|claim| self.extract_string_array(&claims, claim))
                .unwrap_or_default();

            // Extract roles
            let roles = provider
                .roles_claim
                .as_ref()
                .and_then(|claim| self.extract_string_array(&claims, claim))
                .unwrap_or_default();

            // Extract common claims
            let email = claims
                .extra
                .get("email")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let name = claims
                .extra
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            // Validate required claims
            for claim in &provider.required_claims {
                if !claims.extra.contains_key(claim) {
                    return Err(OidcError::MissingClaim(claim.clone()));
                }
            }

            debug!(
                "Validated OIDC token for user '{}' from provider '{}'",
                username, provider.name
            );

            Ok(OidcIdentity {
                username,
                subject: claims.sub,
                issuer: claims.iss,
                provider: provider.name.clone(),
                groups,
                roles,
                email,
                name,
                expires_at: claims.exp,
                claims: claims.extra,
            })
        }

        /// Get JWKS for provider (cached)
        async fn get_jwks(&self, provider: &OidcProviderConfig) -> OidcResult<JwkSet> {
            // Check cache
            {
                let cache = self.jwks_cache.read();
                if let Some(cached) = cache.get(&provider.name) {
                    if !cached.is_expired() {
                        return Ok(cached.jwks.clone());
                    }
                }
            }

            // Fetch JWKS
            let jwks_url = provider.jwks_url();
            debug!("Fetching JWKS from: {}", jwks_url);

            let response = self
                .http_client
                .get(&jwks_url)
                .send()
                .await
                .map_err(|e| OidcError::JwksFetch(format!("HTTP error: {}", e)))?;

            if !response.status().is_success() {
                return Err(OidcError::JwksFetch(format!(
                    "HTTP {} from {}",
                    response.status(),
                    jwks_url
                )));
            }

            let jwks: JwkSet = response
                .json()
                .await
                .map_err(|e| OidcError::Json(format!("Invalid JWKS: {}", e)))?;

            // Update cache
            {
                let mut cache = self.jwks_cache.write();
                cache.insert(
                    provider.name.clone(),
                    CachedJwks {
                        jwks: jwks.clone(),
                        fetched_at: Instant::now(),
                        ttl: Duration::from_secs(provider.jwks_cache_ttl_seconds),
                    },
                );
            }

            info!(
                "Fetched and cached JWKS for provider '{}' ({} keys)",
                provider.name,
                jwks.keys.len()
            );

            Ok(jwks)
        }

        /// Extract a string claim, supporting nested paths like "realm_access.roles"
        fn extract_claim(&self, claims: &StandardClaims, path: &str) -> OidcResult<String> {
            // Handle direct standard claims first
            match path {
                "sub" => return Ok(claims.sub.clone()),
                "iss" => return Ok(claims.iss.clone()),
                _ => {}
            }

            // Handle nested paths
            let parts: Vec<&str> = path.split('.').collect();
            let mut current: &serde_json::Value = claims
                .extra
                .get(parts[0])
                .ok_or_else(|| OidcError::MissingClaim(path.to_string()))?;

            for part in &parts[1..] {
                current = current
                    .get(part)
                    .ok_or_else(|| OidcError::MissingClaim(path.to_string()))?;
            }

            current
                .as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| OidcError::MissingClaim(format!("{} is not a string", path)))
        }

        /// Extract a string array from claims
        fn extract_string_array(&self, claims: &StandardClaims, path: &str) -> Option<Vec<String>> {
            let parts: Vec<&str> = path.split('.').collect();
            let mut current: Option<&serde_json::Value> = claims.extra.get(parts[0]);

            for part in &parts[1..] {
                current = current.and_then(|v| v.get(part));
            }

            current.and_then(|v| {
                v.as_array().map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
            })
        }

        /// Clear JWKS cache (force refresh on next validation)
        pub fn clear_cache(&self) {
            let mut cache = self.jwks_cache.write();
            cache.clear();
            info!("Cleared OIDC JWKS cache");
        }

        /// Clear cache for a specific provider
        pub fn clear_provider_cache(&self, provider_name: &str) {
            let mut cache = self.jwks_cache.write();
            cache.remove(provider_name);
            info!("Cleared OIDC JWKS cache for provider: {}", provider_name);
        }
    }

    impl Default for OidcAuthenticator {
        fn default() -> Self {
            Self::new().unwrap_or_else(|e| {
                tracing::error!(
                    "OIDC authenticator default creation failed: {}. Using no-op.",
                    e
                );
                Self {
                    providers: HashMap::new(),
                    jwks_cache: RwLock::new(HashMap::new()),
                    http_client: reqwest::Client::new(),
                }
            })
        }
    }

    // ============================================================================
    // OIDC SASL Mechanism
    // ============================================================================

    /// SASL mechanism name for OIDC (bearer token)
    pub const OIDC_SASL_MECHANISM: &str = "OAUTHBEARER";

    /// Parse OAUTHBEARER SASL message
    ///
    /// Format: `n,,\x01auth=Bearer <token>\x01\x01`
    pub fn parse_oauthbearer_message(data: &[u8]) -> OidcResult<String> {
        let message = std::str::from_utf8(data)
            .map_err(|_| OidcError::InvalidToken("Invalid UTF-8 in SASL message".to_string()))?;

        // Parse GS2 header and extract token
        // Format: n,,<SOH>auth=Bearer <token><SOH><SOH>
        let parts: Vec<&str> = message.split('\x01').collect();

        for part in parts {
            if let Some(auth) = part.strip_prefix("auth=Bearer ") {
                return Ok(auth.trim().to_string());
            }
        }

        Err(OidcError::InvalidToken(
            "No bearer token found in SASL message".to_string(),
        ))
    }

    /// Build OAUTHBEARER SASL response for successful auth
    pub fn build_oauthbearer_success() -> Vec<u8> {
        vec![]
    }

    /// Build OAUTHBEARER SASL error response
    pub fn build_oauthbearer_error(status: &str, scope: Option<&str>) -> Vec<u8> {
        let mut response = format!("{{\"status\":\"{}\"}}", status);
        if let Some(s) = scope {
            response = format!("{{\"status\":\"{}\",\"scope\":\"{}\"}}", status, s);
        }
        response.into_bytes()
    }

    // ============================================================================
    // Tests
    // ============================================================================

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_provider_config() {
            let config = OidcProviderConfig::new("test", "https://auth.example.com", "test-client")
                .with_groups_claim("groups")
                .with_roles_claim("realm_access.roles");

            assert_eq!(config.name, "test");
            assert_eq!(config.issuer, "https://auth.example.com");
            assert_eq!(config.audience, "test-client");
            assert_eq!(config.groups_claim, Some("groups".to_string()));
            assert_eq!(config.roles_claim, Some("realm_access.roles".to_string()));
            assert_eq!(
                config.jwks_url(),
                "https://auth.example.com/.well-known/jwks.json"
            );
        }

        #[test]
        fn test_claim_value() {
            let single = ClaimValue::Single("test".to_string());
            assert!(single.contains("test"));
            assert!(!single.contains("other"));

            let multiple = ClaimValue::Multiple(vec!["a".to_string(), "b".to_string()]);
            assert!(multiple.contains("a"));
            assert!(multiple.contains("b"));
            assert!(!multiple.contains("c"));
        }

        #[test]
        fn test_identity_methods() {
            let identity = OidcIdentity {
                username: "alice".to_string(),
                subject: "alice-uuid".to_string(),
                issuer: "https://auth.example.com".to_string(),
                provider: "test".to_string(),
                groups: vec!["admins".to_string(), "developers".to_string()],
                roles: vec!["admin".to_string()],
                email: Some("alice@example.com".to_string()),
                name: Some("Alice".to_string()),
                expires_at: 9999999999,
                claims: HashMap::new(),
            };

            assert!(identity.has_group("admins"));
            assert!(identity.has_group("developers"));
            assert!(!identity.has_group("users"));

            assert!(identity.has_role("admin"));
            assert!(!identity.has_role("user"));

            assert!(identity.has_any_group(&["admins", "users"]));
            assert!(identity.has_all_groups(&["admins", "developers"]));
            assert!(!identity.has_all_groups(&["admins", "users"]));
        }

        #[test]
        fn test_parse_oauthbearer() {
            let message = b"n,,\x01auth=Bearer eyJhbGciOiJSUzI1NiJ9.test\x01\x01";
            let token = parse_oauthbearer_message(message).unwrap();
            assert_eq!(token, "eyJhbGciOiJSUzI1NiJ9.test");
        }

        #[test]
        fn test_parse_oauthbearer_invalid() {
            let message = b"n,,\x01invalid\x01\x01";
            let result = parse_oauthbearer_message(message);
            assert!(result.is_err());
        }
    }
}

#[cfg(feature = "oidc")]
pub use oidc_impl::*;

#[cfg(not(feature = "oidc"))]
mod no_oidc {
    //! Stub module when OIDC feature is disabled

    use std::collections::HashMap;
    use thiserror::Error;

    #[derive(Error, Debug)]
    pub enum OidcError {
        #[error("OIDC not enabled. Build with 'oidc' feature.")]
        NotEnabled,
    }

    pub type OidcResult<T> = Result<T, OidcError>;

    #[derive(Debug, Clone)]
    pub struct OidcProviderConfig {
        pub name: String,
        pub issuer: String,
        pub audience: String,
    }

    #[derive(Debug, Clone)]
    pub struct OidcIdentity {
        pub username: String,
        pub subject: String,
        pub issuer: String,
        pub provider: String,
        pub groups: Vec<String>,
        pub roles: Vec<String>,
        pub email: Option<String>,
        pub name: Option<String>,
        pub expires_at: u64,
        pub claims: HashMap<String, serde_json::Value>,
    }

    pub struct OidcAuthenticator;

    impl OidcAuthenticator {
        pub fn new() -> Self {
            Self
        }

        pub async fn validate_token(&self, _token: &str) -> OidcResult<OidcIdentity> {
            Err(OidcError::NotEnabled)
        }
    }

    impl Default for OidcAuthenticator {
        fn default() -> Self {
            Self::new()
        }
    }
}

#[cfg(not(feature = "oidc"))]
pub use no_oidc::*;
