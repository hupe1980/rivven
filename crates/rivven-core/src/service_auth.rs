//! Service-to-Service Authentication for Rivven
//!
//! This module provides authentication mechanisms for service accounts,
//! specifically designed for `rivven-connect` authenticating to `rivven-server`.
//!
//! ## Supported Methods
//!
//! 1. **API Keys** - Simple, rotatable tokens for service authentication
//! 2. **mTLS** - Mutual TLS with certificate-based identity
//! 3. **OIDC Client Credentials** - OAuth2 client credentials flow
//! 4. **SASL/SCRAM** - Password-based with strong hashing
//!
//! ## Security Design
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                    SERVICE AUTHENTICATION FLOW                          │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                         │
//! │  ┌──────────────┐                      ┌──────────────────┐             │
//! │  │rivven-connect│                      │  rivven-server   │             │
//! │  └──────┬───────┘                      └────────┬─────────┘             │
//! │         │                                       │                       │
//! │         │  1. TLS Handshake (optional mTLS)     │                       │
//! │         │◄─────────────────────────────────────►│                       │
//! │         │                                       │                       │
//! │         │  2. ServiceAuth { method, credentials }                       │
//! │         │──────────────────────────────────────►│                       │
//! │         │                                       │                       │
//! │         │  3. Validate credentials              │                       │
//! │         │                                       │ ← Check API key/cert  │
//! │         │                                       │   Extract identity    │
//! │         │                                       │   Create session      │
//! │         │                                       │                       │
//! │         │  4. ServiceAuthResult { session_id, permissions }             │
//! │         │◄──────────────────────────────────────│                       │
//! │         │                                       │                       │
//! │         │  5. Authenticated requests            │                       │
//! │         │──────────────────────────────────────►│                       │
//! │         │                                       │                       │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## API Key Format
//!
//! API keys use a structured format for security and usability:
//!
//! ```text
//! rvn.<version>.<key_id>.<secret>
//! 
//! Example: rvn.v1.a1b2c3d4e5f6.MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI
//!          ^^^ ^^ ^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//!           │   │       │            │
//!           │   │       │            └─ 32-byte random secret (base64)
//!           │   │       └────────────── Key identifier (hex, for rotation)
//!           │   └────────────────────── Version (for format changes)
//!           └────────────────────────── Prefix (identifies Rivven keys)
//! ```

use parking_lot::RwLock;
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;
use tracing::{debug, info, warn};

// ============================================================================
// Error Types
// ============================================================================

#[derive(Error, Debug)]
pub enum ServiceAuthError {
    #[error("Invalid API key format")]
    InvalidKeyFormat,

    #[error("API key not found: {0}")]
    KeyNotFound(String),

    #[error("API key expired")]
    KeyExpired,

    #[error("API key revoked")]
    KeyRevoked,

    #[error("Invalid credentials")]
    InvalidCredentials,

    #[error("Certificate error: {0}")]
    CertificateError(String),

    #[error("Service account not found: {0}")]
    ServiceAccountNotFound(String),

    #[error("Service account disabled: {0}")]
    ServiceAccountDisabled(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Rate limited: too many authentication attempts")]
    RateLimited,

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type ServiceAuthResult<T> = Result<T, ServiceAuthError>;

// ============================================================================
// API Key
// ============================================================================

/// API Key for service authentication
/// 
/// # Security Note
/// 
/// This struct implements a custom Debug that redacts the secret_hash field
/// to prevent accidental leakage to logs.
#[derive(Clone, Serialize, Deserialize)]
pub struct ApiKey {
    /// Key identifier (public, used for lookup)
    pub key_id: String,

    /// Hashed secret (never store plaintext)
    pub secret_hash: String,

    /// Service account this key belongs to
    pub service_account: String,

    /// Human-readable description
    pub description: Option<String>,

    /// When the key was created
    pub created_at: SystemTime,

    /// When the key expires (None = never)
    pub expires_at: Option<SystemTime>,

    /// When the key was last used
    pub last_used_at: Option<SystemTime>,

    /// Whether the key is revoked
    pub revoked: bool,

    /// IP allowlist (empty = all IPs allowed)
    pub allowed_ips: Vec<String>,

    /// Permissions granted to this key
    pub permissions: Vec<String>,
}

impl std::fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiKey")
            .field("key_id", &self.key_id)
            .field("secret_hash", &"[REDACTED]")
            .field("service_account", &self.service_account)
            .field("description", &self.description)
            .field("created_at", &self.created_at)
            .field("expires_at", &self.expires_at)
            .field("last_used_at", &self.last_used_at)
            .field("revoked", &self.revoked)
            .field("allowed_ips", &self.allowed_ips)
            .field("permissions", &self.permissions)
            .finish()
    }
}

impl ApiKey {
    /// Generate a new API key
    pub fn generate(
        service_account: &str,
        description: Option<&str>,
        expires_in: Option<Duration>,
        permissions: Vec<String>,
    ) -> ServiceAuthResult<(Self, String)> {
        let rng = SystemRandom::new();

        // Generate key ID (8 bytes = 16 hex chars)
        let mut key_id_bytes = [0u8; 8];
        rng.fill(&mut key_id_bytes)
            .map_err(|_| ServiceAuthError::Internal("RNG failed".into()))?;
        let key_id = hex::encode(key_id_bytes);

        // Generate secret (32 bytes = high entropy)
        let mut secret_bytes = [0u8; 32];
        rng.fill(&mut secret_bytes)
            .map_err(|_| ServiceAuthError::Internal("RNG failed".into()))?;
        let secret = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD_NO_PAD,
            secret_bytes,
        );

        // Hash the secret for storage
        let secret_hash = Self::hash_secret(&secret);

        // Build the full key string (using . as separator to avoid conflicts with base64)
        let full_key = format!("rvn.v1.{}.{}", key_id, secret);

        let now = SystemTime::now();
        let expires_at = expires_in.map(|d| now + d);

        let api_key = Self {
            key_id,
            secret_hash,
            service_account: service_account.to_string(),
            description: description.map(|s| s.to_string()),
            created_at: now,
            expires_at,
            last_used_at: None,
            revoked: false,
            allowed_ips: vec![],
            permissions,
        };

        Ok((api_key, full_key))
    }

    /// Hash a secret for storage
    fn hash_secret(secret: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(secret.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Parse an API key string and extract components
    pub fn parse_key(key: &str) -> ServiceAuthResult<(String, String)> {
        let parts: Vec<&str> = key.split('.').collect();
        if parts.len() != 4 || parts[0] != "rvn" || parts[1] != "v1" {
            return Err(ServiceAuthError::InvalidKeyFormat);
        }

        let key_id = parts[2].to_string();
        let secret = parts[3].to_string();

        Ok((key_id, secret))
    }

    /// Verify a secret against this key
    pub fn verify_secret(&self, secret: &str) -> bool {
        let provided_hash = Self::hash_secret(secret);
        // Constant-time comparison using subtle crate pattern
        // Compare byte-by-byte with constant time to prevent timing attacks
        if provided_hash.len() != self.secret_hash.len() {
            return false;
        }
        
        let mut result = 0u8;
        for (a, b) in provided_hash.as_bytes().iter().zip(self.secret_hash.as_bytes()) {
            result |= a ^ b;
        }
        result == 0
    }

    /// Check if key is valid (not expired, not revoked)
    pub fn is_valid(&self) -> bool {
        if self.revoked {
            return false;
        }

        if let Some(expires_at) = self.expires_at {
            if SystemTime::now() > expires_at {
                return false;
            }
        }

        true
    }

    /// Check if IP is allowed
    pub fn is_ip_allowed(&self, ip: &str) -> bool {
        if self.allowed_ips.is_empty() {
            return true;
        }

        self.allowed_ips.iter().any(|allowed| {
            // Support CIDR notation
            if allowed.contains('/') {
                Self::ip_in_cidr(ip, allowed)
            } else {
                allowed == ip
            }
        })
    }

    fn ip_in_cidr(ip: &str, cidr: &str) -> bool {
        // Simple CIDR check - in production, use a proper IP library
        let parts: Vec<&str> = cidr.split('/').collect();
        if parts.len() != 2 {
            return false;
        }

        let network = parts[0];
        let prefix_len: u32 = parts[1].parse().unwrap_or(32);

        // Parse IPs
        let ip_parts: Vec<u8> = ip
            .split('.')
            .filter_map(|p| p.parse().ok())
            .collect();
        let net_parts: Vec<u8> = network
            .split('.')
            .filter_map(|p| p.parse().ok())
            .collect();

        if ip_parts.len() != 4 || net_parts.len() != 4 {
            return false;
        }

        let ip_num = u32::from_be_bytes([ip_parts[0], ip_parts[1], ip_parts[2], ip_parts[3]]);
        let net_num = u32::from_be_bytes([net_parts[0], net_parts[1], net_parts[2], net_parts[3]]);

        let mask = if prefix_len == 0 {
            0
        } else {
            !0u32 << (32 - prefix_len)
        };

        (ip_num & mask) == (net_num & mask)
    }
}

// ============================================================================
// Service Account
// ============================================================================

/// A service account represents a non-human identity (like rivven-connect)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceAccount {
    /// Unique identifier
    pub name: String,

    /// Human-readable description
    pub description: Option<String>,

    /// Whether the account is enabled
    pub enabled: bool,

    /// When the account was created
    pub created_at: SystemTime,

    /// Roles assigned to this account
    pub roles: Vec<String>,

    /// Additional metadata
    pub metadata: HashMap<String, String>,

    /// mTLS certificate subject (if using cert auth)
    pub certificate_subject: Option<String>,

    /// OIDC client ID (if using OIDC client credentials)
    pub oidc_client_id: Option<String>,
}

impl ServiceAccount {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            enabled: true,
            created_at: SystemTime::now(),
            roles: vec![],
            metadata: HashMap::new(),
            certificate_subject: None,
            oidc_client_id: None,
        }
    }

    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    pub fn with_roles(mut self, roles: Vec<String>) -> Self {
        self.roles = roles;
        self
    }

    pub fn with_certificate_subject(mut self, subject: impl Into<String>) -> Self {
        self.certificate_subject = Some(subject.into());
        self
    }

    pub fn with_oidc_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.oidc_client_id = Some(client_id.into());
        self
    }
}

// ============================================================================
// Service Session
// ============================================================================

/// A session for an authenticated service
#[derive(Debug, Clone)]
pub struct ServiceSession {
    /// Unique session identifier
    pub id: String,

    /// Service account name
    pub service_account: String,

    /// Authentication method used
    pub auth_method: AuthMethod,

    /// When the session expires (monotonic time for expiration)
    expires_at: Instant,

    /// When the session was created (wall clock for logging)
    pub created_timestamp: SystemTime,

    /// Permissions granted in this session
    pub permissions: Vec<String>,

    /// Client IP that created this session
    pub client_ip: String,

    /// API key ID (if authenticated via API key)
    pub api_key_id: Option<String>,
}

impl ServiceSession {
    pub fn is_expired(&self) -> bool {
        Instant::now() > self.expires_at
    }

    /// Time until expiration
    pub fn time_until_expiration(&self) -> Duration {
        self.expires_at.saturating_duration_since(Instant::now())
    }

    /// Seconds until expiration
    pub fn expires_in_secs(&self) -> u64 {
        self.time_until_expiration().as_secs()
    }
}

/// Authentication method used
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthMethod {
    ApiKey,
    MutualTls,
    OidcClientCredentials,
    SaslScram,
}

// ============================================================================
// Rate Limiter for Auth Attempts
// ============================================================================

struct AuthRateLimiter {
    attempts: HashMap<String, Vec<Instant>>,
    max_attempts: usize,
    window: Duration,
}

impl AuthRateLimiter {
    fn new(max_attempts: usize, window: Duration) -> Self {
        Self {
            attempts: HashMap::new(),
            max_attempts,
            window,
        }
    }

    fn check_and_record(&mut self, key: &str) -> bool {
        let now = Instant::now();
        let cutoff = now - self.window;

        let attempts = self.attempts.entry(key.to_string()).or_default();

        // Remove old attempts
        attempts.retain(|&t| t > cutoff);

        // Check limit
        if attempts.len() >= self.max_attempts {
            return false;
        }

        // Record this attempt
        attempts.push(now);
        true
    }

    fn clear(&mut self, key: &str) {
        self.attempts.remove(key);
    }
}

// ============================================================================
// Service Auth Manager
// ============================================================================

/// Manages service authentication
pub struct ServiceAuthManager {
    /// API keys by key_id
    api_keys: RwLock<HashMap<String, ApiKey>>,

    /// Service accounts by name
    service_accounts: RwLock<HashMap<String, ServiceAccount>>,

    /// Active sessions
    sessions: RwLock<HashMap<String, ServiceSession>>,

    /// Rate limiter for auth attempts
    rate_limiter: RwLock<AuthRateLimiter>,

    /// Session duration
    session_duration: Duration,
}

impl ServiceAuthManager {
    pub fn new() -> Self {
        Self {
            api_keys: RwLock::new(HashMap::new()),
            service_accounts: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            rate_limiter: RwLock::new(AuthRateLimiter::new(10, Duration::from_secs(60))),
            session_duration: Duration::from_secs(3600), // 1 hour default
        }
    }

    pub fn with_session_duration(mut self, duration: Duration) -> Self {
        self.session_duration = duration;
        self
    }

    // ========================================================================
    // Service Account Management
    // ========================================================================

    /// Create a new service account
    pub fn create_service_account(&self, account: ServiceAccount) -> ServiceAuthResult<()> {
        let mut accounts = self.service_accounts.write();

        if accounts.contains_key(&account.name) {
            return Err(ServiceAuthError::Internal(format!(
                "Service account '{}' already exists",
                account.name
            )));
        }

        info!("Created service account: {}", account.name);
        accounts.insert(account.name.clone(), account);
        Ok(())
    }

    /// Get a service account
    pub fn get_service_account(&self, name: &str) -> Option<ServiceAccount> {
        self.service_accounts.read().get(name).cloned()
    }

    /// Disable a service account (revokes all sessions)
    pub fn disable_service_account(&self, name: &str) -> ServiceAuthResult<()> {
        let mut accounts = self.service_accounts.write();

        let account = accounts
            .get_mut(name)
            .ok_or_else(|| ServiceAuthError::ServiceAccountNotFound(name.to_string()))?;

        account.enabled = false;

        // Revoke all sessions for this account
        let mut sessions = self.sessions.write();
        sessions.retain(|_, s| s.service_account != name);

        info!("Disabled service account: {}", name);
        Ok(())
    }

    // ========================================================================
    // API Key Management
    // ========================================================================

    /// Create a new API key for a service account
    pub fn create_api_key(
        &self,
        service_account: &str,
        description: Option<&str>,
        expires_in: Option<Duration>,
        permissions: Vec<String>,
    ) -> ServiceAuthResult<String> {
        // Verify service account exists
        {
            let accounts = self.service_accounts.read();
            if !accounts.contains_key(service_account) {
                return Err(ServiceAuthError::ServiceAccountNotFound(
                    service_account.to_string(),
                ));
            }
        }

        let (api_key, full_key) =
            ApiKey::generate(service_account, description, expires_in, permissions)?;

        let key_id = api_key.key_id.clone();
        self.api_keys.write().insert(key_id.clone(), api_key);

        info!(
            "Created API key '{}' for service account '{}'",
            key_id, service_account
        );

        Ok(full_key)
    }

    /// Revoke an API key
    pub fn revoke_api_key(&self, key_id: &str) -> ServiceAuthResult<()> {
        let mut keys = self.api_keys.write();

        let key = keys
            .get_mut(key_id)
            .ok_or_else(|| ServiceAuthError::KeyNotFound(key_id.to_string()))?;

        key.revoked = true;

        // Revoke all sessions using this key
        let mut sessions = self.sessions.write();
        sessions.retain(|_, s| s.api_key_id.as_deref() != Some(key_id));

        info!("Revoked API key: {}", key_id);
        Ok(())
    }

    /// List API keys for a service account (without secrets)
    pub fn list_api_keys(&self, service_account: &str) -> Vec<ApiKey> {
        self.api_keys
            .read()
            .values()
            .filter(|k| k.service_account == service_account)
            .cloned()
            .collect()
    }

    // ========================================================================
    // Authentication
    // ========================================================================

    /// Authenticate using an API key
    pub fn authenticate_api_key(
        &self,
        key_string: &str,
        client_ip: &str,
    ) -> ServiceAuthResult<ServiceSession> {
        // Rate limit check
        {
            let mut limiter = self.rate_limiter.write();
            if !limiter.check_and_record(client_ip) {
                warn!("Rate limited auth attempt from {}", client_ip);
                return Err(ServiceAuthError::RateLimited);
            }
        }

        // Parse the key
        let (key_id, secret) = ApiKey::parse_key(key_string)?;

        // Look up the key
        let mut keys = self.api_keys.write();
        let api_key = keys
            .get_mut(&key_id)
            .ok_or_else(|| ServiceAuthError::KeyNotFound(key_id.clone()))?;

        // Verify the secret
        if !api_key.verify_secret(&secret) {
            warn!("Invalid API key secret for key_id={}", key_id);
            return Err(ServiceAuthError::InvalidCredentials);
        }

        // Check validity
        if !api_key.is_valid() {
            if api_key.revoked {
                return Err(ServiceAuthError::KeyRevoked);
            } else {
                return Err(ServiceAuthError::KeyExpired);
            }
        }

        // Check IP allowlist
        if !api_key.is_ip_allowed(client_ip) {
            warn!(
                "API key {} used from non-allowed IP {}",
                key_id, client_ip
            );
            return Err(ServiceAuthError::PermissionDenied(
                "IP not in allowlist".to_string(),
            ));
        }

        // Check service account is enabled
        {
            let accounts = self.service_accounts.read();
            let account = accounts
                .get(&api_key.service_account)
                .ok_or_else(|| {
                    ServiceAuthError::ServiceAccountNotFound(api_key.service_account.clone())
                })?;

            if !account.enabled {
                return Err(ServiceAuthError::ServiceAccountDisabled(
                    api_key.service_account.clone(),
                ));
            }
        }

        // Update last used
        api_key.last_used_at = Some(SystemTime::now());

        // Create session
        let session = self.create_session(
            &api_key.service_account,
            AuthMethod::ApiKey,
            client_ip,
            api_key.permissions.clone(),
            Some(key_id.clone()),
        );

        // Clear rate limit on success
        self.rate_limiter.write().clear(client_ip);

        info!(
            "Authenticated service '{}' via API key '{}' from {}",
            api_key.service_account, key_id, client_ip
        );

        Ok(session)
    }

    /// Authenticate using mTLS certificate
    pub fn authenticate_certificate(
        &self,
        cert_subject: &str,
        client_ip: &str,
    ) -> ServiceAuthResult<ServiceSession> {
        // Find service account by certificate subject
        let accounts = self.service_accounts.read();
        let account = accounts
            .values()
            .find(|a| a.certificate_subject.as_deref() == Some(cert_subject))
            .ok_or_else(|| {
                ServiceAuthError::CertificateError(format!(
                    "No service account for certificate: {}",
                    cert_subject
                ))
            })?;

        if !account.enabled {
            return Err(ServiceAuthError::ServiceAccountDisabled(account.name.clone()));
        }

        // Create session with roles from account
        let permissions = account.roles.clone();
        let session = self.create_session(
            &account.name,
            AuthMethod::MutualTls,
            client_ip,
            permissions,
            None,
        );

        info!(
            "Authenticated service '{}' via mTLS certificate from {}",
            account.name, client_ip
        );

        Ok(session)
    }

    /// Create a new session
    fn create_session(
        &self,
        service_account: &str,
        auth_method: AuthMethod,
        client_ip: &str,
        permissions: Vec<String>,
        api_key_id: Option<String>,
    ) -> ServiceSession {
        let rng = SystemRandom::new();
        let mut session_id_bytes = [0u8; 16];
        rng.fill(&mut session_id_bytes).expect("RNG failed");
        let session_id = hex::encode(session_id_bytes);

        let now = Instant::now();
        let session = ServiceSession {
            id: session_id.clone(),
            service_account: service_account.to_string(),
            auth_method,
            expires_at: now + self.session_duration,
            created_timestamp: SystemTime::now(),
            permissions,
            client_ip: client_ip.to_string(),
            api_key_id,
        };

        self.sessions.write().insert(session_id, session.clone());
        session
    }

    /// Validate a session
    pub fn validate_session(&self, session_id: &str) -> Option<ServiceSession> {
        let sessions = self.sessions.read();
        let session = sessions.get(session_id)?;

        if session.is_expired() {
            return None;
        }

        // Verify service account still enabled
        let accounts = self.service_accounts.read();
        let account = accounts.get(&session.service_account)?;
        if !account.enabled {
            return None;
        }

        Some(session.clone())
    }

    /// Invalidate a session
    pub fn invalidate_session(&self, session_id: &str) {
        self.sessions.write().remove(session_id);
    }

    /// Cleanup expired sessions
    pub fn cleanup_expired_sessions(&self) {
        let mut sessions = self.sessions.write();
        let before = sessions.len();
        sessions.retain(|_, s| !s.is_expired());
        let removed = before - sessions.len();
        if removed > 0 {
            debug!("Cleaned up {} expired service sessions", removed);
        }
    }
}

impl Default for ServiceAuthManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Authentication Request/Response Protocol
// ============================================================================

/// Request to authenticate a service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServiceAuthRequest {
    /// Authenticate with API key
    ApiKey { key: String },

    /// Authenticate with mTLS (server extracts cert info)
    MutualTls { certificate_subject: String },

    /// Authenticate with OIDC client credentials
    OidcClientCredentials {
        client_id: String,
        client_secret: String,
    },
}

/// Response to authentication request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServiceAuthResponse {
    /// Authentication successful
    Success {
        session_id: String,
        expires_in_secs: u64,
        permissions: Vec<String>,
    },

    /// Authentication failed
    Failure { error: String },
}

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for service authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceAuthConfig {
    /// Enable API key authentication
    #[serde(default = "default_true")]
    pub api_key_enabled: bool,

    /// Enable mTLS authentication
    #[serde(default)]
    pub mtls_enabled: bool,

    /// Enable OIDC client credentials
    #[serde(default)]
    pub oidc_enabled: bool,

    /// Session duration in seconds
    #[serde(default = "default_session_duration")]
    pub session_duration_secs: u64,

    /// Max auth attempts per IP per minute
    #[serde(default = "default_max_attempts")]
    pub max_auth_attempts: usize,

    /// Pre-configured service accounts
    #[serde(default)]
    pub service_accounts: Vec<ServiceAccountConfig>,
}

fn default_true() -> bool {
    true
}

fn default_session_duration() -> u64 {
    3600
}

fn default_max_attempts() -> usize {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceAccountConfig {
    pub name: String,
    pub description: Option<String>,
    pub roles: Vec<String>,
    pub certificate_subject: Option<String>,
    pub oidc_client_id: Option<String>,
}

impl Default for ServiceAuthConfig {
    fn default() -> Self {
        Self {
            api_key_enabled: true,
            mtls_enabled: false,
            oidc_enabled: false,
            session_duration_secs: 3600,
            max_auth_attempts: 10,
            service_accounts: vec![],
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_key_generation() {
        let (api_key, full_key) =
            ApiKey::generate("test-service", Some("Test key"), None, vec![]).unwrap();

        assert!(!api_key.revoked);
        assert!(api_key.is_valid());
        assert!(full_key.starts_with("rvn.v1."));

        // Parse and verify
        let (key_id, secret) = ApiKey::parse_key(&full_key).unwrap();
        assert_eq!(key_id, api_key.key_id);
        assert!(api_key.verify_secret(&secret));
    }

    #[test]
    fn test_api_key_expiration() {
        let (mut api_key, _) = ApiKey::generate(
            "test-service",
            None,
            Some(Duration::from_secs(0)), // Expires immediately
            vec![],
        )
        .unwrap();

        // Wait a bit
        std::thread::sleep(Duration::from_millis(10));
        assert!(!api_key.is_valid());

        // Test revocation
        api_key.revoked = true;
        assert!(!api_key.is_valid());
    }

    #[test]
    fn test_ip_allowlist() {
        let mut api_key =
            ApiKey::generate("test-service", None, None, vec![])
                .unwrap()
                .0;

        // Empty allowlist = all allowed
        assert!(api_key.is_ip_allowed("192.168.1.1"));

        // With allowlist
        api_key.allowed_ips = vec!["192.168.1.0/24".to_string()];
        assert!(api_key.is_ip_allowed("192.168.1.100"));
        assert!(!api_key.is_ip_allowed("10.0.0.1"));
    }

    #[test]
    fn test_service_auth_manager() {
        let manager = ServiceAuthManager::new();

        // Create service account
        let account = ServiceAccount::new("connector-postgres")
            .with_description("PostgreSQL CDC connector")
            .with_roles(vec!["connector".to_string()]);

        manager.create_service_account(account).unwrap();

        // Create API key
        let full_key = manager
            .create_api_key(
                "connector-postgres",
                Some("Production key"),
                None,
                vec!["topic:read".to_string(), "topic:write".to_string()],
            )
            .unwrap();

        // Authenticate
        let session = manager
            .authenticate_api_key(&full_key, "127.0.0.1")
            .unwrap();

        assert_eq!(session.service_account, "connector-postgres");
        assert_eq!(session.auth_method, AuthMethod::ApiKey);
        assert!(!session.is_expired());

        // Validate session
        let validated = manager.validate_session(&session.id).unwrap();
        assert_eq!(validated.id, session.id);
    }

    #[test]
    fn test_invalid_api_key() {
        let manager = ServiceAuthManager::new();

        // Create service account
        manager
            .create_service_account(ServiceAccount::new("test"))
            .unwrap();

        // Try to authenticate with invalid key (correctly formatted but non-existent)
        let result = manager.authenticate_api_key("rvn.v1.invalid1.secretsecretsecretsecretsecretsecr", "127.0.0.1");
        assert!(matches!(result, Err(ServiceAuthError::KeyNotFound(_))));
    }

    #[test]
    fn test_rate_limiting() {
        let manager = ServiceAuthManager::new();

        // Try many invalid auth attempts
        for _ in 0..15 {
            let _ = manager.authenticate_api_key("rvn.v1.invalid1.secretsecretsecretsecretsecretsecr", "1.2.3.4");
        }

        // Should be rate limited
        let result = manager.authenticate_api_key("rvn.v1.invalid1.secretsecretsecretsecretsecretsecr", "1.2.3.4");
        assert!(matches!(result, Err(ServiceAuthError::RateLimited)));
    }

    #[test]
    fn test_service_account_disable() {
        let manager = ServiceAuthManager::new();

        // Create and authenticate
        manager
            .create_service_account(ServiceAccount::new("test-service"))
            .unwrap();

        let key = manager
            .create_api_key("test-service", None, None, vec![])
            .unwrap();

        let session = manager.authenticate_api_key(&key, "127.0.0.1").unwrap();

        // Disable account
        manager.disable_service_account("test-service").unwrap();

        // Session should be invalid
        assert!(manager.validate_session(&session.id).is_none());

        // New auth should fail
        let result = manager.authenticate_api_key(&key, "127.0.0.1");
        assert!(matches!(result, Err(ServiceAuthError::ServiceAccountDisabled(_))));
    }

    #[test]
    fn test_certificate_auth() {
        let manager = ServiceAuthManager::new();

        // Create service account with certificate
        let account = ServiceAccount::new("connector-orders")
            .with_certificate_subject("CN=connector-orders,O=Rivven")
            .with_roles(vec!["connector".to_string()]);

        manager.create_service_account(account).unwrap();

        // Authenticate with certificate
        let session = manager
            .authenticate_certificate("CN=connector-orders,O=Rivven", "127.0.0.1")
            .unwrap();

        assert_eq!(session.service_account, "connector-orders");
        assert_eq!(session.auth_method, AuthMethod::MutualTls);
    }
    
    #[test]
    fn test_api_key_debug_redacts_secret_hash() {
        let (api_key, _) = ApiKey::generate(
            "test-service",
            Some("Test key"),
            None,
            vec!["read".to_string()],
        ).unwrap();
        
        let debug_output = format!("{:?}", api_key);
        
        // Should contain REDACTED for secret_hash
        assert!(debug_output.contains("[REDACTED]"), 
            "Debug output should contain [REDACTED]: {}", debug_output);
        
        // Should NOT contain the actual hash (which is a base64-like string)
        assert!(!debug_output.contains(&api_key.secret_hash),
            "Debug output should not contain the secret hash");
        
        // Should still show non-sensitive fields
        assert!(debug_output.contains("key_id"),
            "Debug output should show key_id field");
        assert!(debug_output.contains("test-service"),
            "Debug output should show service_account");
    }
}
