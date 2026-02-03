//! Authentication and Authorization (RBAC/ACL) for Rivven
//!
//! This module provides production-grade security for Rivven including:
//! - SASL/PLAIN authentication (compatible with Kafka clients)
//! - SCRAM-SHA-256 authentication (more secure, salted)
//! - Role-Based Access Control (RBAC)
//! - Topic/Schema-level Access Control Lists (ACLs)
//! - Principal management (users, service accounts)
//!
//! ## Security Model
//!
//! Rivven uses a principal-based security model:
//! - **Principal**: An authenticated identity (user, service account)
//! - **Role**: A set of permissions (admin, producer, consumer)
//! - **ACL**: Fine-grained access rules for specific resources
//!
//! ## Threat Model
//!
//! This implementation defends against:
//! - Credential stuffing (rate limiting on auth failures)
//! - Timing attacks (constant-time password comparison)
//! - Replay attacks (nonce-based challenge-response)
//! - Privilege escalation (strict role hierarchy)
//! - Resource enumeration (deny by default)

use parking_lot::RwLock;
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::{debug, warn};

// ============================================================================
// Error Types
// ============================================================================

#[derive(Error, Debug, Clone)]
pub enum AuthError {
    #[error("Authentication failed")]
    AuthenticationFailed,

    #[error("Invalid credentials")]
    InvalidCredentials,

    #[error("Principal not found: {0}")]
    PrincipalNotFound(String),

    #[error("Principal already exists: {0}")]
    PrincipalAlreadyExists(String),

    #[error("Access denied: {0}")]
    AccessDenied(String),

    #[error("Permission denied: {principal} lacks {permission} on {resource}")]
    PermissionDenied {
        principal: String,
        permission: String,
        resource: String,
    },

    #[error("Role not found: {0}")]
    RoleNotFound(String),

    #[error("Invalid token")]
    InvalidToken,

    #[error("Token expired")]
    TokenExpired,

    #[error("Rate limited: too many authentication failures")]
    RateLimited,

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type AuthResult<T> = std::result::Result<T, AuthError>;

// ============================================================================
// Resource Types for ACLs
// ============================================================================

/// Types of resources that can have ACLs
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceType {
    /// All cluster operations
    Cluster,
    /// Specific topic
    Topic(String),
    /// Pattern-matched topics (e.g., "orders-*")
    TopicPattern(String),
    /// Consumer group
    ConsumerGroup(String),
    /// Schema subject
    Schema(String),
    /// Transactional ID
    TransactionalId(String),
}

impl ResourceType {
    /// Check if this resource matches another (for pattern matching)
    pub fn matches(&self, other: &ResourceType) -> bool {
        match (self, other) {
            // Exact match
            (a, b) if a == b => true,

            // Topic pattern matching
            (ResourceType::TopicPattern(pattern), ResourceType::Topic(name)) => {
                Self::glob_match(pattern, name)
            }
            (ResourceType::Topic(name), ResourceType::TopicPattern(pattern)) => {
                Self::glob_match(pattern, name)
            }

            _ => false,
        }
    }

    /// Simple glob matching for patterns like "orders-*"
    fn glob_match(pattern: &str, text: &str) -> bool {
        if pattern == "*" {
            return true;
        }

        if let Some(prefix) = pattern.strip_suffix('*') {
            return text.starts_with(prefix);
        }

        if let Some(suffix) = pattern.strip_prefix('*') {
            return text.ends_with(suffix);
        }

        // Check for middle wildcard (e.g., "pre*suf")
        if let Some(idx) = pattern.find('*') {
            let prefix = &pattern[..idx];
            let suffix = &pattern[idx + 1..];
            return text.starts_with(prefix)
                && text.ends_with(suffix)
                && text.len() >= prefix.len() + suffix.len();
        }

        pattern == text
    }
}

// ============================================================================
// Permissions
// ============================================================================

/// Operations that can be performed on resources
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Permission {
    // Topic operations
    Read,     // Consume from topic
    Write,    // Produce to topic
    Create,   // Create topic
    Delete,   // Delete topic
    Alter,    // Modify topic config
    Describe, // View topic metadata

    // Consumer group operations
    GroupRead,   // Read group state
    GroupDelete, // Delete consumer group

    // Cluster operations
    ClusterAction,   // Cluster-wide actions (rebalance, etc.)
    IdempotentWrite, // Idempotent producer

    // Admin operations
    AlterConfigs,    // Modify broker configs
    DescribeConfigs, // View broker configs

    // Full access
    All, // All permissions (super admin)
}

impl Permission {
    /// Check if this permission implies another permission
    /// A permission implies itself + any subordinate permissions
    pub fn implies(&self, other: &Permission) -> bool {
        // Same permission always implies itself
        if self == other {
            return true;
        }

        match self {
            Permission::All => true, // All implies everything
            // These permissions imply Describe
            Permission::Alter | Permission::Write | Permission::Read => {
                matches!(other, Permission::Describe)
            }
            _ => false,
        }
    }
}

// ============================================================================
// Principal Types
// ============================================================================

/// Type of principal (for audit logging and quotas)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PrincipalType {
    User,
    ServiceAccount,
    Anonymous,
}

/// A security principal (identity)
///
/// # Security Note
///
/// This struct implements a custom Debug that redacts the password_hash field
/// to prevent accidental leakage to logs.
#[derive(Clone, Serialize, Deserialize)]
pub struct Principal {
    /// Principal name (unique identifier)
    pub name: String,

    /// Type of principal
    pub principal_type: PrincipalType,

    /// Hashed password (SCRAM-SHA-256 format)
    pub password_hash: PasswordHash,

    /// Roles assigned to this principal
    pub roles: HashSet<String>,

    /// Whether the principal is enabled
    pub enabled: bool,

    /// Optional metadata (tags, labels)
    pub metadata: HashMap<String, String>,

    /// Creation timestamp
    pub created_at: u64,
}

impl std::fmt::Debug for Principal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Principal")
            .field("name", &self.name)
            .field("principal_type", &self.principal_type)
            .field("password_hash", &"[REDACTED]")
            .field("roles", &self.roles)
            .field("enabled", &self.enabled)
            .field("metadata", &self.metadata)
            .field("created_at", &self.created_at)
            .finish()
    }
}

/// SCRAM-SHA-256 password hash with salt and iterations
///
/// # Security Note
///
/// This struct implements a custom Debug that redacts sensitive key material
/// to prevent accidental leakage to logs.
#[derive(Clone, Serialize, Deserialize)]
pub struct PasswordHash {
    /// Salt (32 bytes, base64 encoded for storage)
    pub salt: Vec<u8>,
    /// Number of iterations
    pub iterations: u32,
    /// Server key
    pub server_key: Vec<u8>,
    /// Stored key
    pub stored_key: Vec<u8>,
}

impl std::fmt::Debug for PasswordHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PasswordHash")
            .field("salt", &"[REDACTED]")
            .field("iterations", &self.iterations)
            .field("server_key", &"[REDACTED]")
            .field("stored_key", &"[REDACTED]")
            .finish()
    }
}

impl PasswordHash {
    /// Create a new password hash from plaintext
    pub fn new(password: &str) -> Self {
        let rng = SystemRandom::new();
        let mut salt = vec![0u8; 32];
        rng.fill(&mut salt).expect("Failed to generate salt");

        Self::with_salt(password, &salt, 4096)
    }

    /// Create a password hash with a specific salt (for testing/migration)
    pub fn with_salt(password: &str, salt: &[u8], iterations: u32) -> Self {
        // PBKDF2-HMAC-SHA256 derivation
        let salted_password = Self::pbkdf2_sha256(password.as_bytes(), salt, iterations);

        // Derive client and server keys
        let client_key = Self::hmac_sha256(&salted_password, b"Client Key");
        let server_key = Self::hmac_sha256(&salted_password, b"Server Key");

        // Stored key = H(client_key)
        let stored_key = Sha256::digest(&client_key).to_vec();

        PasswordHash {
            salt: salt.to_vec(),
            iterations,
            server_key,
            stored_key,
        }
    }

    /// Verify a password against this hash (constant-time comparison)
    pub fn verify(&self, password: &str) -> bool {
        let salted_password = Self::pbkdf2_sha256(password.as_bytes(), &self.salt, self.iterations);
        let client_key = Self::hmac_sha256(&salted_password, b"Client Key");
        let stored_key = Sha256::digest(&client_key);

        // Constant-time comparison to prevent timing attacks
        Self::constant_time_compare(&stored_key, &self.stored_key)
    }

    /// Constant-time comparison to prevent timing attacks
    pub fn constant_time_compare(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }

        // XOR all bytes and accumulate - timing is constant regardless of where mismatch occurs
        let mut result = 0u8;
        for (x, y) in a.iter().zip(b.iter()) {
            result |= x ^ y;
        }
        result == 0
    }

    /// PBKDF2-HMAC-SHA256 key derivation
    fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<Sha256>;

        let mut result = vec![0u8; 32];

        // U1 = PRF(Password, Salt || INT(1))
        let mut mac = HmacSha256::new_from_slice(password).expect("HMAC accepts any key length");
        mac.update(salt);
        mac.update(&1u32.to_be_bytes());
        let mut u = mac.finalize().into_bytes();
        result.copy_from_slice(&u);

        // Ui = PRF(Password, Ui-1)
        for _ in 1..iterations {
            let mut mac =
                HmacSha256::new_from_slice(password).expect("HMAC accepts any key length");
            mac.update(&u);
            u = mac.finalize().into_bytes();

            for (r, ui) in result.iter_mut().zip(u.iter()) {
                *r ^= ui;
            }
        }

        result
    }

    /// HMAC-SHA256
    pub fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<Sha256>;

        let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }
}

// ============================================================================
// Roles
// ============================================================================

/// A role with a set of permissions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    /// Role name
    pub name: String,

    /// Description
    pub description: String,

    /// Permissions granted by this role
    pub permissions: HashSet<(ResourceType, Permission)>,

    /// Whether this is a built-in role
    pub builtin: bool,
}

impl Role {
    /// Create a built-in admin role
    pub fn admin() -> Self {
        let mut permissions = HashSet::new();
        permissions.insert((ResourceType::Cluster, Permission::All));

        Role {
            name: "admin".to_string(),
            description: "Full administrative access to all resources".to_string(),
            permissions,
            builtin: true,
        }
    }

    /// Create a built-in producer role
    pub fn producer() -> Self {
        let mut permissions = HashSet::new();
        permissions.insert((
            ResourceType::TopicPattern("*".to_string()),
            Permission::Write,
        ));
        permissions.insert((
            ResourceType::TopicPattern("*".to_string()),
            Permission::Describe,
        ));
        permissions.insert((ResourceType::Cluster, Permission::IdempotentWrite));

        Role {
            name: "producer".to_string(),
            description: "Can produce to all topics".to_string(),
            permissions,
            builtin: true,
        }
    }

    /// Create a built-in consumer role
    pub fn consumer() -> Self {
        let mut permissions = HashSet::new();
        permissions.insert((
            ResourceType::TopicPattern("*".to_string()),
            Permission::Read,
        ));
        permissions.insert((
            ResourceType::TopicPattern("*".to_string()),
            Permission::Describe,
        ));
        permissions.insert((
            ResourceType::ConsumerGroup("*".to_string()),
            Permission::GroupRead,
        ));

        Role {
            name: "consumer".to_string(),
            description: "Can consume from all topics".to_string(),
            permissions,
            builtin: true,
        }
    }

    /// Create a built-in read-only role
    pub fn read_only() -> Self {
        let mut permissions = HashSet::new();
        permissions.insert((
            ResourceType::TopicPattern("*".to_string()),
            Permission::Read,
        ));
        permissions.insert((
            ResourceType::TopicPattern("*".to_string()),
            Permission::Describe,
        ));

        Role {
            name: "read-only".to_string(),
            description: "Read-only access to all topics".to_string(),
            permissions,
            builtin: true,
        }
    }
}

// ============================================================================
// Access Control List (ACL)
// ============================================================================

/// An ACL entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclEntry {
    /// Principal this ACL applies to (use "*" for all)
    pub principal: String,

    /// Resource this ACL applies to
    pub resource: ResourceType,

    /// Permission granted or denied
    pub permission: Permission,

    /// Whether this is an allow or deny rule
    pub allow: bool,

    /// Host pattern (IP or hostname, "*" for all)
    pub host: String,
}

// ============================================================================
// Session and Token
// ============================================================================

/// An authenticated session
#[derive(Debug, Clone)]
pub struct AuthSession {
    /// Session ID
    pub id: String,

    /// Authenticated principal name
    pub principal_name: String,

    /// Principal type
    pub principal_type: PrincipalType,

    /// Resolved permissions (cached for performance)
    pub permissions: HashSet<(ResourceType, Permission)>,

    /// Session creation time
    pub created_at: Instant,

    /// Session expiration time
    pub expires_at: Instant,

    /// Client IP address
    pub client_ip: String,
}

impl AuthSession {
    /// Check if the session has expired
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    /// Check if this session has a specific permission on a resource
    pub fn has_permission(&self, resource: &ResourceType, permission: &Permission) -> bool {
        // Admin has all permissions
        if self
            .permissions
            .contains(&(ResourceType::Cluster, Permission::All))
        {
            return true;
        }

        // Check direct permission
        if self.permissions.contains(&(resource.clone(), *permission)) {
            return true;
        }

        // Check pattern matches and permission implies
        for (res, perm) in &self.permissions {
            // Check if the granted resource (res) matches the requested resource
            // AND the granted permission (perm) implies the requested permission
            let resource_matches = res.matches(resource);
            let permission_implies = perm.implies(permission);
            if resource_matches && permission_implies {
                return true;
            }
        }

        false
    }
}

// ============================================================================
// Auth Manager
// ============================================================================

/// Configuration for the authentication manager
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Session timeout
    pub session_timeout: Duration,

    /// Maximum failed auth attempts before lockout
    pub max_failed_attempts: u32,

    /// Lockout duration after max failed attempts
    pub lockout_duration: Duration,

    /// Whether to require authentication (false = anonymous access allowed)
    pub require_authentication: bool,

    /// Whether to enable ACL enforcement
    pub enable_acls: bool,

    /// Default deny (true = deny unless explicitly allowed)
    pub default_deny: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        AuthConfig {
            session_timeout: Duration::from_secs(3600), // 1 hour
            max_failed_attempts: 5,
            lockout_duration: Duration::from_secs(300), // 5 minutes
            require_authentication: false,              // Default to open for dev
            enable_acls: false,
            default_deny: true,
        }
    }
}

/// Tracks failed authentication attempts for rate limiting
struct FailedAttemptTracker {
    attempts: HashMap<String, Vec<Instant>>,
    lockouts: HashMap<String, Instant>,
}

impl FailedAttemptTracker {
    fn new() -> Self {
        Self {
            attempts: HashMap::new(),
            lockouts: HashMap::new(),
        }
    }

    /// Check if an identifier is currently locked out
    fn is_locked_out(&self, identifier: &str, lockout_duration: Duration) -> bool {
        if let Some(lockout_time) = self.lockouts.get(identifier) {
            if lockout_time.elapsed() < lockout_duration {
                return true;
            }
        }
        false
    }

    /// Record a failed attempt
    fn record_failure(
        &mut self,
        identifier: &str,
        max_attempts: u32,
        lockout_duration: Duration,
    ) -> bool {
        let now = Instant::now();

        // Clean up old lockouts
        self.lockouts.retain(|_, t| t.elapsed() < lockout_duration);

        // Get or create attempt list
        let attempts = self.attempts.entry(identifier.to_string()).or_default();

        // Remove attempts older than lockout duration
        attempts.retain(|t| t.elapsed() < lockout_duration);

        // Add this attempt
        attempts.push(now);

        // Check if we've exceeded max attempts
        if attempts.len() >= max_attempts as usize {
            warn!(
                "Principal '{}' locked out after {} failed attempts",
                identifier, max_attempts
            );
            self.lockouts.insert(identifier.to_string(), now);
            return true;
        }

        false
    }

    /// Clear failures for an identifier (on successful auth)
    fn clear_failures(&mut self, identifier: &str) {
        self.attempts.remove(identifier);
        self.lockouts.remove(identifier);
    }
}

/// The main authentication and authorization manager
pub struct AuthManager {
    config: AuthConfig,

    /// Principals (users/service accounts)
    principals: RwLock<HashMap<String, Principal>>,

    /// Roles
    roles: RwLock<HashMap<String, Role>>,

    /// ACL entries
    acls: RwLock<Vec<AclEntry>>,

    /// Active sessions
    sessions: RwLock<HashMap<String, AuthSession>>,

    /// Failed attempt tracking
    failed_attempts: RwLock<FailedAttemptTracker>,

    /// Random number generator for session IDs
    rng: SystemRandom,
}

impl AuthManager {
    /// Create a new authentication manager
    pub fn new(config: AuthConfig) -> Self {
        let manager = Self {
            config,
            principals: RwLock::new(HashMap::new()),
            roles: RwLock::new(HashMap::new()),
            acls: RwLock::new(Vec::new()),
            sessions: RwLock::new(HashMap::new()),
            failed_attempts: RwLock::new(FailedAttemptTracker::new()),
            rng: SystemRandom::new(),
        };

        // Initialize built-in roles
        manager.init_builtin_roles();

        manager
    }

    /// Create with default config
    pub fn new_default() -> Self {
        Self::new(AuthConfig::default())
    }

    /// Create an auth manager with authentication enabled
    pub fn with_auth_enabled() -> Self {
        Self::new(AuthConfig {
            require_authentication: true,
            enable_acls: true,
            ..Default::default()
        })
    }

    /// Initialize built-in roles
    fn init_builtin_roles(&self) {
        let mut roles = self.roles.write();
        roles.insert("admin".to_string(), Role::admin());
        roles.insert("producer".to_string(), Role::producer());
        roles.insert("consumer".to_string(), Role::consumer());
        roles.insert("read-only".to_string(), Role::read_only());
    }

    // ========================================================================
    // Principal Management
    // ========================================================================

    /// Create a new principal (user or service account)
    pub fn create_principal(
        &self,
        name: &str,
        password: &str,
        principal_type: PrincipalType,
        roles: HashSet<String>,
    ) -> AuthResult<()> {
        // Validate principal name
        if name.is_empty() || name.len() > 255 {
            return Err(AuthError::Internal("Invalid principal name".to_string()));
        }

        // Validate password strength (minimum requirements)
        if password.len() < 8 {
            return Err(AuthError::Internal(
                "Password must be at least 8 characters".to_string(),
            ));
        }

        // Validate roles exist
        {
            let role_map = self.roles.read();
            for role in &roles {
                if !role_map.contains_key(role) {
                    return Err(AuthError::RoleNotFound(role.clone()));
                }
            }
        }

        let mut principals = self.principals.write();

        if principals.contains_key(name) {
            return Err(AuthError::PrincipalAlreadyExists(name.to_string()));
        }

        let principal = Principal {
            name: name.to_string(),
            principal_type,
            password_hash: PasswordHash::new(password),
            roles,
            enabled: true,
            metadata: HashMap::new(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        principals.insert(name.to_string(), principal);
        debug!("Created principal: {}", name);

        Ok(())
    }

    /// Delete a principal
    pub fn delete_principal(&self, name: &str) -> AuthResult<()> {
        let mut principals = self.principals.write();

        if principals.remove(name).is_none() {
            return Err(AuthError::PrincipalNotFound(name.to_string()));
        }

        // Also invalidate any active sessions for this principal
        let mut sessions = self.sessions.write();
        sessions.retain(|_, s| s.principal_name != name);

        debug!("Deleted principal: {}", name);
        Ok(())
    }

    /// Get a principal by name
    pub fn get_principal(&self, name: &str) -> Option<Principal> {
        self.principals.read().get(name).cloned()
    }

    /// List all principals
    pub fn list_principals(&self) -> Vec<String> {
        self.principals.read().keys().cloned().collect()
    }

    /// Update principal password
    pub fn update_password(&self, name: &str, new_password: &str) -> AuthResult<()> {
        if new_password.len() < 8 {
            return Err(AuthError::Internal(
                "Password must be at least 8 characters".to_string(),
            ));
        }

        let mut principals = self.principals.write();

        let principal = principals
            .get_mut(name)
            .ok_or_else(|| AuthError::PrincipalNotFound(name.to_string()))?;

        principal.password_hash = PasswordHash::new(new_password);

        // Invalidate sessions
        let mut sessions = self.sessions.write();
        sessions.retain(|_, s| s.principal_name != name);

        debug!("Updated password for principal: {}", name);
        Ok(())
    }

    /// Add a role to a principal
    pub fn add_role_to_principal(&self, principal_name: &str, role_name: &str) -> AuthResult<()> {
        // Validate role exists
        if !self.roles.read().contains_key(role_name) {
            return Err(AuthError::RoleNotFound(role_name.to_string()));
        }

        let mut principals = self.principals.write();

        let principal = principals
            .get_mut(principal_name)
            .ok_or_else(|| AuthError::PrincipalNotFound(principal_name.to_string()))?;

        principal.roles.insert(role_name.to_string());

        debug!(
            "Added role '{}' to principal '{}'",
            role_name, principal_name
        );
        Ok(())
    }

    /// Remove a role from a principal
    pub fn remove_role_from_principal(
        &self,
        principal_name: &str,
        role_name: &str,
    ) -> AuthResult<()> {
        let mut principals = self.principals.write();

        let principal = principals
            .get_mut(principal_name)
            .ok_or_else(|| AuthError::PrincipalNotFound(principal_name.to_string()))?;

        principal.roles.remove(role_name);

        debug!(
            "Removed role '{}' from principal '{}'",
            role_name, principal_name
        );
        Ok(())
    }

    // ========================================================================
    // Role Management
    // ========================================================================

    /// Create a custom role
    pub fn create_role(&self, role: Role) -> AuthResult<()> {
        let mut roles = self.roles.write();

        if roles.contains_key(&role.name) {
            return Err(AuthError::Internal(format!(
                "Role '{}' already exists",
                role.name
            )));
        }

        debug!("Created role: {}", role.name);
        roles.insert(role.name.clone(), role);
        Ok(())
    }

    /// Delete a custom role
    pub fn delete_role(&self, name: &str) -> AuthResult<()> {
        let mut roles = self.roles.write();

        if let Some(role) = roles.get(name) {
            if role.builtin {
                return Err(AuthError::Internal(
                    "Cannot delete built-in role".to_string(),
                ));
            }
        } else {
            return Err(AuthError::RoleNotFound(name.to_string()));
        }

        roles.remove(name);
        debug!("Deleted role: {}", name);
        Ok(())
    }

    /// Get a role by name
    pub fn get_role(&self, name: &str) -> Option<Role> {
        self.roles.read().get(name).cloned()
    }

    /// List all roles
    pub fn list_roles(&self) -> Vec<String> {
        self.roles.read().keys().cloned().collect()
    }

    // ========================================================================
    // ACL Management
    // ========================================================================

    /// Add an ACL entry
    pub fn add_acl(&self, entry: AclEntry) {
        let mut acls = self.acls.write();
        acls.push(entry);
    }

    /// Remove ACL entries matching criteria
    pub fn remove_acls(&self, principal: Option<&str>, resource: Option<&ResourceType>) {
        let mut acls = self.acls.write();
        acls.retain(|acl| {
            let principal_match =
                principal.is_none_or(|p| acl.principal == p || acl.principal == "*");
            let resource_match = resource.is_none_or(|r| &acl.resource == r);
            !(principal_match && resource_match)
        });
    }

    /// List ACL entries
    pub fn list_acls(&self) -> Vec<AclEntry> {
        self.acls.read().clone()
    }

    // ========================================================================
    // Authentication
    // ========================================================================

    /// Authenticate a principal and create a session
    pub fn authenticate(
        &self,
        username: &str,
        password: &str,
        client_ip: &str,
    ) -> AuthResult<AuthSession> {
        // Check rate limiting
        {
            let tracker = self.failed_attempts.read();
            if tracker.is_locked_out(username, self.config.lockout_duration) {
                warn!(
                    "Authentication attempt for locked-out principal: {}",
                    username
                );
                return Err(AuthError::RateLimited);
            }
            if tracker.is_locked_out(client_ip, self.config.lockout_duration) {
                warn!("Authentication attempt from locked-out IP: {}", client_ip);
                return Err(AuthError::RateLimited);
            }
        }

        // Look up principal
        let principal = {
            let principals = self.principals.read();
            principals.get(username).cloned()
        };

        let principal = match principal {
            Some(p) if p.enabled => p,
            Some(_) => {
                // Disabled account - don't leak this info
                self.record_auth_failure(username, client_ip);
                return Err(AuthError::AuthenticationFailed);
            }
            None => {
                // Unknown principal - still do constant-time password check
                // to prevent timing attacks that enumerate users
                let dummy = PasswordHash::new("dummy");
                let _ = dummy.verify(password);
                self.record_auth_failure(username, client_ip);
                return Err(AuthError::AuthenticationFailed);
            }
        };

        // Verify password (constant-time comparison)
        if !principal.password_hash.verify(password) {
            self.record_auth_failure(username, client_ip);
            return Err(AuthError::AuthenticationFailed);
        }

        // Clear any failed attempt tracking
        self.failed_attempts.write().clear_failures(username);
        self.failed_attempts.write().clear_failures(client_ip);

        // Build session with resolved permissions
        let permissions = self.resolve_permissions(&principal);

        // Generate session ID
        let mut session_id = vec![0u8; 32];
        self.rng
            .fill(&mut session_id)
            .map_err(|_| AuthError::Internal("RNG failed".to_string()))?;
        let session_id = hex::encode(&session_id);

        let now = Instant::now();
        let session = AuthSession {
            id: session_id.clone(),
            principal_name: principal.name.clone(),
            principal_type: principal.principal_type.clone(),
            permissions,
            created_at: now,
            expires_at: now + self.config.session_timeout,
            client_ip: client_ip.to_string(),
        };

        // Store session
        self.sessions.write().insert(session_id, session.clone());

        debug!("Authenticated principal '{}' from {}", username, client_ip);
        Ok(session)
    }

    /// Record a failed authentication attempt
    fn record_auth_failure(&self, username: &str, client_ip: &str) {
        let mut tracker = self.failed_attempts.write();
        tracker.record_failure(
            username,
            self.config.max_failed_attempts,
            self.config.lockout_duration,
        );
        tracker.record_failure(
            client_ip,
            self.config.max_failed_attempts * 2,
            self.config.lockout_duration,
        );
    }

    /// Get an active session by ID
    pub fn get_session(&self, session_id: &str) -> Option<AuthSession> {
        let sessions = self.sessions.read();
        sessions.get(session_id).and_then(|s| {
            if s.is_expired() {
                None
            } else {
                Some(s.clone())
            }
        })
    }

    /// Invalidate a session (logout)
    pub fn invalidate_session(&self, session_id: &str) {
        self.sessions.write().remove(session_id);
    }

    /// Invalidate all sessions for a principal
    pub fn invalidate_all_sessions(&self, principal_name: &str) {
        self.sessions
            .write()
            .retain(|_, s| s.principal_name != principal_name);
    }

    /// Clean up expired sessions
    pub fn cleanup_expired_sessions(&self) {
        self.sessions.write().retain(|_, s| !s.is_expired());
    }

    /// Create a session for a principal (used by SCRAM after successful auth)
    pub fn create_session(&self, principal: &Principal) -> AuthSession {
        let permissions = self.resolve_permissions(principal);

        let mut session_id = vec![0u8; 32];
        self.rng.fill(&mut session_id).expect("RNG failed");
        let session_id = hex::encode(&session_id);

        let now = Instant::now();
        let session = AuthSession {
            id: session_id.clone(),
            principal_name: principal.name.clone(),
            principal_type: principal.principal_type.clone(),
            permissions,
            created_at: now,
            expires_at: now + self.config.session_timeout,
            client_ip: "scram".to_string(),
        };

        self.sessions.write().insert(session_id, session.clone());
        session
    }

    /// Create a session for an API key authentication
    ///
    /// Creates a synthetic session for API key-based authentication.
    /// The session inherits permissions from the specified roles.
    pub fn create_api_key_session(&self, principal_name: &str, roles: &[String]) -> AuthSession {
        // Resolve permissions from roles
        let mut permissions = HashSet::new();
        {
            let roles_map = self.roles.read();
            for role_name in roles {
                if let Some(role) = roles_map.get(role_name) {
                    permissions.extend(role.permissions.iter().cloned());
                }
            }
        }

        let mut session_id = vec![0u8; 32];
        self.rng.fill(&mut session_id).expect("RNG failed");
        let session_id = hex::encode(&session_id);

        let now = Instant::now();
        let session = AuthSession {
            id: session_id.clone(),
            principal_name: principal_name.to_string(),
            principal_type: PrincipalType::ServiceAccount,
            permissions,
            created_at: now,
            expires_at: now + self.config.session_timeout,
            client_ip: "api-key".to_string(),
        };

        self.sessions.write().insert(session_id, session.clone());
        debug!(principal = %principal_name, "Created API key session");
        session
    }

    /// Create a session for JWT/OIDC authentication
    ///
    /// Creates a synthetic session for JWT-authenticated users.
    /// The session inherits permissions from the specified groups/roles.
    pub fn create_jwt_session(&self, principal_name: &str, groups: &[String]) -> AuthSession {
        // Resolve permissions from groups (treated as roles)
        let mut permissions = HashSet::new();
        {
            let roles_map = self.roles.read();
            for group in groups {
                if let Some(role) = roles_map.get(group) {
                    permissions.extend(role.permissions.iter().cloned());
                }
            }
        }

        let mut session_id = vec![0u8; 32];
        self.rng.fill(&mut session_id).expect("RNG failed");
        let session_id = hex::encode(&session_id);

        let now = Instant::now();
        let session = AuthSession {
            id: session_id.clone(),
            principal_name: principal_name.to_string(),
            principal_type: PrincipalType::User,
            permissions,
            created_at: now,
            expires_at: now + self.config.session_timeout,
            client_ip: "jwt".to_string(),
        };

        self.sessions.write().insert(session_id, session.clone());
        debug!(principal = %principal_name, groups = ?groups, "Created JWT session");
        session
    }

    /// Get a session by principal name (for API key validation)
    pub fn get_session_by_principal(&self, principal_name: &str) -> Option<AuthSession> {
        let sessions = self.sessions.read();
        sessions
            .values()
            .find(|s| s.principal_name == principal_name && !s.is_expired())
            .cloned()
    }

    // ========================================================================
    // Authorization
    // ========================================================================

    /// Resolve all permissions for a principal
    fn resolve_permissions(&self, principal: &Principal) -> HashSet<(ResourceType, Permission)> {
        let mut permissions = HashSet::new();

        let roles = self.roles.read();

        // Collect permissions from all roles
        for role_name in &principal.roles {
            if let Some(role) = roles.get(role_name) {
                permissions.extend(role.permissions.iter().cloned());
            }
        }

        permissions
    }

    /// Check if a session/principal has permission on a resource
    pub fn authorize(
        &self,
        session: &AuthSession,
        resource: &ResourceType,
        permission: Permission,
        client_ip: &str,
    ) -> AuthResult<()> {
        // If auth is not required and no ACLs, allow everything
        if !self.config.require_authentication && !self.config.enable_acls {
            return Ok(());
        }

        // Check session expiration
        if session.is_expired() {
            return Err(AuthError::TokenExpired);
        }

        // Check role-based permissions
        if session.has_permission(resource, &permission) {
            return Ok(());
        }

        // Check ACL entries
        if self.config.enable_acls
            && self.check_acls(&session.principal_name, resource, permission, client_ip)
        {
            return Ok(());
        }

        // Default deny
        if self.config.default_deny {
            warn!(
                "Access denied: {} attempted {} on {:?} from {}",
                session.principal_name,
                format!("{:?}", permission),
                resource,
                client_ip
            );
            return Err(AuthError::PermissionDenied {
                principal: session.principal_name.clone(),
                permission: format!("{:?}", permission),
                resource: format!("{:?}", resource),
            });
        }

        Ok(())
    }

    /// Check ACL entries for authorization
    fn check_acls(
        &self,
        principal: &str,
        resource: &ResourceType,
        permission: Permission,
        client_ip: &str,
    ) -> bool {
        let acls = self.acls.read();

        // Check deny rules first (deny takes precedence)
        for acl in acls.iter() {
            if !acl.allow
                && (acl.principal == principal || acl.principal == "*")
                && (acl.host == client_ip || acl.host == "*")
                && acl.resource.matches(resource)
                && (acl.permission == permission || acl.permission == Permission::All)
            {
                return false; // Explicit deny
            }
        }

        // Check allow rules
        for acl in acls.iter() {
            if acl.allow
                && (acl.principal == principal || acl.principal == "*")
                && (acl.host == client_ip || acl.host == "*")
                && acl.resource.matches(resource)
                && (acl.permission == permission || acl.permission == Permission::All)
            {
                return true;
            }
        }

        false
    }

    /// Simple authorization check without session (for internal use)
    #[allow(unused_variables)]
    pub fn authorize_anonymous(
        &self,
        resource: &ResourceType,
        permission: Permission,
    ) -> AuthResult<()> {
        if !self.config.require_authentication {
            return Ok(());
        }

        Err(AuthError::AuthenticationFailed)
    }
}

// ============================================================================
// SASL/PLAIN Support (Kafka-compatible)
// ============================================================================

/// SASL/PLAIN authentication handler
pub struct SaslPlainAuth {
    auth_manager: Arc<AuthManager>,
}

impl SaslPlainAuth {
    pub fn new(auth_manager: Arc<AuthManager>) -> Self {
        Self { auth_manager }
    }

    /// Parse and authenticate a SASL/PLAIN request
    /// Format: \[authzid\] NUL authcid NUL passwd
    pub fn authenticate(&self, sasl_bytes: &[u8], client_ip: &str) -> AuthResult<AuthSession> {
        // Parse SASL/PLAIN format: [authzid] \0 authcid \0 password
        let parts: Vec<&[u8]> = sasl_bytes.split(|&b| b == 0).collect();

        if parts.len() < 2 {
            return Err(AuthError::InvalidCredentials);
        }

        // Handle both 2-part (authcid, passwd) and 3-part (authzid, authcid, passwd)
        let (username, password) = if parts.len() == 2 {
            (
                std::str::from_utf8(parts[0]).map_err(|_| AuthError::InvalidCredentials)?,
                std::str::from_utf8(parts[1]).map_err(|_| AuthError::InvalidCredentials)?,
            )
        } else {
            // 3-part format - authzid is ignored, use authcid
            (
                std::str::from_utf8(parts[1]).map_err(|_| AuthError::InvalidCredentials)?,
                std::str::from_utf8(parts[2]).map_err(|_| AuthError::InvalidCredentials)?,
            )
        };

        self.auth_manager
            .authenticate(username, password, client_ip)
    }
}

// ============================================================================
// SCRAM-SHA-256 Support (RFC 5802 / RFC 7677)
// ============================================================================

/// SCRAM-SHA-256 authentication state machine
///
/// Implements the full SCRAM protocol for secure password-based authentication.
/// This is significantly more secure than PLAIN because:
/// 1. The password is never sent over the wire (even encrypted)
/// 2. The server stores derived keys, not the password
/// 3. Mutual authentication (server proves it knows the password too)
/// 4. Protection against replay attacks via nonces
#[derive(Debug, Clone)]
pub enum ScramState {
    /// Waiting for client-first-message
    Initial,
    /// Waiting for client-final-message
    ServerFirstSent {
        username: String,
        client_nonce: String,
        server_nonce: String,
        salt: Vec<u8>,
        iterations: u32,
        auth_message: String,
    },
    /// Authentication complete (success or failure pending verification)
    Complete,
}

/// SCRAM-SHA-256 authentication handler
pub struct SaslScramAuth {
    auth_manager: Arc<AuthManager>,
}

impl SaslScramAuth {
    pub fn new(auth_manager: Arc<AuthManager>) -> Self {
        Self { auth_manager }
    }

    /// Process client-first-message and return server-first-message
    ///
    /// Client-first-message format: `n,,n=<username>,r=<client-nonce>`
    /// Server-first-message format: `r=<combined-nonce>,s=<salt>,i=<iterations>`
    pub fn process_client_first(
        &self,
        client_first: &[u8],
        client_ip: &str,
    ) -> AuthResult<(ScramState, Vec<u8>)> {
        let client_first_str =
            std::str::from_utf8(client_first).map_err(|_| AuthError::InvalidCredentials)?;

        // Parse client-first-message
        // Format: gs2-header,client-first-message-bare
        // gs2-header: n,, (no channel binding)
        // client-first-message-bare: n=<user>,r=<nonce>

        let parts: Vec<&str> = client_first_str.splitn(3, ',').collect();
        if parts.len() < 3 {
            return Err(AuthError::InvalidCredentials);
        }

        // Skip gs2-header (parts[0] and parts[1])
        let client_first_bare = if parts[0] == "n" || parts[0] == "y" || parts[0] == "p" {
            // gs2-header present, skip first two parts
            &client_first_str[parts[0].len() + 1 + parts[1].len() + 1..]
        } else {
            // No gs2-header, message is just client-first-message-bare
            client_first_str
        };

        // Parse client-first-message-bare
        let mut username = None;
        let mut client_nonce = None;

        for attr in client_first_bare.split(',') {
            if let Some(value) = attr.strip_prefix("n=") {
                username = Some(Self::unescape_username(value));
            } else if let Some(value) = attr.strip_prefix("r=") {
                client_nonce = Some(value.to_string());
            }
        }

        let username = username.ok_or(AuthError::InvalidCredentials)?;
        let client_nonce = client_nonce.ok_or(AuthError::InvalidCredentials)?;

        // Look up principal to get salt and iterations
        let (salt, iterations) = match self.auth_manager.get_principal(&username) {
            Some(principal) => (
                principal.password_hash.salt.clone(),
                principal.password_hash.iterations,
            ),
            None => {
                // User not found - generate fake salt to prevent enumeration
                // Still continue with the protocol to not leak timing info
                warn!(
                    "SCRAM auth for unknown user '{}' from {}",
                    username, client_ip
                );
                let rng = SystemRandom::new();
                let mut fake_salt = vec![0u8; 32];
                rng.fill(&mut fake_salt).expect("Failed to generate salt");
                (fake_salt, 4096)
            }
        };

        // Generate server nonce (random bytes, base64 encoded)
        let rng = SystemRandom::new();
        let mut server_nonce_bytes = vec![0u8; 24];
        rng.fill(&mut server_nonce_bytes)
            .expect("Failed to generate nonce");
        let server_nonce = base64_encode(&server_nonce_bytes);
        let combined_nonce = format!("{}{}", client_nonce, server_nonce);

        // Build server-first-message
        let salt_b64 = base64_encode(&salt);
        let server_first = format!("r={},s={},i={}", combined_nonce, salt_b64, iterations);

        // Store auth message for later verification
        let auth_message = format!(
            "{},{},c=biws,r={}",
            client_first_bare, server_first, combined_nonce
        );

        let state = ScramState::ServerFirstSent {
            username,
            client_nonce,
            server_nonce,
            salt,
            iterations,
            auth_message,
        };

        Ok((state, server_first.into_bytes()))
    }

    /// Process client-final-message and return server-final-message
    ///
    /// Client-final-message format: `c=<channel-binding>,r=<nonce>,p=<proof>`
    /// Server-final-message format: `v=<verifier>` (on success) or `e=<error>`
    pub fn process_client_final(
        &self,
        state: &ScramState,
        client_final: &[u8],
        client_ip: &str,
    ) -> AuthResult<(AuthSession, Vec<u8>)> {
        let ScramState::ServerFirstSent {
            username,
            client_nonce,
            server_nonce,
            salt: _,       // Not needed for verification, stored in principal
            iterations: _, // Not needed for verification, stored in principal
            auth_message,
        } = state
        else {
            return Err(AuthError::Internal("Invalid SCRAM state".to_string()));
        };

        let client_final_str =
            std::str::from_utf8(client_final).map_err(|_| AuthError::InvalidCredentials)?;

        // Parse client-final-message
        let mut channel_binding = None;
        let mut nonce = None;
        let mut proof = None;

        for attr in client_final_str.split(',') {
            if let Some(value) = attr.strip_prefix("c=") {
                channel_binding = Some(value.to_string());
            } else if let Some(value) = attr.strip_prefix("r=") {
                nonce = Some(value.to_string());
            } else if let Some(value) = attr.strip_prefix("p=") {
                proof = Some(value.to_string());
            }
        }

        let _channel_binding = channel_binding.ok_or(AuthError::InvalidCredentials)?;
        let nonce = nonce.ok_or(AuthError::InvalidCredentials)?;
        let proof_b64 = proof.ok_or(AuthError::InvalidCredentials)?;

        // Verify nonce
        let expected_nonce = format!("{}{}", client_nonce, server_nonce);
        if nonce != expected_nonce {
            warn!("SCRAM nonce mismatch for '{}' from {}", username, client_ip);
            return Err(AuthError::InvalidCredentials);
        }

        // Get principal
        let principal = self
            .auth_manager
            .get_principal(username)
            .ok_or(AuthError::AuthenticationFailed)?;

        // Verify client proof
        // ClientProof = ClientKey XOR ClientSignature
        // ClientSignature = HMAC(StoredKey, AuthMessage)
        // We need to verify: H(ClientKey) == StoredKey

        let client_proof = base64_decode(&proof_b64).map_err(|_| AuthError::InvalidCredentials)?;

        // Compute expected client signature
        let client_signature =
            PasswordHash::hmac_sha256(&principal.password_hash.stored_key, auth_message.as_bytes());

        // Recover ClientKey = ClientProof XOR ClientSignature
        if client_proof.len() != client_signature.len() {
            return Err(AuthError::InvalidCredentials);
        }

        let client_key: Vec<u8> = client_proof
            .iter()
            .zip(client_signature.iter())
            .map(|(p, s)| p ^ s)
            .collect();

        // Verify: H(ClientKey) == StoredKey (constant-time comparison)
        let computed_stored_key = Sha256::digest(&client_key);
        if !PasswordHash::constant_time_compare(
            &computed_stored_key,
            &principal.password_hash.stored_key,
        ) {
            warn!(
                "SCRAM authentication failed for '{}' from {}",
                username, client_ip
            );
            return Err(AuthError::AuthenticationFailed);
        }

        // Compute server signature for mutual authentication
        let server_signature =
            PasswordHash::hmac_sha256(&principal.password_hash.server_key, auth_message.as_bytes());
        let server_final = format!("v={}", base64_encode(&server_signature));

        // Create session
        let session = self.auth_manager.create_session(&principal);
        debug!(
            "SCRAM authentication successful for '{}' from {}",
            username, client_ip
        );

        Ok((session, server_final.into_bytes()))
    }

    /// Unescape SCRAM username (=2C -> , and =3D -> =)
    fn unescape_username(s: &str) -> String {
        s.replace("=2C", ",").replace("=3D", "=")
    }
}

/// Base64 encode (standard alphabet)
fn base64_encode(data: &[u8]) -> String {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    STANDARD.encode(data)
}

/// Base64 decode (standard alphabet)
fn base64_decode(s: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    STANDARD.decode(s)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_hash_verify() {
        let hash = PasswordHash::new("test_password_123");
        assert!(hash.verify("test_password_123"));
        assert!(!hash.verify("wrong_password"));
        assert!(!hash.verify(""));
        assert!(!hash.verify("test_password_12")); // Off by one
    }

    #[test]
    fn test_password_hash_timing_attack_resistant() {
        // Both wrong passwords should take similar time
        // (This is more of a design assertion than a precise timing test)
        let hash = PasswordHash::new("correct_password");

        // Wrong but similar length
        assert!(!hash.verify("wrong_password"));

        // Wrong and very different
        assert!(!hash.verify("x"));

        // Both should still return false (constant-time)
    }

    #[test]
    fn test_create_principal() {
        let auth = AuthManager::new_default();

        let mut roles = HashSet::new();
        roles.insert("producer".to_string());

        auth.create_principal(
            "alice",
            "secure_pass_123",
            PrincipalType::User,
            roles.clone(),
        )
        .expect("Failed to create principal");

        // Duplicate should fail
        assert!(auth
            .create_principal("alice", "other_pass", PrincipalType::User, roles.clone())
            .is_err());

        // Verify principal exists
        let principal = auth.get_principal("alice").expect("Principal not found");
        assert_eq!(principal.name, "alice");
        assert!(principal.roles.contains("producer"));
    }

    #[test]
    fn test_authentication_success() {
        let auth = AuthManager::new_default();

        let mut roles = HashSet::new();
        roles.insert("producer".to_string());

        auth.create_principal("bob", "bob_password", PrincipalType::User, roles)
            .unwrap();

        let session = auth
            .authenticate("bob", "bob_password", "127.0.0.1")
            .expect("Authentication should succeed");

        assert_eq!(session.principal_name, "bob");
        assert!(!session.is_expired());
    }

    #[test]
    fn test_authentication_failure() {
        let auth = AuthManager::new_default();

        let mut roles = HashSet::new();
        roles.insert("producer".to_string());

        auth.create_principal("charlie", "correct_password", PrincipalType::User, roles)
            .unwrap();

        // Wrong password
        let result = auth.authenticate("charlie", "wrong_password", "127.0.0.1");
        assert!(matches!(result, Err(AuthError::AuthenticationFailed)));

        // Unknown user
        let result = auth.authenticate("unknown", "password", "127.0.0.1");
        assert!(matches!(result, Err(AuthError::AuthenticationFailed)));
    }

    #[test]
    fn test_rate_limiting() {
        let config = AuthConfig {
            max_failed_attempts: 3,
            lockout_duration: Duration::from_secs(1),
            ..Default::default()
        };
        let auth = AuthManager::new(config);

        let mut roles = HashSet::new();
        roles.insert("consumer".to_string());
        auth.create_principal("eve", "password", PrincipalType::User, roles)
            .unwrap();

        // Fail 3 times
        for _ in 0..3 {
            let _ = auth.authenticate("eve", "wrong", "192.168.1.1");
        }

        // Now should be rate limited
        let result = auth.authenticate("eve", "password", "192.168.1.1");
        assert!(matches!(result, Err(AuthError::RateLimited)));

        // Wait for lockout to expire
        std::thread::sleep(Duration::from_millis(1100));

        // Should work now
        let result = auth.authenticate("eve", "password", "192.168.1.1");
        assert!(result.is_ok());
    }

    #[test]
    fn test_role_permissions() {
        let auth = AuthManager::with_auth_enabled();

        let mut roles = HashSet::new();
        roles.insert("producer".to_string());
        auth.create_principal("producer_user", "password", PrincipalType::User, roles)
            .unwrap();

        let session = auth
            .authenticate("producer_user", "password", "127.0.0.1")
            .unwrap();

        // Producer should have write permission on topics
        assert!(session.has_permission(
            &ResourceType::Topic("orders".to_string()),
            &Permission::Write
        ));

        // Producer should not have delete permission
        assert!(!session.has_permission(
            &ResourceType::Topic("orders".to_string()),
            &Permission::Delete
        ));
    }

    #[test]
    fn test_admin_has_all_permissions() {
        let auth = AuthManager::with_auth_enabled();

        let mut roles = HashSet::new();
        roles.insert("admin".to_string());
        auth.create_principal("admin_user", "admin_pass", PrincipalType::User, roles)
            .unwrap();

        let session = auth
            .authenticate("admin_user", "admin_pass", "127.0.0.1")
            .unwrap();

        // Admin should have all permissions
        assert!(session.has_permission(&ResourceType::Cluster, &Permission::All));
        assert!(session.has_permission(
            &ResourceType::Topic("any_topic".to_string()),
            &Permission::Delete
        ));
    }

    #[test]
    fn test_resource_pattern_matching() {
        assert!(ResourceType::TopicPattern("*".to_string())
            .matches(&ResourceType::Topic("anything".to_string())));

        assert!(ResourceType::TopicPattern("orders-*".to_string())
            .matches(&ResourceType::Topic("orders-us".to_string())));

        assert!(ResourceType::TopicPattern("orders-*".to_string())
            .matches(&ResourceType::Topic("orders-eu".to_string())));

        assert!(!ResourceType::TopicPattern("orders-*".to_string())
            .matches(&ResourceType::Topic("events-us".to_string())));
    }

    #[test]
    fn test_acl_enforcement() {
        let auth = AuthManager::new(AuthConfig {
            require_authentication: true,
            enable_acls: true,
            default_deny: true,
            ..Default::default()
        });

        let mut roles = HashSet::new();
        roles.insert("read-only".to_string());
        auth.create_principal("reader", "password", PrincipalType::User, roles)
            .unwrap();

        // Add ACL allowing write to specific topic
        auth.add_acl(AclEntry {
            principal: "reader".to_string(),
            resource: ResourceType::Topic("special-topic".to_string()),
            permission: Permission::Write,
            allow: true,
            host: "*".to_string(),
        });

        let session = auth
            .authenticate("reader", "password", "127.0.0.1")
            .unwrap();

        // Should be able to write to special-topic via ACL
        let result = auth.authorize(
            &session,
            &ResourceType::Topic("special-topic".to_string()),
            Permission::Write,
            "127.0.0.1",
        );
        assert!(result.is_ok());

        // Should NOT be able to write to other topics
        let result = auth.authorize(
            &session,
            &ResourceType::Topic("other-topic".to_string()),
            Permission::Write,
            "127.0.0.1",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_sasl_plain_authentication() {
        let auth = Arc::new(AuthManager::new_default());

        let mut roles = HashSet::new();
        roles.insert("producer".to_string());
        auth.create_principal("sasl_user", "sasl_password", PrincipalType::User, roles)
            .unwrap();

        let sasl = SaslPlainAuth::new(auth);

        // Test 2-part format: username\0password
        let two_part = b"sasl_user\0sasl_password";
        let result = sasl.authenticate(two_part, "127.0.0.1");
        assert!(result.is_ok());

        // Test 3-part format: authzid\0username\0password
        let three_part = b"\0sasl_user\0sasl_password";
        let result = sasl.authenticate(three_part, "127.0.0.1");
        assert!(result.is_ok());
    }

    #[test]
    fn test_session_expiration() {
        let config = AuthConfig {
            session_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        let auth = AuthManager::new(config);

        let mut roles = HashSet::new();
        roles.insert("producer".to_string());
        auth.create_principal("expiring", "password", PrincipalType::User, roles)
            .unwrap();

        let session = auth
            .authenticate("expiring", "password", "127.0.0.1")
            .unwrap();
        assert!(!session.is_expired());

        // Wait for session to expire
        std::thread::sleep(Duration::from_millis(150));

        // Session should be expired
        let session = AuthSession {
            expires_at: session.expires_at,
            ..session
        };
        assert!(session.is_expired());
    }

    #[test]
    fn test_delete_principal_invalidates_sessions() {
        let auth = AuthManager::new_default();

        let mut roles = HashSet::new();
        roles.insert("producer".to_string());
        auth.create_principal("deleteme", "password", PrincipalType::User, roles)
            .unwrap();

        let session = auth
            .authenticate("deleteme", "password", "127.0.0.1")
            .unwrap();

        // Session should exist
        assert!(auth.get_session(&session.id).is_some());

        // Delete principal
        auth.delete_principal("deleteme").unwrap();

        // Session should be gone
        assert!(auth.get_session(&session.id).is_none());
    }

    #[test]
    fn test_disabled_principal_cannot_authenticate() {
        let auth = AuthManager::new_default();

        let mut roles = HashSet::new();
        roles.insert("producer".to_string());
        auth.create_principal("disabled_user", "password", PrincipalType::User, roles)
            .unwrap();

        // Disable the principal
        {
            let mut principals = auth.principals.write();
            if let Some(p) = principals.get_mut("disabled_user") {
                p.enabled = false;
            }
        }

        // Should fail to authenticate
        let result = auth.authenticate("disabled_user", "password", "127.0.0.1");
        assert!(matches!(result, Err(AuthError::AuthenticationFailed)));
    }

    #[test]
    fn test_password_hash_debug_redacts_sensitive_data() {
        let hash = PasswordHash::new("super_secret_password");
        let debug_output = format!("{:?}", hash);

        // Should contain REDACTED markers
        assert!(
            debug_output.contains("[REDACTED]"),
            "Debug output should contain [REDACTED]"
        );

        // Should NOT contain actual salt or key material
        // Salt and keys are binary data, but let's ensure no suspicious patterns
        assert!(
            !debug_output.contains("super_secret_password"),
            "Debug output should not contain password"
        );

        // Should show iterations (not sensitive)
        assert!(
            debug_output.contains("iterations"),
            "Debug output should show iterations field"
        );
    }

    #[test]
    fn test_principal_debug_redacts_password_hash() {
        let principal = Principal {
            name: "test_user".to_string(),
            principal_type: PrincipalType::User,
            password_hash: PasswordHash::new("secret_password"),
            roles: HashSet::from(["admin".to_string()]),
            enabled: true,
            metadata: HashMap::new(),
            created_at: 1234567890,
        };

        let debug_output = format!("{:?}", principal);

        // Should contain REDACTED for password_hash
        assert!(
            debug_output.contains("[REDACTED]"),
            "Debug output should contain [REDACTED]: {}",
            debug_output
        );

        // Should still show non-sensitive fields
        assert!(
            debug_output.contains("test_user"),
            "Debug output should show name"
        );
        assert!(
            debug_output.contains("admin"),
            "Debug output should show roles"
        );
    }

    // ========================================================================
    // SCRAM-SHA-256 Tests
    // ========================================================================

    #[test]
    fn test_scram_full_handshake() {
        use sha2::{Digest, Sha256};

        let auth = Arc::new(AuthManager::new_default());

        // Create a user
        let mut roles = HashSet::new();
        roles.insert("producer".to_string());
        auth.create_principal("scram_user", "scram_password", PrincipalType::User, roles)
            .expect("Failed to create principal");

        let scram = SaslScramAuth::new(auth.clone());

        // Step 1: Client sends client-first-message
        let client_nonce = "rOprNGfwEbeRWgbNEkqO";
        let client_first = format!("n,,n=scram_user,r={}", client_nonce);

        let (state, server_first) = scram
            .process_client_first(client_first.as_bytes(), "127.0.0.1")
            .expect("client-first processing should succeed");

        // Verify server-first-message format
        let server_first_str = std::str::from_utf8(&server_first).expect("valid UTF-8");
        assert!(server_first_str.starts_with(&format!("r={}", client_nonce)));
        assert!(server_first_str.contains(",s="));
        assert!(server_first_str.contains(",i="));

        // Parse server-first-message to build client-final
        let ScramState::ServerFirstSent {
            username: _,
            client_nonce: _,
            server_nonce: _,
            salt,
            iterations,
            auth_message: _,
        } = &state
        else {
            panic!("Expected ServerFirstSent state");
        };

        // Step 2: Client computes proof and sends client-final-message
        // ClientProof = ClientKey XOR ClientSignature
        let salted_password = compute_salted_password("scram_password", salt, *iterations);
        let client_key = PasswordHash::hmac_sha256(&salted_password, b"Client Key");
        let stored_key = Sha256::digest(&client_key);

        // Build auth message
        let client_first_bare = format!("n=scram_user,r={}", client_nonce);
        let combined_nonce: String = server_first_str
            .split(',')
            .find(|s| s.starts_with("r="))
            .map(|s| &s[2..])
            .unwrap()
            .to_string();

        let auth_message = format!(
            "{},{},c=biws,r={}",
            client_first_bare, server_first_str, combined_nonce
        );

        let client_signature = PasswordHash::hmac_sha256(&stored_key, auth_message.as_bytes());
        let client_proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(k, s)| k ^ s)
            .collect();

        let client_final = format!(
            "c=biws,r={},p={}",
            combined_nonce,
            base64_encode(&client_proof)
        );

        // Step 3: Server verifies and responds
        let (session, server_final) = scram
            .process_client_final(&state, client_final.as_bytes(), "127.0.0.1")
            .expect("client-final processing should succeed");

        // Verify session was created
        assert_eq!(session.principal_name, "scram_user");
        assert!(!session.is_expired());

        // Verify server-final-message (mutual authentication)
        let server_final_str = std::str::from_utf8(&server_final).expect("valid UTF-8");
        assert!(server_final_str.starts_with("v="));
    }

    #[test]
    fn test_scram_wrong_password() {
        let auth = Arc::new(AuthManager::new_default());

        let mut roles = HashSet::new();
        roles.insert("producer".to_string());
        auth.create_principal(
            "scram_user2",
            "correct_password",
            PrincipalType::User,
            roles,
        )
        .expect("Failed to create principal");

        let scram = SaslScramAuth::new(auth.clone());

        // Client-first with correct username
        let client_nonce = "test_nonce_12345";
        let client_first = format!("n,,n=scram_user2,r={}", client_nonce);

        let (state, server_first) = scram
            .process_client_first(client_first.as_bytes(), "127.0.0.1")
            .expect("client-first processing should succeed");

        // Parse server response
        let server_first_str = std::str::from_utf8(&server_first).expect("valid UTF-8");
        let combined_nonce: String = server_first_str
            .split(',')
            .find(|s| s.starts_with("r="))
            .map(|s| &s[2..])
            .unwrap()
            .to_string();

        // Compute proof with WRONG password
        let ScramState::ServerFirstSent {
            salt, iterations, ..
        } = &state
        else {
            panic!("Expected ServerFirstSent state");
        };

        let salted_password = compute_salted_password("wrong_password", salt, *iterations);
        let client_key = PasswordHash::hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha2::Sha256::digest(&client_key);

        let client_first_bare = format!("n=scram_user2,r={}", client_nonce);
        let auth_message = format!(
            "{},{},c=biws,r={}",
            client_first_bare, server_first_str, combined_nonce
        );

        let client_signature = PasswordHash::hmac_sha256(&stored_key, auth_message.as_bytes());
        let client_proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(k, s)| k ^ s)
            .collect();

        let client_final = format!(
            "c=biws,r={},p={}",
            combined_nonce,
            base64_encode(&client_proof)
        );

        // Should fail
        let result = scram.process_client_final(&state, client_final.as_bytes(), "127.0.0.1");
        assert!(result.is_err());
        assert!(matches!(result, Err(AuthError::AuthenticationFailed)));
    }

    #[test]
    fn test_scram_nonexistent_user() {
        let auth = Arc::new(AuthManager::new_default());
        let scram = SaslScramAuth::new(auth.clone());

        // Client-first for nonexistent user
        let client_first = "n,,n=nonexistent_user,r=test_nonce";

        // Should still return a server-first (to prevent enumeration)
        let result = scram.process_client_first(client_first.as_bytes(), "127.0.0.1");
        assert!(
            result.is_ok(),
            "Should return fake server-first to prevent enumeration"
        );

        let (state, server_first) = result.unwrap();
        let server_first_str = std::str::from_utf8(&server_first).expect("valid UTF-8");

        // Should have valid format (fake salt/iterations)
        assert!(server_first_str.contains("r=test_nonce"));
        assert!(server_first_str.contains(",s="));
        assert!(server_first_str.contains(",i="));

        // Final step should fail
        let combined_nonce: String = server_first_str
            .split(',')
            .find(|s| s.starts_with("r="))
            .map(|s| &s[2..])
            .unwrap()
            .to_string();

        let client_final = format!("c=biws,r={},p=dW5rbm93bg==", combined_nonce);
        let result = scram.process_client_final(&state, client_final.as_bytes(), "127.0.0.1");
        assert!(result.is_err());
    }

    #[test]
    fn test_scram_nonce_mismatch() {
        let auth = Arc::new(AuthManager::new_default());

        let mut roles = HashSet::new();
        roles.insert("producer".to_string());
        auth.create_principal("scram_user3", "password", PrincipalType::User, roles)
            .expect("Failed to create principal");

        let scram = SaslScramAuth::new(auth.clone());

        let client_first = "n,,n=scram_user3,r=original_nonce";
        let (state, _server_first) = scram
            .process_client_first(client_first.as_bytes(), "127.0.0.1")
            .expect("client-first should succeed");

        // Client-final with different nonce prefix (attack attempt)
        let client_final = "c=biws,r=tampered_nonce_plus_server,p=dW5rbm93bg==";
        let result = scram.process_client_final(&state, client_final.as_bytes(), "127.0.0.1");
        assert!(result.is_err());
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    /// Helper: Compute salted password (PBKDF2)
    fn compute_salted_password(password: &str, salt: &[u8], iterations: u32) -> Vec<u8> {
        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<sha2::Sha256>;

        let mut result = vec![0u8; 32];

        let mut mac =
            HmacSha256::new_from_slice(password.as_bytes()).expect("HMAC accepts any key length");
        mac.update(salt);
        mac.update(&1u32.to_be_bytes());
        let mut u = mac.finalize().into_bytes();
        result.copy_from_slice(&u);

        for _ in 1..iterations {
            let mut mac = HmacSha256::new_from_slice(password.as_bytes())
                .expect("HMAC accepts any key length");
            mac.update(&u);
            u = mac.finalize().into_bytes();

            for (r, ui) in result.iter_mut().zip(u.iter()) {
                *r ^= ui;
            }
        }

        result
    }
}
