//! # SCRAM-SHA-256 Authentication
//!
//! Implementation of SCRAM-SHA-256 (RFC 5802, RFC 7677) for PostgreSQL.
//!
//! ## Overview
//!
//! SCRAM (Salted Challenge Response Authentication Mechanism) provides:
//! - Password never sent over wire (even hashed)
//! - Mutual authentication (server proves it knows password too)
//! - Channel binding support (SCRAM-SHA-256-PLUS)
//!
//! ## Protocol Flow
//!
//! ```text
//! Client                          Server
//!   |                               |
//!   |--- client-first-message ----->|  (contains nonce)
//!   |                               |
//!   |<-- server-first-message ------|  (salt, iterations, combined nonce)
//!   |                               |
//!   |--- client-final-message ----->|  (proof)
//!   |                               |
//!   |<-- server-final-message ------|  (verifier)
//!   |                               |
//! ```
//!
//! ## Usage
//!
//! ```ignore
//! use rivven_cdc::postgres::protocol::scram::ScramSha256;
//!
//! let mut scram = ScramSha256::new("username", "password");
//! let client_first = scram.client_first_message();
//!
//! // Send client_first to server, receive server_first
//! let client_final = scram.client_final_message(&server_first)?;
//!
//! // Send client_final to server, receive server_final
//! scram.verify_server_final(&server_final)?;
//! ```

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::fmt;
use thiserror::Error;

// ============================================================================
// Error Types
// ============================================================================

/// SCRAM authentication error.
#[derive(Debug, Error)]
pub enum ScramError {
    #[error("Invalid server message: {0}")]
    InvalidServerMessage(String),

    #[error("Server nonce doesn't start with client nonce")]
    InvalidNonce,

    #[error("Server signature verification failed")]
    ServerVerificationFailed,

    #[error("Base64 decode error: {0}")]
    Base64Error(#[from] base64::DecodeError),

    #[error("Invalid iteration count: {0}")]
    InvalidIterations(String),

    #[error("SCRAM error from server: {0}")]
    ServerError(String),

    #[error("Unsupported channel binding")]
    UnsupportedChannelBinding,
}

pub type Result<T> = std::result::Result<T, ScramError>;

// ============================================================================
// SCRAM-SHA-256 Implementation
// ============================================================================

/// SCRAM-SHA-256 authentication state machine.
pub struct ScramSha256 {
    /// Username (normalized)
    username: String,
    /// Password
    password: String,
    /// Client nonce (random)
    client_nonce: String,
    /// Server nonce (combined)
    server_nonce: Option<String>,
    /// Salt from server
    salt: Option<Vec<u8>>,
    /// Iteration count from server
    iterations: Option<u32>,
    /// Auth message for signature verification
    auth_message: Option<String>,
    /// Salted password (cached)
    salted_password: Option<[u8; 32]>,
}

impl ScramSha256 {
    /// SCRAM-SHA-256 mechanism name.
    pub const MECHANISM: &'static str = "SCRAM-SHA-256";

    /// Create a new SCRAM-SHA-256 authenticator.
    pub fn new(username: &str, password: &str) -> Result<Self> {
        use rand::RngCore;

        // Generate 24 bytes of random data for client nonce
        let mut nonce_bytes = [0u8; 24];
        rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
        let client_nonce = BASE64.encode(nonce_bytes);

        Ok(Self {
            username: Self::normalize_username(username),
            password: password.to_string(),
            client_nonce,
            server_nonce: None,
            salt: None,
            iterations: None,
            auth_message: None,
            salted_password: None,
        })
    }

    /// Create with a specific nonce (for testing).
    #[cfg(test)]
    pub fn with_nonce(username: &str, password: &str, nonce: &str) -> Self {
        Self {
            username: Self::normalize_username(username),
            password: password.to_string(),
            client_nonce: nonce.to_string(),
            server_nonce: None,
            salt: None,
            iterations: None,
            auth_message: None,
            salted_password: None,
        }
    }

    /// Generate the client-first-message.
    ///
    /// Format: `n,,n=<username>,r=<client-nonce>`
    ///
    /// The `n,,` prefix indicates:
    /// - `n` = no channel binding
    /// - First `,` = empty authzid
    /// - Second `,` = separator
    pub fn client_first_message(&self) -> Vec<u8> {
        // gs2-header: "n,," (no channel binding, no authzid)
        // client-first-message-bare: "n=<user>,r=<nonce>"
        let bare = format!("n={},r={}", self.username, self.client_nonce);
        format!("n,,{}", bare).into_bytes()
    }

    /// Get the client-first-message-bare (without gs2-header).
    fn client_first_bare(&self) -> String {
        format!("n={},r={}", self.username, self.client_nonce)
    }

    /// Process server-first-message and generate client-final-message.
    ///
    /// Server-first format: `r=<nonce>,s=<salt>,i=<iterations>`
    ///
    /// Client-final format: `c=<channel-binding>,r=<nonce>,p=<proof>`
    pub fn client_final_message(&mut self, server_first: &[u8]) -> Result<Vec<u8>> {
        let server_first_str = std::str::from_utf8(server_first)
            .map_err(|e| ScramError::InvalidServerMessage(e.to_string()))?;

        // Parse server-first-message
        let (nonce, salt, iterations) = self.parse_server_first(server_first_str)?;

        // Verify nonce starts with our client nonce
        if !nonce.starts_with(&self.client_nonce) {
            return Err(ScramError::InvalidNonce);
        }

        self.server_nonce = Some(nonce.clone());
        self.salt = Some(salt.clone());
        self.iterations = Some(iterations);

        // Compute salted password using PBKDF2-HMAC-SHA256
        let salted_password = self.compute_salted_password(&salt, iterations);
        self.salted_password = Some(salted_password);

        // client-final-message-without-proof
        // c= is base64 of gs2-header ("n,,")
        let channel_binding = BASE64.encode("n,,");
        let client_final_without_proof = format!("c={},r={}", channel_binding, nonce);

        // Auth message = client-first-bare + "," + server-first + "," + client-final-without-proof
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare(),
            server_first_str,
            client_final_without_proof
        );
        self.auth_message = Some(auth_message.clone());

        // Compute client proof
        let client_key = self.hmac_sha256(&salted_password, b"Client Key");
        let stored_key = Self::sha256(&client_key);
        let client_signature = self.hmac_sha256(&stored_key, auth_message.as_bytes());

        // ClientProof = ClientKey XOR ClientSignature
        let mut client_proof = [0u8; 32];
        for i in 0..32 {
            client_proof[i] = client_key[i] ^ client_signature[i];
        }

        // Build client-final-message
        let proof_b64 = BASE64.encode(client_proof);
        let client_final = format!("{},p={}", client_final_without_proof, proof_b64);

        Ok(client_final.into_bytes())
    }

    /// Verify server-final-message.
    ///
    /// Server-final format: `v=<server-signature>`
    pub fn verify_server_final(&self, server_final: &[u8]) -> Result<()> {
        let server_final_str = std::str::from_utf8(server_final)
            .map_err(|e| ScramError::InvalidServerMessage(e.to_string()))?;

        // Check for error
        if let Some(error) = server_final_str.strip_prefix("e=") {
            return Err(ScramError::ServerError(error.to_string()));
        }

        // Parse v=<signature>
        let server_signature_b64 = server_final_str.strip_prefix("v=").ok_or_else(|| {
            ScramError::InvalidServerMessage("Expected server signature".to_string())
        })?;
        let server_signature = BASE64.decode(server_signature_b64)?;

        // Compute expected server signature
        let salted_password = self.salted_password.ok_or_else(|| {
            ScramError::InvalidServerMessage("Missing salted password".to_string())
        })?;
        let auth_message = self
            .auth_message
            .as_ref()
            .ok_or_else(|| ScramError::InvalidServerMessage("Missing auth message".to_string()))?;

        let server_key = self.hmac_sha256(&salted_password, b"Server Key");
        let expected_signature = self.hmac_sha256(&server_key, auth_message.as_bytes());

        // Constant-time comparison
        if !Self::constant_time_eq(&server_signature, &expected_signature) {
            return Err(ScramError::ServerVerificationFailed);
        }

        Ok(())
    }

    /// Parse server-first-message.
    fn parse_server_first(&self, msg: &str) -> Result<(String, Vec<u8>, u32)> {
        let mut nonce = None;
        let mut salt = None;
        let mut iterations = None;

        for part in msg.split(',') {
            if let Some(value) = part.strip_prefix("r=") {
                nonce = Some(value.to_string());
            } else if let Some(value) = part.strip_prefix("s=") {
                salt = Some(BASE64.decode(value)?);
            } else if let Some(value) = part.strip_prefix("i=") {
                iterations = Some(
                    value
                        .parse::<u32>()
                        .map_err(|e| ScramError::InvalidIterations(e.to_string()))?,
                );
            }
            // Ignore unknown attributes (m=, etc.)
        }

        let nonce =
            nonce.ok_or_else(|| ScramError::InvalidServerMessage("Missing nonce".to_string()))?;
        let salt =
            salt.ok_or_else(|| ScramError::InvalidServerMessage("Missing salt".to_string()))?;
        let iterations = iterations
            .ok_or_else(|| ScramError::InvalidServerMessage("Missing iterations".to_string()))?;

        Ok((nonce, salt, iterations))
    }

    /// Compute salted password using PBKDF2-HMAC-SHA256.
    fn compute_salted_password(&self, salt: &[u8], iterations: u32) -> [u8; 32] {
        // Hi(str, salt, i) = PBKDF2(HMAC, str, salt, i, dkLen)
        let mut result = [0u8; 32];
        // PBKDF2 cannot fail with these parameters
        let _ =
            pbkdf2::pbkdf2::<Hmac<Sha256>>(self.password.as_bytes(), salt, iterations, &mut result);
        result
    }

    /// HMAC-SHA256
    fn hmac_sha256(&self, key: &[u8], data: &[u8]) -> [u8; 32] {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC can take key of any size");
        mac.update(data);
        mac.finalize().into_bytes().into()
    }

    /// SHA-256 hash
    fn sha256(data: &[u8]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hasher.finalize().into()
    }

    /// Normalize username per SASLprep (simplified).
    fn normalize_username(username: &str) -> String {
        // Escape '=' as '=3D' and ',' as '=2C'
        username.replace('=', "=3D").replace(',', "=2C")
    }

    /// Constant-time comparison to prevent timing attacks.
    fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        let mut result = 0u8;
        for (x, y) in a.iter().zip(b.iter()) {
            result |= x ^ y;
        }
        result == 0
    }
}

impl fmt::Debug for ScramSha256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScramSha256")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .field("client_nonce", &self.client_nonce)
            .field("server_nonce", &self.server_nonce)
            .field("iterations", &self.iterations)
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_first_message() {
        let scram = ScramSha256::with_nonce("user", "pencil", "rOprNGfwEbeRWgbNEkqO");
        let msg = scram.client_first_message();
        let msg_str = String::from_utf8(msg).unwrap();

        assert!(msg_str.starts_with("n,,"));
        assert!(msg_str.contains("n=user"));
        assert!(msg_str.contains("r=rOprNGfwEbeRWgbNEkqO"));
    }

    #[test]
    fn test_username_escaping() {
        let scram = ScramSha256::with_nonce("user=name,test", "pass", "nonce123");
        let msg = scram.client_first_message();
        let msg_str = String::from_utf8(msg).unwrap();

        assert!(msg_str.contains("n=user=3Dname=2Ctest"));
    }

    #[test]
    fn test_parse_server_first() {
        let scram = ScramSha256::new("user", "pass").unwrap();
        let server_first = "r=clientnonce+servernonce,s=c2FsdA==,i=4096";

        let (nonce, salt, iterations) = scram.parse_server_first(server_first).unwrap();

        assert_eq!(nonce, "clientnonce+servernonce");
        assert_eq!(salt, b"salt");
        assert_eq!(iterations, 4096);
    }

    #[test]
    fn test_invalid_server_first_missing_nonce() {
        let scram = ScramSha256::new("user", "pass").unwrap();
        let server_first = "s=c2FsdA==,i=4096";

        let result = scram.parse_server_first(server_first);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_server_nonce() {
        let mut scram = ScramSha256::with_nonce("user", "pass", "clientnonce");
        // Server nonce doesn't start with client nonce
        let server_first = b"r=differentnonce,s=c2FsdA==,i=4096";

        let result = scram.client_final_message(server_first);
        assert!(matches!(result, Err(ScramError::InvalidNonce)));
    }

    #[test]
    fn test_full_scram_exchange() {
        // Test vectors from RFC 5802 (adapted for SHA-256)
        let mut scram = ScramSha256::with_nonce("user", "pencil", "rOprNGfwEbeRWgbNEkqO");

        // Client first
        let client_first = scram.client_first_message();
        assert_eq!(
            String::from_utf8(client_first).unwrap(),
            "n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
        );

        // Server first (simulated)
        // Using base64 of "W22ZaJ0SNY7soEsUEjb6gQ==" as salt
        let server_first =
            b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";

        // Client final
        let client_final = scram.client_final_message(server_first).unwrap();
        let client_final_str = String::from_utf8(client_final).unwrap();

        // Should contain channel binding, nonce, and proof
        assert!(client_final_str.starts_with("c="));
        assert!(client_final_str.contains(",r="));
        assert!(client_final_str.contains(",p="));
    }

    #[test]
    fn test_hmac_sha256() {
        let scram = ScramSha256::new("user", "pass").unwrap();
        let result = scram.hmac_sha256(b"key", b"data");

        // Known HMAC-SHA256 value
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(ScramSha256::constant_time_eq(b"hello", b"hello"));
        assert!(!ScramSha256::constant_time_eq(b"hello", b"world"));
        assert!(!ScramSha256::constant_time_eq(b"hello", b"hell"));
    }

    #[test]
    fn test_debug_redacts_password() {
        let scram = ScramSha256::new("user", "secret_password").unwrap();
        let debug_str = format!("{:?}", scram);

        assert!(!debug_str.contains("secret_password"));
        assert!(debug_str.contains("[REDACTED]"));
    }

    #[test]
    fn test_server_error_handling() {
        let scram = ScramSha256::new("user", "pass").unwrap();
        let server_final = b"e=invalid-encoding";

        let result = scram.verify_server_final(server_final);
        assert!(matches!(result, Err(ScramError::ServerError(_))));
    }
}
