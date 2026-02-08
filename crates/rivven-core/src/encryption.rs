//! Encryption at rest for Rivven data
//!
//! This module provides transparent encryption for WAL files, segments, and other
//! persistent data. It uses AES-256-GCM for authenticated encryption with per-record
//! nonces derived from LSN to ensure unique encryption contexts.
//!
//! # Features
//!
//! - **AES-256-GCM** authenticated encryption with 12-byte nonces
//! - **Key derivation** using HKDF-SHA256 from master key
//! - **Key rotation** support with key versioning
//! - **Envelope encryption** for data keys encrypted by master key
//! - **Zero-copy decryption** where possible
//!
//! # Security Properties
//!
//! - Confidentiality: AES-256 encryption
//! - Integrity: GCM authentication tag (16 bytes)
//! - Replay protection: LSN-derived nonces prevent nonce reuse
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_core::encryption::{EncryptionConfig, KeyProvider, EncryptionManager};
//!
//! // Create encryption config
//! let config = EncryptionConfig::new()
//!     .with_algorithm(Algorithm::Aes256Gcm)
//!     .with_key_provider(KeyProvider::File("/path/to/master.key".into()));
//!
//! // Initialize encryption manager
//! let manager = EncryptionManager::new(config).await?;
//!
//! // Encrypt data
//! let ciphertext = manager.encrypt(b"sensitive data", 12345)?;
//!
//! // Decrypt data  
//! let plaintext = manager.decrypt(&ciphertext, 12345)?;
//! ```

use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, CHACHA20_POLY1305};
use ring::hkdf::{Salt, HKDF_SHA256};
use ring::rand::{SecureRandom, SystemRandom};
use secrecy::{ExposeSecret, SecretBox};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use thiserror::Error;

/// AES-256-GCM nonce size in bytes
const NONCE_SIZE: usize = 12;

/// AES-256-GCM authentication tag size in bytes
const TAG_SIZE: usize = 16;

/// AES-256 key size in bytes
const KEY_SIZE: usize = 32;

/// Encryption header magic bytes
const ENCRYPTION_MAGIC: [u8; 4] = [0x52, 0x56, 0x45, 0x4E]; // "RVEN"

/// Current encryption format version
const FORMAT_VERSION: u8 = 1;

/// Encryption errors
#[derive(Debug, Error)]
pub enum EncryptionError {
    #[error("key provider error: {0}")]
    KeyProvider(String),

    #[error("encryption failed: {0}")]
    Encryption(String),

    #[error("decryption failed: {0}")]
    Decryption(String),

    #[error("invalid key: {0}")]
    InvalidKey(String),

    #[error("key rotation error: {0}")]
    KeyRotation(String),

    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("invalid format: {0}")]
    InvalidFormat(String),

    #[error("unsupported version: {0}")]
    UnsupportedVersion(u8),

    #[error("key not found: version {0}")]
    KeyNotFound(u32),
}

/// Result type for encryption operations
pub type Result<T> = std::result::Result<T, EncryptionError>;

/// Encryption algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Algorithm {
    /// AES-256-GCM (recommended)
    #[default]
    Aes256Gcm,
    /// ChaCha20-Poly1305 (alternative for systems without AES-NI)
    ChaCha20Poly1305,
}

impl Algorithm {
    /// Get the key size for this algorithm
    pub fn key_size(&self) -> usize {
        match self {
            Algorithm::Aes256Gcm => 32,
            Algorithm::ChaCha20Poly1305 => 32,
        }
    }

    /// Get the nonce size for this algorithm
    pub fn nonce_size(&self) -> usize {
        match self {
            Algorithm::Aes256Gcm => 12,
            Algorithm::ChaCha20Poly1305 => 12,
        }
    }

    /// Get the tag size for this algorithm
    pub fn tag_size(&self) -> usize {
        match self {
            Algorithm::Aes256Gcm => 16,
            Algorithm::ChaCha20Poly1305 => 16,
        }
    }
}

/// Key provider for encryption keys
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum KeyProvider {
    /// Read key from a file (hex-encoded or raw 32 bytes)
    File { path: PathBuf },

    /// Key from environment variable (hex-encoded)
    Environment { variable: String },

    // Future: AWS KMS key (feature = "aws-kms")
    // Future: HashiCorp Vault (feature = "vault")
    /// In-memory key (for testing only)
    #[serde(skip)]
    InMemory(#[serde(skip)] Vec<u8>),
}

impl Default for KeyProvider {
    fn default() -> Self {
        KeyProvider::Environment {
            variable: "RIVVEN_ENCRYPTION_KEY".to_string(),
        }
    }
}

/// Encryption configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    /// Whether encryption is enabled
    #[serde(default)]
    pub enabled: bool,

    /// Encryption algorithm
    #[serde(default)]
    pub algorithm: Algorithm,

    /// Key provider
    #[serde(default)]
    pub key_provider: KeyProvider,

    /// Key rotation interval in days (0 = no rotation)
    #[serde(default)]
    pub key_rotation_days: u32,

    /// Additional authenticated data scope
    #[serde(default = "default_aad_scope")]
    pub aad_scope: String,
}

fn default_aad_scope() -> String {
    "rivven".to_string()
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            algorithm: Algorithm::default(),
            key_provider: KeyProvider::default(),
            key_rotation_days: 0,
            aad_scope: default_aad_scope(),
        }
    }
}

impl EncryptionConfig {
    /// Create a new encryption config with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable encryption
    pub fn enabled(mut self) -> Self {
        self.enabled = true;
        self
    }

    /// Set the encryption algorithm
    pub fn with_algorithm(mut self, algorithm: Algorithm) -> Self {
        self.algorithm = algorithm;
        self
    }

    /// Set the key provider
    pub fn with_key_provider(mut self, provider: KeyProvider) -> Self {
        self.key_provider = provider;
        self
    }

    /// Set key rotation interval
    pub fn with_key_rotation_days(mut self, days: u32) -> Self {
        self.key_rotation_days = days;
        self
    }
}

/// Encrypted data header
///
/// Format:
/// - Magic (4 bytes): "RVEN"
/// - Version (1 byte)
/// - Algorithm (1 byte)
/// - Key version (4 bytes)
/// - Nonce (12 bytes)
/// - Reserved (2 bytes)
/// - Ciphertext + Tag (variable)
#[derive(Debug, Clone)]
pub struct EncryptedHeader {
    pub version: u8,
    pub algorithm: Algorithm,
    pub key_version: u32,
    pub nonce: [u8; NONCE_SIZE],
}

impl EncryptedHeader {
    /// Header size in bytes
    pub const SIZE: usize = 24;

    /// Create a new header
    pub fn new(algorithm: Algorithm, key_version: u32, nonce: [u8; NONCE_SIZE]) -> Self {
        Self {
            version: FORMAT_VERSION,
            algorithm,
            key_version,
            nonce,
        }
    }

    /// Serialize header to bytes
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..4].copy_from_slice(&ENCRYPTION_MAGIC);
        buf[4] = self.version;
        buf[5] = self.algorithm as u8;
        buf[6..10].copy_from_slice(&self.key_version.to_be_bytes());
        buf[10..22].copy_from_slice(&self.nonce);
        // bytes 22-23 reserved
        buf
    }

    /// Parse header from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < Self::SIZE {
            return Err(EncryptionError::InvalidFormat(format!(
                "header too short: {} < {}",
                data.len(),
                Self::SIZE
            )));
        }

        // Check magic
        if data[0..4] != ENCRYPTION_MAGIC {
            return Err(EncryptionError::InvalidFormat("invalid magic bytes".into()));
        }

        let version = data[4];
        if version != FORMAT_VERSION {
            return Err(EncryptionError::UnsupportedVersion(version));
        }

        let algorithm = match data[5] {
            0 => Algorithm::Aes256Gcm,
            1 => Algorithm::ChaCha20Poly1305,
            v => {
                return Err(EncryptionError::InvalidFormat(format!(
                    "unknown algorithm: {}",
                    v
                )))
            }
        };

        let key_version = u32::from_be_bytes([data[6], data[7], data[8], data[9]]);
        let mut nonce = [0u8; NONCE_SIZE];
        nonce.copy_from_slice(&data[10..22]);

        Ok(Self {
            version,
            algorithm,
            key_version,
            nonce,
        })
    }
}

/// Master key wrapper with secure memory handling
pub struct MasterKey {
    key: SecretBox<[u8; KEY_SIZE]>,
    version: u32,
}

impl MasterKey {
    /// Create a new master key from raw bytes
    pub fn new(key: Vec<u8>, version: u32) -> Result<Self> {
        if key.len() != KEY_SIZE {
            return Err(EncryptionError::InvalidKey(format!(
                "key must be {} bytes, got {}",
                KEY_SIZE,
                key.len()
            )));
        }
        let mut key_array = [0u8; KEY_SIZE];
        key_array.copy_from_slice(&key);
        Ok(Self {
            key: SecretBox::new(Box::new(key_array)),
            version,
        })
    }

    /// Generate a new random master key
    pub fn generate(version: u32) -> Result<Self> {
        let rng = SystemRandom::new();
        let mut key = vec![0u8; KEY_SIZE];
        rng.fill(&mut key)
            .map_err(|_| EncryptionError::KeyProvider("failed to generate random key".into()))?;
        Self::new(key, version)
    }

    /// Load master key from provider
    pub fn from_provider(provider: &KeyProvider) -> Result<Self> {
        match provider {
            KeyProvider::File { path } => {
                let data = fs::read(path)?;
                let key = if data.len() == KEY_SIZE {
                    // Raw binary key
                    data
                } else {
                    // Try hex-encoded
                    let hex_str = String::from_utf8(data)
                        .map_err(|_| EncryptionError::InvalidKey("invalid key file format".into()))?
                        .trim()
                        .to_string();
                    hex::decode(&hex_str).map_err(|e| {
                        EncryptionError::InvalidKey(format!("invalid hex key: {}", e))
                    })?
                };
                Self::new(key, 1)
            }
            KeyProvider::Environment { variable } => {
                let hex_key = std::env::var(variable).map_err(|_| {
                    EncryptionError::KeyProvider(format!(
                        "environment variable '{}' not set",
                        variable
                    ))
                })?;
                let key = hex::decode(hex_key.trim()).map_err(|e| {
                    EncryptionError::InvalidKey(format!("invalid hex key in env var: {}", e))
                })?;
                Self::new(key, 1)
            }
            KeyProvider::InMemory(key) => Self::new(key.clone(), 1),
            #[allow(unreachable_patterns)]
            _ => Err(EncryptionError::KeyProvider(
                "unsupported key provider".into(),
            )),
        }
    }

    /// Get key version
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Derive a data encryption key using HKDF
    fn derive_data_key(&self, info: &[u8]) -> Result<[u8; KEY_SIZE]> {
        let salt = Salt::new(HKDF_SHA256, b"rivven-encryption-v1");
        let prk = salt.extract(self.key.expose_secret());
        let info_refs = [info];
        let okm = prk
            .expand(&info_refs, DataKeyLen)
            .map_err(|_| EncryptionError::Encryption("key derivation failed".into()))?;

        let mut data_key = [0u8; KEY_SIZE];
        okm.fill(&mut data_key)
            .map_err(|_| EncryptionError::Encryption("key expansion failed".into()))?;
        Ok(data_key)
    }
}

impl fmt::Debug for MasterKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MasterKey")
            .field("version", &self.version)
            .field("key", &"[REDACTED]")
            .finish()
    }
}

/// HKDF output length marker
struct DataKeyLen;

impl ring::hkdf::KeyType for DataKeyLen {
    fn len(&self) -> usize {
        KEY_SIZE
    }
}

/// Encryption manager for transparent encryption at rest
pub struct EncryptionManager {
    config: EncryptionConfig,
    master_key: MasterKey,
    /// Current key for encryption (latest version)
    data_key: LessSafeKey,
    /// All known keys indexed by version (for decrypting old data)
    key_store: parking_lot::RwLock<HashMap<u32, LessSafeKey>>,
    rng: SystemRandom,
    current_key_version: AtomicU32,
}

/// Select the ring AEAD algorithm based on our Algorithm enum
fn ring_algorithm(algo: Algorithm) -> &'static ring::aead::Algorithm {
    match algo {
        Algorithm::Aes256Gcm => &AES_256_GCM,
        Algorithm::ChaCha20Poly1305 => &CHACHA20_POLY1305,
    }
}

impl EncryptionManager {
    /// Create a new encryption manager
    pub fn new(config: EncryptionConfig) -> Result<Arc<Self>> {
        let master_key = MasterKey::from_provider(&config.key_provider)?;
        let data_key_bytes = master_key.derive_data_key(config.aad_scope.as_bytes())?;

        let algo = ring_algorithm(config.algorithm);
        let unbound_key = UnboundKey::new(algo, &data_key_bytes)
            .map_err(|_| EncryptionError::InvalidKey("failed to create encryption key".into()))?;
        let data_key = LessSafeKey::new(unbound_key);

        // Populate the key store with the initial key version
        let version = master_key.version();
        let store_unbound = UnboundKey::new(algo, &data_key_bytes)
            .map_err(|_| EncryptionError::InvalidKey("failed to create store key".into()))?;
        let mut key_store = HashMap::new();
        key_store.insert(version, LessSafeKey::new(store_unbound));

        Ok(Arc::new(Self {
            config,
            current_key_version: AtomicU32::new(version),
            master_key,
            data_key,
            key_store: parking_lot::RwLock::new(key_store),
            rng: SystemRandom::new(),
        }))
    }

    /// Rotate the data encryption key, deriving a new key from a new master key.
    /// Old keys are retained in the key store so existing data can still be decrypted.
    pub fn rotate_key(&self, new_master: MasterKey) -> Result<()> {
        let new_version = new_master.version();
        if new_version <= self.current_key_version.load(Ordering::Relaxed) {
            return Err(EncryptionError::KeyRotation(
                "new key version must be greater than current".into(),
            ));
        }

        let data_key_bytes = new_master.derive_data_key(self.config.aad_scope.as_bytes())?;
        let algo = ring_algorithm(self.config.algorithm);

        let new_key = UnboundKey::new(algo, &data_key_bytes)
            .map_err(|_| EncryptionError::KeyRotation("failed to create new key".into()))?;

        // Add to key store
        {
            let mut store = self.key_store.write();
            store.insert(new_version, LessSafeKey::new(new_key));
        }

        // Atomically bump the current version — new encryptions will use this version
        self.current_key_version
            .store(new_version, Ordering::Release);

        Ok(())
    }

    /// Get a key by version for decryption
    fn get_key_for_version(&self, version: u32) -> Result<()> {
        let store = self.key_store.read();
        if store.contains_key(&version) {
            Ok(())
        } else {
            Err(EncryptionError::KeyNotFound(version))
        }
    }

    /// Create a no-op encryption manager for when encryption is disabled
    pub fn disabled() -> Arc<DisabledEncryption> {
        Arc::new(DisabledEncryption)
    }

    /// Check if encryption is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get current key version
    pub fn key_version(&self) -> u32 {
        self.current_key_version.load(Ordering::Relaxed)
    }

    /// Generate a nonce from LSN (deterministic but unique per record)
    fn generate_nonce(&self, lsn: u64) -> [u8; NONCE_SIZE] {
        let mut nonce = [0u8; NONCE_SIZE];
        // Use LSN as 8 bytes + 4 random bytes for uniqueness
        nonce[0..8].copy_from_slice(&lsn.to_be_bytes());
        self.rng.fill(&mut nonce[8..12]).ok();
        nonce
    }

    /// Encrypt data with associated LSN for nonce derivation
    pub fn encrypt(&self, plaintext: &[u8], lsn: u64) -> Result<Vec<u8>> {
        let nonce_bytes = self.generate_nonce(lsn);
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);

        let header = EncryptedHeader::new(
            self.config.algorithm,
            self.current_key_version.load(Ordering::Relaxed),
            nonce_bytes,
        );

        // Allocate buffer: header + plaintext + tag
        let mut output = Vec::with_capacity(EncryptedHeader::SIZE + plaintext.len() + TAG_SIZE);
        output.extend_from_slice(&header.to_bytes());
        output.extend_from_slice(plaintext);

        // Encrypt in-place using separate tag method
        let ciphertext_start = EncryptedHeader::SIZE;
        let tag = self
            .data_key
            .seal_in_place_separate_tag(
                nonce,
                Aad::from(self.config.aad_scope.as_bytes()),
                &mut output[ciphertext_start..],
            )
            .map_err(|_| EncryptionError::Encryption("seal failed".into()))?;

        // Append the authentication tag
        output.extend_from_slice(tag.as_ref());

        Ok(output)
    }

    /// Decrypt data
    pub fn decrypt(&self, ciphertext: &[u8], _lsn: u64) -> Result<Vec<u8>> {
        if ciphertext.len() < EncryptedHeader::SIZE + TAG_SIZE {
            return Err(EncryptionError::InvalidFormat(
                "ciphertext too short".into(),
            ));
        }

        let header = EncryptedHeader::from_bytes(ciphertext)?;

        // Look up key by version from the key store (supports rotated keys)
        self.get_key_for_version(header.key_version)?;

        let nonce = Nonce::assume_unique_for_key(header.nonce);

        // Select the correct algorithm from the header (supports cross-algorithm decrypt)
        let algo = ring_algorithm(header.algorithm);

        // Copy ciphertext + tag for in-place decryption
        let mut buffer = ciphertext[EncryptedHeader::SIZE..].to_vec();

        // We need to use the key for this specific version
        let store = self.key_store.read();
        let key = store
            .get(&header.key_version)
            .ok_or(EncryptionError::KeyNotFound(header.key_version))?;

        // Verify the algorithm matches the key
        if *key.algorithm() != *algo {
            // Key was created with a different algorithm — re-derive
            drop(store);
            // For cross-algorithm decrypt: derive key bytes from master, create a temp key
            let data_key_bytes = self
                .master_key
                .derive_data_key(self.config.aad_scope.as_bytes())?;
            let unbound = UnboundKey::new(algo, &data_key_bytes)
                .map_err(|_| EncryptionError::Decryption("key re-derive failed".into()))?;
            let temp_key = LessSafeKey::new(unbound);
            let plaintext = temp_key
                .open_in_place(
                    nonce,
                    Aad::from(self.config.aad_scope.as_bytes()),
                    &mut buffer,
                )
                .map_err(|_| EncryptionError::Decryption("authentication failed".into()))?;
            return Ok(plaintext.to_vec());
        }

        let plaintext = key
            .open_in_place(
                nonce,
                Aad::from(self.config.aad_scope.as_bytes()),
                &mut buffer,
            )
            .map_err(|_| EncryptionError::Decryption("authentication failed".into()))?;

        Ok(plaintext.to_vec())
    }

    /// Calculate encrypted size for given plaintext size
    pub fn encrypted_size(&self, plaintext_len: usize) -> usize {
        EncryptedHeader::SIZE + plaintext_len + TAG_SIZE
    }
}

impl fmt::Debug for EncryptionManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptionManager")
            .field("enabled", &self.config.enabled)
            .field("algorithm", &self.config.algorithm)
            .field("key_version", &self.key_version())
            .finish()
    }
}

/// Placeholder for disabled encryption
#[derive(Debug)]
pub struct DisabledEncryption;

impl DisabledEncryption {
    /// Pass-through "encrypt" (just returns the data as-is)
    pub fn encrypt(&self, plaintext: &[u8], _lsn: u64) -> Result<Vec<u8>> {
        Ok(plaintext.to_vec())
    }

    /// Pass-through "decrypt" (just returns the data as-is)
    pub fn decrypt(&self, ciphertext: &[u8], _lsn: u64) -> Result<Vec<u8>> {
        Ok(ciphertext.to_vec())
    }

    /// No overhead when disabled
    pub fn encrypted_size(&self, plaintext_len: usize) -> usize {
        plaintext_len
    }

    pub fn is_enabled(&self) -> bool {
        false
    }
}

/// Trait for encryption operations (allows dynamic dispatch)
pub trait Encryptor: Send + Sync + std::fmt::Debug {
    fn encrypt(&self, plaintext: &[u8], lsn: u64) -> Result<Vec<u8>>;
    fn decrypt(&self, ciphertext: &[u8], lsn: u64) -> Result<Vec<u8>>;
    fn encrypted_size(&self, plaintext_len: usize) -> usize;
    fn is_enabled(&self) -> bool;
}

impl Encryptor for EncryptionManager {
    fn encrypt(&self, plaintext: &[u8], lsn: u64) -> Result<Vec<u8>> {
        self.encrypt(plaintext, lsn)
    }

    fn decrypt(&self, ciphertext: &[u8], lsn: u64) -> Result<Vec<u8>> {
        self.decrypt(ciphertext, lsn)
    }

    fn encrypted_size(&self, plaintext_len: usize) -> usize {
        self.encrypted_size(plaintext_len)
    }

    fn is_enabled(&self) -> bool {
        self.is_enabled()
    }
}

impl Encryptor for DisabledEncryption {
    fn encrypt(&self, plaintext: &[u8], lsn: u64) -> Result<Vec<u8>> {
        self.encrypt(plaintext, lsn)
    }

    fn decrypt(&self, ciphertext: &[u8], lsn: u64) -> Result<Vec<u8>> {
        self.decrypt(ciphertext, lsn)
    }

    fn encrypted_size(&self, plaintext_len: usize) -> usize {
        self.encrypted_size(plaintext_len)
    }

    fn is_enabled(&self) -> bool {
        false
    }
}

/// Generate a new encryption key and save to file
pub fn generate_key_file(path: &std::path::Path) -> Result<()> {
    let key = MasterKey::generate(1)?;
    let hex_key = hex::encode(key.key.expose_secret());
    fs::write(path, hex_key)?;

    // Set restrictive permissions on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path)?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(path, perms)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> EncryptionConfig {
        let key = vec![0u8; 32]; // Test key
        EncryptionConfig {
            enabled: true,
            algorithm: Algorithm::Aes256Gcm,
            key_provider: KeyProvider::InMemory(key),
            key_rotation_days: 0,
            aad_scope: "test".to_string(),
        }
    }

    #[test]
    fn test_encrypt_decrypt() {
        let manager = EncryptionManager::new(test_config()).unwrap();
        let plaintext = b"Hello, World! This is sensitive data.";
        let lsn = 12345u64;

        let ciphertext = manager.encrypt(plaintext, lsn).unwrap();
        assert_ne!(ciphertext.as_slice(), plaintext);
        assert!(ciphertext.len() > plaintext.len());

        let decrypted = manager.decrypt(&ciphertext, lsn).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encrypted_size() {
        let manager = EncryptionManager::new(test_config()).unwrap();
        let plaintext_len = 1000;

        let expected = EncryptedHeader::SIZE + plaintext_len + TAG_SIZE;
        assert_eq!(manager.encrypted_size(plaintext_len), expected);
    }

    #[test]
    fn test_header_roundtrip() {
        let nonce = [1u8; NONCE_SIZE];
        let header = EncryptedHeader::new(Algorithm::Aes256Gcm, 42, nonce);

        let bytes = header.to_bytes();
        let parsed = EncryptedHeader::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.version, header.version);
        assert_eq!(parsed.algorithm, header.algorithm);
        assert_eq!(parsed.key_version, header.key_version);
        assert_eq!(parsed.nonce, header.nonce);
    }

    #[test]
    fn test_invalid_ciphertext() {
        let manager = EncryptionManager::new(test_config()).unwrap();

        // Too short
        let result = manager.decrypt(&[0u8; 10], 1);
        assert!(result.is_err());

        // Invalid magic
        let mut bad_magic = vec![0u8; 100];
        let result = manager.decrypt(&bad_magic, 1);
        assert!(result.is_err());

        // Valid header but tampered ciphertext
        bad_magic[0..4].copy_from_slice(&ENCRYPTION_MAGIC);
        bad_magic[4] = FORMAT_VERSION;
        let result = manager.decrypt(&bad_magic, 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_tamper_detection() {
        let manager = EncryptionManager::new(test_config()).unwrap();
        let plaintext = b"Sensitive data that must not be tampered with";

        let mut ciphertext = manager.encrypt(plaintext, 1).unwrap();

        // Tamper with ciphertext (flip a bit in the data portion)
        let tamper_pos = EncryptedHeader::SIZE + 10;
        ciphertext[tamper_pos] ^= 0x01;

        // Decryption should fail due to authentication
        let result = manager.decrypt(&ciphertext, 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_different_lsns_produce_different_ciphertexts() {
        let manager = EncryptionManager::new(test_config()).unwrap();
        let plaintext = b"Same plaintext";

        let ct1 = manager.encrypt(plaintext, 1).unwrap();
        let ct2 = manager.encrypt(plaintext, 2).unwrap();

        // Same plaintext should produce different ciphertexts for different LSNs
        assert_ne!(ct1, ct2);

        // Both should decrypt correctly
        assert_eq!(manager.decrypt(&ct1, 1).unwrap(), plaintext);
        assert_eq!(manager.decrypt(&ct2, 2).unwrap(), plaintext);
    }

    #[test]
    fn test_disabled_encryption_passthrough() {
        let disabled = DisabledEncryption;
        let plaintext = b"Not encrypted";

        let encrypted = disabled.encrypt(plaintext, 1).unwrap();
        assert_eq!(&encrypted[..], plaintext);

        let decrypted = disabled.decrypt(plaintext, 1).unwrap();
        assert_eq!(&decrypted[..], plaintext);

        assert_eq!(disabled.encrypted_size(100), 100);
        assert!(!disabled.is_enabled());
    }

    #[test]
    fn test_master_key_validation() {
        // Wrong key size
        let result = MasterKey::new(vec![0u8; 16], 1);
        assert!(result.is_err());

        // Correct size
        let result = MasterKey::new(vec![0u8; 32], 1);
        assert!(result.is_ok());
    }

    #[test]
    fn test_key_derivation_consistency() {
        let key = MasterKey::new(vec![42u8; 32], 1).unwrap();

        let dk1 = key.derive_data_key(b"scope1").unwrap();
        let dk2 = key.derive_data_key(b"scope1").unwrap();
        let dk3 = key.derive_data_key(b"scope2").unwrap();

        // Same scope should derive same key
        assert_eq!(dk1, dk2);

        // Different scopes should derive different keys
        assert_ne!(dk1, dk3);
    }

    #[test]
    fn test_large_data_encryption() {
        let manager = EncryptionManager::new(test_config()).unwrap();

        // Test with 1MB of data
        let plaintext: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

        let ciphertext = manager.encrypt(&plaintext, 999999).unwrap();
        let decrypted = manager.decrypt(&ciphertext, 999999).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_generate_key() {
        let key = MasterKey::generate(1).unwrap();
        assert_eq!(key.version(), 1);
    }

    #[test]
    fn test_chacha20_poly1305_encrypt_decrypt() {
        let config = EncryptionConfig {
            enabled: true,
            algorithm: Algorithm::ChaCha20Poly1305,
            key_provider: KeyProvider::InMemory(vec![0u8; 32]),
            key_rotation_days: 0,
            aad_scope: "test".to_string(),
        };
        let manager = EncryptionManager::new(config).unwrap();
        let plaintext = b"ChaCha20-Poly1305 test payload";
        let lsn = 42u64;

        let ciphertext = manager.encrypt(plaintext, lsn).unwrap();
        assert_ne!(ciphertext.as_slice(), plaintext.as_slice());

        // Verify header encodes ChaCha20
        let header = EncryptedHeader::from_bytes(&ciphertext).unwrap();
        assert_eq!(header.algorithm, Algorithm::ChaCha20Poly1305);

        let decrypted = manager.decrypt(&ciphertext, lsn).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_key_rotation() {
        let config = EncryptionConfig {
            enabled: true,
            algorithm: Algorithm::Aes256Gcm,
            key_provider: KeyProvider::InMemory(vec![1u8; 32]),
            key_rotation_days: 30,
            aad_scope: "test".to_string(),
        };
        let manager = EncryptionManager::new(config).unwrap();

        // Encrypt with version 1
        let plaintext = b"data encrypted with key v1";
        let ct_v1 = manager.encrypt(plaintext, 100).unwrap();
        assert_eq!(manager.key_version(), 1);

        // Rotate to version 2
        let new_master = MasterKey::new(vec![2u8; 32], 2).unwrap();
        manager.rotate_key(new_master).unwrap();
        assert_eq!(manager.key_version(), 2);

        // Old ciphertext (v1) still decryptable
        let decrypted = manager.decrypt(&ct_v1, 100).unwrap();
        assert_eq!(decrypted, plaintext);

        // Rotation with same or lower version should fail
        let bad_master = MasterKey::new(vec![3u8; 32], 1).unwrap();
        assert!(manager.rotate_key(bad_master).is_err());
    }
}
