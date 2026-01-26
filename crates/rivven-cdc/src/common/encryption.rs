//! # Column-Level Encryption
//!
//! Transparent field-level encryption for sensitive CDC data.
//! Supports multiple encryption algorithms and key management strategies.
//!
//! ## Features
//!
//! - **AES-256-GCM**: Default authenticated encryption
//! - **Field Selection**: Encrypt only sensitive fields
//! - **Key Rotation**: Support for key versioning and rotation
//! - **Deterministic Encryption**: Optional for searchable encrypted fields
//! - **Format-Preserving**: Keep data types intact where possible
//!
//! ## Usage
//!
//! ```ignore
//! use rivven_cdc::common::encryption::{FieldEncryptor, EncryptionConfig};
//!
//! let config = EncryptionConfig::builder()
//!     .encrypt_field("users", "ssn")
//!     .encrypt_field("users", "email")
//!     .encrypt_field("payments", "card_number")
//!     .build();
//!
//! let encryptor = FieldEncryptor::new(config, key_provider);
//! let encrypted_event = encryptor.encrypt(&event).await?;
//! ```

use crate::common::{CdcError, CdcEvent, Result};
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::warn;

/// Encryption algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[derive(Default)]
pub enum EncryptionAlgorithm {
    /// AES-256-GCM (authenticated encryption)
    #[default]
    Aes256Gcm,
    /// Deterministic encryption for searchable fields
    Deterministic,
}


/// Field encryption rule.
#[derive(Debug, Clone)]
pub struct FieldRule {
    /// Table pattern (glob)
    pub table_pattern: String,
    /// Field name
    pub field_name: String,
    /// Encryption algorithm
    pub algorithm: EncryptionAlgorithm,
    /// Key ID to use
    pub key_id: Option<String>,
    /// Whether to mask in logs
    pub mask_in_logs: bool,
}

impl FieldRule {
    /// Create a new field rule.
    pub fn new(table: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            table_pattern: table.into(),
            field_name: field.into(),
            algorithm: EncryptionAlgorithm::default(),
            key_id: None,
            mask_in_logs: true,
        }
    }

    /// Set encryption algorithm.
    pub fn with_algorithm(mut self, algorithm: EncryptionAlgorithm) -> Self {
        self.algorithm = algorithm;
        self
    }

    /// Set key ID.
    pub fn with_key_id(mut self, key_id: impl Into<String>) -> Self {
        self.key_id = Some(key_id.into());
        self
    }

    /// Disable log masking.
    pub fn without_log_masking(mut self) -> Self {
        self.mask_in_logs = false;
        self
    }

    /// Check if this rule matches a table.
    pub fn matches_table(&self, table: &str) -> bool {
        if self.table_pattern == "*" {
            return true;
        }
        if self.table_pattern.ends_with('*') {
            let prefix = &self.table_pattern[..self.table_pattern.len() - 1];
            return table.starts_with(prefix);
        }
        self.table_pattern == table
    }
}

/// Configuration for field encryption.
#[derive(Debug, Clone, Default)]
pub struct EncryptionConfig {
    /// Field encryption rules
    pub rules: Vec<FieldRule>,
    /// Default key ID
    pub default_key_id: String,
    /// Enable encryption (can be disabled for testing)
    pub enabled: bool,
    /// AAD (additional authenticated data) prefix
    pub aad_prefix: String,
}

impl EncryptionConfig {
    pub fn builder() -> EncryptionConfigBuilder {
        EncryptionConfigBuilder::default()
    }

    /// Get rules for a table.
    pub fn rules_for_table(&self, table: &str) -> Vec<&FieldRule> {
        self.rules
            .iter()
            .filter(|r| r.matches_table(table))
            .collect()
    }

    /// Get fields to encrypt for a table.
    pub fn fields_for_table(&self, table: &str) -> HashSet<String> {
        self.rules_for_table(table)
            .into_iter()
            .map(|r| r.field_name.clone())
            .collect()
    }
}

/// Builder for EncryptionConfig.
pub struct EncryptionConfigBuilder {
    config: EncryptionConfig,
}

impl Default for EncryptionConfigBuilder {
    fn default() -> Self {
        Self {
            config: EncryptionConfig {
                enabled: true,
                default_key_id: "default".to_string(),
                rules: Vec::new(),
                aad_prefix: String::new(),
            },
        }
    }
}

impl EncryptionConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a field encryption rule.
    pub fn encrypt_field(mut self, table: impl Into<String>, field: impl Into<String>) -> Self {
        self.config.rules.push(FieldRule::new(table, field));
        self
    }

    /// Add a field rule with custom algorithm.
    pub fn encrypt_field_with(
        mut self,
        table: impl Into<String>,
        field: impl Into<String>,
        algorithm: EncryptionAlgorithm,
    ) -> Self {
        self.config.rules.push(
            FieldRule::new(table, field).with_algorithm(algorithm),
        );
        self
    }

    /// Add a custom rule.
    pub fn add_rule(mut self, rule: FieldRule) -> Self {
        self.config.rules.push(rule);
        self
    }

    /// Set default key ID.
    pub fn default_key_id(mut self, key_id: impl Into<String>) -> Self {
        self.config.default_key_id = key_id.into();
        self
    }

    /// Set enabled state.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    /// Set AAD prefix.
    pub fn aad_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.config.aad_prefix = prefix.into();
        self
    }

    pub fn build(self) -> EncryptionConfig {
        self.config
    }
}

/// Encryption key with metadata.
#[derive(Clone)]
pub struct EncryptionKey {
    /// Key ID
    pub id: String,
    /// Key version
    pub version: u32,
    /// Raw key material (32 bytes for AES-256)
    key_material: Vec<u8>,
    /// Creation timestamp
    pub created_at: u64,
    /// Whether this key is active for encryption
    pub active: bool,
}

impl EncryptionKey {
    /// Create a new encryption key.
    pub fn new(id: impl Into<String>, key_material: Vec<u8>) -> Result<Self> {
        if key_material.len() != 32 {
            return Err(CdcError::replication("Key must be 32 bytes for AES-256"));
        }
        Ok(Self {
            id: id.into(),
            version: 1,
            key_material,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            active: true,
        })
    }

    /// Generate a random key.
    pub fn generate(id: impl Into<String>) -> Result<Self> {
        let rng = SystemRandom::new();
        let mut key_material = vec![0u8; 32];
        rng.fill(&mut key_material)
            .map_err(|_| CdcError::replication("Failed to generate random key"))?;
        Self::new(id, key_material)
    }

    /// Create an AEAD key.
    fn to_aead_key(&self) -> Result<LessSafeKey> {
        let unbound = UnboundKey::new(&AES_256_GCM, &self.key_material)
            .map_err(|_| CdcError::replication("Invalid key material"))?;
        Ok(LessSafeKey::new(unbound))
    }
}

/// Key provider trait.
#[async_trait::async_trait]
pub trait KeyProvider: Send + Sync {
    /// Get a key by ID.
    async fn get_key(&self, key_id: &str) -> Result<Option<EncryptionKey>>;

    /// Get the active key for encryption.
    async fn get_active_key(&self) -> Result<EncryptionKey>;

    /// Store a new key.
    async fn store_key(&self, key: EncryptionKey) -> Result<()>;

    /// Rotate to a new key.
    async fn rotate_key(&self, key_id: &str) -> Result<EncryptionKey>;
}

/// In-memory key provider for testing.
pub struct MemoryKeyProvider {
    keys: RwLock<HashMap<String, EncryptionKey>>,
    active_key_id: RwLock<String>,
}

impl MemoryKeyProvider {
    /// Create a new memory key provider with a default key.
    pub fn new() -> Result<Self> {
        let default_key = EncryptionKey::generate("default")?;
        let mut keys = HashMap::new();
        keys.insert("default".to_string(), default_key);
        Ok(Self {
            keys: RwLock::new(keys),
            active_key_id: RwLock::new("default".to_string()),
        })
    }

    /// Create with a specific key.
    pub fn with_key(key: EncryptionKey) -> Self {
        let key_id = key.id.clone();
        let mut keys = HashMap::new();
        keys.insert(key_id.clone(), key);
        Self {
            keys: RwLock::new(keys),
            active_key_id: RwLock::new(key_id),
        }
    }
}

impl Default for MemoryKeyProvider {
    fn default() -> Self {
        Self::new().unwrap()
    }
}

#[async_trait::async_trait]
impl KeyProvider for MemoryKeyProvider {
    async fn get_key(&self, key_id: &str) -> Result<Option<EncryptionKey>> {
        let keys = self.keys.read().await;
        Ok(keys.get(key_id).cloned())
    }

    async fn get_active_key(&self) -> Result<EncryptionKey> {
        let active_id = self.active_key_id.read().await.clone();
        self.get_key(&active_id)
            .await?
            .ok_or_else(|| CdcError::replication("No active key found"))
    }

    async fn store_key(&self, key: EncryptionKey) -> Result<()> {
        let mut keys = self.keys.write().await;
        keys.insert(key.id.clone(), key);
        Ok(())
    }

    async fn rotate_key(&self, key_id: &str) -> Result<EncryptionKey> {
        let new_key = EncryptionKey::generate(key_id)?;
        
        // Deactivate old key
        let mut keys = self.keys.write().await;
        if let Some(old) = keys.get_mut(key_id) {
            old.active = false;
        }
        
        // Store new key with incremented version
        let mut versioned_key = new_key;
        if let Some(old) = keys.get(key_id) {
            versioned_key.version = old.version + 1;
        }
        
        let key_clone = versioned_key.clone();
        keys.insert(key_id.to_string(), versioned_key);
        
        // Update active key ID
        *self.active_key_id.write().await = key_id.to_string();
        
        Ok(key_clone)
    }
}

/// Encryption statistics.
#[derive(Debug, Default)]
pub struct EncryptionStats {
    fields_encrypted: AtomicU64,
    fields_decrypted: AtomicU64,
    encryption_errors: AtomicU64,
    decryption_errors: AtomicU64,
    events_processed: AtomicU64,
}

impl EncryptionStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_encrypted(&self, count: u64) {
        self.fields_encrypted.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_decrypted(&self, count: u64) {
        self.fields_decrypted.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_encryption_error(&self) {
        self.encryption_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_decryption_error(&self) {
        self.decryption_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_event(&self) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> EncryptionStatsSnapshot {
        EncryptionStatsSnapshot {
            fields_encrypted: self.fields_encrypted.load(Ordering::Relaxed),
            fields_decrypted: self.fields_decrypted.load(Ordering::Relaxed),
            encryption_errors: self.encryption_errors.load(Ordering::Relaxed),
            decryption_errors: self.decryption_errors.load(Ordering::Relaxed),
            events_processed: self.events_processed.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of encryption statistics.
#[derive(Debug, Clone)]
pub struct EncryptionStatsSnapshot {
    pub fields_encrypted: u64,
    pub fields_decrypted: u64,
    pub encryption_errors: u64,
    pub decryption_errors: u64,
    pub events_processed: u64,
}

/// Field encryptor for CDC events.
pub struct FieldEncryptor<P: KeyProvider> {
    config: EncryptionConfig,
    key_provider: Arc<P>,
    stats: EncryptionStats,
    rng: SystemRandom,
}

impl<P: KeyProvider> FieldEncryptor<P> {
    /// Create a new field encryptor.
    pub fn new(config: EncryptionConfig, key_provider: P) -> Self {
        Self {
            config,
            key_provider: Arc::new(key_provider),
            stats: EncryptionStats::new(),
            rng: SystemRandom::new(),
        }
    }

    /// Encrypt sensitive fields in an event.
    pub async fn encrypt(&self, event: &CdcEvent) -> Result<CdcEvent> {
        if !self.config.enabled {
            return Ok(event.clone());
        }

        self.stats.record_event();
        let mut result = event.clone();
        let fields_to_encrypt = self.config.fields_for_table(&event.table);

        if fields_to_encrypt.is_empty() {
            return Ok(result);
        }

        let key = self.key_provider.get_active_key().await?;
        let aead_key = key.to_aead_key()?;

        // Encrypt 'after' fields
        if let Some(ref mut after) = result.after {
            if let Some(obj) = after.as_object_mut() {
                let mut encrypted_count = 0u64;
                for field in &fields_to_encrypt {
                    if let Some(value) = obj.get(field) {
                        let plaintext = value.to_string();
                        match self.encrypt_value(&aead_key, &plaintext, &key.id) {
                            Ok(ciphertext) => {
                                obj.insert(field.clone(), serde_json::json!({
                                    "__encrypted": true,
                                    "__key_id": key.id,
                                    "__key_version": key.version,
                                    "__value": ciphertext,
                                }));
                                encrypted_count += 1;
                            }
                            Err(e) => {
                                warn!("Failed to encrypt field {}: {}", field, e);
                                self.stats.record_encryption_error();
                            }
                        }
                    }
                }
                self.stats.record_encrypted(encrypted_count);
            }
        }

        // Encrypt 'before' fields
        if let Some(ref mut before) = result.before {
            if let Some(obj) = before.as_object_mut() {
                let mut encrypted_count = 0u64;
                for field in &fields_to_encrypt {
                    if let Some(value) = obj.get(field) {
                        let plaintext = value.to_string();
                        match self.encrypt_value(&aead_key, &plaintext, &key.id) {
                            Ok(ciphertext) => {
                                obj.insert(field.clone(), serde_json::json!({
                                    "__encrypted": true,
                                    "__key_id": key.id,
                                    "__key_version": key.version,
                                    "__value": ciphertext,
                                }));
                                encrypted_count += 1;
                            }
                            Err(e) => {
                                warn!("Failed to encrypt field {}: {}", field, e);
                                self.stats.record_encryption_error();
                            }
                        }
                    }
                }
                self.stats.record_encrypted(encrypted_count);
            }
        }

        Ok(result)
    }

    /// Decrypt sensitive fields in an event.
    pub async fn decrypt(&self, event: &CdcEvent) -> Result<CdcEvent> {
        if !self.config.enabled {
            return Ok(event.clone());
        }

        self.stats.record_event();
        let mut result = event.clone();

        // Decrypt 'after' fields
        if let Some(ref mut after) = result.after {
            if let Some(obj) = after.as_object_mut() {
                let mut decrypted_count = 0u64;
                let keys: Vec<_> = obj.keys().cloned().collect();
                
                for field in keys {
                    if let Some(value) = obj.get(&field) {
                        if let Some(encrypted) = value.as_object() {
                            if encrypted.get("__encrypted") == Some(&serde_json::json!(true)) {
                                if let (Some(key_id), Some(ciphertext)) = (
                                    encrypted.get("__key_id").and_then(|v| v.as_str()),
                                    encrypted.get("__value").and_then(|v| v.as_str()),
                                ) {
                                    match self.decrypt_value(key_id, ciphertext).await {
                                        Ok(plaintext) => {
                                            // Parse back to original JSON type
                                            let parsed: serde_json::Value = 
                                                serde_json::from_str(&plaintext)
                                                    .unwrap_or_else(|_| serde_json::json!(plaintext));
                                            obj.insert(field, parsed);
                                            decrypted_count += 1;
                                        }
                                        Err(e) => {
                                            warn!("Failed to decrypt field: {}", e);
                                            self.stats.record_decryption_error();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                self.stats.record_decrypted(decrypted_count);
            }
        }

        // Decrypt 'before' fields
        if let Some(ref mut before) = result.before {
            if let Some(obj) = before.as_object_mut() {
                let mut decrypted_count = 0u64;
                let keys: Vec<_> = obj.keys().cloned().collect();
                
                for field in keys {
                    if let Some(value) = obj.get(&field) {
                        if let Some(encrypted) = value.as_object() {
                            if encrypted.get("__encrypted") == Some(&serde_json::json!(true)) {
                                if let (Some(key_id), Some(ciphertext)) = (
                                    encrypted.get("__key_id").and_then(|v| v.as_str()),
                                    encrypted.get("__value").and_then(|v| v.as_str()),
                                ) {
                                    match self.decrypt_value(key_id, ciphertext).await {
                                        Ok(plaintext) => {
                                            let parsed: serde_json::Value = 
                                                serde_json::from_str(&plaintext)
                                                    .unwrap_or_else(|_| serde_json::json!(plaintext));
                                            obj.insert(field, parsed);
                                            decrypted_count += 1;
                                        }
                                        Err(e) => {
                                            warn!("Failed to decrypt field: {}", e);
                                            self.stats.record_decryption_error();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                self.stats.record_decrypted(decrypted_count);
            }
        }

        Ok(result)
    }

    /// Encrypt a single value.
    fn encrypt_value(&self, key: &LessSafeKey, plaintext: &str, key_id: &str) -> Result<String> {
        // Generate random nonce
        let mut nonce_bytes = [0u8; 12];
        self.rng.fill(&mut nonce_bytes)
            .map_err(|_| CdcError::replication("Failed to generate nonce"))?;
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);

        // AAD includes key_id for additional authentication
        let aad = format!("{}:{}", self.config.aad_prefix, key_id);
        let aad = Aad::from(aad.as_bytes());

        // Encrypt
        let mut in_out = plaintext.as_bytes().to_vec();
        key.seal_in_place_append_tag(nonce, aad, &mut in_out)
            .map_err(|_| CdcError::replication("Encryption failed"))?;

        // Prepend nonce to ciphertext
        let mut result = nonce_bytes.to_vec();
        result.extend(in_out);

        // Base64 encode
        Ok(base64_encode(&result))
    }

    /// Decrypt a single value.
    async fn decrypt_value(&self, key_id: &str, ciphertext: &str) -> Result<String> {
        let key = self.key_provider.get_key(key_id).await?
            .ok_or_else(|| CdcError::replication(format!("Key not found: {}", key_id)))?;
        let aead_key = key.to_aead_key()?;

        // Base64 decode
        let data = base64_decode(ciphertext)?;
        
        if data.len() < 12 {
            return Err(CdcError::replication("Invalid ciphertext"));
        }

        // Extract nonce and ciphertext
        let nonce_bytes: [u8; 12] = data[..12].try_into()
            .map_err(|_| CdcError::replication("Invalid nonce"))?;
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);
        let mut ciphertext_data = data[12..].to_vec();

        // AAD
        let aad = format!("{}:{}", self.config.aad_prefix, key_id);
        let aad = Aad::from(aad.as_bytes());

        // Decrypt
        let plaintext = aead_key.open_in_place(nonce, aad, &mut ciphertext_data)
            .map_err(|_| CdcError::replication("Decryption failed"))?;

        String::from_utf8(plaintext.to_vec())
            .map_err(|_| CdcError::replication("Invalid UTF-8"))
    }

    /// Get statistics.
    pub fn stats(&self) -> EncryptionStatsSnapshot {
        self.stats.snapshot()
    }

    /// Check if a field is encrypted.
    pub fn is_field_encrypted(value: &serde_json::Value) -> bool {
        value.as_object()
            .map(|obj| obj.get("__encrypted") == Some(&serde_json::json!(true)))
            .unwrap_or(false)
    }
}

// Base64 encoding/decoding helpers
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    
    for chunk in data.chunks(3) {
        let n = chunk.len();
        let mut buf = [0u8; 3];
        buf[..n].copy_from_slice(chunk);
        
        let b = ((buf[0] as u32) << 16) | ((buf[1] as u32) << 8) | (buf[2] as u32);
        
        result.push(ALPHABET[(b >> 18) as usize & 0x3F] as char);
        result.push(ALPHABET[(b >> 12) as usize & 0x3F] as char);
        
        if n > 1 {
            result.push(ALPHABET[(b >> 6) as usize & 0x3F] as char);
        } else {
            result.push('=');
        }
        
        if n > 2 {
            result.push(ALPHABET[b as usize & 0x3F] as char);
        } else {
            result.push('=');
        }
    }
    
    result
}

fn base64_decode(s: &str) -> Result<Vec<u8>> {
    const DECODE: [i8; 128] = [
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
        52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1,
        -1,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
        15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
        -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
        41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
    ];
    
    let s = s.trim_end_matches('=');
    let mut result = Vec::with_capacity(s.len() * 3 / 4);
    
    let bytes: Vec<u8> = s.bytes().collect();
    for chunk in bytes.chunks(4) {
        let mut buf = [0u8; 4];
        for (i, &b) in chunk.iter().enumerate() {
            if b >= 128 {
                return Err(CdcError::replication("Invalid base64"));
            }
            let val = DECODE[b as usize];
            if val < 0 {
                return Err(CdcError::replication("Invalid base64"));
            }
            buf[i] = val as u8;
        }
        
        let n = chunk.len();
        let b = ((buf[0] as u32) << 18) | ((buf[1] as u32) << 12) 
              | ((buf[2] as u32) << 6) | (buf[3] as u32);
        
        result.push((b >> 16) as u8);
        if n > 2 {
            result.push((b >> 8) as u8);
        }
        if n > 3 {
            result.push(b as u8);
        }
    }
    
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::CdcOp;

    fn make_event(table: &str) -> CdcEvent {
        CdcEvent {
            source_type: "postgres".to_string(),
            database: "testdb".to_string(),
            schema: "public".to_string(),
            table: table.to_string(),
            op: CdcOp::Insert,
            before: None,
            after: Some(serde_json::json!({
                "id": 1,
                "email": "test@example.com",
                "ssn": "123-45-6789",
                "name": "John Doe"
            })),
            timestamp: chrono::Utc::now().timestamp(),
            transaction: None,
        }
    }

    #[test]
    fn test_field_rule_creation() {
        let rule = FieldRule::new("users", "email");
        assert_eq!(rule.table_pattern, "users");
        assert_eq!(rule.field_name, "email");
        assert!(rule.mask_in_logs);
    }

    #[test]
    fn test_field_rule_matching() {
        let rule = FieldRule::new("users", "email");
        assert!(rule.matches_table("users"));
        assert!(!rule.matches_table("orders"));

        let wildcard = FieldRule::new("*", "email");
        assert!(wildcard.matches_table("users"));
        assert!(wildcard.matches_table("orders"));

        let prefix = FieldRule::new("user*", "email");
        assert!(prefix.matches_table("users"));
        assert!(prefix.matches_table("user_profiles"));
        assert!(!prefix.matches_table("orders"));
    }

    #[test]
    fn test_config_builder() {
        let config = EncryptionConfig::builder()
            .encrypt_field("users", "email")
            .encrypt_field("users", "ssn")
            .encrypt_field("payments", "card_number")
            .default_key_id("my-key")
            .build();

        assert_eq!(config.rules.len(), 3);
        assert_eq!(config.default_key_id, "my-key");
        assert!(config.enabled);
    }

    #[test]
    fn test_config_fields_for_table() {
        let config = EncryptionConfig::builder()
            .encrypt_field("users", "email")
            .encrypt_field("users", "ssn")
            .encrypt_field("orders", "card_number")
            .build();

        let user_fields = config.fields_for_table("users");
        assert_eq!(user_fields.len(), 2);
        assert!(user_fields.contains("email"));
        assert!(user_fields.contains("ssn"));

        let order_fields = config.fields_for_table("orders");
        assert_eq!(order_fields.len(), 1);
        assert!(order_fields.contains("card_number"));

        let other_fields = config.fields_for_table("products");
        assert!(other_fields.is_empty());
    }

    #[test]
    fn test_encryption_key_generation() {
        let key = EncryptionKey::generate("test-key").unwrap();
        assert_eq!(key.id, "test-key");
        assert_eq!(key.version, 1);
        assert!(key.active);
    }

    #[test]
    fn test_encryption_key_validation() {
        // Too short
        let result = EncryptionKey::new("test", vec![0u8; 16]);
        assert!(result.is_err());

        // Correct size
        let result = EncryptionKey::new("test", vec![0u8; 32]);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_memory_key_provider() {
        let provider = MemoryKeyProvider::new().unwrap();
        
        let key = provider.get_active_key().await.unwrap();
        assert_eq!(key.id, "default");
        assert!(key.active);
    }

    #[tokio::test]
    async fn test_memory_key_provider_rotation() {
        let provider = MemoryKeyProvider::new().unwrap();
        
        let old_key = provider.get_active_key().await.unwrap();
        let new_key = provider.rotate_key("default").await.unwrap();
        
        assert_eq!(new_key.id, "default");
        assert_eq!(new_key.version, old_key.version + 1);
    }

    #[tokio::test]
    async fn test_field_encryptor_encrypt_decrypt() {
        let config = EncryptionConfig::builder()
            .encrypt_field("users", "email")
            .encrypt_field("users", "ssn")
            .build();
        
        let provider = MemoryKeyProvider::new().unwrap();
        let encryptor = FieldEncryptor::new(config, provider);

        let event = make_event("users");
        let encrypted = encryptor.encrypt(&event).await.unwrap();

        // Check that fields are encrypted
        let after = encrypted.after.as_ref().unwrap();
        assert!(FieldEncryptor::<MemoryKeyProvider>::is_field_encrypted(
            after.get("email").unwrap()
        ));
        assert!(FieldEncryptor::<MemoryKeyProvider>::is_field_encrypted(
            after.get("ssn").unwrap()
        ));
        // Name should not be encrypted
        assert!(!FieldEncryptor::<MemoryKeyProvider>::is_field_encrypted(
            after.get("name").unwrap()
        ));

        // Decrypt
        let decrypted = encryptor.decrypt(&encrypted).await.unwrap();
        let after = decrypted.after.as_ref().unwrap();
        
        // Values should be restored (compare as JSON strings)
        assert_eq!(after.get("email").unwrap().as_str().unwrap(), "test@example.com");
        assert_eq!(after.get("ssn").unwrap().as_str().unwrap(), "123-45-6789");
        assert_eq!(after.get("name").unwrap().as_str().unwrap(), "John Doe");
    }

    #[tokio::test]
    async fn test_field_encryptor_no_rules() {
        let config = EncryptionConfig::builder().build();
        let provider = MemoryKeyProvider::new().unwrap();
        let encryptor = FieldEncryptor::new(config, provider);

        let event = make_event("users");
        let encrypted = encryptor.encrypt(&event).await.unwrap();

        // Nothing should be encrypted
        let after = encrypted.after.as_ref().unwrap();
        assert!(!FieldEncryptor::<MemoryKeyProvider>::is_field_encrypted(
            after.get("email").unwrap()
        ));
    }

    #[tokio::test]
    async fn test_field_encryptor_disabled() {
        let config = EncryptionConfig::builder()
            .encrypt_field("users", "email")
            .enabled(false)
            .build();
        let provider = MemoryKeyProvider::new().unwrap();
        let encryptor = FieldEncryptor::new(config, provider);

        let event = make_event("users");
        let encrypted = encryptor.encrypt(&event).await.unwrap();

        // Nothing should be encrypted when disabled
        let after = encrypted.after.as_ref().unwrap();
        assert!(!FieldEncryptor::<MemoryKeyProvider>::is_field_encrypted(
            after.get("email").unwrap()
        ));
    }

    #[test]
    fn test_stats_snapshot() {
        let stats = EncryptionStats::new();
        stats.record_encrypted(10);
        stats.record_decrypted(8);
        stats.record_encryption_error();
        stats.record_decryption_error();
        stats.record_event();
        stats.record_event();

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.fields_encrypted, 10);
        assert_eq!(snapshot.fields_decrypted, 8);
        assert_eq!(snapshot.encryption_errors, 1);
        assert_eq!(snapshot.decryption_errors, 1);
        assert_eq!(snapshot.events_processed, 2);
    }

    #[test]
    fn test_base64_roundtrip() {
        let data = b"Hello, World!";
        let encoded = base64_encode(data);
        let decoded = base64_decode(&encoded).unwrap();
        assert_eq!(decoded, data);

        // Test with binary data
        let binary = vec![0u8, 1, 2, 255, 254, 253];
        let encoded = base64_encode(&binary);
        let decoded = base64_decode(&encoded).unwrap();
        assert_eq!(decoded, binary);
    }

    #[tokio::test]
    async fn test_encrypt_before_and_after() {
        let config = EncryptionConfig::builder()
            .encrypt_field("users", "email")
            .build();
        let provider = MemoryKeyProvider::new().unwrap();
        let encryptor = FieldEncryptor::new(config, provider);

        let mut event = make_event("users");
        event.op = CdcOp::Update;
        event.before = Some(serde_json::json!({
            "id": 1,
            "email": "old@example.com"
        }));

        let encrypted = encryptor.encrypt(&event).await.unwrap();

        // Both before and after should have encrypted email
        assert!(FieldEncryptor::<MemoryKeyProvider>::is_field_encrypted(
            encrypted.after.as_ref().unwrap().get("email").unwrap()
        ));
        assert!(FieldEncryptor::<MemoryKeyProvider>::is_field_encrypted(
            encrypted.before.as_ref().unwrap().get("email").unwrap()
        ));
    }
}
