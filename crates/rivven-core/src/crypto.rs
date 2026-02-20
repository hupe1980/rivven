//! Shared cryptographic primitives for Rivven encryption subsystems.
//!
//! This module provides common types used by both data-at-rest encryption
//! (WAL/segment encryption in `rivven-core::encryption`) and field-level
//! encryption (CDC column encryption in `rivven-cdc`).
//!
//! ## Types
//!
//! - [`KeyMaterial`] — Zeroize-on-drop AES-256 key material (32 bytes)
//! - [`KeyInfo`] — Metadata about an encryption key (id, version, lifecycle)

use ring::rand::SecureRandom;
use serde::{Deserialize, Serialize};
use std::fmt;

/// AES-256 key size in bytes.
pub const KEY_SIZE: usize = 32;

/// AEAD nonce size in bytes (AES-256-GCM / ChaCha20-Poly1305).
pub const NONCE_SIZE: usize = 12;

/// AEAD authentication tag size in bytes.
pub const TAG_SIZE: usize = 16;

/// Shared key material for AES-256 encryption.
///
/// Provides a unified key representation used by both the data-at-rest
/// encryption system (WAL/segment encryption) and the CDC field-level
/// encryption system. Key material is zeroized on drop to prevent
/// sensitive data from lingering in memory.
///
/// # Safety
/// The inner bytes are overwritten with zeros when this value is dropped
/// via a volatile write to prevent compiler optimization from eliding
/// the zeroing.
#[derive(Clone)]
pub struct KeyMaterial {
    bytes: [u8; KEY_SIZE],
}

impl KeyMaterial {
    /// Create key material from raw bytes. Returns `None` if length != 32.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != KEY_SIZE {
            return None;
        }
        let mut material = [0u8; KEY_SIZE];
        material.copy_from_slice(bytes);
        Some(Self { bytes: material })
    }

    /// Generate random key material using a cryptographically secure RNG.
    ///
    /// Returns an error if the system RNG is unavailable (e.g., early boot
    /// with depleted entropy, or sandboxed environment without `/dev/urandom`).
    pub fn generate() -> crate::Result<Self> {
        let rng = ring::rand::SystemRandom::new();
        let mut bytes = [0u8; KEY_SIZE];
        rng.fill(&mut bytes)
            .map_err(|_| crate::Error::Other("system RNG unavailable".into()))?;
        Ok(Self { bytes })
    }

    /// Access the raw key bytes.
    pub fn as_bytes(&self) -> &[u8; KEY_SIZE] {
        &self.bytes
    }
}

impl Drop for KeyMaterial {
    fn drop(&mut self) {
        // Volatile write prevents the compiler from optimizing away the zeroing
        for byte in self.bytes.iter_mut() {
            unsafe { std::ptr::write_volatile(byte, 0) };
        }
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
    }
}

impl fmt::Debug for KeyMaterial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("KeyMaterial([REDACTED])")
    }
}

/// Metadata about an encryption key, shared across encryption subsystems.
///
/// Used by both at-rest and field-level encryption to track key identity,
/// versioning, and lifecycle state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyInfo {
    /// Unique key identifier (e.g., "master-0", "field-key-1")
    pub id: String,
    /// Key version for rotation tracking
    pub version: u32,
    /// Creation timestamp (Unix epoch seconds)
    pub created_at: u64,
    /// Whether this key is the active encryption key
    pub active: bool,
}

impl KeyInfo {
    /// Create a new key info with the current timestamp.
    pub fn new(id: impl Into<String>, version: u32) -> Self {
        Self {
            id: id.into(),
            version,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            active: true,
        }
    }
}
