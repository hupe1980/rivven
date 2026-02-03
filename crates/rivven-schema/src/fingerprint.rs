//! Schema fingerprinting for deduplication

use sha2::{Digest, Sha256};

/// Schema fingerprint for deduplication
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemaFingerprint {
    /// MD5 hash (16 bytes) - Confluent compatible
    pub md5: [u8; 16],
    /// SHA-256 hash (32 bytes) - more secure
    pub sha256: [u8; 32],
}

impl SchemaFingerprint {
    /// Compute fingerprint from schema string
    pub fn compute(schema: &str) -> Self {
        // Normalize schema by parsing and re-serializing to remove whitespace differences
        let normalized = normalize_json(schema);

        // Compute MD5 (Confluent compatible)
        let md5_hash = md5::compute(normalized.as_bytes());
        let mut md5 = [0u8; 16];
        md5.copy_from_slice(&md5_hash.0);

        // Compute SHA-256
        let mut hasher = Sha256::new();
        hasher.update(normalized.as_bytes());
        let sha256_result = hasher.finalize();
        let mut sha256 = [0u8; 32];
        sha256.copy_from_slice(&sha256_result);

        Self { md5, sha256 }
    }

    /// Get MD5 fingerprint as hex string
    pub fn md5_hex(&self) -> String {
        hex::encode(self.md5)
    }

    /// Get SHA-256 fingerprint as hex string
    pub fn sha256_hex(&self) -> String {
        hex::encode(self.sha256)
    }

    /// Get MD5 fingerprint as base64 string
    pub fn md5_base64(&self) -> String {
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, self.md5)
    }

    /// Create from hex-encoded MD5
    pub fn from_md5_hex(hex_str: &str) -> Option<Self> {
        let md5_bytes = hex::decode(hex_str).ok()?;
        if md5_bytes.len() != 16 {
            return None;
        }
        let mut md5 = [0u8; 16];
        md5.copy_from_slice(&md5_bytes);

        Some(Self {
            md5,
            sha256: [0u8; 32], // Unknown
        })
    }
}

/// Normalize JSON by parsing and re-serializing
fn normalize_json(json: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(json) {
        Ok(value) => {
            // Re-serialize with sorted keys for deterministic output
            serde_json::to_string(&value).unwrap_or_else(|_| json.to_string())
        }
        Err(_) => json.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprint_compute() {
        let schema = r#"{"type": "string"}"#;
        let fp = SchemaFingerprint::compute(schema);

        // Should produce consistent fingerprints
        let fp2 = SchemaFingerprint::compute(schema);
        assert_eq!(fp.md5, fp2.md5);
        assert_eq!(fp.sha256, fp2.sha256);
    }

    #[test]
    fn test_fingerprint_normalization() {
        // These should produce the same fingerprint
        let schema1 = r#"{"type":"string"}"#;
        let schema2 = r#"{ "type" : "string" }"#;

        let fp1 = SchemaFingerprint::compute(schema1);
        let fp2 = SchemaFingerprint::compute(schema2);

        assert_eq!(fp1.md5, fp2.md5);
    }

    #[test]
    fn test_fingerprint_hex() {
        let schema = r#"{"type": "string"}"#;
        let fp = SchemaFingerprint::compute(schema);

        let hex = fp.md5_hex();
        assert_eq!(hex.len(), 32); // 16 bytes = 32 hex chars

        let sha_hex = fp.sha256_hex();
        assert_eq!(sha_hex.len(), 64); // 32 bytes = 64 hex chars
    }

    #[test]
    fn test_from_md5_hex() {
        let schema = r#"{"type": "string"}"#;
        let fp = SchemaFingerprint::compute(schema);
        let hex = fp.md5_hex();

        let fp2 = SchemaFingerprint::from_md5_hex(&hex).unwrap();
        assert_eq!(fp.md5, fp2.md5);
    }
}
