//! Canonical hash functions shared across client and server.
//!
//! Provides the single source of truth for murmur2 hashing to prevent
//! divergence between client partition selection and server partition
//! assignment, which would break key-ordering guarantees.

/// Kafka-compatible murmur2 hash.
///
/// Produces a 32-bit **unsigned** hash matching the Kafka Java client's
/// `Utils.murmur2()`. Uses the same seed (0x9747b28c) and mixing constants
/// as the Java implementation to ensure cross-language partition assignment.
///
/// # Example
/// ```
/// # use rivven_core::hash::murmur2;
/// let hash = murmur2(b"hello");
/// assert_eq!(hash, 1682149141); // matches Kafka Java
/// ```
pub fn murmur2(data: &[u8]) -> u32 {
    // Kafka uses Java int (32-bit signed) arithmetic which wraps identically
    // to Rust u32 wrapping ops (two's complement). We use u32 throughout
    // and cast the SEED constant from its Java signed representation.
    const SEED: u32 = 0x9747b28c;
    const M: u32 = 0x5bd1e995;
    const R: u32 = 24;

    let len = data.len();
    let mut h: u32 = SEED ^ (len as u32);

    // Process 4-byte chunks
    let mut i = 0;
    while i + 4 <= len {
        let mut k = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
        i += 4;
    }

    // Handle remaining bytes
    let remainder = len - i;
    if remainder >= 3 {
        h ^= (data[i + 2] as u32) << 16;
    }
    if remainder >= 2 {
        h ^= (data[i + 1] as u32) << 8;
    }
    if remainder >= 1 {
        h ^= data[i] as u32;
        h = h.wrapping_mul(M);
    }

    // Final mixing
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;

    h
}

/// Kafka-compatible partition assignment (toPositive + modulo).
///
/// Masks the sign bit then takes modulo — identical to Kafka Java's
/// `Utils.toPositive(Utils.murmur2(key)) % numPartitions`.
#[inline]
pub fn murmur2_partition(key: &[u8], num_partitions: u32) -> u32 {
    (murmur2(key) & 0x7fffffff) % num_partitions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_murmur2_known_vectors() {
        // Kafka Java murmur2 reference values (seed 0x9747b28c with final mixing)
        assert_eq!(murmur2(b""), 275646681);
        assert_eq!(murmur2(b"hello"), 1682149141);
        assert_eq!(murmur2(b"kafka"), 1762226537);
    }

    #[test]
    fn test_murmur2_partition_deterministic() {
        let key = b"user-123";
        let p1 = murmur2_partition(key, 10);
        let p2 = murmur2_partition(key, 10);
        assert_eq!(p1, p2);
        assert!(p1 < 10);
    }

    #[test]
    fn test_murmur2_partition_distribution() {
        let mut counts = [0u32; 8];
        for i in 0..1000u32 {
            let key = i.to_be_bytes();
            let p = murmur2_partition(&key, 8);
            counts[p as usize] += 1;
        }
        // Each partition should get some keys (not all to one bucket)
        for count in &counts {
            assert!(*count > 0, "Partition got zero keys — bad distribution");
        }
    }
}
