//! Property-based tests for protocol security
//!
//! These tests use proptest to generate random inputs and verify invariants.
//! This is a lighter-weight alternative to fuzzing that runs in CI.

use crate::protocol::{Request, Response};
use bytes::Bytes;
use proptest::prelude::*;

// Generate arbitrary strings for testing
prop_compose! {
    fn arbitrary_string(_max_len: usize)(s in "[a-zA-Z0-9_\\-\\.]{0,100}") -> String {
        s
    }
}

// Generate arbitrary bytes
prop_compose! {
    fn arbitrary_bytes(max_len: usize)(v in prop::collection::vec(any::<u8>(), 0..max_len)) -> Vec<u8> {
        v
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    /// Test that serialization/deserialization roundtrips correctly
    #[test]
    fn test_request_roundtrip(
        topic in arbitrary_string(100),
        partition in any::<Option<u32>>(),
        offset in any::<u64>(),
        max_messages in 1usize..1000,
        consumer_group in arbitrary_string(100),
    ) {
        let requests = vec![
            Request::Ping,
            Request::ListTopics,
            Request::CreateTopic { name: topic.clone(), partitions: partition },
            Request::DeleteTopic { name: topic.clone() },
            Request::GetMetadata { topic: topic.clone() },
            Request::Consume {
                topic: topic.clone(),
                partition: partition.unwrap_or(0),
                offset,
                max_messages,
                isolation_level: None,
            },
            Request::GetOffset {
                consumer_group: consumer_group.clone(),
                topic: topic.clone(),
                partition: partition.unwrap_or(0)
            },
        ];

        for request in requests {
            let bytes: Vec<u8> = request.to_bytes().expect("Serialization should succeed");
            let decoded = Request::from_bytes(&bytes).expect("Deserialization should succeed");

            // Re-encode and compare bytes (structural equality)
            let re_encoded: Vec<u8> = decoded.to_bytes().expect("Re-serialization should succeed");
            prop_assert_eq!(bytes, re_encoded, "Roundtrip should produce identical bytes");
        }
    }

    /// Test that arbitrary bytes don't crash the deserializer
    #[test]
    fn test_request_from_arbitrary_bytes(data in arbitrary_bytes(10000)) {
        // Should never panic
        let _ = Request::from_bytes(&data);
    }

    /// Test that response deserialization handles arbitrary bytes
    #[test]
    fn test_response_from_arbitrary_bytes(data in arbitrary_bytes(10000)) {
        // Should never panic
        let _ = Response::from_bytes(&data);
    }

    /// Test that truncated serialized data doesn't crash
    #[test]
    fn test_truncated_request(
        topic in arbitrary_string(100),
        truncate_at in 0usize..500,
    ) {
        let request = Request::Publish {
            topic,
            partition: Some(0),
            key: Some(Bytes::from(vec![1, 2, 3, 4, 5])),
            value: Bytes::from(vec![6, 7, 8, 9, 10]),
        };

        if let Ok(bytes) = request.to_bytes() {
            let bytes: Vec<u8> = bytes;
            let truncated = &bytes[..truncate_at.min(bytes.len())];
            // Should not panic, just return error
            let _ = Request::from_bytes(truncated);
        }
    }

    /// Test that very long strings don't cause issues
    #[test]
    fn test_large_string_handling(
        len in 0usize..100000,
    ) {
        let long_string: String = (0..len).map(|_| 'a').collect();

        let request = Request::CreateTopic {
            name: long_string,
            partitions: Some(1),
        };

        // Serialization might fail due to size limits, but shouldn't panic
        let _ = request.to_bytes();
    }

    /// Test that null bytes in strings are handled
    #[test]
    fn test_null_byte_in_string(
        prefix in arbitrary_string(50),
        suffix in arbitrary_string(50),
    ) {
        // Create string with embedded null byte
        let mut topic = prefix;
        topic.push('\0');
        topic.push_str(&suffix);

        let request = Request::GetMetadata { topic };

        // Should handle null bytes gracefully
        if let Ok(bytes) = request.to_bytes() {
            let _ = Request::from_bytes(&bytes);
        }
    }

    /// Test authentication request with arbitrary credentials
    #[test]
    fn test_auth_request_serialization(
        username in arbitrary_string(200),
        password in arbitrary_string(200),
    ) {
        let request = Request::Authenticate { username, password };

        if let Ok(bytes) = request.to_bytes() {
            let decoded = Request::from_bytes(&bytes);
            prop_assert!(decoded.is_ok(), "Auth request should deserialize");
        }
    }

    /// Test SASL auth with arbitrary bytes
    #[test]
    fn test_sasl_auth_serialization(
        mechanism in arbitrary_bytes(100),
        auth_bytes in arbitrary_bytes(500),
    ) {
        let request = Request::SaslAuthenticate {
            mechanism: Bytes::from(mechanism),
            auth_bytes: Bytes::from(auth_bytes),
        };

        if let Ok(bytes) = request.to_bytes() {
            let decoded = Request::from_bytes(&bytes);
            prop_assert!(decoded.is_ok(), "SASL auth request should deserialize");
        }
    }
}

#[cfg(test)]
mod edge_case_tests {
    use super::*;

    #[test]
    fn test_empty_bytes() {
        assert!(Request::from_bytes(&[]).is_err());
        assert!(Response::from_bytes(&[]).is_err());
    }

    #[test]
    fn test_single_byte() {
        for byte in 0u8..=255 {
            let _ = Request::from_bytes(&[byte]);
            let _ = Response::from_bytes(&[byte]);
        }
    }

    #[test]
    fn test_all_zeros() {
        let zeros = vec![0u8; 1000];
        let _ = Request::from_bytes(&zeros);
        let _ = Response::from_bytes(&zeros);
    }

    #[test]
    fn test_all_ones() {
        let ones = vec![0xFFu8; 1000];
        let _ = Request::from_bytes(&ones);
        let _ = Response::from_bytes(&ones);
    }

    #[test]
    fn test_postcard_magic_numbers() {
        // Common attack vectors for postcard
        let test_cases: Vec<&[u8]> = vec![
            // Integer overflow attempts
            &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            // Length prefix attacks
            &[0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00],
            // Negative length (if interpreted as signed)
            &[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            // Maximum u64 length
            &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F],
        ];

        for case in test_cases {
            let _ = Request::from_bytes(case);
            let _ = Response::from_bytes(case);
        }
    }

    #[test]
    fn test_unicode_edge_cases() {
        // Various unicode edge cases
        let test_strings = vec![
            "\u{0000}",   // Null
            "\u{FEFF}",   // BOM
            "\u{FFFF}",   // Non-character
            "\u{10FFFF}", // Max code point
            "🎉🎊🎋",     // Emoji
            "\u{200B}",   // Zero-width space
            "a\u{0300}",  // Combining character
        ];

        for s in test_strings {
            let request = Request::CreateTopic {
                name: s.to_string(),
                partitions: Some(1),
            };

            if let Ok(bytes) = request.to_bytes() {
                let _ = Request::from_bytes(&bytes);
            }
        }
    }
}
