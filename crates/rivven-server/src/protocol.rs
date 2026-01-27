//! Protocol types re-exported from rivven-protocol
//!
//! This module re-exports protocol types from the shared rivven-protocol crate.
//! All protocol types are now centralized in rivven-protocol to ensure
//! wire compatibility between client and server.

pub use rivven_protocol::{
    BrokerInfo, DeleteRecordsResult, MessageData, PartitionMetadata, QuotaAlteration, QuotaEntry,
    Request, Response, TopicConfigDescription, TopicConfigEntry, TopicConfigValue, TopicMetadata,
    MAX_MESSAGE_SIZE, PROTOCOL_VERSION,
};

// Re-export error types for convenience
pub use rivven_protocol::{ProtocolError, Result as ProtocolResult};

#[cfg(test)]
mod protocol_tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_request_roundtrip() {
        let requests = vec![
            Request::Ping,
            Request::ListTopics,
            Request::Authenticate {
                username: "user".to_string(),
                password: "pass".to_string(),
            },
            Request::Publish {
                topic: "test-topic".to_string(),
                partition: Some(0),
                key: Some(Bytes::from(vec![1, 2, 3])),
                value: Bytes::from(b"hello world".to_vec()),
            },
            Request::Consume {
                topic: "test-topic".to_string(),
                partition: 0,
                offset: 100,
                max_messages: 10,
            },
            Request::CreateTopic {
                name: "new-topic".to_string(),
                partitions: Some(3),
            },
            Request::DeleteTopic {
                name: "old-topic".to_string(),
            },
        ];

        for request in requests {
            let bytes = request.to_bytes().expect("serialize failed");
            let decoded = Request::from_bytes(&bytes).expect("deserialize failed");

            // Verify roundtrip by re-serializing
            let bytes2 = decoded.to_bytes().expect("re-serialize failed");
            assert_eq!(bytes, bytes2, "roundtrip failed for {:?}", request);
        }
    }

    #[test]
    fn test_response_roundtrip() {
        let responses = vec![
            Response::Pong,
            Response::Authenticated {
                session_id: "abc123".to_string(),
                expires_in: 3600,
            },
            Response::TopicCreated {
                name: "test".to_string(),
                partitions: 3,
            },
            Response::Messages {
                messages: vec![
                    MessageData::new(0, b"first".to_vec(), 1234567890),
                    MessageData::new(1, b"second".to_vec(), 1234567891).with_key(b"key".to_vec()),
                ],
            },
            Response::Error {
                message: "something went wrong".to_string(),
            },
        ];

        for response in responses {
            let bytes = response.to_bytes().expect("serialize failed");
            let decoded = Response::from_bytes(&bytes).expect("deserialize failed");

            let bytes2 = decoded.to_bytes().expect("re-serialize failed");
            assert_eq!(bytes, bytes2, "roundtrip failed for {:?}", response);
        }
    }

    #[test]
    fn test_sasl_auth_serialization() {
        let request = Request::SaslAuthenticate {
            mechanism: Bytes::from(b"PLAIN".to_vec()),
            auth_bytes: Bytes::from(b"\x00user\x00pass".to_vec()),
        };

        let bytes = request.to_bytes().unwrap();
        let decoded = Request::from_bytes(&bytes).unwrap();

        if let Request::SaslAuthenticate {
            mechanism,
            auth_bytes,
        } = decoded
        {
            assert_eq!(mechanism.as_ref(), b"PLAIN");
            assert_eq!(auth_bytes.as_ref(), b"\x00user\x00pass");
        } else {
            panic!("Expected SaslAuthenticate");
        }
    }

    #[test]
    fn test_large_string_handling() {
        let large_topic = "a".repeat(1000);
        let request = Request::CreateTopic {
            name: large_topic.clone(),
            partitions: Some(1),
        };

        let bytes = request.to_bytes().unwrap();
        let decoded = Request::from_bytes(&bytes).unwrap();

        if let Request::CreateTopic { name, partitions } = decoded {
            assert_eq!(name, large_topic);
            assert_eq!(partitions, Some(1));
        } else {
            panic!("Expected CreateTopic");
        }
    }

    #[test]
    fn test_truncated_request() {
        let request = Request::ListTopics;
        let bytes = request.to_bytes().unwrap();

        // Truncate the data
        let truncated = &bytes[..bytes.len().saturating_sub(1)];

        // Should fail to deserialize
        assert!(Request::from_bytes(truncated).is_err());
    }

    #[test]
    fn test_null_byte_in_string() {
        let request = Request::CreateTopic {
            name: "test\x00topic".to_string(),
            partitions: None,
        };

        let bytes = request.to_bytes().unwrap();
        let decoded = Request::from_bytes(&bytes).unwrap();

        if let Request::CreateTopic { name, .. } = decoded {
            // Postcard handles embedded nulls correctly
            assert_eq!(name, "test\x00topic");
        } else {
            panic!("Expected CreateTopic");
        }
    }

    #[test]
    fn test_request_from_arbitrary_bytes() {
        // Random garbage should not crash, just return error
        let garbage = vec![0xFF, 0xFE, 0x00, 0x01, 0x02];
        let result = Request::from_bytes(&garbage);
        // Either succeeds with some value or errors - neither should panic
        let _ = result;
    }

    #[test]
    fn test_response_from_arbitrary_bytes() {
        // Random garbage should not crash, just return error
        let garbage = vec![0x00, 0x00, 0x00, 0x00, 0xFF];
        let result = Response::from_bytes(&garbage);
        // Either succeeds with some value or errors - neither should panic
        let _ = result;
    }
}
