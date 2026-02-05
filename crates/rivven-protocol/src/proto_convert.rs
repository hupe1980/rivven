//! Protobuf conversion utilities
//!
//! This module provides conversions between the native Request/Response types
//! and the prost-generated protobuf types.
//!
//! This module is only compiled when the `protobuf` feature is enabled.

use crate::proto;
use crate::{Request, Response};
use bytes::Bytes;
use prost::Message;

// ============================================================================
// Request Conversions
// ============================================================================

impl Request {
    /// Convert native Request to protobuf bytes
    pub fn to_proto_bytes(&self) -> crate::Result<Vec<u8>> {
        let proto_request = self.to_proto()?;
        Ok(proto_request.encode_to_vec())
    }

    /// Parse protobuf bytes to native Request
    pub fn from_proto_bytes(data: &[u8]) -> crate::Result<Self> {
        let proto_request = proto::Request::decode(data).map_err(|e| {
            crate::ProtocolError::Serialization(format!("Protobuf decode error: {}", e))
        })?;
        Self::from_proto(&proto_request)
    }

    /// Convert native Request to proto Request
    fn to_proto(&self) -> crate::Result<proto::Request> {
        use proto::request::RequestType;

        let request_type = match self {
            Request::Ping => Some(RequestType::Ping(proto::PingRequest {})),

            Request::ListTopics => Some(RequestType::ListTopics(proto::ListTopicsRequest {
                prefix: String::new(),
            })),

            Request::CreateTopic { name, partitions } => {
                Some(RequestType::CreateTopic(proto::CreateTopicRequest {
                    name: name.clone(),
                    partitions: partitions.unwrap_or(0),
                    replication_factor: 0,
                    config: Default::default(),
                }))
            }

            Request::DeleteTopic { name } => {
                Some(RequestType::DeleteTopic(proto::DeleteTopicRequest {
                    name: name.clone(),
                }))
            }

            Request::Publish {
                topic,
                partition,
                key,
                value,
            } => Some(RequestType::Publish(proto::PublishRequest {
                topic: topic.clone(),
                partition: *partition,
                record: Some(proto::Record {
                    key: key.clone().map(|b| b.to_vec()).unwrap_or_default(),
                    value: value.to_vec(),
                    headers: vec![],
                    timestamp: 0,
                }),
            })),

            Request::Consume {
                topic,
                partition,
                offset,
                max_messages,
                isolation_level,
            } => Some(RequestType::Consume(proto::ConsumeRequest {
                topic: topic.clone(),
                partition: *partition,
                offset: *offset,
                max_messages: *max_messages as u32,
                max_bytes: 0,
                isolation_level: isolation_level.unwrap_or(0) as u32,
            })),

            Request::GetMetadata { topic } => {
                Some(RequestType::GetMetadata(proto::GetMetadataRequest {
                    topics: vec![topic.clone()],
                }))
            }

            Request::CommitOffset {
                consumer_group,
                topic,
                partition,
                offset,
            } => Some(RequestType::CommitOffset(proto::CommitOffsetRequest {
                consumer_group: consumer_group.clone(),
                topic: topic.clone(),
                partition: *partition,
                offset: *offset,
                metadata: String::new(),
            })),

            Request::GetOffset {
                consumer_group,
                topic,
                partition,
            } => Some(RequestType::GetOffset(proto::GetOffsetRequest {
                consumer_group: consumer_group.clone(),
                topic: topic.clone(),
                partition: *partition,
            })),

            Request::Authenticate { username, password } => {
                Some(RequestType::Authenticate(proto::AuthenticateRequest {
                    mechanism: Some(proto::authenticate_request::Mechanism::Plain(
                        proto::PlainAuth {
                            username: username.clone(),
                            password: password.clone(),
                        },
                    )),
                }))
            }

            // For other request types, return an error for now
            _ => {
                return Err(crate::ProtocolError::Serialization(format!(
                    "Request type {:?} not yet supported for protobuf",
                    std::mem::discriminant(self)
                )));
            }
        };

        Ok(proto::Request {
            header: Some(proto::Header {
                version: crate::PROTOCOL_VERSION,
                correlation_id: 0,
                client_id: String::new(),
            }),
            request_type,
        })
    }

    /// Convert proto Request to native Request
    fn from_proto(proto: &proto::Request) -> crate::Result<Self> {
        use proto::request::RequestType;

        let request_type = proto
            .request_type
            .as_ref()
            .ok_or_else(|| crate::ProtocolError::Serialization("Missing request_type".into()))?;

        match request_type {
            RequestType::Ping(_) => Ok(Request::Ping),

            RequestType::ListTopics(_) => Ok(Request::ListTopics),

            RequestType::CreateTopic(req) => Ok(Request::CreateTopic {
                name: req.name.clone(),
                partitions: if req.partitions > 0 {
                    Some(req.partitions)
                } else {
                    None
                },
            }),

            RequestType::DeleteTopic(req) => Ok(Request::DeleteTopic {
                name: req.name.clone(),
            }),

            RequestType::Publish(req) => {
                let record = req
                    .record
                    .as_ref()
                    .ok_or_else(|| crate::ProtocolError::Serialization("Missing record".into()))?;
                Ok(Request::Publish {
                    topic: req.topic.clone(),
                    partition: req.partition,
                    key: if record.key.is_empty() {
                        None
                    } else {
                        Some(Bytes::from(record.key.clone()))
                    },
                    value: Bytes::from(record.value.clone()),
                })
            }

            RequestType::Consume(req) => Ok(Request::Consume {
                topic: req.topic.clone(),
                partition: req.partition,
                offset: req.offset,
                max_messages: req.max_messages as usize,
                isolation_level: if req.isolation_level > 0 {
                    Some(req.isolation_level as u8)
                } else {
                    None
                },
            }),

            RequestType::GetMetadata(req) => Ok(Request::GetMetadata {
                topic: req.topics.first().cloned().unwrap_or_default(),
            }),

            RequestType::CommitOffset(req) => Ok(Request::CommitOffset {
                consumer_group: req.consumer_group.clone(),
                topic: req.topic.clone(),
                partition: req.partition,
                offset: req.offset,
            }),

            RequestType::GetOffset(req) => Ok(Request::GetOffset {
                consumer_group: req.consumer_group.clone(),
                topic: req.topic.clone(),
                partition: req.partition,
            }),

            RequestType::Authenticate(req) => match &req.mechanism {
                Some(proto::authenticate_request::Mechanism::Plain(auth)) => {
                    Ok(Request::Authenticate {
                        username: auth.username.clone(),
                        password: auth.password.clone(),
                    })
                }
                _ => Err(crate::ProtocolError::Serialization(
                    "Unsupported authentication mechanism".into(),
                )),
            },
        }
    }
}

// ============================================================================
// Response Conversions
// ============================================================================

impl Response {
    /// Convert native Response to protobuf bytes
    pub fn to_proto_bytes(&self) -> crate::Result<Vec<u8>> {
        let proto_response = self.to_proto()?;
        Ok(proto_response.encode_to_vec())
    }

    /// Parse protobuf bytes to native Response
    pub fn from_proto_bytes(data: &[u8]) -> crate::Result<Self> {
        let proto_response = proto::Response::decode(data).map_err(|e| {
            crate::ProtocolError::Serialization(format!("Protobuf decode error: {}", e))
        })?;
        Self::from_proto(&proto_response)
    }

    /// Convert native Response to proto Response
    fn to_proto(&self) -> crate::Result<proto::Response> {
        use proto::response::ResponseType;

        let response_type = match self {
            Response::Pong => Some(ResponseType::Ping(proto::PingResponse {})),
            Response::Ok => Some(ResponseType::Ping(proto::PingResponse {})),

            Response::Topics { topics } => {
                Some(ResponseType::ListTopics(proto::ListTopicsResponse {
                    topics: topics.clone(),
                }))
            }

            Response::Published { offset, partition } => {
                Some(ResponseType::Publish(proto::PublishResponse {
                    offset: *offset,
                    partition: *partition,
                    timestamp: 0,
                }))
            }

            Response::TopicCreated { name, partitions } => {
                Some(ResponseType::CreateTopic(proto::CreateTopicResponse {
                    name: name.clone(),
                    partitions: *partitions,
                }))
            }

            Response::TopicDeleted => {
                Some(ResponseType::DeleteTopic(proto::DeleteTopicResponse {}))
            }

            Response::Messages { messages } => {
                Some(ResponseType::Consume(proto::ConsumeResponse {
                    records: messages
                        .iter()
                        .map(|m| proto::ConsumedRecord {
                            offset: m.offset,
                            partition: 0,
                            timestamp: m.timestamp,
                            key: m.key.clone().map(|b| b.to_vec()).unwrap_or_default(),
                            value: m.value.to_vec(),
                            headers: vec![],
                        })
                        .collect(),
                    high_watermark: 0,
                }))
            }

            Response::Metadata { name, partitions } => {
                Some(ResponseType::GetMetadata(proto::GetMetadataResponse {
                    topics: vec![proto::TopicMetadata {
                        name: name.clone(),
                        partitions: (0..*partitions)
                            .map(|id| proto::PartitionMetadata {
                                id,
                                leader: 0,
                                replicas: vec![0],
                                isr: vec![0],
                            })
                            .collect(),
                        internal: false,
                    }],
                    brokers: vec![],
                }))
            }

            Response::OffsetCommitted => {
                Some(ResponseType::CommitOffset(proto::CommitOffsetResponse {}))
            }

            Response::Offset { offset } => {
                Some(ResponseType::GetOffset(proto::GetOffsetResponse {
                    offset: offset.map(|o| o as i64).unwrap_or(-1),
                    metadata: String::new(),
                }))
            }

            Response::Authenticated {
                session_id,
                expires_in,
            } => Some(ResponseType::Authenticate(proto::AuthenticateResponse {
                session_id: session_id.clone(),
                expires_in: *expires_in as u32,
                server_response: vec![],
            })),

            Response::Error { message } => {
                Some(ResponseType::Error(proto::ErrorResponse {
                    code: 1, // ERROR_CODE_UNKNOWN
                    message: message.clone(),
                    details: Default::default(),
                }))
            }

            _ => {
                return Err(crate::ProtocolError::Serialization(format!(
                    "Response type {:?} not yet supported for protobuf",
                    std::mem::discriminant(self)
                )));
            }
        };

        Ok(proto::Response {
            header: Some(proto::Header {
                version: crate::PROTOCOL_VERSION,
                correlation_id: 0,
                client_id: String::new(),
            }),
            response_type,
        })
    }

    /// Convert proto Response to native Response
    fn from_proto(proto: &proto::Response) -> crate::Result<Self> {
        use proto::response::ResponseType;

        let response_type = proto
            .response_type
            .as_ref()
            .ok_or_else(|| crate::ProtocolError::Serialization("Missing response_type".into()))?;

        match response_type {
            ResponseType::Ping(_) => Ok(Response::Pong),

            ResponseType::ListTopics(resp) => Ok(Response::Topics {
                topics: resp.topics.clone(),
            }),

            ResponseType::Publish(resp) => Ok(Response::Published {
                offset: resp.offset,
                partition: resp.partition,
            }),

            ResponseType::CreateTopic(resp) => Ok(Response::TopicCreated {
                name: resp.name.clone(),
                partitions: resp.partitions,
            }),

            ResponseType::DeleteTopic(_) => Ok(Response::TopicDeleted),

            ResponseType::Consume(resp) => Ok(Response::Messages {
                messages: resp
                    .records
                    .iter()
                    .map(|r| crate::MessageData {
                        offset: r.offset,
                        timestamp: r.timestamp,
                        key: if r.key.is_empty() {
                            None
                        } else {
                            Some(Bytes::from(r.key.clone()))
                        },
                        value: Bytes::from(r.value.clone()),
                    })
                    .collect(),
            }),

            ResponseType::GetMetadata(resp) => {
                let topic = resp.topics.first().ok_or_else(|| {
                    crate::ProtocolError::Serialization("No topic in metadata".into())
                })?;
                Ok(Response::Metadata {
                    name: topic.name.clone(),
                    partitions: topic.partitions.len() as u32,
                })
            }

            ResponseType::CommitOffset(_) => Ok(Response::OffsetCommitted),

            ResponseType::GetOffset(resp) => Ok(Response::Offset {
                offset: if resp.offset >= 0 {
                    Some(resp.offset as u64)
                } else {
                    None
                },
            }),

            ResponseType::Authenticate(resp) => Ok(Response::Authenticated {
                session_id: resp.session_id.clone(),
                expires_in: resp.expires_in as u64,
            }),

            ResponseType::Error(resp) => Ok(Response::Error {
                message: resp.message.clone(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping_roundtrip() {
        let request = Request::Ping;
        let bytes = request.to_proto_bytes().unwrap();
        let decoded = Request::from_proto_bytes(&bytes).unwrap();
        assert!(matches!(decoded, Request::Ping));
    }

    #[test]
    fn test_list_topics_roundtrip() {
        let request = Request::ListTopics;
        let bytes = request.to_proto_bytes().unwrap();
        let decoded = Request::from_proto_bytes(&bytes).unwrap();
        assert!(matches!(decoded, Request::ListTopics));
    }

    #[test]
    fn test_create_topic_roundtrip() {
        let request = Request::CreateTopic {
            name: "test-topic".to_string(),
            partitions: Some(4),
        };
        let bytes = request.to_proto_bytes().unwrap();
        let decoded = Request::from_proto_bytes(&bytes).unwrap();

        if let Request::CreateTopic { name, partitions } = decoded {
            assert_eq!(name, "test-topic");
            assert_eq!(partitions, Some(4));
        } else {
            panic!("Expected CreateTopic");
        }
    }

    #[test]
    fn test_publish_roundtrip() {
        let request = Request::Publish {
            topic: "my-topic".to_string(),
            partition: Some(0),
            key: Some(Bytes::from("key-1")),
            value: Bytes::from("Hello, protobuf!"),
        };
        let bytes = request.to_proto_bytes().unwrap();
        let decoded = Request::from_proto_bytes(&bytes).unwrap();

        if let Request::Publish {
            topic,
            partition,
            key,
            value,
        } = decoded
        {
            assert_eq!(topic, "my-topic");
            assert_eq!(partition, Some(0));
            assert_eq!(key, Some(Bytes::from("key-1")));
            assert_eq!(value, Bytes::from("Hello, protobuf!"));
        } else {
            panic!("Expected Publish");
        }
    }

    #[test]
    fn test_response_publish_roundtrip() {
        let response = Response::Published {
            offset: 42,
            partition: 3,
        };
        let bytes = response.to_proto_bytes().unwrap();
        let decoded = Response::from_proto_bytes(&bytes).unwrap();

        if let Response::Published { offset, partition } = decoded {
            assert_eq!(offset, 42);
            assert_eq!(partition, 3);
        } else {
            panic!("Expected Published");
        }
    }

    #[test]
    fn test_response_error_roundtrip() {
        let response = Response::Error {
            message: "Something went wrong".to_string(),
        };
        let bytes = response.to_proto_bytes().unwrap();
        let decoded = Response::from_proto_bytes(&bytes).unwrap();

        if let Response::Error { message } = decoded {
            assert_eq!(message, "Something went wrong");
        } else {
            panic!("Expected Error");
        }
    }
}
