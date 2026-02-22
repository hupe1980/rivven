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

/// Safely convert a proto `u32` producer_epoch to the internal `u16`.
///
/// Returns an error if the value exceeds `u16::MAX`, preventing silent
/// truncation of wire data from untrusted clients.
fn safe_producer_epoch(epoch: u32) -> Result<u16, crate::ProtocolError> {
    u16::try_from(epoch).map_err(|_| {
        crate::ProtocolError::InvalidFormat(format!(
            "producer_epoch {} exceeds u16::MAX ({})",
            epoch,
            u16::MAX
        ))
    })
}

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
                leader_epoch,
            } => Some(RequestType::Publish(proto::PublishRequest {
                topic: topic.clone(),
                partition: *partition,
                record: Some(proto::Record {
                    key: key.clone().map(|b| b.to_vec()).unwrap_or_default(),
                    value: value.to_vec(),
                    headers: vec![],
                    timestamp: 0,
                    has_key: key.is_some(),
                }),
                leader_epoch: *leader_epoch,
            })),

            Request::Consume {
                topic,
                partition,
                offset,
                max_messages,
                isolation_level,
                max_wait_ms,
            } => Some(RequestType::Consume(proto::ConsumeRequest {
                topic: topic.clone(),
                partition: *partition,
                offset: *offset,
                max_messages: u32::try_from(*max_messages).unwrap_or(u32::MAX),
                max_bytes: 0,
                isolation_level: u32::from(isolation_level.unwrap_or(0)),
                max_wait_ms: *max_wait_ms,
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

            #[allow(deprecated)]
            Request::Authenticate {
                username,
                password,
                require_tls,
            } => Some(RequestType::Authenticate(proto::AuthenticateRequest {
                mechanism: Some(proto::authenticate_request::Mechanism::Plain(
                    proto::PlainAuth {
                        username: username.clone(),
                        password: password.clone(),
                        require_tls: *require_tls,
                    },
                )),
            })),

            Request::GetClusterMetadata { topics } => Some(RequestType::GetClusterMetadata(
                proto::GetClusterMetadataRequest {
                    topics: topics.clone(),
                },
            )),

            Request::GetOffsetBounds { topic, partition } => Some(RequestType::GetOffsetBounds(
                proto::GetOffsetBoundsRequest {
                    topic: topic.clone(),
                    partition: *partition,
                },
            )),

            Request::ListGroups => Some(RequestType::ListGroups(proto::ListGroupsRequest {})),

            Request::DescribeGroup { consumer_group } => {
                Some(RequestType::DescribeGroup(proto::DescribeGroupRequest {
                    consumer_group: consumer_group.clone(),
                }))
            }

            Request::DeleteGroup { consumer_group } => {
                Some(RequestType::DeleteGroup(proto::DeleteGroupRequest {
                    consumer_group: consumer_group.clone(),
                }))
            }

            Request::GetOffsetForTimestamp {
                topic,
                partition,
                timestamp_ms,
            } => Some(RequestType::GetOffsetForTimestamp(
                proto::GetOffsetForTimestampRequest {
                    topic: topic.clone(),
                    partition: *partition,
                    timestamp_ms: *timestamp_ms,
                },
            )),

            Request::InitProducerId { producer_id } => {
                Some(RequestType::InitProducerId(proto::InitProducerIdRequest {
                    producer_id: *producer_id,
                }))
            }

            Request::IdempotentPublish {
                topic,
                partition,
                key,
                value,
                producer_id,
                producer_epoch,
                sequence,
                leader_epoch,
            } => Some(RequestType::IdempotentPublish(
                proto::IdempotentPublishRequest {
                    topic: topic.clone(),
                    partition: *partition,
                    record: Some(proto::Record {
                        key: key.clone().map(|b| b.to_vec()).unwrap_or_default(),
                        value: value.to_vec(),
                        headers: vec![],
                        timestamp: 0,
                        has_key: key.is_some(),
                    }),
                    producer_id: *producer_id,
                    producer_epoch: u32::from(*producer_epoch),
                    sequence: *sequence,
                    leader_epoch: *leader_epoch,
                },
            )),

            Request::BeginTransaction {
                txn_id,
                producer_id,
                producer_epoch,
                timeout_ms,
            } => Some(RequestType::BeginTransaction(
                proto::BeginTransactionRequest {
                    txn_id: txn_id.clone(),
                    producer_id: *producer_id,
                    producer_epoch: u32::from(*producer_epoch),
                    timeout_ms: *timeout_ms,
                },
            )),

            Request::CommitTransaction {
                txn_id,
                producer_id,
                producer_epoch,
            } => Some(RequestType::CommitTransaction(
                proto::CommitTransactionRequest {
                    txn_id: txn_id.clone(),
                    producer_id: *producer_id,
                    producer_epoch: u32::from(*producer_epoch),
                },
            )),

            Request::AbortTransaction {
                txn_id,
                producer_id,
                producer_epoch,
            } => Some(RequestType::AbortTransaction(
                proto::AbortTransactionRequest {
                    txn_id: txn_id.clone(),
                    producer_id: *producer_id,
                    producer_epoch: u32::from(*producer_epoch),
                },
            )),

            Request::AlterTopicConfig { topic, configs } => Some(RequestType::AlterTopicConfig(
                proto::AlterTopicConfigRequest {
                    topic: topic.clone(),
                    configs: configs
                        .iter()
                        .map(|c| proto::TopicConfigEntryProto {
                            key: c.key.clone(),
                            value: c.value.clone(),
                        })
                        .collect(),
                },
            )),

            Request::DescribeTopicConfigs { topics } => Some(RequestType::DescribeTopicConfigs(
                proto::DescribeTopicConfigsRequest {
                    topics: topics.clone(),
                },
            )),

            Request::CreatePartitions {
                topic,
                new_partition_count,
                ..
            } => Some(RequestType::CreatePartitions(
                proto::CreatePartitionsRequest {
                    topic: topic.clone(),
                    new_partition_count: *new_partition_count,
                },
            )),

            Request::DeleteRecords {
                topic,
                partition_offsets,
            } => Some(RequestType::DeleteRecords(proto::DeleteRecordsRequest {
                topic: topic.clone(),
                partition_offsets: partition_offsets
                    .iter()
                    .map(|(p, o)| proto::PartitionOffset {
                        partition: *p,
                        offset: *o,
                    })
                    .collect(),
            })),

            // SASL/SCRAM variants use the Authenticate proto with mechanism field
            Request::SaslAuthenticate {
                mechanism,
                auth_bytes,
            } => Some(RequestType::Authenticate(proto::AuthenticateRequest {
                mechanism: Some(proto::authenticate_request::Mechanism::Scram(
                    proto::ScramAuth {
                        mechanism: String::from_utf8_lossy(mechanism).to_string(),
                        client_first: auth_bytes.to_vec(),
                    },
                )),
            })),

            Request::ScramClientFirst { message } => Some(RequestType::ScramClientFirst(
                proto::ScramClientFirstRequest {
                    message: message.to_vec(),
                },
            )),

            Request::ScramClientFinal { message } => Some(RequestType::ScramClientFinal(
                proto::ScramClientFinalRequest {
                    message: message.to_vec(),
                },
            )),

            Request::TransactionalPublish {
                txn_id,
                topic,
                partition,
                key,
                value,
                producer_id,
                producer_epoch,
                sequence,
                leader_epoch,
            } => Some(RequestType::TransactionalPublish(
                proto::TransactionalPublishRequest {
                    txn_id: txn_id.clone(),
                    topic: topic.clone(),
                    partition: *partition,
                    record: Some(proto::Record {
                        key: key.clone().map(|b| b.to_vec()).unwrap_or_default(),
                        value: value.to_vec(),
                        headers: vec![],
                        timestamp: 0,
                        has_key: key.is_some(),
                    }),
                    producer_id: *producer_id,
                    producer_epoch: u32::from(*producer_epoch),
                    sequence: *sequence,
                    leader_epoch: *leader_epoch,
                },
            )),

            Request::AddPartitionsToTxn {
                txn_id,
                producer_id,
                producer_epoch,
                partitions,
            } => Some(RequestType::AddPartitionsToTxn(
                proto::AddPartitionsToTxnRequest {
                    txn_id: txn_id.clone(),
                    producer_id: *producer_id,
                    producer_epoch: u32::from(*producer_epoch),
                    partitions: partitions
                        .iter()
                        .map(|(t, p)| proto::TopicPartition {
                            topic: t.clone(),
                            partition: *p,
                        })
                        .collect(),
                },
            )),

            Request::AddOffsetsToTxn {
                txn_id,
                producer_id,
                producer_epoch,
                group_id,
                offsets,
            } => Some(RequestType::AddOffsetsToTxn(
                proto::AddOffsetsToTxnRequest {
                    txn_id: txn_id.clone(),
                    producer_id: *producer_id,
                    producer_epoch: u32::from(*producer_epoch),
                    group_id: group_id.clone(),
                    offsets: offsets
                        .iter()
                        .map(|(t, p, o)| proto::TopicPartitionOffset {
                            topic: t.clone(),
                            partition: *p,
                            offset: *o,
                        })
                        .collect(),
                },
            )),

            Request::DescribeQuotas { entities } => {
                Some(RequestType::DescribeQuotas(proto::DescribeQuotasRequest {
                    entities: entities
                        .iter()
                        .map(|(et, en)| proto::QuotaEntity {
                            entity_type: et.clone(),
                            entity_name: en.clone(),
                        })
                        .collect(),
                }))
            }

            Request::AlterQuotas { alterations } => {
                Some(RequestType::AlterQuotas(proto::AlterQuotasRequest {
                    alterations: alterations
                        .iter()
                        .map(|a| proto::QuotaAlterationProto {
                            entity_type: a.entity_type.clone(),
                            entity_name: a.entity_name.clone(),
                            quota_key: a.quota_key.clone(),
                            quota_value: a.quota_value,
                        })
                        .collect(),
                }))
            }

            Request::Handshake {
                protocol_version,
                client_id,
            } => Some(RequestType::Handshake(proto::HandshakeRequest {
                protocol_version: *protocol_version,
                client_id: client_id.clone(),
            })),
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
                    key: if record.has_key {
                        Some(Bytes::from(record.key.clone()))
                    } else {
                        None
                    },
                    value: Bytes::from(record.value.clone()),
                    leader_epoch: req.leader_epoch,
                })
            }

            RequestType::Consume(req) => Ok(Request::Consume {
                topic: req.topic.clone(),
                partition: req.partition,
                offset: req.offset,
                max_messages: usize::try_from(req.max_messages).map_err(|_| {
                    crate::ProtocolError::InvalidFormat(format!(
                        "max_messages {} exceeds usize::MAX",
                        req.max_messages
                    ))
                })?,
                isolation_level: if req.isolation_level > 0 {
                    Some(u8::try_from(req.isolation_level).map_err(|_| {
                        crate::ProtocolError::InvalidFormat(format!(
                            "isolation_level {} exceeds u8::MAX ({})",
                            req.isolation_level,
                            u8::MAX
                        ))
                    })?)
                } else {
                    None
                },
                max_wait_ms: req.max_wait_ms,
            }),

            RequestType::GetMetadata(req) => {
                if req.topics.len() > 1 {
                    tracing::debug!(
                        dropped = req.topics.len() - 1,
                        "GetMetadata request contained multiple topics; \
                         only the first is used. Use GetClusterMetadata \
                         for multi-topic queries."
                    );
                }
                Ok(Request::GetMetadata {
                    topic: req.topics.first().cloned().unwrap_or_default(),
                })
            }

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

            #[allow(deprecated)]
            RequestType::Authenticate(req) => match &req.mechanism {
                Some(proto::authenticate_request::Mechanism::Plain(auth)) => {
                    Ok(Request::Authenticate {
                        username: auth.username.clone(),
                        password: auth.password.clone(),
                        require_tls: auth.require_tls,
                    })
                }
                Some(proto::authenticate_request::Mechanism::Scram(scram)) => {
                    Ok(Request::SaslAuthenticate {
                        mechanism: Bytes::from(scram.mechanism.clone()),
                        auth_bytes: Bytes::from(scram.client_first.clone()),
                    })
                }
                _ => Err(crate::ProtocolError::Serialization(
                    "Unsupported authentication mechanism".into(),
                )),
            },

            RequestType::GetClusterMetadata(req) => Ok(Request::GetClusterMetadata {
                topics: req.topics.clone(),
            }),

            RequestType::GetOffsetBounds(req) => Ok(Request::GetOffsetBounds {
                topic: req.topic.clone(),
                partition: req.partition,
            }),

            RequestType::ListGroups(_) => Ok(Request::ListGroups),

            RequestType::DescribeGroup(req) => Ok(Request::DescribeGroup {
                consumer_group: req.consumer_group.clone(),
            }),

            RequestType::DeleteGroup(req) => Ok(Request::DeleteGroup {
                consumer_group: req.consumer_group.clone(),
            }),

            RequestType::GetOffsetForTimestamp(req) => Ok(Request::GetOffsetForTimestamp {
                topic: req.topic.clone(),
                partition: req.partition,
                timestamp_ms: req.timestamp_ms,
            }),

            RequestType::InitProducerId(req) => Ok(Request::InitProducerId {
                producer_id: req.producer_id,
            }),

            RequestType::IdempotentPublish(req) => {
                let record = req
                    .record
                    .as_ref()
                    .ok_or_else(|| crate::ProtocolError::Serialization("Missing record".into()))?;
                Ok(Request::IdempotentPublish {
                    topic: req.topic.clone(),
                    partition: req.partition,
                    key: if record.has_key {
                        Some(Bytes::from(record.key.clone()))
                    } else {
                        None
                    },
                    value: Bytes::from(record.value.clone()),
                    producer_id: req.producer_id,
                    producer_epoch: safe_producer_epoch(req.producer_epoch)?,
                    sequence: req.sequence,
                    leader_epoch: req.leader_epoch,
                })
            }

            RequestType::BeginTransaction(req) => Ok(Request::BeginTransaction {
                txn_id: req.txn_id.clone(),
                producer_id: req.producer_id,
                producer_epoch: safe_producer_epoch(req.producer_epoch)?,
                timeout_ms: req.timeout_ms,
            }),

            RequestType::CommitTransaction(req) => Ok(Request::CommitTransaction {
                txn_id: req.txn_id.clone(),
                producer_id: req.producer_id,
                producer_epoch: safe_producer_epoch(req.producer_epoch)?,
            }),

            RequestType::AbortTransaction(req) => Ok(Request::AbortTransaction {
                txn_id: req.txn_id.clone(),
                producer_id: req.producer_id,
                producer_epoch: safe_producer_epoch(req.producer_epoch)?,
            }),

            RequestType::AlterTopicConfig(req) => Ok(Request::AlterTopicConfig {
                topic: req.topic.clone(),
                configs: req
                    .configs
                    .iter()
                    .map(|c| crate::TopicConfigEntry {
                        key: c.key.clone(),
                        value: c.value.clone(),
                    })
                    .collect(),
            }),

            RequestType::DescribeTopicConfigs(req) => Ok(Request::DescribeTopicConfigs {
                topics: req.topics.clone(),
            }),

            RequestType::CreatePartitions(req) => Ok(Request::CreatePartitions {
                topic: req.topic.clone(),
                new_partition_count: req.new_partition_count,
                assignments: vec![],
            }),

            RequestType::DeleteRecords(req) => Ok(Request::DeleteRecords {
                topic: req.topic.clone(),
                partition_offsets: req
                    .partition_offsets
                    .iter()
                    .map(|po| (po.partition, po.offset))
                    .collect(),
            }),

            RequestType::ScramClientFirst(req) => Ok(Request::ScramClientFirst {
                message: Bytes::from(req.message.clone()),
            }),

            RequestType::ScramClientFinal(req) => Ok(Request::ScramClientFinal {
                message: Bytes::from(req.message.clone()),
            }),

            RequestType::TransactionalPublish(req) => {
                let record = req
                    .record
                    .as_ref()
                    .ok_or_else(|| crate::ProtocolError::Serialization("Missing record".into()))?;
                Ok(Request::TransactionalPublish {
                    txn_id: req.txn_id.clone(),
                    topic: req.topic.clone(),
                    partition: req.partition,
                    key: if record.has_key {
                        Some(Bytes::from(record.key.clone()))
                    } else {
                        None
                    },
                    value: Bytes::from(record.value.clone()),
                    producer_id: req.producer_id,
                    producer_epoch: safe_producer_epoch(req.producer_epoch)?,
                    sequence: req.sequence,
                    leader_epoch: req.leader_epoch,
                })
            }

            RequestType::AddPartitionsToTxn(req) => Ok(Request::AddPartitionsToTxn {
                txn_id: req.txn_id.clone(),
                producer_id: req.producer_id,
                producer_epoch: safe_producer_epoch(req.producer_epoch)?,
                partitions: req
                    .partitions
                    .iter()
                    .map(|tp| (tp.topic.clone(), tp.partition))
                    .collect(),
            }),

            RequestType::AddOffsetsToTxn(req) => Ok(Request::AddOffsetsToTxn {
                txn_id: req.txn_id.clone(),
                producer_id: req.producer_id,
                producer_epoch: safe_producer_epoch(req.producer_epoch)?,
                group_id: req.group_id.clone(),
                offsets: req
                    .offsets
                    .iter()
                    .map(|tpo| (tpo.topic.clone(), tpo.partition, tpo.offset))
                    .collect(),
            }),

            RequestType::DescribeQuotas(req) => Ok(Request::DescribeQuotas {
                entities: req
                    .entities
                    .iter()
                    .map(|e| (e.entity_type.clone(), e.entity_name.clone()))
                    .collect(),
            }),

            RequestType::AlterQuotas(req) => Ok(Request::AlterQuotas {
                alterations: req
                    .alterations
                    .iter()
                    .map(|a| crate::QuotaAlteration {
                        entity_type: a.entity_type.clone(),
                        entity_name: a.entity_name.clone(),
                        quota_key: a.quota_key.clone(),
                        quota_value: a.quota_value,
                    })
                    .collect(),
            }),

            RequestType::Handshake(req) => Ok(Request::Handshake {
                protocol_version: req.protocol_version,
                client_id: req.client_id.clone(),
            }),
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
            Response::Ok => Some(ResponseType::Ok(proto::OkResponse {})),

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
                            partition: m.partition,
                            timestamp: m.timestamp,
                            key: m.key.clone().map(|b| b.to_vec()).unwrap_or_default(),
                            value: m.value.to_vec(),
                            headers: m
                                .headers
                                .iter()
                                .map(|(k, v)| proto::RecordHeader {
                                    key: k.clone(),
                                    value: v.clone(),
                                })
                                .collect(),
                            has_key: m.key.is_some(),
                        })
                        .collect(),
                    high_watermark: 0,
                }))
            }

            //  note: Response::Metadata only carries topic name + partition count,
            // not actual leader/replica/ISR placement. The proto conversion synthesizes
            // placeholder values. gRPC clients that need leader-aware routing should
            // use GetClusterMetadata instead, which returns real placement info.
            Response::Metadata { name, partitions } => {
                Some(ResponseType::GetMetadata(proto::GetMetadataResponse {
                    topics: vec![proto::TopicMetadata {
                        name: name.clone(),
                        partitions: (0..*partitions)
                            .map(|id| proto::PartitionMetadata {
                                id,
                                leader: Some(0),
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
                    offset: offset
                        .map(|o| i64::try_from(o).unwrap_or(i64::MAX))
                        .unwrap_or(-1),
                    metadata: String::new(),
                }))
            }

            Response::Authenticated {
                session_id,
                expires_in,
            } => Some(ResponseType::Authenticate(proto::AuthenticateResponse {
                session_id: session_id.clone(),
                expires_in: u32::try_from(*expires_in).unwrap_or(u32::MAX),
                server_response: vec![],
            })),

            Response::Error { message } => {
                Some(ResponseType::Error(proto::ErrorResponse {
                    code: 1, // ERROR_CODE_UNKNOWN
                    message: message.clone(),
                    details: Default::default(),
                }))
            }

            Response::ClusterMetadata {
                controller_id,
                brokers,
                topics,
            } => Some(ResponseType::GetClusterMetadata(
                proto::GetClusterMetadataResponse {
                    controller_id: controller_id.clone(),
                    brokers: brokers
                        .iter()
                        .map(|b| proto::BrokerInfo {
                            id: b.node_id.parse().unwrap_or_else(|_| {
                                tracing::warn!(
                                    node_id = %b.node_id,
                                    "BrokerInfo.id: failed to parse node_id as u32, using sentinel u32::MAX",
                                );
                                u32::MAX
                            }),
                            host: b.host.clone(),
                            port: u32::from(b.port),
                            rack: b.rack.clone().unwrap_or_default(),
                        })
                        .collect(),
                    topics: topics
                        .iter()
                        .map(|t| proto::TopicMetadata {
                            name: t.name.clone(),
                            partitions: t
                                .partitions
                                .iter()
                                .map(|p| proto::PartitionMetadata {
                                    id: p.partition,
                                    leader: p
                                        .leader
                                        .as_ref()
                                        .and_then(|l| l.parse().ok()),
                                    replicas: p
                                        .replicas
                                        .iter()
                                        .filter_map(|r| r.parse().ok())
                                        .collect(),
                                    isr: p.isr.iter().filter_map(|i| i.parse().ok()).collect(),
                                })
                                .collect(),
                            internal: t.is_internal,
                        })
                        .collect(),
                },
            )),

            Response::OffsetBounds { earliest, latest } => Some(ResponseType::GetOffsetBounds(
                proto::GetOffsetBoundsResponse {
                    earliest: *earliest,
                    latest: *latest,
                },
            )),

            Response::Groups { groups } => {
                Some(ResponseType::ListGroups(proto::ListGroupsResponse {
                    groups: groups.clone(),
                }))
            }

            Response::GroupDescription {
                consumer_group,
                offsets,
            } => Some(ResponseType::DescribeGroup(proto::DescribeGroupResponse {
                consumer_group: consumer_group.clone(),
                topic_offsets: offsets
                    .iter()
                    .map(|(topic, partitions)| proto::GroupTopicOffsets {
                        topic: topic.clone(),
                        partition_offsets: partitions
                            .iter()
                            .map(|(p, o)| proto::GroupPartitionOffset {
                                partition: *p,
                                offset: *o,
                            })
                            .collect(),
                    })
                    .collect(),
            })),

            Response::GroupDeleted => {
                Some(ResponseType::DeleteGroup(proto::DeleteGroupResponse {}))
            }

            Response::OffsetForTimestamp { offset } => Some(ResponseType::GetOffsetForTimestamp(
                proto::GetOffsetForTimestampResponse { offset: *offset },
            )),

            Response::ProducerIdInitialized {
                producer_id,
                producer_epoch,
            } => Some(ResponseType::InitProducerId(
                proto::InitProducerIdResponse {
                    producer_id: *producer_id,
                    producer_epoch: u32::from(*producer_epoch),
                },
            )),

            Response::IdempotentPublished {
                offset,
                partition,
                duplicate,
            } => Some(ResponseType::IdempotentPublish(
                proto::IdempotentPublishResponse {
                    offset: *offset,
                    partition: *partition,
                    duplicate: *duplicate,
                },
            )),

            Response::TransactionStarted { txn_id } => Some(ResponseType::BeginTransaction(
                proto::BeginTransactionResponse {
                    txn_id: txn_id.clone(),
                },
            )),

            Response::TransactionCommitted { txn_id } => Some(ResponseType::CommitTransaction(
                proto::CommitTransactionResponse {
                    txn_id: txn_id.clone(),
                },
            )),

            Response::TransactionAborted { txn_id } => Some(ResponseType::AbortTransaction(
                proto::AbortTransactionResponse {
                    txn_id: txn_id.clone(),
                },
            )),

            Response::TopicConfigAltered {
                topic,
                changed_count,
            } => Some(ResponseType::AlterTopicConfig(
                proto::AlterTopicConfigResponse {
                    topic: topic.clone(),
                    changed_count: u32::try_from(*changed_count).unwrap_or(u32::MAX),
                },
            )),

            Response::TopicConfigsDescribed { configs } => Some(
                ResponseType::DescribeTopicConfigs(proto::DescribeTopicConfigsResponse {
                    configs: configs
                        .iter()
                        .map(|c| proto::TopicConfigDescriptionProto {
                            topic: c.topic.clone(),
                            entries: c
                                .configs
                                .iter()
                                .map(|(key, cv)| proto::TopicConfigValueProto {
                                    key: key.clone(),
                                    value: cv.value.clone(),
                                    is_default: cv.is_default,
                                    is_read_only: cv.is_read_only,
                                    is_sensitive: cv.is_sensitive,
                                })
                                .collect(),
                        })
                        .collect(),
                }),
            ),

            Response::PartitionsCreated {
                topic,
                new_partition_count,
            } => Some(ResponseType::CreatePartitions(
                proto::CreatePartitionsResponse {
                    topic: topic.clone(),
                    new_partition_count: *new_partition_count,
                },
            )),

            Response::RecordsDeleted { topic, results } => {
                Some(ResponseType::DeleteRecords(proto::DeleteRecordsResponse {
                    topic: topic.clone(),
                    results: results
                        .iter()
                        .map(|r| proto::DeleteRecordsResultProto {
                            partition: r.partition,
                            low_watermark: r.low_watermark,
                            error: r.error.clone().unwrap_or_default(),
                        })
                        .collect(),
                }))
            }

            Response::ScramServerFirst { message } => Some(ResponseType::ScramServerFirst(
                proto::ScramServerFirstResponse {
                    message: message.to_vec(),
                },
            )),

            Response::ScramServerFinal {
                message,
                session_id,
                expires_in,
            } => Some(ResponseType::ScramServerFinal(
                proto::ScramServerFinalResponse {
                    message: message.to_vec(),
                    session_id: session_id.clone(),
                    expires_in: *expires_in,
                },
            )),

            Response::PartitionsAddedToTxn {
                txn_id,
                partition_count,
            } => Some(ResponseType::PartitionsAddedToTxn(
                proto::PartitionsAddedToTxnResponse {
                    txn_id: txn_id.clone(),
                    partition_count: u32::try_from(*partition_count).unwrap_or(u32::MAX),
                },
            )),

            Response::TransactionalPublished {
                offset,
                partition,
                sequence,
            } => Some(ResponseType::TransactionalPublish(
                proto::TransactionalPublishResponse {
                    offset: *offset,
                    partition: *partition,
                    sequence: *sequence,
                },
            )),

            Response::OffsetsAddedToTxn { txn_id } => Some(ResponseType::OffsetsAddedToTxn(
                proto::OffsetsAddedToTxnResponse {
                    txn_id: txn_id.clone(),
                },
            )),

            Response::QuotasDescribed { entries } => Some(ResponseType::QuotasDescribed(
                proto::QuotasDescribedResponse {
                    entries: entries
                        .iter()
                        .map(|e| proto::QuotaEntryProto {
                            entity_type: e.entity_type.clone(),
                            entity_name: e.entity_name.clone(),
                            quotas: e.quotas.clone(),
                        })
                        .collect(),
                },
            )),

            Response::QuotasAltered { altered_count } => Some(ResponseType::QuotasAltered(
                proto::QuotasAlteredResponse {
                    altered_count: u32::try_from(*altered_count).unwrap_or(u32::MAX),
                },
            )),

            Response::Throttled {
                throttle_time_ms,
                quota_type,
                entity,
            } => Some(ResponseType::Throttled(proto::ThrottledResponse {
                throttle_time_ms: *throttle_time_ms,
                quota_type: quota_type.clone(),
                entity: entity.clone(),
            })),

            Response::HandshakeResult {
                server_version,
                compatible,
                message,
            } => Some(ResponseType::HandshakeResult(
                proto::HandshakeResultResponse {
                    server_version: *server_version,
                    compatible: *compatible,
                    message: message.clone(),
                },
            )),
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
            ResponseType::Ok(_) => Ok(Response::Ok),

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
                        partition: r.partition,
                        timestamp: r.timestamp,
                        key: if r.has_key {
                            Some(Bytes::from(r.key.clone()))
                        } else {
                            None
                        },
                        value: Bytes::from(r.value.clone()),
                        headers: r
                            .headers
                            .iter()
                            .map(|h| (h.key.clone(), h.value.clone()))
                            .collect(),
                    })
                    .collect(),
            }),

            ResponseType::GetMetadata(resp) => {
                if resp.topics.len() > 1 {
                    tracing::debug!(
                        dropped = resp.topics.len() - 1,
                        "GetMetadata response contained multiple topics; \
                         only the first is returned. Use GetClusterMetadata \
                         for multi-topic queries."
                    );
                }
                let topic = resp.topics.first().ok_or_else(|| {
                    crate::ProtocolError::Serialization("No topic in metadata".into())
                })?;
                Ok(Response::Metadata {
                    name: topic.name.clone(),
                    partitions: topic.partitions.len().try_into().unwrap_or(u32::MAX),
                })
            }

            ResponseType::CommitOffset(_) => Ok(Response::OffsetCommitted),

            ResponseType::GetOffset(resp) => Ok(Response::Offset {
                offset: resp.offset.try_into().ok(),
            }),

            ResponseType::Authenticate(resp) => Ok(Response::Authenticated {
                session_id: resp.session_id.clone(),
                expires_in: resp.expires_in.into(),
            }),

            ResponseType::Error(resp) => Ok(Response::Error {
                message: resp.message.clone(),
            }),

            ResponseType::GetClusterMetadata(resp) => Ok(Response::ClusterMetadata {
                controller_id: resp.controller_id.clone(),
                brokers: resp
                    .brokers
                    .iter()
                    .map(|b| crate::BrokerInfo {
                        node_id: b.id.to_string(),
                        host: b.host.clone(),
                        port: u16::try_from(b.port).unwrap_or_else(|_| {
                            tracing::warn!(
                                port = b.port,
                                "BrokerInfo.port exceeds u16 range, clamping to u16::MAX"
                            );
                            u16::MAX
                        }),
                        rack: if b.rack.is_empty() {
                            None
                        } else {
                            Some(b.rack.clone())
                        },
                    })
                    .collect(),
                topics: resp
                    .topics
                    .iter()
                    .map(|t| crate::TopicMetadata {
                        name: t.name.clone(),
                        is_internal: t.internal,
                        partitions: t
                            .partitions
                            .iter()
                            .map(|p| crate::PartitionMetadata {
                                partition: p.id,
                                leader: p.leader.map(|l| l.to_string()),
                                replicas: p.replicas.iter().map(|r| r.to_string()).collect(),
                                isr: p.isr.iter().map(|i| i.to_string()).collect(),
                                offline: p.leader.is_none() && p.replicas.is_empty(),
                            })
                            .collect(),
                    })
                    .collect(),
            }),

            ResponseType::GetOffsetBounds(resp) => Ok(Response::OffsetBounds {
                earliest: resp.earliest,
                latest: resp.latest,
            }),

            ResponseType::ListGroups(resp) => Ok(Response::Groups {
                groups: resp.groups.clone(),
            }),

            ResponseType::DescribeGroup(resp) => {
                use std::collections::HashMap;
                let mut offsets: HashMap<String, HashMap<u32, u64>> = HashMap::new();
                for to in &resp.topic_offsets {
                    let partition_map = offsets.entry(to.topic.clone()).or_default();
                    for po in &to.partition_offsets {
                        partition_map.insert(po.partition, po.offset);
                    }
                }
                Ok(Response::GroupDescription {
                    consumer_group: resp.consumer_group.clone(),
                    offsets,
                })
            }

            ResponseType::DeleteGroup(_) => Ok(Response::GroupDeleted),

            ResponseType::GetOffsetForTimestamp(resp) => Ok(Response::OffsetForTimestamp {
                offset: resp.offset,
            }),

            ResponseType::InitProducerId(resp) => Ok(Response::ProducerIdInitialized {
                producer_id: resp.producer_id,
                producer_epoch: safe_producer_epoch(resp.producer_epoch)?,
            }),

            ResponseType::IdempotentPublish(resp) => Ok(Response::IdempotentPublished {
                offset: resp.offset,
                partition: resp.partition,
                duplicate: resp.duplicate,
            }),

            ResponseType::BeginTransaction(resp) => Ok(Response::TransactionStarted {
                txn_id: resp.txn_id.clone(),
            }),

            ResponseType::CommitTransaction(resp) => Ok(Response::TransactionCommitted {
                txn_id: resp.txn_id.clone(),
            }),

            ResponseType::AbortTransaction(resp) => Ok(Response::TransactionAborted {
                txn_id: resp.txn_id.clone(),
            }),

            ResponseType::AlterTopicConfig(resp) => Ok(Response::TopicConfigAltered {
                topic: resp.topic.clone(),
                changed_count: resp.changed_count.try_into().unwrap_or(0),
            }),

            ResponseType::DescribeTopicConfigs(resp) => Ok(Response::TopicConfigsDescribed {
                configs: resp
                    .configs
                    .iter()
                    .map(|c| {
                        let mut config_map = std::collections::HashMap::new();
                        for e in &c.entries {
                            config_map.insert(
                                e.key.clone(),
                                crate::TopicConfigValue {
                                    value: e.value.clone(),
                                    is_default: e.is_default,
                                    is_read_only: e.is_read_only,
                                    is_sensitive: e.is_sensitive,
                                },
                            );
                        }
                        crate::TopicConfigDescription {
                            topic: c.topic.clone(),
                            configs: config_map,
                        }
                    })
                    .collect(),
            }),

            ResponseType::CreatePartitions(resp) => Ok(Response::PartitionsCreated {
                topic: resp.topic.clone(),
                new_partition_count: resp.new_partition_count,
            }),

            ResponseType::DeleteRecords(resp) => Ok(Response::RecordsDeleted {
                topic: resp.topic.clone(),
                results: resp
                    .results
                    .iter()
                    .map(|r| crate::DeleteRecordsResult {
                        partition: r.partition,
                        low_watermark: r.low_watermark,
                        error: if r.error.is_empty() {
                            None
                        } else {
                            Some(r.error.clone())
                        },
                    })
                    .collect(),
            }),

            ResponseType::ScramServerFirst(resp) => Ok(Response::ScramServerFirst {
                message: Bytes::from(resp.message.clone()),
            }),

            ResponseType::ScramServerFinal(resp) => Ok(Response::ScramServerFinal {
                message: Bytes::from(resp.message.clone()),
                session_id: resp.session_id.clone(),
                expires_in: resp.expires_in,
            }),

            ResponseType::PartitionsAddedToTxn(resp) => Ok(Response::PartitionsAddedToTxn {
                txn_id: resp.txn_id.clone(),
                partition_count: resp.partition_count.try_into().unwrap_or(0),
            }),

            ResponseType::TransactionalPublish(resp) => Ok(Response::TransactionalPublished {
                offset: resp.offset,
                partition: resp.partition,
                sequence: resp.sequence,
            }),

            ResponseType::OffsetsAddedToTxn(resp) => Ok(Response::OffsetsAddedToTxn {
                txn_id: resp.txn_id.clone(),
            }),

            ResponseType::QuotasDescribed(resp) => Ok(Response::QuotasDescribed {
                entries: resp
                    .entries
                    .iter()
                    .map(|e| crate::QuotaEntry {
                        entity_type: e.entity_type.clone(),
                        entity_name: e.entity_name.clone(),
                        quotas: e.quotas.clone(),
                    })
                    .collect(),
            }),

            ResponseType::QuotasAltered(resp) => Ok(Response::QuotasAltered {
                altered_count: resp.altered_count.try_into().unwrap_or(0),
            }),

            ResponseType::Throttled(resp) => Ok(Response::Throttled {
                throttle_time_ms: resp.throttle_time_ms,
                quota_type: resp.quota_type.clone(),
                entity: resp.entity.clone(),
            }),

            ResponseType::HandshakeResult(resp) => Ok(Response::HandshakeResult {
                server_version: resp.server_version,
                compatible: resp.compatible,
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
            leader_epoch: Some(42),
        };
        let bytes = request.to_proto_bytes().unwrap();
        let decoded = Request::from_proto_bytes(&bytes).unwrap();

        if let Request::Publish {
            topic,
            partition,
            key,
            value,
            leader_epoch,
        } = decoded
        {
            assert_eq!(topic, "my-topic");
            assert_eq!(partition, Some(0));
            assert_eq!(key, Some(Bytes::from("key-1")));
            assert_eq!(value, Bytes::from("Hello, protobuf!"));
            assert_eq!(leader_epoch, Some(42));
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
