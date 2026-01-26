use crate::partitioner::{StickyPartitioner, StickyPartitionerConfig};
use crate::protocol::{MessageData, Request, Response};
use rivven_core::{Message, OffsetManager, TopicManager, Validator, schema_registry::{EmbeddedSchemaRegistry, SchemaRegistry}};
use bytes::Bytes;
use std::sync::Arc;
use tracing::{debug, error, warn};

/// Handles incoming requests
pub struct RequestHandler {
    topic_manager: TopicManager,
    offset_manager: OffsetManager,
    schema_registry: Arc<EmbeddedSchemaRegistry>,
    partitioner: StickyPartitioner,
}

impl RequestHandler {
    /// Create a new request handler with default partitioner settings
    pub fn new(
        topic_manager: TopicManager,
        offset_manager: OffsetManager,
        schema_registry: Arc<EmbeddedSchemaRegistry>,
    ) -> Self {
        Self {
            topic_manager,
            offset_manager,
            schema_registry,
            partitioner: StickyPartitioner::new(),
        }
    }

    /// Create a new request handler with custom partitioner config
    pub fn with_partitioner_config(
        topic_manager: TopicManager,
        offset_manager: OffsetManager,
        schema_registry: Arc<EmbeddedSchemaRegistry>,
        partitioner_config: StickyPartitionerConfig,
    ) -> Self {
        Self {
            topic_manager,
            offset_manager,
            schema_registry,
            partitioner: StickyPartitioner::with_config(partitioner_config),
        }
    }

    /// Handle a request and return a response
    pub async fn handle(&self, request: Request) -> Response {
        match request {
            // Authentication is handled by AuthenticatedHandler, not here
            Request::Authenticate { .. } 
            | Request::SaslAuthenticate { .. }
            | Request::ScramClientFirst { .. }
            | Request::ScramClientFinal { .. } => {
                Response::Error {
                    message: "UNSUPPORTED: Use authenticated endpoint for authentication".to_string(),
                }
            }
            
            Request::Publish {
                topic,
                partition,
                key,
                value,
            } => self.handle_publish(topic, partition, key, value).await,

            Request::Consume {
                topic,
                partition,
                offset,
                max_messages,
            } => self.handle_consume(topic, partition, offset, max_messages).await,

            Request::CreateTopic { name, partitions } => {
                self.handle_create_topic(name, partitions).await
            }

            Request::ListTopics => self.handle_list_topics().await,

            Request::DeleteTopic { name } => self.handle_delete_topic(name).await,

            Request::CommitOffset {
                consumer_group,
                topic,
                partition,
                offset,
            } => {
                self.handle_commit_offset(consumer_group, topic, partition, offset)
                    .await
            }

            Request::GetOffset {
                consumer_group,
                topic,
                partition,
            } => self.handle_get_offset(consumer_group, topic, partition).await,

            Request::GetOffsetBounds { topic, partition } => {
                self.handle_get_offset_bounds(topic, partition).await
            }

            Request::GetMetadata { topic } => self.handle_get_metadata(topic).await,
            
            Request::GetClusterMetadata { .. } => {
                // Cluster metadata is handled by the router, not here
                // If it gets here, we're in standalone mode without full cluster support
                Response::Error {
                    message: "Cluster metadata not available in basic handler".to_string(),
                }
            }

            Request::Ping => Response::Pong,

            Request::RegisterSchema { subject, schema } => {
                self.handle_register_schema(subject, schema).await
            }

            Request::GetSchema { id } => self.handle_get_schema(id).await,

            Request::ListGroups => self.handle_list_groups().await,

            Request::DescribeGroup { consumer_group } => {
                self.handle_describe_group(consumer_group).await
            }

            Request::DeleteGroup { consumer_group } => {
                self.handle_delete_group(consumer_group).await
            }

            Request::GetOffsetForTimestamp {
                topic,
                partition,
                timestamp_ms,
            } => {
                self.handle_get_offset_for_timestamp(topic, partition, timestamp_ms)
                    .await
            }
        }
    }

    async fn handle_publish(
        &self,
        topic_name: String,
        partition: Option<u32>,
        key: Option<Bytes>,
        value: Bytes,
    ) -> Response {
        // Validate topic name
        if let Err(e) = Validator::validate_topic_name(&topic_name) {
            warn!("Invalid topic name '{}': {}", Validator::sanitize_for_log(&topic_name, 50), e);
            return Response::Error {
                message: format!("INVALID_TOPIC_NAME: {}", e),
            };
        }

        // Get or create topic
        let topic = match self.topic_manager.get_or_create_topic(topic_name).await {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to get/create topic: {}", e);
                return Response::Error {
                    message: e.to_string(),
                };
            }
        };

        // Determine partition using sticky partitioner
        // - Explicit partition: use as-is
        // - With key: hash-based (same key → same partition)
        // - Without key: sticky partitioning (batched rotation for efficiency)
        let partition_id = partition.unwrap_or_else(|| {
            self.partitioner.partition(
                topic.name(),
                key.as_ref().map(|k| k.as_ref()),
                topic.num_partitions() as u32,
            )
        });

        // Create message
        let message = if let Some(k) = key {
            Message::with_key(k, value)
        } else {
            Message::new(value)
        };

        // Append to partition
        match topic.append(partition_id, message).await {
            Ok(offset) => {
                debug!("Published to partition {} at offset {}", partition_id, offset);
                Response::Published {
                    offset,
                    partition: partition_id,
                }
            }
            Err(e) => {
                error!("Failed to append message: {}", e);
                Response::Error {
                    message: e.to_string(),
                }
            }
        }
    }

    async fn handle_consume(
        &self,
        topic_name: String,
        partition: u32,
        offset: u64,
        max_messages: usize,
    ) -> Response {
        // Validate topic name
        if let Err(e) = Validator::validate_topic_name(&topic_name) {
            warn!("Invalid topic name '{}': {}", Validator::sanitize_for_log(&topic_name, 50), e);
            return Response::Error {
                message: format!("INVALID_TOPIC_NAME: {}", e),
            };
        }

        // Enforce reasonable limits on max_messages
        let max_messages = max_messages.min(10_000); // Cap at 10k messages per request

        let topic = match self.topic_manager.get_topic(&topic_name).await {
            Ok(t) => t,
            Err(e) => {
                return Response::Error {
                    message: e.to_string(),
                }
            }
        };

        // Validate partition
        if let Err(e) = Validator::validate_partition(partition, topic.num_partitions() as u32) {
            return Response::Error {
                message: format!("INVALID_PARTITION: {}", e),
            };
        }

        match topic.read(partition, offset, max_messages).await {
            Ok(messages) => {
                let message_data: Vec<MessageData> = messages
                    .into_iter()
                    .map(|msg| MessageData {
                        offset: msg.offset,
                        key: msg.key,
                        value: msg.value,
                        timestamp: msg.timestamp.timestamp_millis(),
                    })
                    .collect();

                Response::Messages {
                    messages: message_data,
                }
            }
            Err(e) => Response::Error {
                message: e.to_string(),
            },
        }
    }

    async fn handle_create_topic(&self, name: String, partitions: Option<u32>) -> Response {
        // Validate topic name
        if let Err(e) = Validator::validate_topic_name(&name) {
            warn!("Invalid topic name '{}': {}", Validator::sanitize_for_log(&name, 50), e);
            return Response::Error {
                message: format!("INVALID_TOPIC_NAME: {}", e),
            };
        }

        // Validate partition count (reasonable limits)
        if let Some(p) = partitions {
            if p == 0 || p > 1000 {
                return Response::Error {
                    message: format!("INVALID_PARTITION_COUNT: must be between 1 and 1000, got {}", p),
                };
            }
        }

        match self.topic_manager.create_topic(name.clone(), partitions).await {
            Ok(topic) => Response::TopicCreated {
                name,
                partitions: topic.num_partitions() as u32,
            },
            Err(e) => Response::Error {
                message: e.to_string(),
            },
        }
    }

    async fn handle_list_topics(&self) -> Response {
        let topics = self.topic_manager.list_topics().await;
        Response::Topics { topics }
    }

    async fn handle_delete_topic(&self, name: String) -> Response {
        // Validate topic name
        if let Err(e) = Validator::validate_topic_name(&name) {
            warn!("Invalid topic name '{}': {}", Validator::sanitize_for_log(&name, 50), e);
            return Response::Error {
                message: format!("INVALID_TOPIC_NAME: {}", e),
            };
        }

        match self.topic_manager.delete_topic(&name).await {
            Ok(_) => Response::TopicDeleted,
            Err(e) => Response::Error {
                message: e.to_string(),
            },
        }
    }

    async fn handle_commit_offset(
        &self,
        consumer_group: String,
        topic: String,
        partition: u32,
        offset: u64,
    ) -> Response {
        // Validate consumer group
        if let Err(e) = Validator::validate_consumer_group_id(&consumer_group) {
            warn!("Invalid consumer group '{}': {}", Validator::sanitize_for_log(&consumer_group, 50), e);
            return Response::Error {
                message: format!("INVALID_CONSUMER_GROUP: {}", e),
            };
        }

        // Validate topic name
        if let Err(e) = Validator::validate_topic_name(&topic) {
            return Response::Error {
                message: format!("INVALID_TOPIC_NAME: {}", e),
            };
        }

        self.offset_manager
            .commit_offset(&consumer_group, &topic, partition, offset)
            .await;
        Response::OffsetCommitted
    }

    async fn handle_get_offset(
        &self,
        consumer_group: String,
        topic: String,
        partition: u32,
    ) -> Response {
        // Validate consumer group
        if let Err(e) = Validator::validate_consumer_group_id(&consumer_group) {
            return Response::Error {
                message: format!("INVALID_CONSUMER_GROUP: {}", e),
            };
        }

        // Validate topic name
        if let Err(e) = Validator::validate_topic_name(&topic) {
            return Response::Error {
                message: format!("INVALID_TOPIC_NAME: {}", e),
            };
        }

        let offset = self
            .offset_manager
            .get_offset(&consumer_group, &topic, partition)
            .await;
        Response::Offset { offset }
    }

    async fn handle_get_offset_bounds(&self, topic_name: String, partition: u32) -> Response {
        // Validate topic name
        if let Err(e) = Validator::validate_topic_name(&topic_name) {
            return Response::Error {
                message: format!("INVALID_TOPIC_NAME: {}", e),
            };
        }

        match self.topic_manager.get_topic(&topic_name).await {
            Ok(topic) => {
                match topic.partition(partition) {
                    Ok(p) => {
                        let earliest = p.earliest_offset().await.unwrap_or(0);
                        let latest = p.latest_offset().await;
                        Response::OffsetBounds { earliest, latest }
                    }
                    Err(_) => Response::Error {
                        message: format!("Partition {} not found for topic '{}'", partition, topic_name),
                    },
                }
            }
            Err(e) => Response::Error {
                message: e.to_string(),
            },
        }
    }

    async fn handle_get_metadata(&self, topic_name: String) -> Response {
        // Validate topic name
        if let Err(e) = Validator::validate_topic_name(&topic_name) {
            return Response::Error {
                message: format!("INVALID_TOPIC_NAME: {}", e),
            };
        }

        match self.topic_manager.get_topic(&topic_name).await {
            Ok(topic) => Response::Metadata {
                name: topic_name,
                partitions: topic.num_partitions() as u32,
            },
            Err(e) => Response::Error {
                message: e.to_string(),
            },
        }
    }

    async fn handle_register_schema(&self, subject: String, schema: String) -> Response {
        match self.schema_registry.register(&subject, &schema).await {
            Ok(id) => Response::SchemaRegistered { id },
            Err(e) => Response::Error {
                message: format!("Failed to register schema: {}", e),
            },
        }
    }

    async fn handle_get_schema(&self, id: i32) -> Response {
        match self.schema_registry.get_schema(id).await {
            Ok(schema) => {
                // Return schema as JSON string (Apache Avro schema's default serialization)
                // Assuming we want the canonical form or just the JSON representation.
                // apache_avro::Schema doesn't impl Display directly in a way that gives JSON always,
                // but we can use canonical_form() or just format if it implements it.
                // Let's assume canonical_form() is best for "schema string".
                Response::Schema {
                    id,
                    schema: schema.canonical_form(),
                }
            },
            Err(e) => Response::Error {
                message: format!("Failed to get schema: {}", e),
            },
        }
    }

    async fn handle_list_groups(&self) -> Response {
        let groups = self.offset_manager.list_groups().await;
        Response::Groups { groups }
    }

    async fn handle_describe_group(&self, consumer_group: String) -> Response {
        // Validate consumer group name
        if let Err(e) = Validator::validate_consumer_group_id(&consumer_group) {
            return Response::Error {
                message: format!("INVALID_GROUP_ID: {}", e),
            };
        }

        match self.offset_manager.get_group_offsets(&consumer_group).await {
            Some(offsets) => Response::GroupDescription {
                consumer_group,
                offsets,
            },
            None => Response::Error {
                message: format!("Consumer group '{}' not found", consumer_group),
            },
        }
    }

    async fn handle_delete_group(&self, consumer_group: String) -> Response {
        // Validate consumer group name
        if let Err(e) = Validator::validate_consumer_group_id(&consumer_group) {
            return Response::Error {
                message: format!("INVALID_GROUP_ID: {}", e),
            };
        }

        if self.offset_manager.delete_group(&consumer_group).await {
            Response::GroupDeleted
        } else {
            Response::Error {
                message: format!("Consumer group '{}' not found", consumer_group),
            }
        }
    }

    async fn handle_get_offset_for_timestamp(
        &self,
        topic_name: String,
        partition: u32,
        timestamp_ms: i64,
    ) -> Response {
        // Validate topic name
        if let Err(e) = Validator::validate_topic_name(&topic_name) {
            return Response::Error {
                message: format!("INVALID_TOPIC_NAME: {}", e),
            };
        }

        let topic = match self.topic_manager.get_topic(&topic_name).await {
            Ok(t) => t,
            Err(e) => {
                return Response::Error {
                    message: e.to_string(),
                }
            }
        };

        // Validate partition
        if let Err(e) = Validator::validate_partition(partition, topic.num_partitions() as u32) {
            return Response::Error {
                message: format!("INVALID_PARTITION: {}", e),
            };
        }

        match topic.find_offset_for_timestamp(partition, timestamp_ms).await {
            Ok(offset) => Response::OffsetForTimestamp { offset },
            Err(e) => Response::Error {
                message: format!("Failed to find offset for timestamp: {}", e),
            },
        }
    }
}
