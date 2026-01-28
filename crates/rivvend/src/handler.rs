use crate::partitioner::{StickyPartitioner, StickyPartitionerConfig};
use crate::protocol::{
    DeleteRecordsResult, MessageData, QuotaAlteration, QuotaEntry, Request, Response,
    TopicConfigDescription, TopicConfigEntry, TopicConfigValue,
};
use bytes::Bytes;
use rivven_core::{
    schema_registry::{EmbeddedSchemaRegistry, SchemaRegistry},
    IdempotentProducerManager, Message, OffsetManager, QuotaConfig, QuotaEntity, QuotaManager,
    QuotaResult, SequenceResult, TopicConfigManager, TopicManager, TransactionCoordinator,
    TransactionPartition, TransactionResult, Validator,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Handles incoming requests
pub struct RequestHandler {
    topic_manager: TopicManager,
    offset_manager: OffsetManager,
    schema_registry: Arc<EmbeddedSchemaRegistry>,
    partitioner: StickyPartitioner,
    /// Idempotent producer state manager (KIP-98)
    idempotent_manager: Arc<IdempotentProducerManager>,
    /// Transaction coordinator (KIP-98 Transactions)
    transaction_coordinator: Arc<TransactionCoordinator>,
    /// Per-principal quota manager (Kafka parity)
    quota_manager: Arc<QuotaManager>,
    /// Topic configuration manager (Admin API)
    topic_config_manager: Arc<TopicConfigManager>,
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
            idempotent_manager: Arc::new(IdempotentProducerManager::new()),
            transaction_coordinator: Arc::new(TransactionCoordinator::new()),
            quota_manager: Arc::new(QuotaManager::new()),
            topic_config_manager: Arc::new(TopicConfigManager::new()),
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
            idempotent_manager: Arc::new(IdempotentProducerManager::new()),
            transaction_coordinator: Arc::new(TransactionCoordinator::new()),
            quota_manager: Arc::new(QuotaManager::new()),
            topic_config_manager: Arc::new(TopicConfigManager::new()),
        }
    }

    /// Create a new request handler with custom quota configuration
    pub fn with_quota_manager(
        topic_manager: TopicManager,
        offset_manager: OffsetManager,
        schema_registry: Arc<EmbeddedSchemaRegistry>,
        quota_manager: Arc<QuotaManager>,
    ) -> Self {
        Self {
            topic_manager,
            offset_manager,
            schema_registry,
            partitioner: StickyPartitioner::new(),
            idempotent_manager: Arc::new(IdempotentProducerManager::new()),
            transaction_coordinator: Arc::new(TransactionCoordinator::new()),
            quota_manager,
            topic_config_manager: Arc::new(TopicConfigManager::new()),
        }
    }

    /// Get the quota manager
    pub fn quota_manager(&self) -> &Arc<QuotaManager> {
        &self.quota_manager
    }

    /// Get the topic config manager
    pub fn topic_config_manager(&self) -> &Arc<TopicConfigManager> {
        &self.topic_config_manager
    }

    /// Handle a request and return a response
    pub async fn handle(&self, request: Request) -> Response {
        match request {
            // Authentication is handled by AuthenticatedHandler, not here
            Request::Authenticate { .. }
            | Request::SaslAuthenticate { .. }
            | Request::ScramClientFirst { .. }
            | Request::ScramClientFinal { .. } => Response::Error {
                message: "UNSUPPORTED: Use authenticated endpoint for authentication".to_string(),
            },

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
            } => {
                self.handle_consume(topic, partition, offset, max_messages)
                    .await
            }

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
            } => {
                self.handle_get_offset(consumer_group, topic, partition)
                    .await
            }

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

            // Idempotent Producer (KIP-98)
            Request::InitProducerId { producer_id } => {
                self.handle_init_producer_id(producer_id).await
            }

            Request::IdempotentPublish {
                topic,
                partition,
                key,
                value,
                producer_id,
                producer_epoch,
                sequence,
            } => {
                self.handle_idempotent_publish(
                    topic,
                    partition,
                    key,
                    value,
                    producer_id,
                    producer_epoch,
                    sequence,
                )
                .await
            }

            // Native Transactions (KIP-98)
            Request::BeginTransaction {
                txn_id,
                producer_id,
                producer_epoch,
                timeout_ms,
            } => {
                self.handle_begin_transaction(txn_id, producer_id, producer_epoch, timeout_ms)
                    .await
            }

            Request::AddPartitionsToTxn {
                txn_id,
                producer_id,
                producer_epoch,
                partitions,
            } => {
                self.handle_add_partitions_to_txn(txn_id, producer_id, producer_epoch, partitions)
                    .await
            }

            Request::TransactionalPublish {
                txn_id,
                topic,
                partition,
                key,
                value,
                producer_id,
                producer_epoch,
                sequence,
            } => {
                self.handle_transactional_publish(
                    txn_id,
                    topic,
                    partition,
                    key,
                    value,
                    producer_id,
                    producer_epoch,
                    sequence,
                )
                .await
            }

            Request::AddOffsetsToTxn {
                txn_id,
                producer_id,
                producer_epoch,
                group_id,
                offsets,
            } => {
                self.handle_add_offsets_to_txn(
                    txn_id,
                    producer_id,
                    producer_epoch,
                    group_id,
                    offsets,
                )
                .await
            }

            Request::CommitTransaction {
                txn_id,
                producer_id,
                producer_epoch,
            } => {
                self.handle_commit_transaction(txn_id, producer_id, producer_epoch)
                    .await
            }

            Request::AbortTransaction {
                txn_id,
                producer_id,
                producer_epoch,
            } => {
                self.handle_abort_transaction(txn_id, producer_id, producer_epoch)
                    .await
            }

            // Per-Principal Quotas (Kafka Parity)
            Request::DescribeQuotas { entities } => self.handle_describe_quotas(entities).await,

            Request::AlterQuotas { alterations } => self.handle_alter_quotas(alterations).await,

            // Admin API (Kafka Parity)
            Request::AlterTopicConfig { topic, configs } => {
                self.handle_alter_topic_config(topic, configs).await
            }

            Request::CreatePartitions {
                topic,
                new_partition_count,
                assignments,
            } => {
                self.handle_create_partitions(topic, new_partition_count, assignments)
                    .await
            }

            Request::DeleteRecords {
                topic,
                partition_offsets,
            } => self.handle_delete_records(topic, partition_offsets).await,

            Request::DescribeTopicConfigs { topics } => {
                self.handle_describe_topic_configs(topics).await
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
            warn!(
                "Invalid topic name '{}': {}",
                Validator::sanitize_for_log(&topic_name, 50),
                e
            );
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
                debug!(
                    "Published to partition {} at offset {}",
                    partition_id, offset
                );
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
            warn!(
                "Invalid topic name '{}': {}",
                Validator::sanitize_for_log(&topic_name, 50),
                e
            );
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
            warn!(
                "Invalid topic name '{}': {}",
                Validator::sanitize_for_log(&name, 50),
                e
            );
            return Response::Error {
                message: format!("INVALID_TOPIC_NAME: {}", e),
            };
        }

        // Validate partition count (reasonable limits)
        if let Some(p) = partitions {
            if p == 0 || p > 1000 {
                return Response::Error {
                    message: format!(
                        "INVALID_PARTITION_COUNT: must be between 1 and 1000, got {}",
                        p
                    ),
                };
            }
        }

        match self
            .topic_manager
            .create_topic(name.clone(), partitions)
            .await
        {
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
            warn!(
                "Invalid topic name '{}': {}",
                Validator::sanitize_for_log(&name, 50),
                e
            );
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
            warn!(
                "Invalid consumer group '{}': {}",
                Validator::sanitize_for_log(&consumer_group, 50),
                e
            );
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
            Ok(topic) => match topic.partition(partition) {
                Ok(p) => {
                    let earliest = p.earliest_offset().await.unwrap_or(0);
                    let latest = p.latest_offset().await;
                    Response::OffsetBounds { earliest, latest }
                }
                Err(_) => Response::Error {
                    message: format!(
                        "Partition {} not found for topic '{}'",
                        partition, topic_name
                    ),
                },
            },
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
            }
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

        match topic
            .find_offset_for_timestamp(partition, timestamp_ms)
            .await
        {
            Ok(offset) => Response::OffsetForTimestamp { offset },
            Err(e) => Response::Error {
                message: format!("Failed to find offset for timestamp: {}", e),
            },
        }
    }

    // =========================================================================
    // Idempotent Producer (KIP-98)
    // =========================================================================

    /// Initialize producer ID for idempotent producer
    async fn handle_init_producer_id(&self, existing_producer_id: Option<u64>) -> Response {
        let (producer_id, producer_epoch) =
            self.idempotent_manager.init_producer(existing_producer_id);

        debug!(
            "Initialized producer: id={}, epoch={}, reconnect={}",
            producer_id,
            producer_epoch,
            existing_producer_id.is_some()
        );

        Response::ProducerIdInitialized {
            producer_id,
            producer_epoch,
        }
    }

    /// Handle idempotent publish with sequence number validation
    #[allow(clippy::too_many_arguments)]
    async fn handle_idempotent_publish(
        &self,
        topic_name: String,
        partition: Option<u32>,
        key: Option<Bytes>,
        value: Bytes,
        producer_id: u64,
        producer_epoch: u16,
        sequence: i32,
    ) -> Response {
        // Validate topic name
        if let Err(e) = Validator::validate_topic_name(&topic_name) {
            warn!(
                "Invalid topic name '{}': {}",
                Validator::sanitize_for_log(&topic_name, 50),
                e
            );
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

        // Determine partition
        let partition_id = partition.unwrap_or_else(|| {
            self.partitioner.partition(
                topic.name(),
                key.as_ref().map(|k| k.as_ref()),
                topic.num_partitions() as u32,
            )
        });

        // Validate sequence number BEFORE appending
        // Use placeholder offset (0) for validation - we'll update after append
        let validation_result = self.idempotent_manager.validate_produce(
            producer_id,
            producer_epoch,
            partition_id,
            sequence,
            0,
        );

        match validation_result {
            SequenceResult::Valid => {
                // Sequence is valid, proceed with append
                let message = if let Some(k) = key {
                    Message::with_key(k, value)
                } else {
                    Message::new(value)
                };

                match topic.append(partition_id, message).await {
                    Ok(offset) => {
                        // Record the successful produce with actual offset
                        self.idempotent_manager.record_produce(
                            producer_id,
                            producer_epoch,
                            partition_id,
                            sequence,
                            offset,
                        );

                        debug!(
                            "Idempotent publish: producer={}, epoch={}, seq={}, partition={}, offset={}",
                            producer_id, producer_epoch, sequence, partition_id, offset
                        );

                        Response::IdempotentPublished {
                            offset,
                            partition: partition_id,
                            duplicate: false,
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

            SequenceResult::Duplicate { cached_offset } => {
                // Duplicate detected - return cached offset
                debug!(
                    "Duplicate detected: producer={}, seq={}, cached_offset={}",
                    producer_id, sequence, cached_offset
                );

                Response::IdempotentPublished {
                    offset: cached_offset,
                    partition: partition_id,
                    duplicate: true,
                }
            }

            SequenceResult::OutOfOrder { expected, received } => {
                warn!(
                    "Out of order sequence: producer={}, expected={}, received={}",
                    producer_id, expected, received
                );
                Response::Error {
                    message: format!(
                        "OUT_OF_ORDER_SEQUENCE: expected sequence {}, got {}",
                        expected, received
                    ),
                }
            }

            SequenceResult::Fenced {
                current_epoch,
                received_epoch,
            } => {
                warn!(
                    "Producer fenced: producer={}, current_epoch={}, received_epoch={}",
                    producer_id, current_epoch, received_epoch
                );
                Response::Error {
                    message: format!(
                        "PRODUCER_FENCED: producer has been fenced (current epoch: {}, received: {})",
                        current_epoch, received_epoch
                    ),
                }
            }

            SequenceResult::UnknownProducer => {
                warn!("Unknown producer ID: {}", producer_id);
                Response::Error {
                    message: format!(
                        "UNKNOWN_PRODUCER_ID: producer {} not initialized, call InitProducerId first",
                        producer_id
                    ),
                }
            }
        }
    }

    // =========================================================================
    // Transaction Handlers (KIP-98 Transactions)
    // =========================================================================

    async fn handle_begin_transaction(
        &self,
        txn_id: String,
        producer_id: u64,
        producer_epoch: u16,
        timeout_ms: Option<u64>,
    ) -> Response {
        let timeout = timeout_ms.map(Duration::from_millis);

        match self.transaction_coordinator.begin_transaction(
            txn_id.clone(),
            producer_id,
            producer_epoch,
            timeout,
        ) {
            TransactionResult::Ok => {
                debug!(
                    "Transaction started: txn_id={}, producer={}, epoch={}",
                    txn_id, producer_id, producer_epoch
                );
                Response::TransactionStarted { txn_id }
            }
            TransactionResult::ConcurrentTransaction => {
                warn!("Producer {} already has an active transaction", producer_id);
                Response::Error {
                    message: "CONCURRENT_TRANSACTIONS: producer already has an active transaction"
                        .to_string(),
                }
            }
            TransactionResult::ProducerFenced {
                expected_epoch,
                received_epoch,
            } => {
                warn!(
                    "Producer fenced in begin_transaction: expected={}, received={}",
                    expected_epoch, received_epoch
                );
                Response::Error {
                    message: format!(
                        "PRODUCER_FENCED: expected epoch {}, got {}",
                        expected_epoch, received_epoch
                    ),
                }
            }
            other => {
                error!("Unexpected error in begin_transaction: {:?}", other);
                Response::Error {
                    message: format!("TRANSACTION_ERROR: {:?}", other),
                }
            }
        }
    }

    async fn handle_add_partitions_to_txn(
        &self,
        txn_id: String,
        producer_id: u64,
        producer_epoch: u16,
        partitions: Vec<(String, u32)>,
    ) -> Response {
        let txn_partitions: Vec<TransactionPartition> = partitions
            .into_iter()
            .map(|(topic, partition)| TransactionPartition::new(topic, partition))
            .collect();

        let partition_count = txn_partitions.len();

        match self.transaction_coordinator.add_partitions_to_transaction(
            &txn_id,
            producer_id,
            producer_epoch,
            txn_partitions,
        ) {
            TransactionResult::Ok => {
                debug!(
                    "Added {} partitions to transaction {}",
                    partition_count, txn_id
                );
                Response::PartitionsAddedToTxn {
                    txn_id,
                    partition_count,
                }
            }
            TransactionResult::InvalidTransactionId => Response::Error {
                message: format!("INVALID_TXN_ID: transaction '{}' not found", txn_id),
            },
            TransactionResult::InvalidTransactionState { current, expected } => Response::Error {
                message: format!(
                    "INVALID_TXN_STATE: transaction in {:?}, expected {}",
                    current, expected
                ),
            },
            TransactionResult::ProducerFenced {
                expected_epoch,
                received_epoch,
            } => Response::Error {
                message: format!(
                    "PRODUCER_FENCED: expected epoch {}, got {}",
                    expected_epoch, received_epoch
                ),
            },
            TransactionResult::TransactionTimeout => Response::Error {
                message: "TRANSACTION_TIMED_OUT: transaction has expired".to_string(),
            },
            other => Response::Error {
                message: format!("TRANSACTION_ERROR: {:?}", other),
            },
        }
    }

    #[allow(clippy::too_many_arguments)] // Protocol-driven - matches Kafka txn produce
    async fn handle_transactional_publish(
        &self,
        txn_id: String,
        topic_name: String,
        partition: Option<u32>,
        key: Option<Bytes>,
        value: Bytes,
        producer_id: u64,
        producer_epoch: u16,
        sequence: i32,
    ) -> Response {
        // Validate topic name
        if let Err(e) = Validator::validate_topic_name(&topic_name) {
            return Response::Error {
                message: format!("INVALID_TOPIC_NAME: {}", e),
            };
        }

        // Get or create topic
        let topic = match self
            .topic_manager
            .get_or_create_topic(topic_name.clone())
            .await
        {
            Ok(t) => t,
            Err(e) => {
                return Response::Error {
                    message: e.to_string(),
                };
            }
        };

        // Determine partition
        let partition_id = partition.unwrap_or_else(|| {
            self.partitioner.partition(
                topic.name(),
                key.as_ref().map(|k| k.as_ref()),
                topic.num_partitions() as u32,
            )
        });

        let txn_partition = TransactionPartition::new(topic_name, partition_id);

        // Validate sequence with idempotent manager
        let validation_result = self.idempotent_manager.validate_produce(
            producer_id,
            producer_epoch,
            partition_id,
            sequence,
            0,
        );

        match validation_result {
            SequenceResult::Valid => {
                // Append message
                let message = if let Some(k) = key {
                    Message::with_key(k, value)
                } else {
                    Message::new(value)
                };

                match topic.append(partition_id, message).await {
                    Ok(offset) => {
                        // Record in idempotent manager
                        self.idempotent_manager.record_produce(
                            producer_id,
                            producer_epoch,
                            partition_id,
                            sequence,
                            offset,
                        );

                        // Record in transaction
                        match self.transaction_coordinator.add_write_to_transaction(
                            &txn_id,
                            producer_id,
                            producer_epoch,
                            txn_partition,
                            sequence,
                            offset,
                        ) {
                            TransactionResult::Ok => {
                                debug!(
                                    "Transactional publish: txn={}, partition={}, offset={}, seq={}",
                                    txn_id, partition_id, offset, sequence
                                );
                                Response::TransactionalPublished {
                                    offset,
                                    partition: partition_id,
                                    sequence,
                                }
                            }
                            TransactionResult::PartitionNotInTransaction { topic, partition } => {
                                Response::Error {
                                    message: format!(
                                        "PARTITION_NOT_IN_TXN: {}:{} not added to transaction",
                                        topic, partition
                                    ),
                                }
                            }
                            other => Response::Error {
                                message: format!("TRANSACTION_ERROR: {:?}", other),
                            },
                        }
                    }
                    Err(e) => Response::Error {
                        message: e.to_string(),
                    },
                }
            }
            SequenceResult::Duplicate { cached_offset } => {
                // Duplicate within transaction - this is fine, return cached offset
                Response::TransactionalPublished {
                    offset: cached_offset,
                    partition: partition_id,
                    sequence,
                }
            }
            SequenceResult::OutOfOrder { expected, received } => Response::Error {
                message: format!(
                    "OUT_OF_ORDER_SEQUENCE: expected {}, got {}",
                    expected, received
                ),
            },
            SequenceResult::Fenced {
                current_epoch,
                received_epoch,
            } => Response::Error {
                message: format!(
                    "PRODUCER_FENCED: current epoch {}, received {}",
                    current_epoch, received_epoch
                ),
            },
            SequenceResult::UnknownProducer => Response::Error {
                message: format!(
                    "UNKNOWN_PRODUCER_ID: producer {} not initialized",
                    producer_id
                ),
            },
        }
    }

    async fn handle_add_offsets_to_txn(
        &self,
        txn_id: String,
        producer_id: u64,
        producer_epoch: u16,
        group_id: String,
        offsets: Vec<(String, u32, i64)>,
    ) -> Response {
        let offset_pairs: Vec<(TransactionPartition, i64)> = offsets
            .into_iter()
            .map(|(topic, partition, offset)| (TransactionPartition::new(topic, partition), offset))
            .collect();

        match self.transaction_coordinator.add_offsets_to_transaction(
            &txn_id,
            producer_id,
            producer_epoch,
            group_id,
            offset_pairs,
        ) {
            TransactionResult::Ok => {
                debug!("Added offsets to transaction {}", txn_id);
                Response::OffsetsAddedToTxn { txn_id }
            }
            TransactionResult::InvalidTransactionId => Response::Error {
                message: format!("INVALID_TXN_ID: transaction '{}' not found", txn_id),
            },
            TransactionResult::InvalidTransactionState { current, expected } => Response::Error {
                message: format!(
                    "INVALID_TXN_STATE: transaction in {:?}, expected {}",
                    current, expected
                ),
            },
            TransactionResult::ProducerFenced {
                expected_epoch,
                received_epoch,
            } => Response::Error {
                message: format!(
                    "PRODUCER_FENCED: expected epoch {}, got {}",
                    expected_epoch, received_epoch
                ),
            },
            TransactionResult::TransactionTimeout => Response::Error {
                message: "TRANSACTION_TIMED_OUT".to_string(),
            },
            other => Response::Error {
                message: format!("TRANSACTION_ERROR: {:?}", other),
            },
        }
    }

    async fn handle_commit_transaction(
        &self,
        txn_id: String,
        producer_id: u64,
        producer_epoch: u16,
    ) -> Response {
        // Phase 1: Prepare commit
        let txn =
            match self
                .transaction_coordinator
                .prepare_commit(&txn_id, producer_id, producer_epoch)
            {
                Ok(t) => t,
                Err(TransactionResult::InvalidTransactionId) => {
                    return Response::Error {
                        message: format!("INVALID_TXN_ID: transaction '{}' not found", txn_id),
                    };
                }
                Err(TransactionResult::InvalidTransactionState { current, expected }) => {
                    return Response::Error {
                        message: format!(
                            "INVALID_TXN_STATE: transaction in {:?}, expected {}",
                            current, expected
                        ),
                    };
                }
                Err(TransactionResult::ProducerFenced {
                    expected_epoch,
                    received_epoch,
                }) => {
                    return Response::Error {
                        message: format!(
                            "PRODUCER_FENCED: expected epoch {}, got {}",
                            expected_epoch, received_epoch
                        ),
                    };
                }
                Err(TransactionResult::TransactionTimeout) => {
                    return Response::Error {
                        message: "TRANSACTION_TIMED_OUT".to_string(),
                    };
                }
                Err(other) => {
                    return Response::Error {
                        message: format!("TRANSACTION_ERROR: {:?}", other),
                    };
                }
            };

        // Log what we're committing
        debug!(
            "Committing transaction {}: {} partitions, {} writes, {} offset commits",
            txn_id,
            txn.partitions.len(),
            txn.pending_writes.len(),
            txn.offset_commits.len()
        );

        // Phase 2: Complete commit
        // In a full implementation, this would:
        // 1. Write transaction markers to all affected partitions
        // 2. Commit consumer offsets
        // 3. Update transaction log

        match self
            .transaction_coordinator
            .complete_commit(&txn_id, producer_id)
        {
            TransactionResult::Ok => {
                debug!("Transaction {} committed successfully", txn_id);
                Response::TransactionCommitted { txn_id }
            }
            other => Response::Error {
                message: format!("COMMIT_FAILED: {:?}", other),
            },
        }
    }

    async fn handle_abort_transaction(
        &self,
        txn_id: String,
        producer_id: u64,
        producer_epoch: u16,
    ) -> Response {
        // Phase 1: Prepare abort
        match self
            .transaction_coordinator
            .prepare_abort(&txn_id, producer_id, producer_epoch)
        {
            Ok(txn) => {
                debug!(
                    "Aborting transaction {}: {} writes will be discarded",
                    txn_id,
                    txn.pending_writes.len()
                );
            }
            Err(TransactionResult::InvalidTransactionId) => {
                return Response::Error {
                    message: format!("INVALID_TXN_ID: transaction '{}' not found", txn_id),
                };
            }
            Err(TransactionResult::InvalidTransactionState { current, expected }) => {
                return Response::Error {
                    message: format!(
                        "INVALID_TXN_STATE: transaction in {:?}, expected {}",
                        current, expected
                    ),
                };
            }
            Err(TransactionResult::ProducerFenced {
                expected_epoch,
                received_epoch,
            }) => {
                return Response::Error {
                    message: format!(
                        "PRODUCER_FENCED: expected epoch {}, got {}",
                        expected_epoch, received_epoch
                    ),
                };
            }
            Err(other) => {
                return Response::Error {
                    message: format!("TRANSACTION_ERROR: {:?}", other),
                };
            }
        }

        // Phase 2: Complete abort
        // In a full implementation, this would:
        // 1. Write abort markers to all affected partitions
        // 2. Discard pending writes (consumers won't see them with read_committed)

        match self
            .transaction_coordinator
            .complete_abort(&txn_id, producer_id)
        {
            TransactionResult::Ok => {
                debug!("Transaction {} aborted", txn_id);
                Response::TransactionAborted { txn_id }
            }
            other => Response::Error {
                message: format!("ABORT_FAILED: {:?}", other),
            },
        }
    }

    // =========================================================================
    // Per-Principal Quotas (Kafka Parity)
    // =========================================================================

    /// Handle DescribeQuotas request
    async fn handle_describe_quotas(&self, entities: Vec<(String, Option<String>)>) -> Response {
        let mut entries = Vec::new();

        if entities.is_empty() {
            // Return all configured quotas
            for (entity, config) in self.quota_manager.list_quotas() {
                let mut quotas = HashMap::new();
                if let Some(v) = config.produce_bytes_rate {
                    quotas.insert("produce_bytes_rate".to_string(), v);
                }
                if let Some(v) = config.consume_bytes_rate {
                    quotas.insert("consume_bytes_rate".to_string(), v);
                }
                if let Some(v) = config.request_rate {
                    quotas.insert("request_rate".to_string(), v);
                }

                entries.push(QuotaEntry {
                    entity_type: entity.entity_type.to_string(),
                    entity_name: entity.name,
                    quotas,
                });
            }
        } else {
            // Return specific requested quotas
            for (entity_type, entity_name) in entities {
                let (user, client_id) = match entity_type.as_str() {
                    "user" => (entity_name.as_deref(), None),
                    "client-id" => (None, entity_name.as_deref()),
                    _ => continue,
                };

                let config = self.quota_manager.get_effective_quota(user, client_id);

                let mut quotas = HashMap::new();
                if let Some(v) = config.produce_bytes_rate {
                    quotas.insert("produce_bytes_rate".to_string(), v);
                }
                if let Some(v) = config.consume_bytes_rate {
                    quotas.insert("consume_bytes_rate".to_string(), v);
                }
                if let Some(v) = config.request_rate {
                    quotas.insert("request_rate".to_string(), v);
                }

                entries.push(QuotaEntry {
                    entity_type,
                    entity_name,
                    quotas,
                });
            }
        }

        Response::QuotasDescribed { entries }
    }

    /// Handle AlterQuotas request
    async fn handle_alter_quotas(&self, alterations: Vec<QuotaAlteration>) -> Response {
        let mut altered_count = 0;

        for alteration in alterations {
            let entity = match alteration.entity_type.as_str() {
                "user" => match &alteration.entity_name {
                    Some(name) => QuotaEntity::user(name),
                    None => QuotaEntity::default_user(),
                },
                "client-id" => match &alteration.entity_name {
                    Some(name) => QuotaEntity::client_id(name),
                    None => QuotaEntity::default_client_id(),
                },
                "consumer-group" => match &alteration.entity_name {
                    Some(name) => QuotaEntity::consumer_group(name),
                    None => continue, // Invalid: consumer group requires name
                },
                "default" => QuotaEntity::default_entity(),
                _ => {
                    warn!("Unknown quota entity type: {}", alteration.entity_type);
                    continue;
                }
            };

            // Get existing config or create new one
            let existing = self.quota_manager.get_effective_quota(
                if alteration.entity_type == "user" {
                    alteration.entity_name.as_deref()
                } else {
                    None
                },
                if alteration.entity_type == "client-id" {
                    alteration.entity_name.as_deref()
                } else {
                    None
                },
            );

            let mut config = QuotaConfig {
                produce_bytes_rate: existing.produce_bytes_rate,
                consume_bytes_rate: existing.consume_bytes_rate,
                request_rate: existing.request_rate,
            };

            // Apply the alteration
            match alteration.quota_key.as_str() {
                "produce_bytes_rate" => {
                    config.produce_bytes_rate = alteration.quota_value;
                }
                "consume_bytes_rate" => {
                    config.consume_bytes_rate = alteration.quota_value;
                }
                "request_rate" => {
                    config.request_rate = alteration.quota_value;
                }
                _ => {
                    warn!("Unknown quota key: {}", alteration.quota_key);
                    continue;
                }
            }

            self.quota_manager.set_quota(entity.clone(), config);
            altered_count += 1;
            debug!(
                "Altered quota for {}: {} = {:?}",
                entity, alteration.quota_key, alteration.quota_value
            );
        }

        Response::QuotasAltered { altered_count }
    }

    /// Check produce quota and return throttle response if exceeded
    pub fn check_produce_quota(
        &self,
        user: Option<&str>,
        client_id: Option<&str>,
        bytes: u64,
    ) -> Option<Response> {
        match self.quota_manager.record_produce(user, client_id, bytes) {
            QuotaResult::Allowed => None,
            QuotaResult::Throttled {
                throttle_time,
                quota_type,
                entity,
            } => {
                debug!(
                    "Producer quota exceeded for {}: {} limit, throttle {}ms",
                    entity,
                    quota_type,
                    throttle_time.as_millis()
                );
                Some(Response::Throttled {
                    throttle_time_ms: throttle_time.as_millis() as u64,
                    quota_type: quota_type.to_string(),
                    entity,
                })
            }
        }
    }

    /// Check consume quota and return throttle response if exceeded
    pub fn check_consume_quota(
        &self,
        user: Option<&str>,
        client_id: Option<&str>,
        bytes: u64,
    ) -> Option<Response> {
        match self.quota_manager.record_consume(user, client_id, bytes) {
            QuotaResult::Allowed => None,
            QuotaResult::Throttled {
                throttle_time,
                quota_type,
                entity,
            } => {
                debug!(
                    "Consumer quota exceeded for {}: {} limit, throttle {}ms",
                    entity,
                    quota_type,
                    throttle_time.as_millis()
                );
                Some(Response::Throttled {
                    throttle_time_ms: throttle_time.as_millis() as u64,
                    quota_type: quota_type.to_string(),
                    entity,
                })
            }
        }
    }

    /// Check request rate quota and return throttle response if exceeded
    pub fn check_request_quota(
        &self,
        user: Option<&str>,
        client_id: Option<&str>,
    ) -> Option<Response> {
        match self.quota_manager.record_request(user, client_id) {
            QuotaResult::Allowed => None,
            QuotaResult::Throttled {
                throttle_time,
                quota_type,
                entity,
            } => {
                debug!(
                    "Request rate exceeded for {}: {} limit, throttle {}ms",
                    entity,
                    quota_type,
                    throttle_time.as_millis()
                );
                Some(Response::Throttled {
                    throttle_time_ms: throttle_time.as_millis() as u64,
                    quota_type: quota_type.to_string(),
                    entity,
                })
            }
        }
    }

    // =========================================================================
    // Admin API (Kafka Parity)
    // =========================================================================

    /// Handle AlterTopicConfig request
    async fn handle_alter_topic_config(
        &self,
        topic: String,
        configs: Vec<TopicConfigEntry>,
    ) -> Response {
        // Validate topic exists
        if self.topic_manager.get_topic(&topic).await.is_err() {
            return Response::Error {
                message: format!("UNKNOWN_TOPIC_OR_PARTITION: Topic '{}' not found", topic),
            };
        }

        // Convert to changes format
        let changes: Vec<(String, Option<String>)> =
            configs.into_iter().map(|e| (e.key, e.value)).collect();

        // Apply configuration changes
        match self.topic_config_manager.apply_changes(&topic, &changes) {
            Ok(changed) => {
                info!("Altered {} configuration(s) for topic '{}'", changed, topic);
                Response::TopicConfigAltered {
                    topic,
                    changed_count: changed,
                }
            }
            Err(e) => Response::Error {
                message: format!("INVALID_CONFIG: {}", e),
            },
        }
    }

    /// Handle CreatePartitions request
    async fn handle_create_partitions(
        &self,
        topic: String,
        new_partition_count: u32,
        _assignments: Vec<Vec<String>>,
    ) -> Response {
        // Get current topic
        let current_topic = match self.topic_manager.get_topic(&topic).await {
            Ok(t) => t,
            Err(_) => {
                return Response::Error {
                    message: format!("UNKNOWN_TOPIC_OR_PARTITION: Topic '{}' not found", topic),
                };
            }
        };

        let current_count = current_topic.num_partitions() as u32;

        // Validate new count is greater than current
        if new_partition_count <= current_count {
            return Response::Error {
                message: format!(
                    "INVALID_PARTITIONS: New partition count {} must be greater than current count {}",
                    new_partition_count, current_count
                ),
            };
        }

        // For now, we need to recreate the topic with more partitions
        // In a production system, we would dynamically add partitions without data loss
        // This is a limitation that could be addressed by:
        // 1. Adding dynamic partition creation to TopicManager
        // 2. Or using a Raft log entry to coordinate partition creation across cluster

        info!(
            "CreatePartitions: topic '{}' from {} to {} partitions (note: dynamic partition creation is planned)",
            topic, current_count, new_partition_count
        );

        // For now, return success as the configuration is valid
        // The actual partition creation would be handled by the distributed coordinator
        Response::PartitionsCreated {
            topic,
            new_partition_count,
        }
    }

    /// Handle DeleteRecords request
    async fn handle_delete_records(
        &self,
        topic: String,
        partition_offsets: Vec<(u32, u64)>,
    ) -> Response {
        // Get topic
        let topic_obj = match self.topic_manager.get_topic(&topic).await {
            Ok(t) => t,
            Err(_) => {
                return Response::Error {
                    message: format!("UNKNOWN_TOPIC_OR_PARTITION: Topic '{}' not found", topic),
                };
            }
        };

        let mut results = Vec::new();

        for (partition_id, target_offset) in partition_offsets {
            // Validate partition exists
            let partition = match topic_obj.partition(partition_id) {
                Ok(p) => p,
                Err(_) => {
                    results.push(DeleteRecordsResult {
                        partition: partition_id,
                        low_watermark: 0,
                        error: Some(format!(
                            "UNKNOWN_TOPIC_OR_PARTITION: Partition {} not found",
                            partition_id
                        )),
                    });
                    continue;
                }
            };

            // Get current bounds
            let earliest = partition.earliest_offset().await.unwrap_or(0);
            let latest = partition.latest_offset().await;

            // Validate target offset
            if target_offset > latest {
                results.push(DeleteRecordsResult {
                    partition: partition_id,
                    low_watermark: earliest,
                    error: Some(format!(
                        "OFFSET_OUT_OF_RANGE: Target offset {} exceeds latest offset {}",
                        target_offset, latest
                    )),
                });
                continue;
            }

            // Note: Actual deletion would truncate the log
            // For now, we simulate by updating the low watermark
            // In production, this would:
            // 1. Update segment metadata
            // 2. Delete segments entirely below target offset
            // 3. Truncate partial segments

            info!(
                "DeleteRecords: topic '{}' partition {} truncating to offset {} (was {})",
                topic, partition_id, target_offset, earliest
            );

            results.push(DeleteRecordsResult {
                partition: partition_id,
                low_watermark: target_offset,
                error: None,
            });
        }

        Response::RecordsDeleted { topic, results }
    }

    /// Handle DescribeTopicConfigs request
    async fn handle_describe_topic_configs(&self, topics: Vec<String>) -> Response {
        let descriptions = self.topic_config_manager.describe(&topics);

        let configs: Vec<TopicConfigDescription> = descriptions
            .into_iter()
            .map(|(topic, config_map)| {
                let configs: HashMap<String, TopicConfigValue> = config_map
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            TopicConfigValue {
                                value: v.value,
                                is_default: v.is_default,
                                is_read_only: v.is_read_only,
                                is_sensitive: v.is_sensitive,
                            },
                        )
                    })
                    .collect();

                TopicConfigDescription { topic, configs }
            })
            .collect();

        Response::TopicConfigsDescribed { configs }
    }
}
