//! Request handling — split into submodules by concern
//!
//! | Module | Responsibility |
//! |--------|---------------|
//! | `produce` | Publish, idempotent publish, producer init |
//! | `consume` | Fetch, offset management, consumer groups, topic CRUD |
//! | `transaction` | Begin/commit/abort transactions, partition/offset txn ops |
//! | `admin` | Quotas, topic config, create partitions, delete records |

mod admin;
mod consume;
mod produce;
mod transaction;

use crate::partitioner::{StickyPartitioner, StickyPartitionerConfig};
use crate::protocol::{Request, Response};
use rivven_core::{
    GroupCommitWal, IdempotentProducerManager, Message, OffsetManager, QuotaManager,
    TopicConfigManager, TopicManager, TransactionCoordinator,
};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tracing::{debug, warn};

/// Callback to resolve the current leader epoch for a (topic, partition) pair.
/// Returns `None` if epoch fencing is not applicable (standalone mode) or the
/// partition is unknown. Returns `Some(epoch)` with the server's current epoch.
pub type LeaderEpochChecker = Arc<dyn Fn(&str, u32) -> Option<u64> + Send + Sync>;

/// Handles incoming requests
pub struct RequestHandler {
    pub(crate) topic_manager: TopicManager,
    pub(crate) offset_manager: OffsetManager,
    pub(crate) partitioner: StickyPartitioner,
    /// Idempotent producer state manager
    pub(crate) idempotent_manager: Arc<IdempotentProducerManager>,
    /// Transaction coordinator
    pub(crate) transaction_coordinator: Arc<TransactionCoordinator>,
    /// Per-principal quota manager
    pub(crate) quota_manager: Arc<QuotaManager>,
    /// Topic configuration manager (Admin API)
    pub(crate) topic_config_manager: Arc<TopicConfigManager>,
    /// Whether topics are auto-created on first publish (default: true)
    pub(crate) auto_create_topics: bool,
    /// Backpressure: maximum pending publish bytes before rejecting (default: 256 MB)
    pub(crate) max_pending_bytes: usize,
    /// Backpressure: current pending publish bytes counter
    pub(crate) pending_bytes: Arc<AtomicUsize>,
    /// §2.4: Optional epoch checker for data-path leader fencing.
    /// When set, publish requests carrying a `leader_epoch` are validated
    /// against the server's current epoch. Stale-epoch writes are rejected
    /// before appending, preventing data loss from a stale leader.
    pub(crate) leader_epoch_checker: Option<LeaderEpochChecker>,
    /// §3.3: Write-ahead log for crash-safe durability.
    /// When set, every produce is WAL-written before being appended to the
    /// topic partition. Group-commit batching keeps the overhead minimal.
    pub(crate) wal: Option<Arc<GroupCommitWal>>,
}

impl RequestHandler {
    /// Create a new request handler with default partitioner settings
    /// Default publish backpressure limit: 256 MB
    const DEFAULT_MAX_PENDING_BYTES: usize = 256 * 1024 * 1024;

    pub fn new(topic_manager: TopicManager, offset_manager: OffsetManager) -> Self {
        Self {
            topic_manager,
            offset_manager,
            partitioner: StickyPartitioner::new(),
            idempotent_manager: Arc::new(IdempotentProducerManager::new()),
            transaction_coordinator: Arc::new(TransactionCoordinator::new()),
            quota_manager: Arc::new(QuotaManager::new()),
            topic_config_manager: Arc::new(TopicConfigManager::new()),
            auto_create_topics: true,
            max_pending_bytes: Self::DEFAULT_MAX_PENDING_BYTES,
            pending_bytes: Arc::new(AtomicUsize::new(0)),
            leader_epoch_checker: None,
            wal: None,
        }
    }

    /// Create a new request handler with custom partitioner config
    pub fn with_partitioner_config(
        topic_manager: TopicManager,
        offset_manager: OffsetManager,
        partitioner_config: StickyPartitionerConfig,
    ) -> Self {
        Self {
            topic_manager,
            offset_manager,
            partitioner: StickyPartitioner::with_config(partitioner_config),
            idempotent_manager: Arc::new(IdempotentProducerManager::new()),
            transaction_coordinator: Arc::new(TransactionCoordinator::new()),
            quota_manager: Arc::new(QuotaManager::new()),
            topic_config_manager: Arc::new(TopicConfigManager::new()),
            auto_create_topics: true,
            max_pending_bytes: Self::DEFAULT_MAX_PENDING_BYTES,
            pending_bytes: Arc::new(AtomicUsize::new(0)),
            leader_epoch_checker: None,
            wal: None,
        }
    }

    /// Create a new request handler with custom quota configuration
    pub fn with_quota_manager(
        topic_manager: TopicManager,
        offset_manager: OffsetManager,
        quota_manager: Arc<QuotaManager>,
    ) -> Self {
        Self {
            topic_manager,
            offset_manager,
            partitioner: StickyPartitioner::new(),
            idempotent_manager: Arc::new(IdempotentProducerManager::new()),
            transaction_coordinator: Arc::new(TransactionCoordinator::new()),
            quota_manager,
            topic_config_manager: Arc::new(TopicConfigManager::new()),
            auto_create_topics: true,
            max_pending_bytes: Self::DEFAULT_MAX_PENDING_BYTES,
            pending_bytes: Arc::new(AtomicUsize::new(0)),
            leader_epoch_checker: None,
            wal: None,
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

    /// Get the transaction coordinator (for background reaper task)
    pub fn transaction_coordinator(&self) -> &Arc<TransactionCoordinator> {
        &self.transaction_coordinator
    }

    /// Set whether topics are auto-created on first publish
    pub fn set_auto_create_topics(&mut self, enabled: bool) {
        self.auto_create_topics = enabled;
    }

    /// Set the WAL for crash-safe durability on the produce path (§3.3).
    pub fn set_wal(&mut self, wal: Arc<GroupCommitWal>) {
        self.wal = Some(wal);
    }

    /// §3.3: Write a message to the WAL before appending to the topic.
    ///
    /// Encodes the full `Message` (including key, headers, producer metadata)
    /// into a WAL record via `TopicManager::build_wal_record()`, then performs
    /// a group-commit write.
    ///
    /// The call is a no-op when no WAL is configured.
    pub(crate) async fn wal_write(
        &self,
        topic_name: &str,
        partition_id: u32,
        message: &Message,
    ) -> Result<(), String> {
        let wal = match &self.wal {
            Some(w) => w,
            None => return Ok(()),
        };
        let record = TopicManager::build_wal_record(topic_name, partition_id, message)
            .map_err(|e| format!("WAL record encode error: {}", e))?;
        wal.write(record)
            .await
            .map_err(|e| format!("WAL write error: {}", e))?;
        Ok(())
    }

    /// Resolve a topic by name: get or create based on auto_create_topics config
    pub(crate) async fn resolve_topic(
        &self,
        name: String,
    ) -> Result<Arc<rivven_core::Topic>, String> {
        if self.auto_create_topics {
            self.topic_manager
                .get_or_create_topic(name)
                .await
                .map_err(|e| e.to_string())
        } else {
            self.topic_manager.get_topic(&name).await.map_err(|_| {
                format!(
                    "UNKNOWN_TOPIC_OR_PARTITION: topic '{}' does not exist",
                    name
                )
            })
        }
    }

    /// Handle a request and return a response (anonymous/no-auth path)
    pub async fn handle(&self, request: Request) -> Response {
        self.handle_with_principal(request, None, None).await
    }

    /// Handle a request with principal context for per-user quota enforcement
    ///
    /// When called from the authenticated path, `user` is the principal name.
    /// This avoids double-counting quotas when AuthenticatedHandler delegates here.
    pub async fn handle_with_principal(
        &self,
        request: Request,
        user: Option<&str>,
        client_id: Option<&str>,
    ) -> Response {
        // Per-principal quota enforcement
        if let Some(throttle) = self.check_request_quota(user, client_id) {
            return throttle;
        }

        // Check produce quota for write requests
        match &request {
            Request::Publish { value, .. }
            | Request::IdempotentPublish { value, .. }
            | Request::TransactionalPublish { value, .. } => {
                let bytes = value.len() as u64;
                if let Some(throttle) = self.check_produce_quota(user, client_id, bytes) {
                    return throttle;
                }
            }
            _ => {}
        }

        match request {
            // Authentication is handled by AuthenticatedHandler, not here
            #[allow(deprecated)]
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
                leader_epoch,
            } => {
                self.handle_publish(topic, partition, key, value, leader_epoch)
                    .await
            }

            Request::Consume {
                topic,
                partition,
                offset,
                max_messages,
                isolation_level,
                max_wait_ms,
            } => {
                let response = self
                    .handle_consume(
                        topic,
                        partition,
                        offset,
                        max_messages,
                        isolation_level,
                        max_wait_ms,
                    )
                    .await;

                // Post-fetch consume quota: account for bytes returned
                if let Response::Messages { ref messages, .. } = response {
                    let bytes: u64 = messages.iter().map(|m| m.value.len() as u64).sum();
                    if let Some(throttle) = self.check_consume_quota(user, client_id, bytes) {
                        return throttle;
                    }
                }

                response
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
                Response::Error {
                    message: "Cluster metadata not available in basic handler".to_string(),
                }
            }

            Request::Ping => Response::Pong,

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

            // Idempotent Producer
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
                leader_epoch,
            } => {
                self.handle_idempotent_publish(
                    topic,
                    partition,
                    key,
                    value,
                    producer_id,
                    producer_epoch,
                    sequence,
                    leader_epoch,
                )
                .await
            }

            // Native Transactions
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
                leader_epoch,
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
                    leader_epoch,
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

            // Protocol version handshake
            Request::Handshake {
                protocol_version,
                client_id,
            } => {
                let server_version = rivven_protocol::PROTOCOL_VERSION;
                let compatible = protocol_version == server_version;
                if compatible {
                    debug!(
                        client_id = %client_id,
                        version = protocol_version,
                        "Protocol handshake succeeded"
                    );
                } else {
                    warn!(
                        client_id = %client_id,
                        client_version = protocol_version,
                        server_version,
                        "Protocol version mismatch"
                    );
                }
                Response::HandshakeResult {
                    server_version,
                    compatible,
                    message: if compatible {
                        "OK".to_string()
                    } else {
                        format!(
                            "Version mismatch: client={}, server={}",
                            protocol_version, server_version
                        )
                    },
                }
            }
        }
    }
}
