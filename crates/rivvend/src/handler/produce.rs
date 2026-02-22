//! Produce request handlers (publish, idempotent publish)

use super::RequestHandler;
use crate::protocol::Response;
use bytes::Bytes;
use rivven_core::{Message, SequenceResult, Validator};
use std::sync::atomic::Ordering;
use tracing::{debug, error, warn};

impl RequestHandler {
    pub(crate) async fn handle_publish(
        &self,
        topic_name: String,
        partition: Option<u32>,
        key: Option<Bytes>,
        value: Bytes,
        leader_epoch: Option<u64>,
    ) -> Response {
        // Backpressure — reject if pending bytes exceed limit.
        // This prevents OOM when Raft or followers are slow.
        let msg_size = value.len() + key.as_ref().map_or(0, |k| k.len());
        // Use AcqRel ordering so the counter is visible across threads
        // in a timely manner, preventing stale reads that bypass backpressure.
        let current = self.pending_bytes.fetch_add(msg_size, Ordering::AcqRel);
        if current + msg_size > self.max_pending_bytes {
            self.pending_bytes.fetch_sub(msg_size, Ordering::AcqRel);
            warn!(
                "Publish rejected: pending bytes {} + {} exceeds limit {}",
                current, msg_size, self.max_pending_bytes
            );
            return Response::Error {
                message: "BUFFER_FULL: Server publish buffer is full, retry later".to_string(),
            };
        }

        // Validate topic name
        if let Err(e) = Validator::validate_topic_name(&topic_name) {
            // Release backpressure bytes on early return
            self.pending_bytes.fetch_sub(msg_size, Ordering::AcqRel);
            warn!(
                "Invalid topic name '{}': {}",
                Validator::sanitize_for_log(&topic_name, 50),
                e
            );
            return Response::Error {
                message: format!("INVALID_TOPIC_NAME: {}", e),
            };
        }

        // Get or create topic (respects auto_create_topics setting)
        let topic = match self.resolve_topic(topic_name).await {
            Ok(t) => t,
            Err(e) => {
                // Release backpressure bytes on early return
                self.pending_bytes.fetch_sub(msg_size, Ordering::AcqRel);
                error!("Failed to resolve topic: {}", e);
                return Response::Error { message: e };
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

        // Validate partition ID against topic partition count
        if let Err(e) = Validator::validate_partition(partition_id, topic.num_partitions() as u32) {
            self.pending_bytes.fetch_sub(msg_size, Ordering::AcqRel);
            warn!(
                "Invalid partition {} for topic '{}': {}",
                partition_id,
                topic.name(),
                e
            );
            return Response::Error {
                message: format!("INVALID_PARTITION: {}", e),
            };
        }

        // §2.4: Data-path epoch fencing.
        // If the client sends a leader_epoch, validate it against the server's
        // current epoch for this partition. A stale epoch means this broker is
        // no longer the leader — reject the write before appending to prevent
        // data loss from split-brain scenarios.
        if let Some(client_epoch) = leader_epoch {
            if let Some(ref checker) = self.leader_epoch_checker {
                if let Some(server_epoch) = checker(topic.name(), partition_id) {
                    if client_epoch < server_epoch {
                        self.pending_bytes.fetch_sub(msg_size, Ordering::AcqRel);
                        warn!(
                            topic = %topic.name(),
                            partition = partition_id,
                            client_epoch,
                            server_epoch,
                            "Publish rejected: stale leader epoch"
                        );
                        return Response::Error {
                            message: format!(
                                "FENCED_LEADER_EPOCH: client epoch {} < server epoch {}",
                                client_epoch, server_epoch
                            ),
                        };
                    }
                }
            }
        }

        // Create message
        let message = if let Some(k) = key {
            Message::with_key(k, value)
        } else {
            Message::new(value)
        };

        // §3.3: WAL-first write — persist to WAL before appending to partition.
        // If the process crashes after WAL write but before segment flush,
        // recovery replays the record. Group-commit batching keeps overhead low.
        if let Err(e) = self.wal_write(topic.name(), partition_id, &message).await {
            self.pending_bytes.fetch_sub(msg_size, Ordering::AcqRel);
            error!("WAL write failed: {}", e);
            return Response::Error {
                message: format!("WAL_ERROR: {}", e),
            };
        }

        // Append to partition
        let result = match topic.append(partition_id, message).await {
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
        };

        // Release backpressure count after write completes
        self.pending_bytes.fetch_sub(msg_size, Ordering::AcqRel);

        result
    }

    /// Initialize producer ID for idempotent producer
    pub(crate) async fn handle_init_producer_id(
        &self,
        existing_producer_id: Option<u64>,
    ) -> Response {
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
    pub(crate) async fn handle_idempotent_publish(
        &self,
        topic_name: String,
        partition: Option<u32>,
        key: Option<Bytes>,
        value: Bytes,
        producer_id: u64,
        producer_epoch: u16,
        sequence: i32,
        leader_epoch: Option<u64>,
    ) -> Response {
        // Backpressure — reject if pending bytes exceed limit
        let msg_size = value.len() + key.as_ref().map_or(0, |k| k.len());
        let current = self.pending_bytes.fetch_add(msg_size, Ordering::AcqRel);
        if current + msg_size > self.max_pending_bytes {
            self.pending_bytes.fetch_sub(msg_size, Ordering::AcqRel);
            warn!(
                "Idempotent publish rejected: pending bytes {} + {} exceeds limit {}",
                current, msg_size, self.max_pending_bytes
            );
            return Response::Error {
                message: "BUFFER_FULL: Server publish buffer is full, retry later".to_string(),
            };
        }

        // Validate topic name
        if let Err(e) = Validator::validate_topic_name(&topic_name) {
            self.pending_bytes.fetch_sub(msg_size, Ordering::AcqRel);
            warn!(
                "Invalid topic name '{}': {}",
                Validator::sanitize_for_log(&topic_name, 50),
                e
            );
            return Response::Error {
                message: format!("INVALID_TOPIC_NAME: {}", e),
            };
        }

        // Get or create topic (respects auto_create_topics setting)
        let topic = match self.resolve_topic(topic_name).await {
            Ok(t) => t,
            Err(e) => {
                self.pending_bytes.fetch_sub(msg_size, Ordering::AcqRel);
                error!("Failed to resolve topic: {}", e);
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

        // Validate partition ID against topic partition count
        if let Err(e) = Validator::validate_partition(partition_id, topic.num_partitions() as u32) {
            self.pending_bytes.fetch_sub(msg_size, Ordering::AcqRel);
            warn!(
                "Invalid partition {} for topic '{}': {}",
                partition_id,
                topic.name(),
                e
            );
            return Response::Error {
                message: format!("INVALID_PARTITION: {}", e),
            };
        }

        // Epoch fencing for idempotent publish
        if let Some(client_epoch) = leader_epoch {
            if let Some(ref checker) = self.leader_epoch_checker {
                if let Some(server_epoch) = checker(topic.name(), partition_id) {
                    if client_epoch < server_epoch {
                        self.pending_bytes.fetch_sub(msg_size, Ordering::AcqRel);
                        warn!(
                            topic = %topic.name(),
                            partition = partition_id,
                            client_epoch,
                            server_epoch,
                            "Idempotent publish rejected: stale leader epoch"
                        );
                        return Response::Error {
                            message: format!(
                                "FENCED_LEADER_EPOCH: client epoch {} < server epoch {}",
                                client_epoch, server_epoch
                            ),
                        };
                    }
                }
            }
        }

        // Validate sequence number BEFORE appending
        // Use placeholder offset (0) for validation - we'll update after append
        let validation_result = self.idempotent_manager.validate_produce(
            producer_id,
            producer_epoch,
            partition_id,
            sequence,
            0,
        );

        let result = match validation_result {
            SequenceResult::Valid => {
                // Sequence is valid, proceed with append
                let message = if let Some(k) = key {
                    Message::with_key(k, value)
                } else {
                    Message::new(value)
                };

                // §3.3: WAL-first write for idempotent publish
                if let Err(e) = self.wal_write(topic.name(), partition_id, &message).await {
                    self.pending_bytes.fetch_sub(msg_size, Ordering::AcqRel);
                    error!("WAL write failed (idempotent): {}", e);
                    return Response::Error {
                        message: format!("WAL_ERROR: {}", e),
                    };
                }

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
        };

        // Release backpressure count after processing completes
        self.pending_bytes.fetch_sub(msg_size, Ordering::AcqRel);
        result
    }
}
