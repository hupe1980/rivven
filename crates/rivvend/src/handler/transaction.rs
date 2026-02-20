//! Transaction request handlers (begin, commit, abort, add partitions, offsets)

use super::RequestHandler;
use crate::protocol::Response;
use bytes::Bytes;
use rivven_core::{
    Message, SequenceResult, TransactionMarker, TransactionPartition, TransactionResult, Validator,
};
use std::time::Duration;
use tracing::{debug, error, warn};

impl RequestHandler {
    pub(crate) async fn handle_begin_transaction(
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

    pub(crate) async fn handle_add_partitions_to_txn(
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
    pub(crate) async fn handle_transactional_publish(
        &self,
        txn_id: String,
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
        let current = self
            .pending_bytes
            .fetch_add(msg_size, std::sync::atomic::Ordering::AcqRel);
        if current + msg_size > self.max_pending_bytes {
            self.pending_bytes
                .fetch_sub(msg_size, std::sync::atomic::Ordering::AcqRel);
            warn!(
                "Transactional publish rejected: pending bytes {} + {} exceeds limit {}",
                current, msg_size, self.max_pending_bytes
            );
            return Response::Error {
                message: "BUFFER_FULL: Server publish buffer is full, retry later".to_string(),
            };
        }

        // Validate topic name
        if let Err(e) = Validator::validate_topic_name(&topic_name) {
            self.pending_bytes
                .fetch_sub(msg_size, std::sync::atomic::Ordering::AcqRel);
            return Response::Error {
                message: format!("INVALID_TOPIC_NAME: {}", e),
            };
        }

        // Get or create topic (respects auto_create_topics)
        let topic = match self.resolve_topic(topic_name.clone()).await {
            Ok(t) => t,
            Err(e) => {
                self.pending_bytes
                    .fetch_sub(msg_size, std::sync::atomic::Ordering::AcqRel);
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

        // Epoch fencing for transactional publish
        if let Some(client_epoch) = leader_epoch {
            if let Some(ref checker) = self.leader_epoch_checker {
                if let Some(server_epoch) = checker(topic.name(), partition_id) {
                    if client_epoch < server_epoch {
                        self.pending_bytes
                            .fetch_sub(msg_size, std::sync::atomic::Ordering::AcqRel);
                        warn!(
                            topic = %topic.name(),
                            partition = partition_id,
                            client_epoch,
                            server_epoch,
                            "Transactional publish rejected: stale leader epoch"
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

        // Validate sequence with idempotent manager
        let validation_result = self.idempotent_manager.validate_produce(
            producer_id,
            producer_epoch,
            partition_id,
            sequence,
            0,
        );

        let result = match validation_result {
            SequenceResult::Valid => {
                // Validate partition membership BEFORE appending data.
                match self.transaction_coordinator.validate_transaction_write(
                    &txn_id,
                    producer_id,
                    producer_epoch,
                    &txn_partition,
                ) {
                    TransactionResult::Ok => {} // Validated, proceed to append
                    TransactionResult::PartitionNotInTransaction { topic, partition } => {
                        self.pending_bytes
                            .fetch_sub(msg_size, std::sync::atomic::Ordering::AcqRel);
                        return Response::Error {
                            message: format!(
                                "PARTITION_NOT_IN_TXN: {}:{} not added to transaction",
                                topic, partition
                            ),
                        };
                    }
                    other => {
                        self.pending_bytes
                            .fetch_sub(msg_size, std::sync::atomic::Ordering::AcqRel);
                        return Response::Error {
                            message: format!("TRANSACTION_ERROR: {:?}", other),
                        };
                    }
                }

                // Append transactional message with producer metadata
                let message = if let Some(k) = key {
                    Message::transactional_with_key(k, value, producer_id, producer_epoch)
                } else {
                    Message::transactional(value, producer_id, producer_epoch)
                };

                // §3.3: WAL-first write for transactional publish
                if let Err(e) = self.wal_write(topic.name(), partition_id, &message).await {
                    self.pending_bytes
                        .fetch_sub(msg_size, std::sync::atomic::Ordering::AcqRel);
                    return Response::Error {
                        message: format!("WAL_ERROR: {}", e),
                    };
                }

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

                        // Record in transaction (partition already validated above)
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
        };

        // Release backpressure count after processing completes
        self.pending_bytes
            .fetch_sub(msg_size, std::sync::atomic::Ordering::AcqRel);
        result
    }

    pub(crate) async fn handle_add_offsets_to_txn(
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

    pub(crate) async fn handle_commit_transaction(
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

        // Write COMMIT markers to all affected partitions
        // If any marker write fails, abort the transaction
        // Track which partitions got COMMIT markers for rollback
        // WAL-write the commit decision BEFORE writing partition markers.
        // On crash-recovery, this TxnCommit record tells the recovery logic
        // that the commit decision was durable even if individual partition
        // markers were only partially written.
        if let Some(ref wal) = self.wal {
            let commit_record = format!("txn_commit:{}:{}:{}", txn_id, producer_id, producer_epoch);
            if let Err(e) = wal
                .write_with_type(
                    bytes::Bytes::from(commit_record),
                    rivven_core::RecordType::TxnCommit,
                )
                .await
            {
                error!(
                    "WAL write failed for TxnCommit record of txn {}: {}",
                    txn_id, e
                );
                // Cannot commit if the decision isn't durable
                match self
                    .transaction_coordinator
                    .complete_abort(&txn_id, producer_id)
                {
                    TransactionResult::Ok => {}
                    other => {
                        error!(
                            "Failed to abort txn '{}' after WAL failure: {:?}",
                            txn_id, other
                        );
                    }
                }
                return Response::Error {
                    message: format!(
                        "WAL_ERROR: failed to persist commit decision for txn '{}': {}",
                        txn_id, e
                    ),
                };
            }
        }

        let mut marker_failed = false;
        let mut committed_partitions: Vec<rivven_core::TransactionPartition> = Vec::new();
        for tp in &txn.partitions {
            if let Ok(topic_obj) = self.topic_manager.get_topic(&tp.topic).await {
                if let Ok(partition) = topic_obj.partition(tp.partition) {
                    let marker = Message {
                        producer_id: Some(producer_id),
                        transaction_marker: Some(TransactionMarker::Commit),
                        is_transactional: true,
                        ..Message::new(Bytes::new())
                    };
                    if let Err(e) = partition.append(marker).await {
                        error!(
                            "Failed to write COMMIT marker for txn {} to {}/{}: {} — aborting transaction",
                            txn_id, tp.topic, tp.partition, e
                        );
                        marker_failed = true;
                        break;
                    }
                    committed_partitions.push(tp.clone());
                } else {
                    error!(
                        "Partition {}/{} not found during commit of txn {} — aborting transaction",
                        tp.topic, tp.partition, txn_id
                    );
                    marker_failed = true;
                    break;
                }
            } else {
                error!(
                    "Topic {} not found during commit of txn {} — aborting transaction",
                    tp.topic, txn_id
                );
                marker_failed = true;
                break;
            }
        }

        if marker_failed {
            // Write ABORT markers to partitions that already received
            // COMMIT markers, so consumers with read_committed isolation don't
            // see partial commits.
            for tp in &committed_partitions {
                if let Ok(topic_obj) = self.topic_manager.get_topic(&tp.topic).await {
                    if let Ok(partition) = topic_obj.partition(tp.partition) {
                        let abort_marker = Message {
                            producer_id: Some(producer_id),
                            transaction_marker: Some(TransactionMarker::Abort),
                            is_transactional: true,
                            ..Message::new(Bytes::new())
                        };
                        if let Err(e) = partition.append(abort_marker).await {
                            error!(
                                "Failed to write compensating ABORT marker for txn {} to {}/{}: {}",
                                txn_id, tp.topic, tp.partition, e
                            );
                        }
                    }
                }
            }

            // Abort instead of proceeding with partial commit
            match self
                .transaction_coordinator
                .complete_abort(&txn_id, producer_id)
            {
                TransactionResult::Ok => {}
                other => {
                    error!(
                        "Failed to abort txn '{}' after commit marker failure: {:?}",
                        txn_id, other
                    );
                }
            }
            return Response::Error {
                message: format!(
                    "COMMIT_FAILED: marker write failed for txn '{}', transaction aborted",
                    txn_id
                ),
            };
        }

        // Phase 2: Complete commit
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

    pub(crate) async fn handle_abort_transaction(
        &self,
        txn_id: String,
        producer_id: u64,
        producer_epoch: u16,
    ) -> Response {
        // Phase 1: Prepare abort
        let txn =
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
                    txn
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
            };

        // Write ABORT markers to all affected partitions
        // Track failures and surface them
        // WAL-write the abort decision BEFORE writing partition markers.
        if let Some(ref wal) = self.wal {
            let abort_record = format!("txn_abort:{}:{}:{}", txn_id, producer_id, producer_epoch);
            if let Err(e) = wal
                .write_with_type(
                    bytes::Bytes::from(abort_record),
                    rivven_core::RecordType::TxnAbort,
                )
                .await
            {
                warn!(
                    "WAL write failed for TxnAbort record of txn {}: {} — proceeding with abort anyway",
                    txn_id, e
                );
                // Abort must proceed even if WAL fails: not persisting the abort
                // decision is acceptable because the data writes are uncommitted
                // and will be filtered by read_committed consumers.
            }
        }

        let mut abort_marker_failures: Vec<String> = Vec::new();
        for tp in &txn.partitions {
            if let Ok(topic_obj) = self.topic_manager.get_topic(&tp.topic).await {
                if let Ok(partition) = topic_obj.partition(tp.partition) {
                    let marker = Message {
                        producer_id: Some(producer_id),
                        transaction_marker: Some(TransactionMarker::Abort),
                        is_transactional: true,
                        ..Message::new(Bytes::new())
                    };
                    if let Err(e) = partition.append(marker).await {
                        error!(
                            "Failed to write ABORT marker for txn {} to {}/{}: {}",
                            txn_id, tp.topic, tp.partition, e
                        );
                        abort_marker_failures.push(format!("{}/{}", tp.topic, tp.partition));
                    }
                }
            }
        }

        // Phase 2: Complete abort
        match self
            .transaction_coordinator
            .complete_abort(&txn_id, producer_id)
        {
            TransactionResult::Ok => {
                if abort_marker_failures.is_empty() {
                    debug!("Transaction {} aborted", txn_id);
                    Response::TransactionAborted { txn_id }
                } else {
                    // Surface abort marker failures as an error so
                    // clients know some partitions may have uncommitted
                    // transactional data visible under read_committed.
                    error!(
                        "Transaction {} aborted but ABORT markers failed for: {:?}",
                        txn_id, abort_marker_failures
                    );
                    Response::Error {
                        message: format!(
                            "ABORT_PARTIAL_FAILURE: transaction '{}' aborted but ABORT markers \
                             failed for partitions: {}. Affected partitions may expose \
                             uncommitted data under read_committed isolation.",
                            txn_id,
                            abort_marker_failures.join(", ")
                        ),
                    }
                }
            }
            other => Response::Error {
                message: format!("ABORT_FAILED: {:?}", other),
            },
        }
    }
}
