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

        // Validate partition ID against topic partition count
        if let Err(e) = Validator::validate_partition(partition_id, topic.num_partitions() as u32) {
            self.pending_bytes
                .fetch_sub(msg_size, std::sync::atomic::Ordering::AcqRel);
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
        // markers were only partially written.  The payload includes the
        // list of affected partitions so WAL replay can re-write COMMIT
        // markers without consulting the TransactionLog.
        if let Some(ref wal) = self.wal {
            let payload = rivven_core::TxnWalPayload {
                txn_id: txn_id.clone(),
                producer_id,
                producer_epoch,
                partitions: txn
                    .partitions
                    .iter()
                    .map(|tp| (tp.topic.clone(), tp.partition))
                    .collect(),
            };
            let commit_bytes = match postcard::to_allocvec(&payload) {
                Ok(bytes) => bytes,
                Err(e) => {
                    error!(
                        "Failed to serialise TxnCommit payload for txn {}: {}",
                        txn_id, e
                    );
                    // State is PrepareCommit — must transition through PrepareAbort
                    // before completing the abort (complete_abort requires PrepareAbort).
                    match self.transaction_coordinator.prepare_abort(
                        &txn_id,
                        producer_id,
                        producer_epoch,
                    ) {
                        Ok(_) => match self
                            .transaction_coordinator
                            .complete_abort(&txn_id, producer_id)
                        {
                            TransactionResult::Ok => {}
                            other => {
                                error!(
                                    "Failed to complete abort for txn '{}' after serialisation failure: {:?}",
                                    txn_id, other
                                );
                            }
                        },
                        Err(other) => {
                            error!(
                                "Failed to prepare abort for txn '{}' after serialisation failure: {:?}",
                                txn_id, other
                            );
                        }
                    }
                    return Response::Error {
                        message: format!(
                            "INTERNAL_ERROR: failed to serialise commit payload for txn '{}': {}",
                            txn_id, e
                        ),
                    };
                }
            };
            if let Err(e) = wal
                .write_with_type(
                    bytes::Bytes::from(commit_bytes),
                    rivven_core::RecordType::TxnCommit,
                )
                .await
            {
                error!(
                    "WAL write failed for TxnCommit record of txn {}: {}",
                    txn_id, e
                );
                // Cannot commit if the decision isn't durable.
                // State is PrepareCommit — must transition through PrepareAbort
                // before completing the abort (complete_abort requires PrepareAbort).
                match self.transaction_coordinator.prepare_abort(
                    &txn_id,
                    producer_id,
                    producer_epoch,
                ) {
                    Ok(_) => match self
                        .transaction_coordinator
                        .complete_abort(&txn_id, producer_id)
                    {
                        TransactionResult::Ok => {}
                        other => {
                            error!(
                                "Failed to complete abort for txn '{}' after WAL failure: {:?}",
                                txn_id, other
                            );
                        }
                    },
                    Err(other) => {
                        error!(
                            "Failed to prepare abort for txn '{}' after WAL failure: {:?}",
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

        // Phase 1: Write COMMIT markers to all partitions.
        //
        // INVARIANT (BROKER-C2 fix): Once TxnCommit is durable in the WAL,
        // the commit decision is FINAL. We must NEVER write a compensating
        // TxnAbort on top of a durable TxnCommit — that creates ambiguous
        // recovery where the abort WAL write itself can fail, leaving only
        // the TxnCommit and causing crash recovery to re-commit a supposedly
        // aborted transaction.
        //
        // Instead, like Kafka's transaction coordinator, we retry COMMIT
        // marker writes with exponential backoff. If a partition/topic is
        // genuinely gone (permanent error), crash recovery will re-apply
        // the COMMIT markers from the WAL record anyway.
        let max_marker_retries: u32 = 5;
        let mut all_markers_written = true;
        for tp in &txn.partitions {
            let mut marker_written = false;
            // Construct the COMMIT marker once outside the retry loop so that
            // all attempts carry identical content and timestamps.
            let marker = Message {
                producer_id: Some(producer_id),
                transaction_marker: Some(TransactionMarker::Commit),
                is_transactional: true,
                ..Message::new(Bytes::new())
            };
            for attempt in 0..max_marker_retries {
                match self.topic_manager.get_topic(&tp.topic).await {
                    Ok(topic_obj) => match topic_obj.partition(tp.partition) {
                        Ok(partition) => match partition.append(marker.clone()).await {
                            Ok(_) => {
                                marker_written = true;
                                break;
                            }
                            Err(e) => {
                                let backoff_ms = 50 * 2u64.pow(attempt);
                                warn!(
                                        "COMMIT marker write attempt {}/{} for txn {} to {}/{} failed: {} (retrying in {}ms)",
                                        attempt + 1, max_marker_retries, txn_id, tp.topic, tp.partition, e, backoff_ms
                                    );
                                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms))
                                    .await;
                            }
                        },
                        Err(e) => {
                            warn!(
                                "Partition {}/{} not found during commit of txn {} (attempt {}/{}): {}",
                                tp.topic, tp.partition, txn_id, attempt + 1, max_marker_retries, e
                            );
                            let backoff_ms = 50 * 2u64.pow(attempt);
                            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                        }
                    },
                    Err(e) => {
                        warn!(
                            "Topic {} not found during commit of txn {} (attempt {}/{}): {}",
                            tp.topic,
                            txn_id,
                            attempt + 1,
                            max_marker_retries,
                            e
                        );
                        let backoff_ms = 50 * 2u64.pow(attempt);
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    }
                }
            }
            if !marker_written {
                // Exhausted retries for this partition. The TxnCommit WAL record
                // is durable, so crash recovery will re-apply this marker.
                // Log at error level but do NOT abort — the commit decision is final.
                error!(
                    "COMMIT marker for txn {} to {}/{} failed after {} attempts — \
                     WAL recovery will re-apply on restart",
                    txn_id, tp.topic, tp.partition, max_marker_retries
                );
                all_markers_written = false;
            }
        }

        if !all_markers_written {
            warn!(
                "Transaction {} committed (WAL-durable) but some COMMIT markers \
                 could not be written — they will be re-applied on WAL recovery",
                txn_id
            );
        }

        // Phase 1.5: Apply transactional offset commits (BROKER-C1 fix).
        // These offsets were registered via SendOffsetsToTransaction and must
        // be committed atomically with the transaction. Without this step,
        // exactly-once consumer semantics (read-process-write) are broken
        // because the consumer offsets would be silently discarded.
        for oc in &txn.offset_commits {
            for (tp, offset) in &oc.offsets {
                self.offset_manager
                    .commit_offset(&oc.group_id, &tp.topic, tp.partition, *offset as u64)
                    .await;
                debug!(
                    "Applied transactional offset commit: group={}, topic={}, partition={}, offset={} (txn={})",
                    oc.group_id, tp.topic, tp.partition, offset, txn_id
                );
            }
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

        // Write ABORT markers only to partitions that had writes.
        //
        // Derive the set of written partitions from `pending_writes`
        // and only write ABORT markers there. This avoids unnecessary I/O
        // and ensures every partition that actually has uncommitted data
        // gets a proper ABORT marker. If a marker write fails, we track
        // the failure and report it to the client.
        //
        // WAL-write the abort decision BEFORE writing partition markers.
        if let Some(ref wal) = self.wal {
            // Build the set of partitions that actually received writes BEFORE
            // the WAL record, so we can include them in the payload for
            // self-contained replay.
            let written_partitions_for_wal: Vec<(String, u32)> = txn
                .pending_writes
                .iter()
                .map(|w| (w.partition.topic.clone(), w.partition.partition))
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();

            let payload = rivven_core::TxnWalPayload {
                txn_id: txn_id.clone(),
                producer_id,
                producer_epoch,
                partitions: written_partitions_for_wal,
            };
            match postcard::to_allocvec(&payload) {
                Ok(abort_bytes) => {
                    if let Err(e) = wal
                        .write_with_type(
                            bytes::Bytes::from(abort_bytes),
                            rivven_core::RecordType::TxnAbort,
                        )
                        .await
                    {
                        error!(
                            "WAL write failed for TxnAbort record of txn {}: {} — abort is not durable",
                            txn_id, e
                        );
                        return Response::Error {
                            message: format!("WAL failure during abort: {e}"),
                        };
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to serialise TxnAbort payload for txn {}: {} — cannot persist abort",
                        txn_id, e
                    );
                    return Response::Error {
                        message: format!("Failed to serialise TxnAbort: {e}"),
                    };
                }
            }
        }

        // Build the set of partitions that actually received writes
        // from pending_writes, so we target cleanup precisely.
        let written_partitions: std::collections::HashSet<_> = txn
            .pending_writes
            .iter()
            .map(|w| w.partition.clone())
            .collect();

        debug!(
            "Transaction {} abort: {} registered partitions, {} with actual writes",
            txn_id,
            txn.partitions.len(),
            written_partitions.len()
        );

        let mut abort_marker_failures: Vec<String> = Vec::new();
        let mut abort_marker_successes: Vec<String> = Vec::new();
        let max_abort_marker_retries: u32 = 5;
        for tp in &written_partitions {
            // Construct the ABORT marker once outside the retry loop.
            let marker = Message {
                producer_id: Some(producer_id),
                transaction_marker: Some(TransactionMarker::Abort),
                is_transactional: true,
                ..Message::new(Bytes::new())
            };
            let mut marker_written = false;
            for attempt in 0..max_abort_marker_retries {
                match self.topic_manager.get_topic(&tp.topic).await {
                    Ok(topic_obj) => match topic_obj.partition(tp.partition) {
                        Ok(partition) => match partition.append(marker.clone()).await {
                            Ok(_) => {
                                marker_written = true;
                                break;
                            }
                            Err(e) => {
                                let backoff_ms = 50 * 2u64.pow(attempt);
                                warn!(
                                        "ABORT marker write attempt {}/{} for txn {} to {}/{} failed: {} (retrying in {}ms)",
                                        attempt + 1, max_abort_marker_retries, txn_id, tp.topic, tp.partition, e, backoff_ms
                                    );
                                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms))
                                    .await;
                            }
                        },
                        Err(e) => {
                            let backoff_ms = 50 * 2u64.pow(attempt);
                            warn!(
                                "Partition {}/{} not found during abort of txn {} (attempt {}/{}): {} (retrying in {}ms)",
                                tp.topic, tp.partition, txn_id, attempt + 1, max_abort_marker_retries, e, backoff_ms
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                        }
                    },
                    Err(e) => {
                        let backoff_ms = 50 * 2u64.pow(attempt);
                        warn!(
                            "Topic {} not found during abort of txn {} (attempt {}/{}): {} (retrying in {}ms)",
                            tp.topic, txn_id, attempt + 1, max_abort_marker_retries, e, backoff_ms
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    }
                }
            }
            if marker_written {
                abort_marker_successes.push(format!("{}/{}", tp.topic, tp.partition));
            } else {
                error!(
                    "ABORT marker for txn {} to {}/{} failed after {} attempts — \
                     WAL recovery will re-apply on restart",
                    txn_id, tp.topic, tp.partition, max_abort_marker_retries
                );
                abort_marker_failures.push(format!("{}/{}", tp.topic, tp.partition));
            }
        }

        // Phase 2: Complete abort
        match self
            .transaction_coordinator
            .complete_abort(&txn_id, producer_id)
        {
            TransactionResult::Ok => {
                if !abort_marker_failures.is_empty() {
                    // The abort itself succeeded — the transaction is no longer
                    // active and the producer can start a new one. Some partition
                    // ABORT markers may not have been written; WAL recovery will
                    // re-apply them on restart. Return success so the client does
                    // NOT retry the abort (which would hit INVALID_TXN_ID).
                    warn!(
                        "Transaction {} aborted but ABORT markers failed for: {:?} — \
                         WAL recovery will re-apply on restart",
                        txn_id, abort_marker_failures
                    );
                }
                debug!("Transaction {} aborted", txn_id);
                Response::TransactionAborted { txn_id }
            }
            other => Response::Error {
                message: format!("ABORT_FAILED: {:?}", other),
            },
        }
    }
}
