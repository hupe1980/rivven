//! Consume request handlers (fetch, offset management, consumer groups)

use super::RequestHandler;
use crate::protocol::{MessageData, Response};
use rivven_core::Validator;
use tracing::warn;

impl RequestHandler {
    pub(crate) async fn handle_consume(
        &self,
        topic_name: String,
        partition: u32,
        offset: u64,
        max_messages: usize,
        isolation_level: Option<u8>,
        max_wait_ms: Option<u64>,
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
        const MAX_MESSAGES_LIMIT: usize = 10_000;
        if max_messages > MAX_MESSAGES_LIMIT {
            warn!(
                "max_messages {} exceeds server limit of {}, capping",
                max_messages, MAX_MESSAGES_LIMIT
            );
        }
        let max_messages = max_messages.min(MAX_MESSAGES_LIMIT);

        // Parse isolation level (0 = read_uncommitted, 1 = read_committed)
        let read_committed = isolation_level.map(|l| l == 1).unwrap_or(false);

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

        // Long-polling support (§4.5).
        // If max_wait_ms is set and no data is available, wait for new data
        // to arrive (up to the timeout) instead of returning an empty response.
        // This reduces CPU usage and network traffic for idle consumers.
        let max_wait = max_wait_ms
            .map(|ms| std::time::Duration::from_millis(ms.min(30_000))) // Cap at 30s
            .unwrap_or(std::time::Duration::ZERO);

        let messages = if max_wait.is_zero() {
            // Immediate fetch (backward compatible)
            match topic.read(partition, offset, max_messages).await {
                Ok(msgs) => msgs,
                Err(e) => {
                    return Response::Error {
                        message: e.to_string(),
                    };
                }
            }
        } else {
            // Long-polling: wait for data using partition write notifications.
            // The partition's `write_notify` is signalled on every append,
            // so consumers wake immediately instead of polling at 50 ms.
            let deadline = tokio::time::Instant::now() + max_wait;
            let partition_ref = match topic.partition(partition) {
                Ok(p) => p,
                Err(e) => {
                    return Response::Error {
                        message: e.to_string(),
                    };
                }
            };
            let notify = partition_ref.write_notify();

            loop {
                // Register the notification future BEFORE reading to prevent
                // missed wakeups: if a write occurs between read() returning
                // empty and select! polling notified(), the notification would
                // be lost. We create the Notified, pin it, and call enable()
                // to register as a waiter before checking the condition.
                // This is robust even if the partition switches from
                // notify_waiters() to notify_one() in the future.
                let notified = notify.notified();
                let mut notified = std::pin::pin!(notified);
                notified.as_mut().enable();

                match topic.read(partition, offset, max_messages).await {
                    Ok(msgs) if !msgs.is_empty() => break msgs,
                    Ok(_) => {
                        // No data yet — wait for a write or timeout
                        let remaining =
                            deadline.saturating_duration_since(tokio::time::Instant::now());
                        if remaining.is_zero() {
                            break Vec::new();
                        }
                        // Race-free: notified was registered before read()
                        tokio::select! {
                            _ = notified => { /* retry read */ }
                            _ = tokio::time::sleep(remaining) => {
                                // Final attempt before returning empty
                                match topic.read(partition, offset, max_messages).await {
                                    Ok(msgs) => break msgs,
                                    Err(e) => {
                                        return Response::Error {
                                            message: e.to_string(),
                                        };
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        return Response::Error {
                            message: e.to_string(),
                        };
                    }
                }
            }
        };

        // Filter and convert messages in a single pass
        let message_data: Vec<MessageData> = if read_committed {
            // For read_committed, filter out:
            // 1. Messages from aborted transactions
            // 2. Uncommitted transactional messages (beyond LSO)
            // 3. Control records (transaction markers)
            messages
                .into_iter()
                .filter(|msg| {
                    // Skip control records (markers)
                    if msg.is_control_record() {
                        return false;
                    }
                    // Non-transactional messages pass through
                    if !msg.is_transactional {
                        return true;
                    }
                    // Check if the transactional message is committed
                    if let Some(pid) = msg.producer_id {
                        !self.transaction_coordinator.is_aborted(pid, msg.offset)
                    } else {
                        true
                    }
                })
                .map(|msg| MessageData {
                    offset: msg.offset,
                    partition,
                    key: msg.key,
                    value: msg.value,
                    timestamp: msg.timestamp.timestamp_millis(),
                    headers: msg.headers,
                })
                .collect()
        } else {
            // read_uncommitted: return all data messages (skip control records only)
            messages
                .into_iter()
                .filter(|msg| !msg.is_control_record())
                .map(|msg| MessageData {
                    offset: msg.offset,
                    partition,
                    key: msg.key,
                    value: msg.value,
                    timestamp: msg.timestamp.timestamp_millis(),
                    headers: msg.headers,
                })
                .collect()
        };

        Response::Messages {
            messages: message_data,
        }
    }

    pub(crate) async fn handle_commit_offset(
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

        // Validate offset is within the partition's valid range.
        // A consumer committing u64::MAX would permanently skip all future messages.
        match self.topic_manager.get_topic(&topic).await {
            Ok(topic_obj) => match topic_obj.partition(partition) {
                Ok(p) => {
                    let latest = p.latest_offset().await;
                    if offset > latest {
                        warn!(
                            "Offset commit rejected: offset {} > latest {} for {}/{}",
                            offset, latest, topic, partition
                        );
                        return Response::Error {
                            message: format!(
                                "OFFSET_OUT_OF_RANGE: offset {} exceeds latest offset {} for partition {}",
                                offset, latest, partition
                            ),
                        };
                    }
                }
                Err(_) => {
                    return Response::Error {
                        message: format!(
                            "UNKNOWN_PARTITION: partition {} does not exist in topic '{}'",
                            partition, topic
                        ),
                    };
                }
            },
            Err(_) => {
                return Response::Error {
                    message: format!("UNKNOWN_TOPIC: topic '{}' does not exist", topic),
                };
            }
        }

        self.offset_manager
            .commit_offset(&consumer_group, &topic, partition, offset)
            .await;
        Response::OffsetCommitted
    }

    pub(crate) async fn handle_get_offset(
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

    pub(crate) async fn handle_get_offset_bounds(
        &self,
        topic_name: String,
        partition: u32,
    ) -> Response {
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

    pub(crate) async fn handle_get_offset_for_timestamp(
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

    pub(crate) async fn handle_list_groups(&self) -> Response {
        let groups = self.offset_manager.list_groups().await;
        Response::Groups { groups }
    }

    pub(crate) async fn handle_describe_group(&self, consumer_group: String) -> Response {
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

    pub(crate) async fn handle_delete_group(&self, consumer_group: String) -> Response {
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

    pub(crate) async fn handle_get_metadata(&self, topic_name: String) -> Response {
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

    pub(crate) async fn handle_create_topic(
        &self,
        name: String,
        partitions: Option<u32>,
    ) -> Response {
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

    pub(crate) async fn handle_list_topics(&self) -> Response {
        let topics = self.topic_manager.list_topics().await;
        Response::Topics { topics }
    }

    pub(crate) async fn handle_delete_topic(&self, name: String) -> Response {
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
}
