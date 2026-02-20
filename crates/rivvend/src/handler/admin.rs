//! Admin API request handlers (quotas, topic config, partitions, delete records)

use super::RequestHandler;
use crate::protocol::{
    DeleteRecordsResult, QuotaAlteration, QuotaEntry, Response, TopicConfigDescription,
    TopicConfigEntry, TopicConfigValue,
};
use rivven_core::{QuotaConfig, QuotaEntity, QuotaResult};
use std::collections::HashMap;
use tracing::{debug, info, warn};

impl RequestHandler {
    // =========================================================================
    // Per-Principal Quotas (Kafka Parity)
    // =========================================================================

    /// Handle DescribeQuotas request
    pub(crate) async fn handle_describe_quotas(
        &self,
        entities: Vec<(String, Option<String>)>,
    ) -> Response {
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
    pub(crate) async fn handle_alter_quotas(&self, alterations: Vec<QuotaAlteration>) -> Response {
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
    pub(crate) async fn handle_alter_topic_config(
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
    pub(crate) async fn handle_create_partitions(
        &self,
        topic: String,
        new_partition_count: u32,
        assignments: Vec<Vec<String>>,
    ) -> Response {
        // Log when assignments are provided but not applied
        if !assignments.is_empty() {
            warn!(
                "CreatePartitions: partition assignments provided for topic '{}' but not applied \
                 — automatic placement will be used. Assignment hints are not yet supported.",
                topic
            );
        }

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

        // Dynamically add new partitions
        match self
            .topic_manager
            .add_partitions(&topic, new_partition_count)
            .await
        {
            Ok(added) => {
                info!(
                    "CreatePartitions: topic '{}' expanded from {} to {} partitions (+{})",
                    topic, current_count, new_partition_count, added
                );
                Response::PartitionsCreated {
                    topic,
                    new_partition_count,
                }
            }
            Err(e) => Response::Error {
                message: format!("PARTITION_CREATE_FAILED: {}", e),
            },
        }
    }

    /// Handle DeleteRecords request
    pub(crate) async fn handle_delete_records(
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

            // Set the low watermark
            partition.set_low_watermark(target_offset).await;

            info!(
                "DeleteRecords: topic '{}' partition {} low watermark set to {} (was {})",
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
    pub(crate) async fn handle_describe_topic_configs(&self, topics: Vec<String>) -> Response {
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
