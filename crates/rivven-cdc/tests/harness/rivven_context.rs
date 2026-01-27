//! Rivven test context for CDC integration tests

use anyhow::{Context, Result};
use rivven_cdc::common::{CdcConnector, CdcEvent, CdcSource};
use rivven_cdc::postgres::{PostgresCdc, PostgresCdcConfig};
use rivven_core::{Config, Message, Partition, TopicManager};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, info};

/// Rivven context for CDC testing
pub struct RivvenTestContext {
    pub topic_manager: TopicManager,
    pub connector: Arc<CdcConnector>,
    pub partitions: HashMap<String, Arc<Partition>>,
    pub data_dir: PathBuf,
    cdc_instance: Option<PostgresCdc>,
    event_routing_task: Option<JoinHandle<()>>,
}

#[allow(dead_code)]
impl RivvenTestContext {
    pub async fn new() -> Result<Self> {
        let data_dir = PathBuf::from(format!("/tmp/rivven_cdc_test_{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&data_dir).await?;

        let config = Config {
            data_dir: data_dir.to_string_lossy().to_string(),
            ..Default::default()
        };

        let topic_manager = TopicManager::new(config);
        let connector = Arc::new(CdcConnector::new());

        Ok(Self {
            topic_manager,
            connector,
            partitions: HashMap::new(),
            data_dir,
            cdc_instance: None,
            event_routing_task: None,
        })
    }

    pub async fn register_cdc_topic(
        &mut self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Arc<Partition>> {
        let topic_name = format!("cdc.{}.{}.{}", database, schema, table);

        let topic = self
            .topic_manager
            .create_topic(topic_name.clone(), None)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        let partition = topic.partition(0).map_err(|e| anyhow::anyhow!("{}", e))?;
        self.connector
            .register_topic(topic_name.clone(), partition.clone())
            .await;
        self.partitions.insert(topic_name, partition.clone());

        info!(
            "📌 Registered CDC topic for {}.{}.{}",
            database, schema, table
        );
        Ok(partition)
    }

    pub async fn start_cdc(
        &mut self,
        connection_string: &str,
        slot_name: &str,
        publication_name: &str,
    ) -> Result<()> {
        let config = PostgresCdcConfig::builder()
            .connection_string(connection_string)
            .slot_name(slot_name)
            .publication_name(publication_name)
            .build()
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        let mut cdc = PostgresCdc::new(config);

        let event_rx = cdc
            .take_event_receiver()
            .ok_or_else(|| anyhow::anyhow!("Failed to get event receiver"))?;
        let connector_clone = self.connector.clone();
        let event_routing_task = tokio::spawn(async move {
            let mut rx = event_rx;
            while let Some(event) = rx.recv().await {
                if let Err(e) = connector_clone.route_event(&event).await {
                    debug!("Failed to route event: {:?}", e);
                }
            }
        });
        self.event_routing_task = Some(event_routing_task);

        cdc.start().await?;
        self.cdc_instance = Some(cdc);

        sleep(Duration::from_millis(super::CDC_STARTUP_DELAY_MS)).await;

        info!(
            "🚀 CDC started with slot {} and publication {}",
            slot_name, publication_name
        );
        Ok(())
    }

    pub async fn stop_cdc(&mut self) -> Result<()> {
        if let Some(task) = self.event_routing_task.take() {
            task.abort();
        }
        if let Some(mut cdc) = self.cdc_instance.take() {
            cdc.stop().await?;
        }
        Ok(())
    }

    pub async fn wait_for_messages(
        &self,
        topic: &str,
        expected_count: usize,
        timeout_duration: Duration,
    ) -> Result<Vec<Message>> {
        let start = std::time::Instant::now();
        let partition = self
            .partitions
            .get(topic)
            .context(format!("Topic not found: {}", topic))?;

        loop {
            let messages = partition
                .read(0, expected_count + 10)
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))?;

            if messages.len() >= expected_count {
                return Ok(messages);
            }

            if start.elapsed() > timeout_duration {
                anyhow::bail!(
                    "Timeout waiting for {} messages in {}, got {}",
                    expected_count,
                    topic,
                    messages.len()
                );
            }

            sleep(Duration::from_millis(100)).await;
        }
    }

    pub fn parse_events(&self, messages: &[Message]) -> Result<Vec<CdcEvent>> {
        messages
            .iter()
            .map(|msg| {
                serde_json::from_slice::<CdcEvent>(&msg.value).context("Failed to parse CDC event")
            })
            .collect()
    }

    /// Read messages from a topic starting at a specific offset
    pub async fn read_messages(
        &self,
        topic: &str,
        start_offset: u64,
        limit: usize,
    ) -> Result<Vec<Message>> {
        let partition = self
            .partitions
            .get(topic)
            .context(format!("Topic not found: {}", topic))?;

        let messages = partition
            .read(start_offset, limit)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        Ok(messages)
    }
}

impl Drop for RivvenTestContext {
    fn drop(&mut self) {
        if self.data_dir.exists() {
            let _ = std::fs::remove_dir_all(&self.data_dir);
        }
    }
}
