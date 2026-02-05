//! Rivven client for Python

use crate::consumer::Consumer;
use crate::error::IntoPyErr;
use crate::producer::Producer;
use pyo3::prelude::*;
use rivven_client::Client;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Main client for interacting with a Rivven broker
///
/// The client provides methods for:
/// - Topic management (create, list, delete)
/// - Producing messages
/// - Consuming messages
/// - Consumer group management
/// - Schema registry operations
///
/// Example:
///     >>> import rivven
///     >>>
///     >>> async def main():
///     ...     client = await rivven.connect("localhost:9092")
///     ...     
///     ...     # Create topic
///     ...     await client.create_topic("events", partitions=3)
///     ...     
///     ...     # Produce
///     ...     producer = client.producer("events")
///     ...     await producer.send(b"Hello!")
///     ...     
///     ...     # Consume
///     ...     consumer = client.consumer("events", group="my-app")
///     ...     messages = await consumer.fetch()
#[pyclass]
pub struct RivvenClient {
    client: Arc<Mutex<Client>>,
}

impl RivvenClient {
    /// Create a new RivvenClient wrapping the Rust client
    pub fn new(client: Client) -> Self {
        Self {
            client: Arc::new(Mutex::new(client)),
        }
    }
}

#[pymethods]
impl RivvenClient {
    // ========================================================================
    // Topic Management
    // ========================================================================

    /// Create a new topic
    ///
    /// Args:
    ///     name (str): Topic name
    ///     partitions (int, optional): Number of partitions (default: 1)
    ///
    /// Returns:
    ///     int: Number of partitions created
    ///
    /// Raises:
    ///     RivvenError: If topic creation fails
    ///
    /// Example:
    ///     >>> await client.create_topic("events")
    ///     >>> await client.create_topic("orders", partitions=8)
    #[pyo3(signature = (name, partitions=None))]
    pub fn create_topic<'py>(
        &self,
        py: Python<'py>,
        name: String,
        partitions: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let result = guard.create_topic(&name, partitions).await.into_py_err()?;
            Ok(result)
        })
    }

    /// List all topics
    ///
    /// Returns:
    ///     list[str]: List of topic names
    ///
    /// Example:
    ///     >>> topics = await client.list_topics()
    ///     >>> print(f"Found {len(topics)} topics")
    pub fn list_topics<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let topics = guard.list_topics().await.into_py_err()?;
            Ok(topics)
        })
    }

    /// Delete a topic
    ///
    /// Args:
    ///     name (str): Topic name to delete
    ///
    /// Raises:
    ///     RivvenError: If deletion fails
    ///
    /// Example:
    ///     >>> await client.delete_topic("old-topic")
    pub fn delete_topic<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            guard.delete_topic(&name).await.into_py_err()?;
            Ok(())
        })
    }

    /// Get topic metadata
    ///
    /// Args:
    ///     name (str): Topic name
    ///
    /// Returns:
    ///     tuple[str, int]: (topic_name, partition_count)
    ///
    /// Example:
    ///     >>> name, partitions = await client.get_metadata("events")
    ///     >>> print(f"Topic {name} has {partitions} partitions")
    pub fn get_metadata<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let metadata = guard.get_metadata(&name).await.into_py_err()?;
            Ok(metadata)
        })
    }

    // ========================================================================
    // Producer / Consumer Factory
    // ========================================================================

    /// Create a producer for a topic
    ///
    /// Args:
    ///     topic (str): Topic to produce to
    ///
    /// Returns:
    ///     Producer: A producer instance bound to the topic
    ///
    /// Example:
    ///     >>> producer = client.producer("events")
    ///     >>> await producer.send(b"Hello!")
    pub fn producer(&self, topic: String) -> Producer {
        Producer::new(Arc::clone(&self.client), topic)
    }

    /// Create a consumer for a topic partition
    ///
    /// Args:
    ///     topic (str): Topic to consume from
    ///     partition (int): Partition to consume (default: 0)
    ///     group (str, optional): Consumer group for offset tracking
    ///     start_offset (int, optional): Starting offset (default: 0 or committed)
    ///     batch_size (int): Messages per fetch (default: 100)
    ///     auto_commit (bool): Auto-commit on iteration (default: True)
    ///
    /// Returns:
    ///     Consumer: A consumer instance
    ///
    /// Example:
    ///     >>> consumer = await client.consumer("events", group="my-app")
    ///     >>> async for message in consumer:
    ///     ...     print(message.value)
    #[pyo3(signature = (topic, partition=0, group=None, start_offset=None, batch_size=100, auto_commit=true))]
    #[allow(clippy::too_many_arguments)]
    pub fn consumer<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        partition: u32,
        group: Option<String>,
        start_offset: Option<u64>,
        batch_size: usize,
        auto_commit: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        let group_clone = group.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Determine starting offset
            let offset = match (start_offset, &group_clone) {
                (Some(off), _) => off,
                (None, Some(g)) => {
                    // Try to get committed offset
                    let mut guard = client.lock().await;
                    match guard.get_offset(g, &topic, partition).await {
                        Ok(Some(off)) => off,
                        Ok(None) => 0,
                        Err(_) => 0,
                    }
                }
                (None, None) => 0,
            };

            Ok(Consumer::new(
                client,
                topic,
                partition,
                group_clone,
                offset,
                batch_size,
                auto_commit,
            ))
        })
    }

    // ========================================================================
    // Consumer Group Management
    // ========================================================================

    /// Authenticate with username and password
    ///
    /// Args:
    ///     username (str): Username
    ///     password (str): Password
    ///
    /// Returns:
    ///     tuple[str, int]: (session_id, expires_in_seconds)
    ///
    /// Raises:
    ///     RivvenError: If authentication fails
    ///
    /// Example:
    ///     >>> session_id, expires = await client.authenticate("user", "password")
    pub fn authenticate<'py>(
        &self,
        py: Python<'py>,
        username: String,
        password: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let session = guard
                .authenticate(&username, &password)
                .await
                .into_py_err()?;
            Ok((session.session_id, session.expires_in))
        })
    }

    /// Authenticate with SCRAM-SHA-256 (secure challenge-response)
    ///
    /// SCRAM is more secure than plaintext authentication as the password
    /// never goes over the wire.
    ///
    /// Args:
    ///     username (str): Username
    ///     password (str): Password
    ///
    /// Returns:
    ///     tuple[str, int]: (session_id, expires_in_seconds)
    ///
    /// Raises:
    ///     RivvenError: If authentication fails
    ///
    /// Example:
    ///     >>> session_id, expires = await client.authenticate_scram("user", "password")
    pub fn authenticate_scram<'py>(
        &self,
        py: Python<'py>,
        username: String,
        password: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let session = guard
                .authenticate_scram(&username, &password)
                .await
                .into_py_err()?;
            Ok((session.session_id, session.expires_in))
        })
    }

    /// List all consumer groups
    ///
    /// Returns:
    ///     list[str]: List of consumer group names
    ///
    /// Example:
    ///     >>> groups = await client.list_groups()
    pub fn list_groups<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let groups = guard.list_groups().await.into_py_err()?;
            Ok(groups)
        })
    }

    /// Describe a consumer group (get all committed offsets)
    ///
    /// Args:
    ///     group (str): Consumer group name
    ///
    /// Returns:
    ///     dict[str, dict[int, int]]: topic → partition → offset mapping
    ///
    /// Example:
    ///     >>> offsets = await client.describe_group("my-app")
    ///     >>> for topic, parts in offsets.items():
    ///     ...     for partition, offset in parts.items():
    ///     ...         print(f"{topic}[{partition}] @ {offset}")
    pub fn describe_group<'py>(
        &self,
        py: Python<'py>,
        group: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let desc = guard.describe_group(&group).await.into_py_err()?;
            Ok(desc)
        })
    }

    /// Delete a consumer group
    ///
    /// Args:
    ///     group (str): Consumer group to delete
    ///
    /// Raises:
    ///     RivvenError: If deletion fails
    ///
    /// Example:
    ///     >>> await client.delete_group("old-group")
    pub fn delete_group<'py>(&self, py: Python<'py>, group: String) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            guard.delete_group(&group).await.into_py_err()?;
            Ok(())
        })
    }

    /// Commit an offset for a consumer group
    ///
    /// Args:
    ///     group (str): Consumer group
    ///     topic (str): Topic name
    ///     partition (int): Partition ID
    ///     offset (int): Offset to commit
    ///
    /// Example:
    ///     >>> await client.commit_offset("my-app", "events", 0, 100)
    pub fn commit_offset<'py>(
        &self,
        py: Python<'py>,
        group: String,
        topic: String,
        partition: u32,
        offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            guard
                .commit_offset(&group, &topic, partition, offset)
                .await
                .into_py_err()?;
            Ok(())
        })
    }

    /// Get committed offset for a consumer group
    ///
    /// Args:
    ///     group (str): Consumer group
    ///     topic (str): Topic name
    ///     partition (int): Partition ID
    ///
    /// Returns:
    ///     int | None: Committed offset or None if not committed
    ///
    /// Example:
    ///     >>> offset = await client.get_offset("my-app", "events", 0)
    pub fn get_offset<'py>(
        &self,
        py: Python<'py>,
        group: String,
        topic: String,
        partition: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let offset = guard
                .get_offset(&group, &topic, partition)
                .await
                .into_py_err()?;
            Ok(offset)
        })
    }

    // ========================================================================
    // Direct Produce/Consume (convenience methods)
    // ========================================================================

    /// Publish a message directly (convenience method)
    ///
    /// For high-throughput scenarios, prefer using a Producer instance.
    ///
    /// Args:
    ///     topic (str): Topic to publish to
    ///     value (bytes): Message payload
    ///     key (bytes, optional): Optional key for partitioning
    ///
    /// Returns:
    ///     int: Offset where message was written
    ///
    /// Example:
    ///     >>> offset = await client.publish("events", b"Hello!")
    #[pyo3(signature = (topic, value, key=None))]
    pub fn publish<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        value: Vec<u8>,
        key: Option<Vec<u8>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let offset = match key {
                Some(k) => {
                    guard
                        .publish_with_key(
                            &topic,
                            Some(bytes::Bytes::from(k)),
                            bytes::Bytes::from(value),
                        )
                        .await
                }
                None => guard.publish(&topic, bytes::Bytes::from(value)).await,
            }
            .into_py_err()?;
            Ok(offset)
        })
    }

    /// Consume messages directly (convenience method)
    ///
    /// For continuous consumption, prefer using a Consumer instance.
    ///
    /// Args:
    ///     topic (str): Topic to consume from
    ///     partition (int): Partition ID
    ///     offset (int): Starting offset
    ///     max_messages (int): Maximum messages to fetch
    ///
    /// Returns:
    ///     `list[Message]`: List of messages
    ///
    /// Example:
    ///     >>> messages = await client.consume("events", 0, 0, 100)
    pub fn consume<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        partition: u32,
        offset: u64,
        max_messages: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        let topic_clone = topic.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let messages = guard
                .consume(&topic, partition, offset, max_messages)
                .await
                .into_py_err()?;

            let result: Vec<crate::Message> = messages
                .into_iter()
                .map(|m| crate::Message::from_client_data(m, partition, topic_clone.clone()))
                .collect();
            Ok(result)
        })
    }

    // ========================================================================
    // Health / Utilities
    // ========================================================================

    /// Ping the broker to check connectivity
    ///
    /// Raises:
    ///     RivvenError: If ping fails
    ///
    /// Example:
    ///     >>> await client.ping()
    ///     >>> print("Broker is reachable")
    pub fn ping<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            guard.ping().await.into_py_err()?;
            Ok(())
        })
    }

    // ========================================================================
    // Admin Operations
    // ========================================================================

    /// Get offset for a timestamp
    ///
    /// Finds the earliest offset whose timestamp is >= the given timestamp.
    ///
    /// Args:
    ///     topic (str): Topic name
    ///     partition (int): Partition ID
    ///     timestamp_ms (int): Unix timestamp in milliseconds
    ///
    /// Returns:
    ///     int | None: The offset, or None if no messages after timestamp
    ///
    /// Example:
    ///     >>> # Find offset for messages from 1 hour ago
    ///     >>> import time
    ///     >>> ts = int((time.time() - 3600) * 1000)
    ///     >>> offset = await client.get_offset_for_timestamp("events", 0, ts)
    pub fn get_offset_for_timestamp<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        partition: u32,
        timestamp_ms: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let offset = guard
                .get_offset_for_timestamp(&topic, partition, timestamp_ms)
                .await
                .into_py_err()?;
            Ok(offset)
        })
    }

    /// Describe topic configuration
    ///
    /// Args:
    ///     topic (str): Topic name
    ///
    /// Returns:
    ///     dict[str, str]: Configuration key-value pairs
    ///
    /// Example:
    ///     >>> config = await client.describe_topic_configs("events")
    ///     >>> print(f"Retention: {config.get('retention.ms', 'default')}")
    pub fn describe_topic_configs<'py>(
        &self,
        py: Python<'py>,
        topic: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let topics = [topic.as_str()];
            let configs = guard.describe_topic_configs(&topics).await.into_py_err()?;
            // Extract the config for the single topic
            let topic_config = configs.get(&topic).cloned().unwrap_or_default();
            Ok(topic_config)
        })
    }

    /// Alter topic configuration
    ///
    /// Modifies configuration for an existing topic. Pass None as value to reset
    /// a configuration key to its default.
    ///
    /// Args:
    ///     topic (str): Topic name
    ///     configs (list[tuple[str, str | None]]): List of (key, value) pairs
    ///
    /// Returns:
    ///     int: Number of configurations changed
    ///
    /// Example:
    ///     >>> # Set retention to 1 day
    ///     >>> changed = await client.alter_topic_config("events", [("retention.ms", "86400000")])
    ///     >>> # Reset to default
    ///     >>> await client.alter_topic_config("events", [("retention.ms", None)])
    pub fn alter_topic_config<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        configs: Vec<(String, Option<String>)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            // Convert to the expected format
            let config_refs: Vec<(&str, Option<&str>)> = configs
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_deref()))
                .collect();
            let result = guard
                .alter_topic_config(&topic, &config_refs)
                .await
                .into_py_err()?;
            Ok(result.changed_count)
        })
    }

    /// Add partitions to an existing topic
    ///
    /// Args:
    ///     topic (str): Topic name
    ///     new_total (int): New total number of partitions (must be > current)
    ///
    /// Returns:
    ///     int: The new partition count
    ///
    /// Example:
    ///     >>> new_count = await client.create_partitions("events", 8)
    pub fn create_partitions<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        new_total: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let count = guard
                .create_partitions(&topic, new_total)
                .await
                .into_py_err()?;
            Ok(count)
        })
    }

    /// Delete records before a given offset
    ///
    /// Deletes all records with offset < before_offset for a single partition.
    ///
    /// Args:
    ///     topic (str): Topic name
    ///     partition (int): Partition ID
    ///     before_offset (int): Delete records before this offset
    ///
    /// Returns:
    ///     int: The new low watermark (earliest available offset)
    ///
    /// Example:
    ///     >>> new_low = await client.delete_records("events", 0, 1000)
    ///     >>> print(f"Earliest offset is now: {new_low}")
    pub fn delete_records<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        partition: u32,
        before_offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let partition_offsets = [(partition, before_offset)];
            let results = guard
                .delete_records(&topic, &partition_offsets)
                .await
                .into_py_err()?;

            // Extract the low watermark for the single partition
            let low_watermark = results
                .first()
                .map(|r| r.low_watermark)
                .unwrap_or(before_offset);
            Ok(low_watermark)
        })
    }

    /// Delete records for multiple partitions
    ///
    /// Deletes records across multiple partitions in a single request.
    ///
    /// Args:
    ///     topic (str): Topic name
    ///     partition_offsets (list[tuple[int, int]]): List of (partition, before_offset) pairs
    ///
    /// Returns:
    ///     list[tuple[int, int, str | None]]: List of (partition, low_watermark, error) tuples
    ///
    /// Example:
    ///     >>> results = await client.delete_records_batch("events", [
    ///     ...     (0, 1000), (1, 2000), (2, 3000)
    ///     ... ])
    ///     >>> for partition, low_watermark, error in results:
    ///     ...     if error:
    ///     ...         print(f"Partition {partition} failed: {error}")
    ///     ...     else:
    ///     ...         print(f"Partition {partition} low watermark: {low_watermark}")
    pub fn delete_records_batch<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        partition_offsets: Vec<(u32, u64)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let results = guard
                .delete_records(&topic, &partition_offsets)
                .await
                .into_py_err()?;

            // Convert to Python-friendly format
            let py_results: Vec<(u32, u64, Option<String>)> = results
                .into_iter()
                .map(|r| (r.partition, r.low_watermark, r.error))
                .collect();
            Ok(py_results)
        })
    }

    // ========================================================================
    // Transaction Support
    // ========================================================================

    /// Initialize a producer ID for idempotent/transactional producing
    ///
    /// Returns a ProducerState that tracks the producer ID, epoch, and sequence
    /// numbers for exactly-once semantics.
    ///
    /// Args:
    ///     previous_producer_id (int, optional): Previous producer ID for recovery
    ///
    /// Returns:
    ///     ProducerState: State object for idempotent/transactional producing
    ///
    /// Example:
    ///     >>> producer_state = await client.init_producer_id()
    ///     >>> print(f"Producer ID: {await producer_state.producer_id}")
    ///
    ///     >>> # Use for idempotent producing
    ///     >>> offset, partition, dup = await client.publish_idempotent(
    ///     ...     "events", b"payload", producer_state
    ///     ... )
    #[pyo3(signature = (previous_producer_id=None))]
    pub fn init_producer_id<'py>(
        &self,
        py: Python<'py>,
        previous_producer_id: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let state = guard
                .init_producer_id(previous_producer_id)
                .await
                .into_py_err()?;
            Ok(crate::ProducerState::new(state))
        })
    }

    /// Publish with idempotent semantics (exactly-once)
    ///
    /// Uses producer state for exactly-once delivery. The broker deduplicates
    /// messages based on producer_id/epoch/sequence, making retries safe.
    ///
    /// Args:
    ///     topic (str): Topic name
    ///     value (bytes): Message payload
    ///     producer_state (ProducerState): State from init_producer_id
    ///     key (bytes, optional): Message key for partitioning
    ///
    /// Returns:
    ///     tuple[int, int, bool]: (offset, partition, was_duplicate)
    ///
    /// Example:
    ///     >>> producer_state = await client.init_producer_id()
    ///     >>> offset, partition, dup = await client.publish_idempotent(
    ///     ...     "events", b"message", producer_state
    ///     ... )
    ///     >>> if dup:
    ///     ...     print("Message was a retry (already existed)")
    #[pyo3(signature = (topic, value, producer_state, key=None))]
    pub fn publish_idempotent<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        value: Vec<u8>,
        producer_state: crate::ProducerState,
        key: Option<Vec<u8>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        let state_inner = Arc::clone(&producer_state.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let mut state_guard = state_inner.lock().await;
            let result = guard
                .publish_idempotent(
                    &topic,
                    key.map(bytes::Bytes::from),
                    bytes::Bytes::from(value),
                    &mut state_guard,
                )
                .await
                .into_py_err()?;
            Ok(result)
        })
    }

    /// Begin a transaction
    ///
    /// Starts an atomic transaction that can span multiple topics/partitions.
    /// All writes in the transaction either all succeed or all fail.
    ///
    /// Args:
    ///     transactional_id (str): Unique transaction identifier
    ///     producer_state (ProducerState): State from init_producer_id
    ///     timeout_ms (int, optional): Transaction timeout in milliseconds
    ///
    /// Example:
    ///     >>> producer_state = await client.init_producer_id()
    ///     >>> await client.begin_transaction("my-txn", producer_state)
    ///     >>> await client.publish_idempotent("events", b"msg", producer_state)
    ///     >>> await client.commit_transaction("my-txn", producer_state)
    #[pyo3(signature = (transactional_id, producer_state, timeout_ms=None))]
    pub fn begin_transaction<'py>(
        &self,
        py: Python<'py>,
        transactional_id: String,
        producer_state: crate::ProducerState,
        timeout_ms: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        let state_inner = Arc::clone(&producer_state.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let state_guard = state_inner.lock().await;
            guard
                .begin_transaction(&transactional_id, &state_guard, timeout_ms)
                .await
                .into_py_err()?;
            Ok(())
        })
    }

    /// Commit a transaction
    ///
    /// Atomically commits all writes in the transaction. If this fails,
    /// the transaction should be aborted.
    ///
    /// Args:
    ///     transactional_id (str): Transaction identifier
    ///     producer_state (ProducerState): State from init_producer_id
    ///
    /// Example:
    ///     >>> await client.commit_transaction("my-txn", producer_state)
    pub fn commit_transaction<'py>(
        &self,
        py: Python<'py>,
        transactional_id: String,
        producer_state: crate::ProducerState,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        let state_inner = Arc::clone(&producer_state.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let state_guard = state_inner.lock().await;
            guard
                .commit_transaction(&transactional_id, &state_guard)
                .await
                .into_py_err()?;
            Ok(())
        })
    }

    /// Abort a transaction
    ///
    /// Discards all writes in the transaction. Call this if any write fails
    /// or if you need to cancel the transaction.
    ///
    /// Args:
    ///     transactional_id (str): Transaction identifier
    ///     producer_state (ProducerState): State from init_producer_id
    ///
    /// Example:
    ///     >>> try:
    ///     ...     await client.publish_idempotent("events", b"msg", producer_state)
    ///     ...     await client.commit_transaction("my-txn", producer_state)
    ///     >>> except Exception:
    ///     ...     await client.abort_transaction("my-txn", producer_state)
    pub fn abort_transaction<'py>(
        &self,
        py: Python<'py>,
        transactional_id: String,
        producer_state: crate::ProducerState,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        let state_inner = Arc::clone(&producer_state.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let state_guard = state_inner.lock().await;
            guard
                .abort_transaction(&transactional_id, &state_guard)
                .await
                .into_py_err()?;
            Ok(())
        })
    }

    /// Add partitions to an active transaction
    ///
    /// Registers partitions that will be written to within the transaction.
    /// Must be called before publishing to a new partition in a transaction.
    ///
    /// Args:
    ///     transactional_id (str): Transaction identifier
    ///     producer_state (ProducerState): State from init_producer_id
    ///     partitions (list[tuple[str, int]]): List of (topic, partition) pairs
    ///
    /// Returns:
    ///     int: Number of partitions added
    ///
    /// Example:
    ///     >>> await client.add_partitions_to_txn("my-txn", producer_state, [
    ///     ...     ("events", 0), ("events", 1), ("orders", 0)
    ///     ... ])
    pub fn add_partitions_to_txn<'py>(
        &self,
        py: Python<'py>,
        transactional_id: String,
        producer_state: crate::ProducerState,
        partitions: Vec<(String, u32)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        let state_inner = Arc::clone(&producer_state.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let state_guard = state_inner.lock().await;
            let partition_refs: Vec<(&str, u32)> =
                partitions.iter().map(|(t, p)| (t.as_str(), *p)).collect();
            let count = guard
                .add_partitions_to_txn(&transactional_id, &state_guard, &partition_refs)
                .await
                .into_py_err()?;
            Ok(count)
        })
    }

    fn __repr__(&self) -> String {
        "RivvenClient(connected)".to_string()
    }
}
