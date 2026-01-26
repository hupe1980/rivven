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
            let result = guard
                .create_topic(&name, partitions)
                .await
                .into_py_err()?;
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
                        .publish_with_key(&topic, Some(bytes::Bytes::from(k)), bytes::Bytes::from(value))
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
    // Schema Registry
    // ========================================================================

    /// Register a schema
    ///
    /// Args:
    ///     subject (str): Schema subject (e.g., "events-value")
    ///     schema (str): Schema definition (JSON string)
    ///
    /// Returns:
    ///     int: Schema ID
    ///
    /// Example:
    ///     >>> schema_id = await client.register_schema(
    ///     ...     "events-value",
    ///     ...     '{"type": "record", "name": "Event", ...}'
    ///     ... )
    pub fn register_schema<'py>(
        &self,
        py: Python<'py>,
        subject: String,
        schema: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let id = guard
                .register_schema(&subject, &schema)
                .await
                .into_py_err()?;
            Ok(id)
        })
    }

    /// Get a schema by ID
    ///
    /// Args:
    ///     schema_id (int): Schema ID
    ///
    /// Returns:
    ///     str: Schema definition
    ///
    /// Example:
    ///     >>> schema = await client.get_schema(1)
    pub fn get_schema<'py>(&self, py: Python<'py>, schema_id: i32) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let schema = guard
                .get_schema(schema_id)
                .await
                .into_py_err()?;
            Ok(schema)
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

    fn __repr__(&self) -> String {
        "RivvenClient(connected)".to_string()
    }
}
