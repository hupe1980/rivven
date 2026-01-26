//! Producer for publishing messages to Rivven topics

use crate::error::IntoPyErr;
use pyo3::prelude::*;
use rivven_client::Client;
use std::sync::Arc;
use tokio::sync::Mutex;

/// A producer for publishing messages to a Rivven topic
///
/// Producers are created from a RivvenClient and are bound to a specific topic.
/// Use the `send` method to publish messages.
///
/// Example:
///     >>> producer = client.producer("my-topic")
///     >>> offset = await producer.send(b"Hello!")
///     >>> print(f"Produced at offset {offset}")
///
///     # With key for partitioning
///     >>> offset = await producer.send(b"event data", key=b"user-123")
///
///     # Batch send
///     >>> offsets = await producer.send_batch([
///     ...     (b"msg1", None),
///     ...     (b"msg2", b"key2"),
///     ... ])
#[pyclass]
pub struct Producer {
    client: Arc<Mutex<Client>>,
    topic: String,
}

impl Producer {
    /// Create a new producer
    pub fn new(client: Arc<Mutex<Client>>, topic: String) -> Self {
        Self { client, topic }
    }
}

#[pymethods]
impl Producer {
    /// Get the topic this producer is bound to
    #[getter]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Send a message to the topic
    ///
    /// Args:
    ///     value (bytes): The message payload
    ///     key (bytes, optional): Optional key for partitioning
    ///
    /// Returns:
    ///     int: The offset where the message was written
    ///
    /// Raises:
    ///     RivvenError: If the send fails
    ///
    /// Example:
    ///     >>> offset = await producer.send(b"Hello, World!")
    ///     >>> offset = await producer.send(b"event", key=b"user-123")
    #[pyo3(signature = (value, key=None))]
    pub fn send<'py>(
        &self,
        py: Python<'py>,
        value: Vec<u8>,
        key: Option<Vec<u8>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        let topic = self.topic.clone();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let offset = match key {
                Some(k) => guard.publish_with_key(&topic, Some(bytes::Bytes::from(k)), bytes::Bytes::from(value)).await,
                None => guard.publish(&topic, bytes::Bytes::from(value)).await,
            }.into_py_err()?;
            Ok(offset)
        })
    }

    /// Send a message to a specific partition
    ///
    /// Args:
    ///     value (bytes): The message payload
    ///     partition (int): Target partition ID
    ///     key (bytes, optional): Optional key
    ///
    /// Returns:
    ///     int: The offset where the message was written
    ///
    /// Raises:
    ///     RivvenError: If the send fails or partition doesn't exist
    ///
    /// Example:
    ///     >>> offset = await producer.send_to_partition(b"event", partition=0)
    #[pyo3(signature = (value, partition, key=None))]
    pub fn send_to_partition<'py>(
        &self,
        py: Python<'py>,
        value: Vec<u8>,
        partition: u32,
        key: Option<Vec<u8>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        let topic = self.topic.clone();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let key_bytes = key.map(bytes::Bytes::from);
            let offset = guard
                .publish_to_partition(&topic, partition, key_bytes, bytes::Bytes::from(value))
                .await
                .into_py_err()?;
            Ok(offset)
        })
    }

    /// Send multiple messages in a batch
    ///
    /// Args:
    ///     messages: List of (value, key) tuples where key can be None
    ///
    /// Returns:
    ///     `list[int]`: List of offsets for each message
    ///
    /// Raises:
    ///     RivvenError: If any send fails
    ///
    /// Example:
    ///     >>> messages = [
    ///     ...     (b"msg1", None),
    ///     ...     (b"msg2", b"key2"),
    ///     ...     (b"msg3", b"key3"),
    ///     ... ]
    ///     >>> offsets = await producer.send_batch(messages)
    pub fn send_batch<'py>(
        &self,
        py: Python<'py>,
        messages: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        let topic = self.topic.clone();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let mut offsets = Vec::with_capacity(messages.len());
            
            for (value, key) in messages {
                let offset = match key {
                    Some(k) => guard.publish_with_key(&topic, Some(bytes::Bytes::from(k)), bytes::Bytes::from(value)).await,
                    None => guard.publish(&topic, bytes::Bytes::from(value)).await,
                }.into_py_err()?;
                offsets.push(offset);
            }
            
            Ok(offsets)
        })
    }

    fn __repr__(&self) -> String {
        format!("Producer(topic={:?})", self.topic)
    }
}
