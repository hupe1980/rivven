//! High-performance producer for publishing messages to Rivven topics
//!
//! Uses the batching [`rivven_client::Producer`] internally.  All
//! `send()` calls enqueue records into the producer's sender task which
//! coalesces them into wire-level batches (controlled by `batch_size`
//! and `linger_ms`), eliminating the N-round-trip problem of the
//! low-level `Client::publish()` path (PY-01).

use crate::error::IntoPyErr;
use bytes::Bytes;
use pyo3::prelude::*;
use rivven_client::{Producer as RustProducer, RecordMetadata};
use std::sync::Arc;

/// A producer for publishing messages to a Rivven topic
///
/// Producers are created from a RivvenClient and are bound to a specific topic.
/// Internally they use a batching sender task — multiple `send()` calls are
/// coalesced into a single wire-level batch for maximum throughput.
///
/// Example:
///     >>> producer = await client.producer("my-topic")
///     >>> offset = await producer.send(b"Hello!")
///     >>> print(f"Produced at offset {offset}")
///
///     # With key for partitioning
///     >>> offset = await producer.send(b"event data", key=b"user-123")
///
///     # Batch send (all messages are enqueued concurrently)
///     >>> offsets = await producer.send_batch([
///     ...     (b"msg1", None),
///     ...     (b"msg2", b"key2"),
///     ... ])
#[pyclass]
pub struct Producer {
    producer: Arc<RustProducer>,
    topic: String,
}

impl Producer {
    /// Create a new producer wrapping a high-level batching producer
    pub fn new(producer: Arc<RustProducer>, topic: String) -> Self {
        Self { producer, topic }
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
    /// The message is enqueued into the internal batch buffer and
    /// delivered asynchronously.  The returned future resolves once the
    /// broker acknowledges the record.
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
        let producer = Arc::clone(&self.producer);
        let topic = self.topic.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let meta: RecordMetadata = producer
                .send_with_key(topic, key.map(Bytes::from), Bytes::from(value))
                .await
                .into_py_err()?;
            Ok(meta.offset)
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
        let producer = Arc::clone(&self.producer);
        let topic = self.topic.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let meta: RecordMetadata = producer
                .send_to_partition(topic, partition, key.map(Bytes::from), Bytes::from(value))
                .await
                .into_py_err()?;
            Ok(meta.offset)
        })
    }

    /// Send multiple messages in a batch
    ///
    /// All messages are enqueued concurrently into the producer's internal
    /// batch buffer.  The sender task coalesces them into minimal wire-level
    /// batches automatically — no N-round-trip overhead.
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
        let producer = Arc::clone(&self.producer);
        let topic = self.topic.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Enqueue all records concurrently — the sender task batches
            // them on the wire, turning N sends into ≤ ceil(N/batch_size) RPCs.
            let futures: Vec<_> = messages
                .into_iter()
                .map(|(value, key)| {
                    let p = Arc::clone(&producer);
                    let t = topic.clone();
                    async move {
                        p.send_with_key(t, key.map(Bytes::from), Bytes::from(value))
                            .await
                    }
                })
                .collect();

            let results = futures::future::join_all(futures).await;

            let mut offsets = Vec::with_capacity(results.len());
            for result in results {
                let meta = result.into_py_err()?;
                offsets.push(meta.offset);
            }
            Ok(offsets)
        })
    }

    /// Flush all pending records
    ///
    /// Waits until all enqueued messages have been delivered to the broker.
    ///
    /// Example:
    ///     >>> await producer.flush()
    pub fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let producer = Arc::clone(&self.producer);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            producer.flush().await.into_py_err()?;
            Ok(())
        })
    }

    /// Close the producer, flushing pending records first
    ///
    /// Example:
    ///     >>> await producer.close()
    pub fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let producer = Arc::clone(&self.producer);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            producer.close().await;
            Ok(())
        })
    }

    fn __repr__(&self) -> String {
        format!("Producer(topic={:?})", self.topic)
    }
}
