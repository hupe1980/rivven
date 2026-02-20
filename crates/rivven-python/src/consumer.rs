//! Consumer for reading messages from Rivven topics

use crate::error::{IntoPyErr, RivvenError};
use crate::message::Message;
use pyo3::prelude::*;
use rivven_client::Client;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// A consumer for reading messages from a Rivven topic
///
/// Consumers support both async iteration and manual fetch.
/// They can track offsets via consumer groups for resumable consumption.
///
/// # Example
///
/// ```text
/// consumer = client.consumer("my-topic", group="my-group")
///
/// # Async iteration (auto-commits on iteration)
/// async for message in consumer:
///     print(f"Got: {message.value.decode()}")
///
/// # Manual fetch with explicit commit
/// messages = await consumer.fetch(max_messages=100)
/// for msg in messages:
///     process(msg)
/// await consumer.commit()
/// ```
#[pyclass]
pub struct Consumer {
    client: Arc<Mutex<Client>>,
    topic: String,
    partition: u32,
    group: Option<String>,
    offset: Arc<Mutex<u64>>,
    batch_size: usize,
    auto_commit: bool,
    /// Poll interval for empty fetch retries in `__anext__`.
    /// Prevents StopAsyncIteration on momentarily empty topics.
    poll_interval: Duration,
    /// Prefetch buffer to avoid one-message-per-RPC overhead.
    /// `__anext__` fetches `batch_size` messages at once, then drains
    /// the buffer one-by-one for each Python iteration step.
    prefetch: Arc<Mutex<VecDeque<rivven_client::MessageData>>>,
}

impl Consumer {
    /// Create a new consumer
    pub fn new(
        client: Arc<Mutex<Client>>,
        topic: String,
        partition: u32,
        group: Option<String>,
        start_offset: u64,
        batch_size: usize,
        auto_commit: bool,
    ) -> Self {
        Self {
            client,
            topic,
            partition,
            group,
            offset: Arc::new(Mutex::new(start_offset)),
            batch_size,
            auto_commit,
            poll_interval: Duration::from_millis(100),
            prefetch: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

#[pymethods]
impl Consumer {
    /// Get the topic being consumed
    #[getter]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Get the partition being consumed
    #[getter]
    pub fn partition(&self) -> u32 {
        self.partition
    }

    /// Get the consumer group (if any)
    #[getter]
    pub fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    /// Get the current offset
    pub fn current_offset<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let offset = Arc::clone(&self.offset);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = offset.lock().await;
            Ok(*guard)
        })
    }

    /// Fetch messages from the topic
    ///
    /// Args:
    ///     max_messages (int): Maximum number of messages to fetch (default: batch_size)
    ///
    /// Returns:
    ///     `list[Message]`: List of consumed messages
    ///
    /// Raises:
    ///     RivvenError: If fetch fails
    ///
    /// Example:
    ///     >>> messages = await consumer.fetch(max_messages=100)
    ///     >>> for msg in messages:
    ///     ...     print(msg.value)
    #[pyo3(signature = (max_messages=None))]
    pub fn fetch<'py>(
        &self,
        py: Python<'py>,
        max_messages: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        let topic = self.topic.clone();
        let partition = self.partition;
        let offset = Arc::clone(&self.offset);
        let batch_size = max_messages.unwrap_or(self.batch_size);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let current_offset = {
                let guard = offset.lock().await;
                *guard
            };

            let mut guard = client.lock().await;
            let messages = guard
                .consume(&topic, partition, current_offset, batch_size)
                .await
                .into_py_err()?;

            // Update offset if we got messages
            if let Some(last) = messages.last() {
                let mut offset_guard = offset.lock().await;
                *offset_guard = last.offset + 1;
            }

            // Convert to Python messages
            let result: Vec<Message> = messages
                .into_iter()
                .map(|m| Message::from_client_data(m, partition, topic.clone()))
                .collect();
            Ok(result)
        })
    }

    /// Commit the current offset to the consumer group
    ///
    /// Stores the current read position so consumption can resume after restart.
    /// Only works if consumer was created with a group.
    ///
    /// Raises:
    ///     RivvenError: If commit fails or no group is configured
    ///
    /// Example:
    ///     >>> messages = await consumer.fetch()
    ///     >>> for msg in messages:
    ///     ...     process(msg)
    ///     >>> await consumer.commit()  # Save position
    pub fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        let topic = self.topic.clone();
        let partition = self.partition;
        let group = self.group.clone();
        let offset = Arc::clone(&self.offset);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let group = group.ok_or_else(|| {
                RivvenError::invalid_config("Cannot commit without a consumer group")
            })?;

            let current_offset = {
                let guard = offset.lock().await;
                *guard
            };

            let mut guard = client.lock().await;
            guard
                .commit_offset(&group, &topic, partition, current_offset)
                .await
                .into_py_err()?;

            Ok(())
        })
    }

    /// Seek to a specific offset
    ///
    /// Args:
    ///     offset (int): The offset to seek to
    ///
    /// Example:
    ///     >>> await consumer.seek(0)  # Start from beginning
    ///     >>> await consumer.seek(100)  # Jump to offset 100
    pub fn seek<'py>(&self, py: Python<'py>, new_offset: u64) -> PyResult<Bound<'py, PyAny>> {
        let offset = Arc::clone(&self.offset);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = offset.lock().await;
            *guard = new_offset;
            Ok(())
        })
    }

    /// Seek to the beginning of the partition
    ///
    /// Example:
    ///     >>> await consumer.seek_to_beginning()
    pub fn seek_to_beginning<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        let topic = self.topic.clone();
        let partition = self.partition;
        let offset = Arc::clone(&self.offset);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let (earliest, _) = guard
                .get_offset_bounds(&topic, partition)
                .await
                .into_py_err()?;

            let mut offset_guard = offset.lock().await;
            *offset_guard = earliest;
            Ok(())
        })
    }

    /// Seek to the end of the partition (latest offset)
    ///
    /// Example:
    ///     >>> await consumer.seek_to_end()
    pub fn seek_to_end<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        let topic = self.topic.clone();
        let partition = self.partition;
        let offset = Arc::clone(&self.offset);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let (_, latest) = guard
                .get_offset_bounds(&topic, partition)
                .await
                .into_py_err()?;

            let mut offset_guard = offset.lock().await;
            *offset_guard = latest;
            Ok(())
        })
    }

    /// Get offset bounds for the partition
    ///
    /// Returns:
    ///     tuple[int, int]: (earliest_offset, latest_offset)
    ///
    /// Example:
    ///     >>> earliest, latest = await consumer.get_offset_bounds()
    ///     >>> print(f"Available offsets: {earliest} to {latest}")
    pub fn get_offset_bounds<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        let topic = self.topic.clone();
        let partition = self.partition;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = client.lock().await;
            let bounds = guard
                .get_offset_bounds(&topic, partition)
                .await
                .into_py_err()?;
            Ok(bounds)
        })
    }

    /// Enable async iteration over messages
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Get next message for async iteration
    ///
    /// Fetches `batch_size` messages at once into a prefetch buffer,
    /// then serves them one at a time. This amortizes the RPC overhead across
    /// many Python iteration steps instead of doing one RPC per message.
    ///
    /// When the broker returns zero messages, sleeps for a short
    /// poll interval and retries instead of raising StopAsyncIteration. This
    /// keeps `async for` loops alive for long-lived event processing, matching
    /// expected Kafka-style consumer behavior.
    ///
    /// Auto-commit fires once per batch refill (not per message),
    /// committing the highest consumed offset when the prefetch buffer is
    /// drained. This reduces commit RPCs from N to 1 per batch.
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let client = Arc::clone(&self.client);
        let topic = self.topic.clone();
        let partition = self.partition;
        let offset = Arc::clone(&self.offset);
        let group = self.group.clone();
        let auto_commit = self.auto_commit;
        let prefetch = Arc::clone(&self.prefetch);
        let batch_size = self.batch_size;
        let poll_interval = self.poll_interval;

        let fut = pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Fast path: serve from prefetch buffer (no commit — deferred to refill)
            {
                let mut buf = prefetch.lock().await;
                if let Some(msg_data) = buf.pop_front() {
                    let new_offset = msg_data.offset + 1;
                    {
                        let mut offset_guard = offset.lock().await;
                        *offset_guard = new_offset;
                    }
                    let msg = Message::from_client_data(msg_data, partition, topic);
                    return Ok(Some(msg));
                }
            }

            // Buffer drained — commit the highest consumed offset before refilling
            if auto_commit {
                if let Some(ref group) = group {
                    let current_offset = {
                        let guard = offset.lock().await;
                        *guard
                    };
                    if current_offset > 0 {
                        let mut guard = client.lock().await;
                        guard
                            .commit_offset(group, &topic, partition, current_offset)
                            .await
                            .into_py_err()?;
                    }
                }
            }

            // Poll with backoff until data arrives
            loop {
                let current_offset = {
                    let guard = offset.lock().await;
                    *guard
                };

                let messages = {
                    let mut guard = client.lock().await;
                    guard
                        .consume(&topic, partition, current_offset, batch_size)
                        .await
                        .into_py_err()?
                };

                if messages.is_empty() {
                    // No data yet — sleep and retry (keeps async for loop alive)
                    tokio::time::sleep(poll_interval).await;
                    continue;
                }

                // Fill the prefetch buffer with all but the first message
                let mut iter = messages.into_iter();
                let first = iter.next().unwrap();

                {
                    let mut buf = prefetch.lock().await;
                    buf.extend(iter);
                }

                // Return the first message
                let new_offset = first.offset + 1;
                {
                    let mut offset_guard = offset.lock().await;
                    *offset_guard = new_offset;
                }
                let msg = Message::from_client_data(first, partition, topic);
                return Ok(Some(msg));
            }
        })?;

        Ok(Some(fut))
    }

    fn __repr__(&self) -> String {
        match &self.group {
            Some(g) => format!(
                "Consumer(topic={:?}, partition={}, group={:?})",
                self.topic, self.partition, g
            ),
            None => format!(
                "Consumer(topic={:?}, partition={})",
                self.topic, self.partition
            ),
        }
    }
}
