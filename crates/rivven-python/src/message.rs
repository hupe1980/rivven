//! Message type for consumed records

use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// A message consumed from a Rivven topic
///
/// Contains the message payload, key (optional), offset, and timestamp.
///
/// Attributes:
///     value (bytes): The message payload
///     key (bytes | None): Optional message key for partitioning
///     offset (int): The message offset within the partition
///     timestamp (int): Unix timestamp in milliseconds when message was produced
///     partition (int): The partition this message was consumed from
///     topic (str): The topic this message was consumed from
///
/// Example:
///     >>> async for message in consumer:
///     ...     print(f"Offset {message.offset}: {message.value.decode()}")
///     ...     if message.key:
///     ...         print(f"Key: {message.key.decode()}")
#[pyclass]
#[derive(Clone)]
pub struct Message {
    /// Message value (payload) as raw bytes
    value_bytes: Vec<u8>,

    /// Optional message key as raw bytes
    key_bytes: Option<Vec<u8>>,

    /// Message offset in partition
    #[pyo3(get)]
    pub offset: u64,

    /// Timestamp in milliseconds since epoch
    #[pyo3(get)]
    pub timestamp: i64,

    /// Partition ID
    #[pyo3(get)]
    pub partition: u32,

    /// Topic name
    #[pyo3(get)]
    pub topic: String,
}

#[pymethods]
impl Message {
    /// Create a new Message (primarily for internal use)
    #[new]
    #[pyo3(signature = (value, offset, timestamp, partition, topic, key=None))]
    pub fn new(
        value: Vec<u8>,
        offset: u64,
        timestamp: i64,
        partition: u32,
        topic: String,
        key: Option<Vec<u8>>,
    ) -> Self {
        Self {
            value_bytes: value,
            key_bytes: key,
            offset,
            timestamp,
            partition,
            topic,
        }
    }

    /// Get the message value as bytes
    #[getter]
    pub fn value<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.value_bytes)
    }

    /// Get the message key as bytes (or None)
    #[getter]
    pub fn key<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.key_bytes.as_ref().map(|k| PyBytes::new(py, k))
    }

    /// Get value as UTF-8 string
    ///
    /// Returns:
    ///     str: The message value decoded as UTF-8
    ///
    /// Raises:
    ///     UnicodeDecodeError: If value is not valid UTF-8
    pub fn value_str(&self) -> PyResult<String> {
        String::from_utf8(self.value_bytes.clone())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyUnicodeDecodeError, _>(e.to_string()))
    }

    /// Get key as UTF-8 string
    ///
    /// Returns:
    ///     str | None: The message key decoded as UTF-8, or None if no key
    ///
    /// Raises:
    ///     UnicodeDecodeError: If key is not valid UTF-8
    pub fn key_str(&self) -> PyResult<Option<String>> {
        match &self.key_bytes {
            Some(key) => String::from_utf8(key.clone()).map(Some).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyUnicodeDecodeError, _>(e.to_string())
            }),
            None => Ok(None),
        }
    }

    /// Get the message size in bytes (value + key)
    ///
    /// Returns:
    ///     int: Total size of message payload
    pub fn size(&self) -> usize {
        let value_size = self.value_bytes.len();
        let key_size = self.key_bytes.as_ref().map(|k| k.len()).unwrap_or(0);
        value_size + key_size
    }

    fn __repr__(&self) -> String {
        format!(
            "Message(topic={:?}, partition={}, offset={}, timestamp={})",
            self.topic, self.partition, self.offset, self.timestamp
        )
    }

    fn __str__(&self) -> String {
        format!(
            "Message(topic={}, partition={}, offset={})",
            self.topic, self.partition, self.offset
        )
    }
}

impl Message {
    /// Create a Message from client MessageData
    pub fn from_client_data(
        data: rivven_client::MessageData,
        partition: u32,
        topic: String,
    ) -> Self {
        Self {
            value_bytes: data.value.to_vec(),
            key_bytes: data.key.map(|k| k.to_vec()),
            offset: data.offset,
            timestamp: data.timestamp,
            partition,
            topic,
        }
    }
}
