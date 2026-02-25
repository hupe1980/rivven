//! Producer state for idempotent and transactional producing

use pyo3::prelude::*;
use std::sync::Arc;
use tokio::sync::Mutex;

/// State for an idempotent/transactional producer
///
/// This wraps the Rust ProducerState and is required for:
/// - Idempotent producing (exactly-once semantics)
/// - Transactional producing (atomic multi-partition writes)
///
/// Obtain a ProducerState via `client.init_producer_id()`.
///
/// Example:
///     >>> # Idempotent producing
///     >>> producer_state = await client.init_producer_id()
///     >>> offset, partition, was_dup = await client.publish_idempotent(
///     ...     "events", b"payload", producer_state
///     ... )
///
///     >>> # Transactional producing
///     >>> producer_state = await client.init_producer_id()
///     >>> await client.begin_transaction("my-txn", producer_state)
///     >>> await client.publish_idempotent("events", b"msg1", producer_state)
///     >>> await client.commit_transaction("my-txn", producer_state)
#[pyclass]
#[derive(Clone)]
pub struct ProducerState {
    /// Inner state wrapped in `Arc<Mutex>` for thread-safe mutation
    pub(crate) inner: Arc<Mutex<rivven_client::ProducerState>>,
}

impl ProducerState {
    /// Create a new ProducerState from the Rust client state
    pub fn new(state: rivven_client::ProducerState) -> Self {
        Self {
            inner: Arc::new(Mutex::new(state)),
        }
    }
}

#[pymethods]
impl ProducerState {
    /// Get the producer ID assigned by the broker
    #[getter]
    pub fn producer_id(&self) -> PyResult<u64> {
        let guard = self.inner.try_lock().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "ProducerState is currently locked by another operation",
            )
        })?;
        Ok(guard.producer_id)
    }

    /// Get the current producer epoch
    #[getter]
    pub fn producer_epoch(&self) -> PyResult<u16> {
        let guard = self.inner.try_lock().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "ProducerState is currently locked by another operation",
            )
        })?;
        Ok(guard.producer_epoch)
    }

    /// Get the next sequence number
    #[getter]
    pub fn next_sequence(&self) -> PyResult<i32> {
        let guard = self.inner.try_lock().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "ProducerState is currently locked by another operation",
            )
        })?;
        Ok(guard.next_sequence)
    }

    fn __repr__(&self) -> String {
        "ProducerState(...)".to_string()
    }
}
