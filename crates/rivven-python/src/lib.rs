//! Python bindings for Rivven event streaming platform
//!
//! This crate provides Python bindings using PyO3 for the Rivven client library.
//!
//! # Example (Python)
//!
//! ```text
//! import asyncio
//! import rivven
//!
//! async def main():
//!     # Connect to broker
//!     client = await rivven.connect("localhost:9092")
//!
//!     # Create a topic
//!     await client.create_topic("events", partitions=3)
//!
//!     # Produce messages
//!     producer = client.producer("events")
//!     offset = await producer.send(b"Hello, World!")
//!     print(f"Produced at offset {offset}")
//!
//!     # Consume messages
//!     consumer = client.consumer("events", group="my-group")
//!     async for message in consumer:
//!         print(f"Received: {message.value}")
//!         await message.ack()
//!
//! asyncio.run(main())
//! ```

// Allow useless_conversion because PyO3 async requires ? for control flow even
// when the error type is already PyErr
#![allow(clippy::useless_conversion)]

mod client;
mod error;
mod message;
mod consumer;
mod producer;

use pyo3::prelude::*;

pub use client::RivvenClient;
pub use error::RivvenError;
pub use message::Message;
pub use consumer::Consumer;
pub use producer::Producer;

use error::IntoPyErr;

/// Connect to a Rivven broker
///
/// Args:
///     address: Broker address (e.g., "localhost:9092")
///
/// Returns:
///     RivvenClient: Connected client instance
///
/// Raises:
///     RivvenError: If connection fails
///
/// Example:
///     >>> client = await rivven.connect("localhost:9092")
#[pyfunction]
fn connect<'py>(py: Python<'py>, address: String) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let inner = rivven_client::Client::connect(&address)
            .await
            .into_py_err()?;
        Ok(RivvenClient::new(inner))
    })
}

/// Connect to a Rivven broker with TLS
///
/// Args:
///     address: Broker address (e.g., "localhost:9092")
///     ca_cert_path: Path to CA certificate file (PEM format)
///     server_name: Server hostname for certificate verification
///     client_cert_path: Optional client certificate for mTLS
///     client_key_path: Optional client private key for mTLS
///
/// Returns:
///     RivvenClient: Connected client instance with TLS
///
/// Raises:
///     RivvenError: If connection or TLS setup fails
///
/// Example:
///     >>> client = await rivven.connect_tls(
///     ...     "localhost:9093",
///     ...     ca_cert_path="/path/to/ca.pem",
///     ...     server_name="broker.example.com"
///     ... )
#[cfg(feature = "tls")]
#[pyfunction]
#[pyo3(signature = (address, ca_cert_path, server_name, client_cert_path=None, client_key_path=None))]
fn connect_tls<'py>(
    py: Python<'py>,
    address: String,
    ca_cert_path: String,
    server_name: String,
    client_cert_path: Option<String>,
    client_key_path: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        use rivven_client::TlsConfig;
        use rivven_core::tls::CertificateSource;

        let tls_config = match (client_cert_path, client_key_path) {
            (Some(cert), Some(key)) => TlsConfig::mtls_from_pem_files(cert, key, &ca_cert_path),
            _ => TlsConfig {
                enabled: true,
                root_ca: Some(CertificateSource::File { path: ca_cert_path.into() }),
                ..Default::default()
            },
        };

        let inner = rivven_client::Client::connect_tls(&address, &tls_config, &server_name)
            .await
            .into_py_err()?;
        Ok(RivvenClient::new(inner))
    })
}

/// Get the version of the rivven package
#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Python module definition
#[pymodule]
fn rivven(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Module metadata
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("__doc__", "Python bindings for Rivven event streaming platform")?;

    // Top-level functions
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;

    #[cfg(feature = "tls")]
    m.add_function(wrap_pyfunction!(connect_tls, m)?)?;

    // Classes
    m.add_class::<RivvenClient>()?;
    m.add_class::<Message>()?;
    m.add_class::<Consumer>()?;
    m.add_class::<Producer>()?;
    m.add_class::<RivvenError>()?;

    Ok(())
}
