//! Test helpers and utilities

use anyhow::Result;
use std::future::Future;
use std::sync::Once;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tracing::debug;

static CRYPTO_PROVIDER_INIT: Once = Once::new();

/// Initialize the rustls crypto provider (call before any TLS operations)
pub fn init_crypto_provider() {
    CRYPTO_PROVIDER_INIT.call_once(|| {
        // Install aws-lc-rs as the default crypto provider for rustls
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// Initialize tracing for tests (call once at start of test)
pub fn init_tracing() {
    // Also initialize crypto provider when initializing tracing
    init_crypto_provider();

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("rivven=debug".parse().unwrap())
                .add_directive("rivvend=debug".parse().unwrap()),
        )
        .with_test_writer()
        .try_init();
}

/// Wait for a condition to become true with timeout
pub async fn wait_for<F, Fut>(
    condition: F,
    timeout_duration: Duration,
    poll_interval: Duration,
) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = std::time::Instant::now();

    while start.elapsed() < timeout_duration {
        if condition().await {
            return Ok(());
        }
        sleep(poll_interval).await;
    }

    anyhow::bail!("Condition not met within {:?}", timeout_duration)
}

/// Wait for a condition with default timeout (30s) and poll interval (100ms)
pub async fn wait_for_condition<F, Fut>(condition: F) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = bool>,
{
    wait_for(
        condition,
        Duration::from_secs(30),
        Duration::from_millis(100),
    )
    .await
}

/// Retry an operation with exponential backoff
pub async fn retry_with_backoff<F, Fut, T, E>(
    mut operation: F,
    max_retries: usize,
    initial_delay: Duration,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
{
    let mut delay = initial_delay;
    let mut last_error = None;

    for attempt in 0..max_retries {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                debug!(
                    "Attempt {} failed: {:?}, retrying in {:?}",
                    attempt + 1,
                    e,
                    delay
                );
                last_error = Some(e);
                sleep(delay).await;
                delay *= 2; // Exponential backoff
            }
        }
    }

    Err(last_error.unwrap())
}

/// Assert that a future completes within the given timeout
pub async fn assert_completes_within<F, Fut, T>(future: F, duration: Duration) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    timeout(duration, future())
        .await
        .unwrap_or_else(|_| panic!("Operation did not complete within {:?}", duration))
}

/// Measure execution time of an async operation
pub async fn measure_time<F, Fut, T>(operation: F) -> (T, Duration)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let start = std::time::Instant::now();
    let result = operation().await;
    (result, start.elapsed())
}

/// Generate a unique topic name for tests
pub fn unique_topic_name(prefix: &str) -> String {
    let uuid = uuid::Uuid::new_v4().to_string();
    format!("{}-{}", prefix, &uuid[..8])
}

/// Generate a unique consumer group name
pub fn unique_consumer_group(prefix: &str) -> String {
    let uuid = uuid::Uuid::new_v4().to_string();
    format!("{}-cg-{}", prefix, &uuid[..8])
}

/// TCP port checker
pub async fn is_port_open(host: &str, port: u16) -> bool {
    tokio::net::TcpStream::connect(format!("{}:{}", host, port))
        .await
        .is_ok()
}

/// Wait for a TCP port to become available
pub async fn wait_for_port(host: &str, port: u16, timeout_duration: Duration) -> Result<()> {
    wait_for(
        || async { is_port_open(host, port).await },
        timeout_duration,
        Duration::from_millis(100),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_wait_for_condition() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let result = wait_for(
            || {
                let c = counter_clone.clone();
                async move {
                    c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    c.load(std::sync::atomic::Ordering::SeqCst) >= 3
                }
            },
            Duration::from_secs(5),
            Duration::from_millis(10),
        )
        .await;

        assert!(result.is_ok());
        assert!(counter.load(std::sync::atomic::Ordering::SeqCst) >= 3);
    }

    #[test]
    fn test_unique_topic_name() {
        let name1 = unique_topic_name("test");
        let name2 = unique_topic_name("test");

        assert!(name1.starts_with("test-"));
        assert!(name2.starts_with("test-"));
        assert_ne!(name1, name2);
    }
}
