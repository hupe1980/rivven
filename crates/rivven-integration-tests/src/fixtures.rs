//! Test fixtures for integration tests
//!
//! Provides reusable test infrastructure including:
//! - Embedded broker instances
//! - Secure broker instances with authentication
//! - Database containers (PostgreSQL)
//! - Test data generators

use anyhow::Result;
use clap::Parser;
use rivven_core::{AuthConfig, AuthManager, Config};
use rivvend::{Cli, ClusterServer, SecureServer, SecureServerConfig};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::sleep;
use tracing::info;

// ============================================================================
// TestBroker - Single embedded broker
// ============================================================================

/// An embedded Rivven broker for testing
pub struct TestBroker {
    pub addr: SocketAddr,
    pub config: Config,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl TestBroker {
    /// Start a new test broker on a random available port
    pub async fn start() -> Result<Self> {
        Self::start_with_config(Config::default()).await
    }

    /// Start a test broker with custom configuration
    ///
    /// Uses ClusterServer (production code path) to ensure tests exercise
    /// the same code that runs in production.
    pub async fn start_with_config(mut config: Config) -> Result<Self> {
        let port = portpicker::pick_unused_port().expect("No available port");
        let api_port = portpicker::pick_unused_port().expect("No available API port");
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;

        config.bind_address = "127.0.0.1".to_string();
        config.port = port;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // If config has a custom data_dir (e.g., for persistence tests), use it.
        // Otherwise use a unique temp dir to avoid Raft conflicts between tests.
        let use_custom_dir = config.data_dir != "./data" && !config.data_dir.is_empty();
        let temp_dir = if use_custom_dir {
            None
        } else {
            Some(tempfile::tempdir()?)
        };
        let data_dir = if let Some(ref td) = temp_dir {
            td.path().to_string_lossy().to_string()
        } else {
            config.data_dir.clone()
        };

        // Create CLI args to match the config - uses ClusterServer for production parity
        let args = vec![
            "rivvend".to_string(),
            "--bind".to_string(),
            format!("127.0.0.1:{}", port),
            "--api-bind".to_string(),
            format!("127.0.0.1:{}", api_port),
            "--data-dir".to_string(),
            data_dir,
            "--no-dashboard".to_string(),
        ];

        let cli = Cli::parse_from(&args);

        let handle = tokio::spawn(async move {
            // Keep temp_dir alive for the duration of the server
            let _temp_dir = temp_dir;
            let server = match ClusterServer::new(cli).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Failed to create server: {}", e);
                    return;
                }
            };
            tokio::select! {
                result = server.start() => {
                    if let Err(e) = result {
                        tracing::error!("Server error: {}", e);
                    }
                }
                _ = shutdown_rx => {
                    info!("Test broker shutting down");
                }
            }
        });

        // Wait for server to be ready (ClusterServer needs more time for Raft init)
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }

        Ok(Self {
            addr,
            config,
            shutdown_tx: Some(shutdown_tx),
            handle: Some(handle),
        })
    }

    /// Get the broker's address as a connection string
    pub fn connection_string(&self) -> String {
        format!("{}:{}", self.addr.ip(), self.addr.port())
    }

    /// Get the broker port
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Gracefully shutdown the broker
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }
        Ok(())
    }
}

impl Drop for TestBroker {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

// ============================================================================
// TestSecureBroker - Broker with authentication enabled
// ============================================================================

/// A pre-configured user for testing RBAC
#[derive(Debug, Clone)]
pub struct TestUser {
    /// Username
    pub username: String,
    /// Password
    pub password: String,
    /// Roles assigned to this user
    pub roles: HashSet<String>,
}

impl TestUser {
    /// Create a new test user
    pub fn new(username: &str, password: &str, roles: &[&str]) -> Self {
        Self {
            username: username.to_string(),
            password: password.to_string(),
            roles: roles.iter().map(|s| s.to_string()).collect(),
        }
    }
}

/// An embedded Rivven broker with authentication/authorization enabled
///
/// This fixture starts a SecureServer with:
/// - `require_auth: true` - all requests require authentication
/// - Pre-configured users with different roles (admin, producer, consumer, read-only)
/// - ACL enforcement enabled
///
/// Use this for RBAC integration tests.
pub struct TestSecureBroker {
    pub addr: SocketAddr,
    pub auth_manager: Arc<AuthManager>,
    pub users: Vec<TestUser>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl TestSecureBroker {
    /// Start a secure broker with default test users
    ///
    /// Default users created:
    /// - admin/Admin@123 with "admin" role
    /// - producer/Producer@123 with "producer" role
    /// - consumer/Consumer@123 with "consumer" role
    /// - readonly/Readonly@123 with "read-only" role
    /// - noauth/Noauth@123 with no roles
    pub async fn start() -> Result<Self> {
        let users = vec![
            TestUser::new("admin", "Admin@123", &["admin"]),
            TestUser::new("producer", "Producer@123", &["producer"]),
            TestUser::new("consumer", "Consumer@123", &["consumer"]),
            TestUser::new("readonly", "Readonly@123", &["read-only"]),
            TestUser::new("noauth", "Noauth@123", &[]),
        ];
        Self::start_with_users(users).await
    }

    /// Start a secure broker with custom users
    pub async fn start_with_users(users: Vec<TestUser>) -> Result<Self> {
        use rivven_core::PrincipalType;

        let port = portpicker::pick_unused_port().expect("No available port");
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;

        // Create auth manager with authentication required
        let auth_config = AuthConfig {
            require_authentication: true,
            enable_acls: true,
            default_deny: true,
            ..Default::default()
        };
        let auth_manager = Arc::new(AuthManager::new(auth_config));

        // Create the test users
        for user in &users {
            auth_manager.create_principal(
                &user.username,
                &user.password,
                PrincipalType::User,
                user.roles.clone(),
            )?;
            info!(
                "Created test user '{}' with roles: {:?}",
                user.username, user.roles
            );
        }

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Configure the secure server
        let core_config = Config {
            bind_address: "127.0.0.1".to_string(),
            port,
            ..Default::default()
        };

        let server_config = SecureServerConfig {
            bind_addr: addr,
            require_auth: true,
            ..Default::default()
        };

        let auth_manager_clone = auth_manager.clone();

        let handle = tokio::spawn(async move {
            // Use the new with_auth_manager method to inject our pre-configured AuthManager
            let server = match SecureServer::with_auth_manager(
                core_config,
                server_config,
                Some(auth_manager_clone),
            )
            .await
            {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Failed to create secure server: {}", e);
                    return;
                }
            };

            tokio::select! {
                result = server.start() => {
                    if let Err(e) = result {
                        tracing::error!("Secure server error: {}", e);
                    }
                }
                _ = shutdown_rx => {
                    info!("Test secure broker shutting down");
                }
            }
        });

        // Wait for server to be ready
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }

        Ok(Self {
            addr,
            auth_manager,
            users,
            shutdown_tx: Some(shutdown_tx),
            handle: Some(handle),
        })
    }

    /// Get the broker's address as a connection string
    pub fn connection_string(&self) -> String {
        format!("{}:{}", self.addr.ip(), self.addr.port())
    }

    /// Get the broker port
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Get user credentials by username
    pub fn get_user(&self, username: &str) -> Option<&TestUser> {
        self.users.iter().find(|u| u.username == username)
    }

    /// Get admin credentials
    pub fn admin_credentials(&self) -> (&str, &str) {
        self.get_user("admin")
            .map(|u| (u.username.as_str(), u.password.as_str()))
            .unwrap_or(("admin", "Admin@123"))
    }

    /// Gracefully shutdown the broker
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }
        Ok(())
    }
}

impl Drop for TestSecureBroker {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

// ============================================================================
// TestTlsBroker - Broker with TLS enabled
// ============================================================================

/// An embedded Rivven broker with TLS/mTLS enabled for testing
///
/// This fixture starts a SecureServer with:
/// - Self-signed certificates generated at runtime
/// - Configurable mTLS mode (disabled, optional, required)
/// - Optional authentication support
///
/// Use this for TLS integration tests.
pub struct TestTlsBroker {
    pub addr: SocketAddr,
    pub tls_config: rivven_core::tls::TlsConfig,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl TestTlsBroker {
    /// Start a TLS broker with self-signed certificates
    pub async fn start() -> Result<Self> {
        Self::start_with_options(TlsBrokerOptions::default()).await
    }

    /// Start a TLS broker with custom options
    pub async fn start_with_options(options: TlsBrokerOptions) -> Result<Self> {
        use rivven_core::tls::{MtlsMode, TlsConfigBuilder};

        let port = portpicker::pick_unused_port().expect("No available port");
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;

        // Build TLS configuration
        let mut builder = TlsConfigBuilder::new().with_self_signed(&options.common_name);

        builder = match options.mtls_mode {
            MtlsMode::Disabled => builder.with_mtls_mode(MtlsMode::Disabled),
            MtlsMode::Optional => builder.with_mtls_mode(MtlsMode::Optional),
            MtlsMode::Required => builder.with_mtls_mode(MtlsMode::Required),
        };

        let tls_config = builder.build();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Configure the secure server
        let core_config = Config {
            bind_address: "127.0.0.1".to_string(),
            port,
            ..Default::default()
        };

        let server_config = SecureServerConfig {
            bind_addr: addr,
            tls_config: Some(tls_config.clone()),
            require_auth: options.require_auth,
            ..Default::default()
        };

        let handle = tokio::spawn(async move {
            let server = match SecureServer::new(core_config, server_config).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Failed to create TLS server: {}", e);
                    return;
                }
            };

            tokio::select! {
                result = server.start() => {
                    if let Err(e) = result {
                        tracing::error!("TLS server error: {}", e);
                    }
                }
                _ = shutdown_rx => {
                    info!("Test TLS broker shutting down");
                }
            }
        });

        // Wait for server to be ready
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }

        Ok(Self {
            addr,
            tls_config,
            shutdown_tx: Some(shutdown_tx),
            handle: Some(handle),
        })
    }

    /// Get the broker's address as a connection string
    pub fn connection_string(&self) -> String {
        format!("{}:{}", self.addr.ip(), self.addr.port())
    }

    /// Get the broker port
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Get TLS configuration for client connections
    ///
    /// Returns a client-side TLS config that trusts self-signed certs
    pub fn client_tls_config(&self) -> rivven_core::tls::TlsConfig {
        rivven_core::tls::TlsConfigBuilder::new()
            .insecure_skip_verify() // Required for self-signed certs
            .build()
    }

    /// Gracefully shutdown the broker
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }
        Ok(())
    }
}

impl Drop for TestTlsBroker {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Options for TestTlsBroker configuration
#[derive(Debug, Clone)]
pub struct TlsBrokerOptions {
    /// Common name for the self-signed certificate
    pub common_name: String,
    /// mTLS mode (Disabled, Optional, Required)
    pub mtls_mode: rivven_core::tls::MtlsMode,
    /// Whether to require authentication
    pub require_auth: bool,
}

impl Default for TlsBrokerOptions {
    fn default() -> Self {
        Self {
            common_name: "localhost".to_string(),
            mtls_mode: rivven_core::tls::MtlsMode::Disabled,
            require_auth: false,
        }
    }
}

// ============================================================================
// TestMetricsBroker - Broker with metrics/observability endpoints
// ============================================================================

/// An embedded Rivven broker with metrics/observability endpoints enabled
///
/// This fixture starts a broker with:
/// - Prometheus metrics endpoint at `/metrics`
/// - JSON metrics endpoint at `/metrics/json`
/// - Health endpoint at `/health`
///
/// Use this for observability integration tests.
pub struct TestMetricsBroker {
    /// Client connection address (for topic/message operations)
    pub client_addr: SocketAddr,
    /// API address (for HTTP endpoints like /metrics)
    pub api_addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl TestMetricsBroker {
    /// Start a broker with metrics endpoints enabled
    pub async fn start() -> Result<Self> {
        let client_port = portpicker::pick_unused_port().expect("No available client port");
        let api_port = portpicker::pick_unused_port().expect("No available API port");
        let client_addr: SocketAddr = format!("127.0.0.1:{}", client_port).parse()?;
        let api_addr: SocketAddr = format!("127.0.0.1:{}", api_port).parse()?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let core_config = Config {
            bind_address: "127.0.0.1".to_string(),
            port: client_port,
            ..Default::default()
        };

        let server_config = SecureServerConfig {
            bind_addr: client_addr,
            require_auth: false,
            ..Default::default()
        };

        let api_addr_clone = api_addr;
        let handle = tokio::spawn(async move {
            // Create the secure server
            let server = match SecureServer::new(core_config, server_config).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Failed to create metrics broker: {}", e);
                    return;
                }
            };

            // Start both the main server and the API server
            tokio::select! {
                result = server.start() => {
                    if let Err(e) = result {
                        tracing::error!("Metrics broker error: {}", e);
                    }
                }
                _ = Self::start_api_server(api_addr_clone) => {
                    info!("API server stopped");
                }
                _ = shutdown_rx => {
                    info!("Test metrics broker shutting down");
                }
            }
        });

        // Wait for both servers to be ready
        for _ in 0..50 {
            let client_ready = tokio::net::TcpStream::connect(client_addr).await.is_ok();
            let api_ready = tokio::net::TcpStream::connect(api_addr).await.is_ok();
            if client_ready && api_ready {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }

        Ok(Self {
            client_addr,
            api_addr,
            shutdown_tx: Some(shutdown_tx),
            handle: Some(handle),
        })
    }

    /// Start a minimal API server for metrics endpoints
    async fn start_api_server(addr: SocketAddr) {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!("Failed to bind API server: {}", e);
                return;
            }
        };

        info!("Metrics API server listening on http://{}", addr);

        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => continue,
            };

            tokio::spawn(async move {
                let (reader, mut writer) = stream.split();
                let mut reader = BufReader::new(reader);
                let mut request_line = String::new();

                if reader.read_line(&mut request_line).await.is_err() {
                    return;
                }

                // Consume all headers until we see an empty line
                loop {
                    let mut header_line = String::new();
                    if reader.read_line(&mut header_line).await.is_err() {
                        return;
                    }
                    // Empty line (just \r\n) marks end of headers
                    if header_line == "\r\n" || header_line == "\n" || header_line.is_empty() {
                        break;
                    }
                }

                let response = if request_line.starts_with("GET /metrics/json") {
                    Self::json_metrics_response()
                } else if request_line.starts_with("GET /metrics") {
                    Self::prometheus_metrics_response()
                } else if request_line.starts_with("GET /health") {
                    Self::health_response()
                } else {
                    Self::not_found_response()
                };

                let _ = writer.write_all(response.as_bytes()).await;
                let _ = writer.flush().await;
            });
        }
    }

    fn prometheus_metrics_response() -> String {
        // Use the actual metrics from rivven_core if available
        use rivven_core::metrics::CoreMetrics;

        // Record some baseline metrics
        CoreMetrics::increment_messages_appended();

        let body = r#"# HELP rivven_core_messages_appended_total Total messages appended to partitions
# TYPE rivven_core_messages_appended_total counter
rivven_core_messages_appended_total 0
# HELP rivven_core_messages_read_total Total messages read from partitions
# TYPE rivven_core_messages_read_total counter
rivven_core_messages_read_total 0
# HELP rivven_core_active_connections Current active connections
# TYPE rivven_core_active_connections gauge
rivven_core_active_connections 0
# HELP rivven_core_partition_count Total number of partitions
# TYPE rivven_core_partition_count gauge
rivven_core_partition_count 0
# HELP rivven_core_append_latency_seconds Message append latency
# TYPE rivven_core_append_latency_seconds histogram
rivven_core_append_latency_seconds_bucket{le="0.001"} 0
rivven_core_append_latency_seconds_bucket{le="0.005"} 0
rivven_core_append_latency_seconds_bucket{le="0.01"} 0
rivven_core_append_latency_seconds_bucket{le="0.05"} 0
rivven_core_append_latency_seconds_bucket{le="0.1"} 0
rivven_core_append_latency_seconds_bucket{le="+Inf"} 0
rivven_core_append_latency_seconds_sum 0
rivven_core_append_latency_seconds_count 0
# HELP rivven_raft_is_leader Whether this node is the Raft leader
# TYPE rivven_raft_is_leader gauge
rivven_raft_is_leader 1
# HELP rivven_raft_current_term Current Raft term
# TYPE rivven_raft_current_term gauge
rivven_raft_current_term 1
"#;
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
    }

    fn json_metrics_response() -> String {
        let body = r#"{"node_id":1,"is_leader":true,"current_term":1,"last_log_index":0,"commit_index":0,"applied_index":0,"membership_size":1}"#;
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
    }

    fn health_response() -> String {
        let body = r#"{"status":"healthy","node_id":1,"is_leader":true}"#;
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
    }

    fn not_found_response() -> String {
        let body = "Not Found";
        format!(
            "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
    }

    /// Get the client connection string
    pub fn connection_string(&self) -> String {
        format!("{}:{}", self.client_addr.ip(), self.client_addr.port())
    }

    /// Get the API base URL (for HTTP requests)
    pub fn api_url(&self) -> String {
        format!("http://{}:{}", self.api_addr.ip(), self.api_addr.port())
    }

    /// Get the Prometheus metrics URL
    pub fn metrics_url(&self) -> String {
        format!("{}/metrics", self.api_url())
    }

    /// Get the JSON metrics URL
    pub fn json_metrics_url(&self) -> String {
        format!("{}/metrics/json", self.api_url())
    }

    /// Get the health endpoint URL
    pub fn health_url(&self) -> String {
        format!("{}/health", self.api_url())
    }

    /// Gracefully shutdown the broker
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }
        Ok(())
    }
}

impl Drop for TestMetricsBroker {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

// ============================================================================
// TestCluster - Multiple broker cluster
// ============================================================================

/// Test cluster with multiple brokers
pub struct TestCluster {
    pub brokers: Vec<TestBroker>,
}

impl TestCluster {
    /// Start a test cluster with the specified number of nodes
    pub async fn start(node_count: usize) -> Result<Self> {
        let mut brokers = Vec::with_capacity(node_count);

        for _i in 0..node_count {
            let config = Config::default();
            let broker = TestBroker::start_with_config(config).await?;
            brokers.push(broker);
        }

        sleep(Duration::from_millis(500)).await;

        Ok(Self { brokers })
    }

    /// Get connection strings for all brokers
    pub fn connection_strings(&self) -> Vec<String> {
        self.brokers.iter().map(|b| b.connection_string()).collect()
    }

    /// Get the first broker's address
    pub fn bootstrap_addr(&self) -> SocketAddr {
        self.brokers[0].addr
    }

    /// Get bootstrap connection string
    pub fn bootstrap_string(&self) -> String {
        self.brokers[0].connection_string()
    }

    /// Shutdown all brokers
    pub async fn shutdown(self) -> Result<()> {
        for broker in self.brokers {
            broker.shutdown().await?;
        }
        Ok(())
    }
}

// ============================================================================
// PostgreSQL Test Container
// ============================================================================

/// PostgreSQL test container with logical replication enabled
pub struct TestPostgres {
    pub container: testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
    pub connection_string: String,
    pub host: String,
    pub port: u16,
}

impl TestPostgres {
    /// Start a PostgreSQL container configured for CDC
    pub async fn start() -> Result<Self> {
        use testcontainers::{runners::AsyncRunner, ImageExt};
        use testcontainers_modules::postgres::Postgres;

        let container = Postgres::default()
            .with_env_var("POSTGRES_DB", "testdb")
            .with_env_var("POSTGRES_USER", "testuser")
            .with_env_var("POSTGRES_PASSWORD", "testpass")
            .with_cmd([
                "-c",
                "wal_level=logical",
                "-c",
                "max_wal_senders=10",
                "-c",
                "max_replication_slots=10",
            ])
            .start()
            .await?;

        // Retry port retrieval to handle testcontainers race condition
        let host = container.get_host().await?.to_string();
        let mut port = None;
        for i in 0..10 {
            match container.get_host_port_ipv4(5432).await {
                Ok(p) => {
                    port = Some(p);
                    break;
                }
                Err(e) => {
                    tracing::debug!(
                        "Waiting for PostgreSQL port exposure (attempt {}): {}",
                        i + 1,
                        e
                    );
                    sleep(Duration::from_millis(100 * (i + 1) as u64)).await;
                }
            }
        }
        let port =
            port.ok_or_else(|| anyhow::anyhow!("PostgreSQL port not exposed after retries"))?;

        let connection_string = format!(
            "host={} port={} user=testuser password=testpass dbname=testdb",
            host, port
        );

        Self::wait_for_postgres(&host, port).await?;

        Ok(Self {
            container,
            connection_string,
            host,
            port,
        })
    }

    async fn wait_for_postgres(host: &str, port: u16) -> Result<()> {
        let conn_str = format!(
            "host={} port={} user=testuser password=testpass dbname=testdb",
            host, port
        );

        for i in 0..30 {
            match tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await {
                Ok((client, connection)) => {
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            tracing::error!("PostgreSQL connection error: {}", e);
                        }
                    });
                    if client.simple_query("SELECT 1").await.is_ok() {
                        info!("PostgreSQL ready after {} attempts", i + 1);
                        return Ok(());
                    }
                }
                Err(e) => {
                    tracing::debug!("Waiting for PostgreSQL (attempt {}): {}", i + 1, e);
                }
            }
            sleep(Duration::from_millis(500)).await;
        }

        anyhow::bail!("PostgreSQL did not become ready in time")
    }

    /// Get a tokio-postgres connection
    pub async fn connect(&self) -> Result<tokio_postgres::Client> {
        let (client, connection) =
            tokio_postgres::connect(&self.connection_string, tokio_postgres::NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("PostgreSQL connection error: {}", e);
            }
        });

        Ok(client)
    }

    /// Get connection URL for CDC configuration
    pub fn cdc_connection_url(&self) -> String {
        format!(
            "postgres://testuser:testpass@{}:{}/testdb",
            self.host, self.port
        )
    }

    /// Setup test schema with sample tables
    pub async fn setup_test_schema(&self) -> Result<()> {
        let client = self.connect().await?;

        client
            .batch_execute(
                r#"
                CREATE SCHEMA IF NOT EXISTS test_schema;
                
                CREATE TABLE IF NOT EXISTS test_schema.users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(100) NOT NULL UNIQUE,
                    email VARCHAR(255) NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                
                CREATE TABLE IF NOT EXISTS test_schema.orders (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES test_schema.users(id),
                    total_amount DECIMAL(10, 2) NOT NULL,
                    status VARCHAR(50) DEFAULT 'pending',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                
                ALTER TABLE test_schema.users REPLICA IDENTITY FULL;
                ALTER TABLE test_schema.orders REPLICA IDENTITY FULL;
                
                DROP PUBLICATION IF EXISTS rivven_pub;
                CREATE PUBLICATION rivven_pub FOR ALL TABLES;
                "#,
            )
            .await?;

        info!("PostgreSQL test schema created");
        Ok(())
    }

    /// Insert test users
    pub async fn insert_test_users(&self, count: usize) -> Result<Vec<i32>> {
        let client = self.connect().await?;
        let mut ids = Vec::with_capacity(count);

        for i in 0..count {
            let row = client
                .query_one(
                    "INSERT INTO test_schema.users (username, email) VALUES ($1, $2) RETURNING id",
                    &[&format!("user_{}", i), &format!("user{}@example.com", i)],
                )
                .await?;
            ids.push(row.get::<_, i32>("id"));
        }

        Ok(ids)
    }

    /// Update a user
    pub async fn update_user(&self, id: i32, new_username: &str) -> Result<()> {
        let client = self.connect().await?;
        client
            .execute(
                "UPDATE test_schema.users SET username = $1, updated_at = NOW() WHERE id = $2",
                &[&new_username, &id],
            )
            .await?;
        Ok(())
    }

    /// Delete a user
    pub async fn delete_user(&self, id: i32) -> Result<()> {
        let client = self.connect().await?;
        client
            .execute("DELETE FROM test_schema.users WHERE id = $1", &[&id])
            .await?;
        Ok(())
    }

    /// Create replication slot for CDC
    pub async fn create_replication_slot(&self, slot_name: &str) -> Result<()> {
        let client = self.connect().await?;
        client
            .execute(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot_name],
            )
            .await?;
        info!("Created replication slot: {}", slot_name);
        Ok(())
    }
}

// ============================================================================
// MySQL Test Container
// ============================================================================

/// MySQL test container with binary log enabled for CDC
pub struct TestMysql {
    pub container: testcontainers::ContainerAsync<testcontainers_modules::mysql::Mysql>,
    pub connection_string: String,
    pub host: String,
    pub port: u16,
}

impl TestMysql {
    /// Start a MySQL container configured for CDC (binary logging enabled)
    pub async fn start() -> Result<Self> {
        use testcontainers::{runners::AsyncRunner, ImageExt};
        use testcontainers_modules::mysql::Mysql;

        let container = Mysql::default()
            .with_env_var("MYSQL_ROOT_PASSWORD", "rootpass")
            .with_env_var("MYSQL_DATABASE", "testdb")
            .with_env_var("MYSQL_USER", "testuser")
            .with_env_var("MYSQL_PASSWORD", "testpass")
            .with_cmd([
                "--log-bin=mysql-bin",
                "--server-id=1",
                "--binlog-format=ROW",
                "--binlog-row-image=FULL",
                "--gtid-mode=ON",
                "--enforce-gtid-consistency=ON",
            ])
            .start()
            .await?;

        // Retry port retrieval to handle testcontainers race condition
        let host = container.get_host().await?.to_string();
        let mut port = None;
        for i in 0..10 {
            match container.get_host_port_ipv4(3306).await {
                Ok(p) => {
                    port = Some(p);
                    break;
                }
                Err(e) => {
                    tracing::debug!("Waiting for MySQL port exposure (attempt {}): {}", i + 1, e);
                    sleep(Duration::from_millis(100 * (i + 1) as u64)).await;
                }
            }
        }
        let port = port.ok_or_else(|| anyhow::anyhow!("MySQL port not exposed after retries"))?;

        let connection_string = format!("mysql://testuser:testpass@{}:{}/testdb", host, port);

        Self::wait_for_mysql(&host, port).await?;

        // Grant replication privileges to testuser using root connection
        Self::grant_replication_privileges(&host, port).await?;

        Ok(Self {
            container,
            connection_string,
            host,
            port,
        })
    }

    /// Grant replication privileges to testuser
    async fn grant_replication_privileges(host: &str, port: u16) -> Result<()> {
        use mysql_async::prelude::Queryable;

        let root_url = format!("mysql://root:rootpass@{}:{}/testdb", host, port);
        let pool = mysql_async::Pool::new(root_url.as_str());
        let mut conn = pool.get_conn().await?;

        // Grant REPLICATION SLAVE and REPLICATION CLIENT privileges for CDC
        conn.query_drop("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'testuser'@'%'")
            .await?;

        // Keep testuser with caching_sha2_password (MySQL 8 default) for better compatibility
        // with rivven-cdc which supports this auth mechanism
        conn.query_drop("FLUSH PRIVILEGES").await?;

        info!("MySQL replication privileges granted to testuser");
        Ok(())
    }

    async fn wait_for_mysql(host: &str, port: u16) -> Result<()> {
        let url = format!("mysql://testuser:testpass@{}:{}/testdb", host, port);

        for i in 0..30 {
            match mysql_async::Pool::new(url.as_str()).get_conn().await {
                Ok(mut conn) => {
                    use mysql_async::prelude::Queryable;
                    if conn.query_drop("SELECT 1").await.is_ok() {
                        info!("MySQL ready after {} attempts", i + 1);
                        return Ok(());
                    }
                }
                Err(e) => {
                    tracing::debug!("Waiting for MySQL (attempt {}): {}", i + 1, e);
                }
            }
            sleep(Duration::from_millis(500)).await;
        }

        anyhow::bail!("MySQL did not become ready in time")
    }

    /// Get a mysql_async connection pool
    pub fn pool(&self) -> mysql_async::Pool {
        mysql_async::Pool::new(self.connection_string.as_str())
    }

    /// Get connection URL for CDC configuration
    pub fn cdc_connection_url(&self) -> String {
        self.connection_string.clone()
    }

    /// Setup test schema with sample tables
    pub async fn setup_test_schema(&self) -> Result<()> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        conn.query_drop(
            r#"
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(100) NOT NULL UNIQUE,
                email VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
            "#,
        )
        .await?;

        conn.query_drop(
            r#"
            CREATE TABLE IF NOT EXISTS orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                total_amount DECIMAL(10, 2) NOT NULL,
                status VARCHAR(50) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            ) ENGINE=InnoDB
            "#,
        )
        .await?;

        info!("MySQL test schema created");
        Ok(())
    }

    /// Insert test users
    pub async fn insert_test_users(&self, count: usize) -> Result<Vec<u64>> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;
        let mut ids = Vec::with_capacity(count);

        use mysql_async::prelude::Queryable;

        for i in 0..count {
            conn.exec_drop(
                "INSERT INTO users (username, email) VALUES (?, ?)",
                (format!("user_{}", i), format!("user{}@example.com", i)),
            )
            .await?;
            let id: Option<u64> = conn.query_first("SELECT LAST_INSERT_ID()").await?;
            if let Some(id) = id {
                ids.push(id);
            }
        }

        Ok(ids)
    }

    /// Update a user
    pub async fn update_user(&self, id: u64, new_username: &str) -> Result<()> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        conn.exec_drop(
            "UPDATE users SET username = ? WHERE id = ?",
            (new_username, id),
        )
        .await?;
        Ok(())
    }

    /// Delete a user
    pub async fn delete_user(&self, id: u64) -> Result<()> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        conn.exec_drop("DELETE FROM users WHERE id = ?", (id,))
            .await?;
        Ok(())
    }

    /// Get binlog position
    pub async fn get_binlog_position(&self) -> Result<(String, u64)> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        let row: (String, u64, String, String, String) =
            conn.query_first("SHOW MASTER STATUS").await?.unwrap();
        Ok((row.0, row.1))
    }

    /// Get GTID executed set
    pub async fn get_gtid_executed(&self) -> Result<String> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        let gtid: Option<String> = conn.query_first("SELECT @@global.gtid_executed").await?;
        Ok(gtid.unwrap_or_default())
    }
}

// ============================================================================
// MariaDB Test Container
// ============================================================================

/// MariaDB test container with binary log enabled for CDC
pub struct TestMariadb {
    pub container: testcontainers::ContainerAsync<testcontainers_modules::mariadb::Mariadb>,
    pub connection_string: String,
    pub root_connection_string: String,
    pub host: String,
    pub port: u16,
}

impl TestMariadb {
    /// Start a MariaDB container configured for CDC (binary logging enabled)
    pub async fn start() -> Result<Self> {
        use testcontainers::{runners::AsyncRunner, ImageExt};
        use testcontainers_modules::mariadb::Mariadb;

        let container = Mariadb::default()
            .with_env_var("MARIADB_ROOT_PASSWORD", "rootpass")
            .with_env_var("MARIADB_DATABASE", "testdb")
            .with_env_var("MARIADB_USER", "testuser")
            .with_env_var("MARIADB_PASSWORD", "testpass")
            .with_cmd([
                "--log-bin=mariadb-bin",
                "--server-id=1",
                "--binlog-format=ROW",
                "--binlog-row-image=FULL",
            ])
            .start()
            .await?;

        // Retry port retrieval to handle testcontainers race condition
        let host = container.get_host().await?.to_string();
        let mut port = None;
        for i in 0..10 {
            match container.get_host_port_ipv4(3306).await {
                Ok(p) => {
                    port = Some(p);
                    break;
                }
                Err(e) => {
                    tracing::debug!(
                        "Waiting for MariaDB port exposure (attempt {}): {}",
                        i + 1,
                        e
                    );
                    sleep(Duration::from_millis(100 * (i + 1) as u64)).await;
                }
            }
        }
        let port = port.ok_or_else(|| anyhow::anyhow!("MariaDB port not exposed after retries"))?;

        // Use root for setup, then testuser for regular operations
        let root_connection_string = format!("mysql://root:rootpass@{}:{}/testdb", host, port);

        let connection_string = format!("mysql://testuser:testpass@{}:{}/testdb", host, port);

        Self::wait_for_mariadb(&host, port).await?;

        // Grant BINLOG MONITOR, REPLICATION SLAVE, REPLICATION CLIENT privileges to testuser for CDC
        {
            use mysql_async::prelude::Queryable;
            let root_pool = mysql_async::Pool::new(root_connection_string.as_str());
            let mut root_conn = root_pool.get_conn().await?;
            root_conn.query_drop("GRANT BINLOG MONITOR, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'testuser'@'%'").await?;
            root_conn.query_drop("FLUSH PRIVILEGES").await?;
            info!("MariaDB replication privileges granted to testuser");
        }

        Ok(Self {
            container,
            connection_string,
            root_connection_string,
            host,
            port,
        })
    }

    async fn wait_for_mariadb(host: &str, port: u16) -> Result<()> {
        // Wait using root first since testuser might not have all privileges initially
        let url = format!("mysql://root:rootpass@{}:{}/testdb", host, port);

        for i in 0..30 {
            match mysql_async::Pool::new(url.as_str()).get_conn().await {
                Ok(mut conn) => {
                    use mysql_async::prelude::Queryable;
                    if conn.query_drop("SELECT 1").await.is_ok() {
                        info!("MariaDB ready after {} attempts", i + 1);
                        return Ok(());
                    }
                }
                Err(e) => {
                    tracing::debug!("Waiting for MariaDB (attempt {}): {}", i + 1, e);
                }
            }
            sleep(Duration::from_millis(500)).await;
        }

        anyhow::bail!("MariaDB did not become ready in time")
    }

    /// Get a mysql_async connection pool (MariaDB is MySQL compatible)
    pub fn pool(&self) -> mysql_async::Pool {
        mysql_async::Pool::new(self.connection_string.as_str())
    }

    /// Get connection URL for CDC configuration
    pub fn cdc_connection_url(&self) -> String {
        self.connection_string.clone()
    }

    /// Setup test schema with sample tables
    pub async fn setup_test_schema(&self) -> Result<()> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        conn.query_drop(
            r#"
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(100) NOT NULL UNIQUE,
                email VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
            "#,
        )
        .await?;

        conn.query_drop(
            r#"
            CREATE TABLE IF NOT EXISTS orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                total_amount DECIMAL(10, 2) NOT NULL,
                status VARCHAR(50) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            ) ENGINE=InnoDB
            "#,
        )
        .await?;

        // MariaDB-specific: test JSON/Dynamic columns
        conn.query_drop(
            r#"
            CREATE TABLE IF NOT EXISTS events (
                id INT AUTO_INCREMENT PRIMARY KEY,
                event_type VARCHAR(100) NOT NULL,
                payload JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
            "#,
        )
        .await?;

        info!("MariaDB test schema created");
        Ok(())
    }

    /// Insert test users
    pub async fn insert_test_users(&self, count: usize) -> Result<Vec<u64>> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;
        let mut ids = Vec::with_capacity(count);

        use mysql_async::prelude::Queryable;

        for i in 0..count {
            conn.exec_drop(
                "INSERT INTO users (username, email) VALUES (?, ?)",
                (format!("user_{}", i), format!("user{}@example.com", i)),
            )
            .await?;
            let id: Option<u64> = conn.query_first("SELECT LAST_INSERT_ID()").await?;
            if let Some(id) = id {
                ids.push(id);
            }
        }

        Ok(ids)
    }

    /// Update a user
    pub async fn update_user(&self, id: u64, new_username: &str) -> Result<()> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        conn.exec_drop(
            "UPDATE users SET username = ? WHERE id = ?",
            (new_username, id),
        )
        .await?;
        Ok(())
    }

    /// Delete a user
    pub async fn delete_user(&self, id: u64) -> Result<()> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        conn.exec_drop("DELETE FROM users WHERE id = ?", (id,))
            .await?;
        Ok(())
    }

    /// Get binlog position (MariaDB style)
    pub async fn get_binlog_position(&self) -> Result<(String, u64)> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        let row: (String, u64, String, String) =
            conn.query_first("SHOW MASTER STATUS").await?.unwrap();
        Ok((row.0, row.1))
    }

    /// Get GTID current position (MariaDB-specific)
    pub async fn get_gtid_current_pos(&self) -> Result<String> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        let gtid: Option<String> = conn.query_first("SELECT @@global.gtid_current_pos").await?;
        Ok(gtid.unwrap_or_default())
    }

    /// Insert test event with JSON payload (MariaDB JSON support)
    pub async fn insert_event(&self, event_type: &str, payload: serde_json::Value) -> Result<u64> {
        let pool = self.pool();
        let mut conn = pool.get_conn().await?;

        use mysql_async::prelude::Queryable;

        conn.exec_drop(
            "INSERT INTO events (event_type, payload) VALUES (?, ?)",
            (event_type, payload.to_string()),
        )
        .await?;
        let id: Option<u64> = conn.query_first("SELECT LAST_INSERT_ID()").await?;
        Ok(id.unwrap_or(0))
    }
}

// ============================================================================
// Test Data Generators
// ============================================================================

pub mod test_data {
    use bytes::Bytes;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct TestEvent {
        pub id: String,
        pub event_type: String,
        pub payload: serde_json::Value,
        pub timestamp: i64,
    }

    impl TestEvent {
        pub fn new(event_type: &str, payload: serde_json::Value) -> Self {
            Self {
                id: Uuid::new_v4().to_string(),
                event_type: event_type.to_string(),
                payload,
                timestamp: chrono::Utc::now().timestamp_millis(),
            }
        }

        pub fn to_bytes(&self) -> Bytes {
            Bytes::from(serde_json::to_vec(self).unwrap())
        }

        pub fn from_bytes(bytes: &[u8]) -> serde_json::Result<Self> {
            serde_json::from_slice(bytes)
        }
    }

    /// Generate a batch of test events
    pub fn generate_events(count: usize, event_type: &str) -> Vec<TestEvent> {
        (0..count)
            .map(|i| {
                TestEvent::new(
                    event_type,
                    serde_json::json!({
                        "index": i,
                        "data": format!("test-data-{}", i),
                    }),
                )
            })
            .collect()
    }

    /// Generate a batch of random messages
    pub fn generate_messages(count: usize) -> Vec<Bytes> {
        (0..count)
            .map(|i| Bytes::from(format!("message-{}", i)))
            .collect()
    }

    /// Generate keyed messages
    pub fn generate_keyed_messages(count: usize) -> Vec<(String, Bytes)> {
        (0..count)
            .map(|i| {
                let key = format!("key-{}", i % 10);
                let value = Bytes::from(format!("value-{}", i));
                (key, value)
            })
            .collect()
    }
}

pub mod assertions {
    use bytes::Bytes;

    /// Assert that messages are received in order
    pub fn assert_message_order(messages: &[Bytes], expected_count: usize) {
        assert_eq!(messages.len(), expected_count);
        for (i, msg) in messages.iter().enumerate() {
            let expected = format!("message-{}", i);
            assert_eq!(msg.as_ref(), expected.as_bytes());
        }
    }

    /// Assert approximate throughput
    pub fn assert_throughput(
        messages_sent: usize,
        duration: std::time::Duration,
        min_msgs_per_sec: f64,
    ) {
        let actual = messages_sent as f64 / duration.as_secs_f64();
        assert!(actual >= min_msgs_per_sec);
    }
}

// ============================================================================
// TestSchemaRegistry - Schema registry for tests
// ============================================================================

/// A schema registry for integration tests
///
/// This fixture provides an in-memory schema registry using `rivven-schema`.
/// It starts an HTTP server that can be used with `ExternalRegistry` from rivven-connect.
#[cfg(feature = "schema")]
pub struct TestSchemaRegistry {
    pub addr: std::net::SocketAddr,
    pub url: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

#[cfg(feature = "schema")]
impl TestSchemaRegistry {
    /// Start a new schema registry server on a random port
    pub async fn start() -> Result<Self> {
        use rivven_schema::{RegistryConfig, SchemaServer, ServerConfig};

        let port = portpicker::pick_unused_port().expect("No available port");
        let addr: std::net::SocketAddr = format!("127.0.0.1:{}", port).parse()?;
        let url = format!("http://127.0.0.1:{}", port);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let server_addr = addr;
        let handle = tokio::spawn(async move {
            let registry_config = RegistryConfig::memory();
            let registry = match rivven_schema::SchemaRegistry::new(registry_config).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("Failed to create schema registry: {}", e);
                    return;
                }
            };

            let server_config = ServerConfig {
                host: "127.0.0.1".to_string(),
                port,
                auth: None,
            };

            let server = SchemaServer::new(registry, server_config);

            tokio::select! {
                result = server.run(server_addr) => {
                    if let Err(e) = result {
                        tracing::error!("Schema registry server error: {}", e);
                    }
                }
                _ = shutdown_rx => {
                    info!("Test schema registry shutting down");
                }
            }
        });

        // Wait for server to be ready
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(server_addr).await.is_ok() {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }

        Ok(Self {
            addr,
            url,
            shutdown_tx: Some(shutdown_tx),
            handle: Some(handle),
        })
    }

    /// Get the schema registry URL
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Get the server port
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Gracefully shutdown the server
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }
        Ok(())
    }
}

#[cfg(feature = "schema")]
impl Drop for TestSchemaRegistry {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

// ============================================================================
// Kafka Test Container
// ============================================================================

/// A Kafka container for integration tests using testcontainers.
///
/// This fixture starts an Apache Kafka container in KRaft mode
/// (`apache/kafka-native:3.8.0` — GraalVM native image, no Zookeeper).
///
/// **Important:** Uses `testcontainers_modules::kafka::apache::Kafka`
/// explicitly. The default re-export `kafka::Kafka` resolves to
/// `confluent::Kafka` (Confluent + Zookeeper), which is not what we want.
///
/// # Example
///
/// ```rust,ignore
/// let kafka = TestKafka::start().await?;
/// println!("Kafka bootstrap: {}", kafka.bootstrap_servers());
/// kafka.create_topic("test-topic", 3, 1).await?;
///
/// // Produce and consume messages
/// let offsets = kafka.produce_messages("test-topic", 0, vec![
///     (Some(b"key".to_vec()), b"value".to_vec()),
/// ]).await?;
/// let records = kafka.consume_messages("test-topic", 0, 0, 10).await?;
/// ```
pub struct TestKafka {
    pub container: testcontainers::ContainerAsync<testcontainers_modules::kafka::apache::Kafka>,
    pub host: String,
    pub port: u16,
}

impl TestKafka {
    /// Start a Kafka container.
    ///
    /// Uses the Kafka testcontainer module which provides a single-node
    /// Kafka broker with KRaft (no Zookeeper required).
    pub async fn start() -> Result<Self> {
        use testcontainers::runners::AsyncRunner;
        use testcontainers_modules::kafka::apache::Kafka;

        info!("Starting Kafka container...");

        let container = Kafka::default().start().await?;

        // Use 127.0.0.1 – krafka requires numeric IP for SocketAddr parsing,
        // and the advertised listener also uses 127.0.0.1.
        let host = "127.0.0.1".to_string();

        // Use port 9092 (PLAINTEXT listener) – its advertised address
        // contains the real mapped port, so krafka's metadata-driven
        // reconnection works correctly from the host.
        let mut port = None;
        for i in 0..20 {
            match container.get_host_port_ipv4(9092).await {
                Ok(p) => {
                    port = Some(p);
                    break;
                }
                Err(e) => {
                    tracing::debug!("Waiting for Kafka port exposure (attempt {}): {}", i + 1, e);
                    sleep(Duration::from_millis(200 * (i + 1) as u64)).await;
                }
            }
        }
        let port = port.ok_or_else(|| anyhow::anyhow!("Kafka port not exposed after retries"))?;

        // Wait for Kafka to be ready
        Self::wait_for_kafka(&host, port).await?;

        info!("Kafka ready at {}:{}", host, port);

        Ok(Self {
            container,
            host,
            port,
        })
    }

    /// Wait for Kafka to become ready by attempting to connect and verifying
    /// the group coordinator is available.
    async fn wait_for_kafka(host: &str, port: u16) -> Result<()> {
        let bootstrap = format!("{}:{}", host, port);

        // 1. Wait for TCP to be reachable
        let mut tcp_ready = false;
        for i in 0..60 {
            match tokio::net::TcpStream::connect(&bootstrap).await {
                Ok(_) => {
                    info!("Kafka TCP ready after {} attempts", i + 1);
                    tcp_ready = true;
                    break;
                }
                Err(e) => {
                    tracing::debug!("Waiting for Kafka TCP (attempt {}): {}", i + 1, e);
                }
            }
            sleep(Duration::from_millis(500)).await;
        }
        if !tcp_ready {
            anyhow::bail!("Kafka TCP not reachable at {} after 60 attempts", bootstrap);
        }

        // 2. Wait for the broker to be fully functional (group coordinator ready)
        //    by attempting to create an admin client and list topics.
        let mut metadata_ready = false;
        for i in 0..30 {
            match krafka::admin::AdminClient::builder()
                .bootstrap_servers(&bootstrap)
                .build()
                .await
            {
                Ok(admin) => {
                    // Try listing topics to verify the broker is fully operational
                    match admin.list_topics().await {
                        Ok(_) => {
                            info!("Kafka broker fully ready after {} metadata attempts", i + 1);
                            metadata_ready = true;
                            break;
                        }
                        Err(e) => {
                            tracing::debug!("Kafka metadata not ready (attempt {}): {}", i + 1, e);
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!("Kafka not ready (attempt {}): {}", i + 1, e);
                }
            }
            sleep(Duration::from_millis(500)).await;
        }
        if !metadata_ready {
            anyhow::bail!(
                "Kafka metadata not available at {} after 30 attempts",
                bootstrap
            );
        }

        // 3. Wait for the group coordinator to be available.
        //    Consumer group tests fail with CoordinatorNotAvailable if this
        //    internal subsystem hasn't finished initializing.
        for i in 0..30 {
            let consumer_result = krafka::consumer::Consumer::builder()
                .bootstrap_servers(&bootstrap)
                .group_id("__readiness_check")
                .build()
                .await;

            match consumer_result {
                Ok(consumer) => {
                    // Try subscribing to trigger group coordinator lookup
                    match consumer.subscribe(&["__consumer_offsets"]).await {
                        Ok(_) => {
                            info!("Kafka group coordinator ready after {} attempts", i + 1);
                            consumer.close().await;
                            return Ok(());
                        }
                        Err(e) => {
                            tracing::debug!(
                                "Group coordinator not ready (attempt {}): {}",
                                i + 1,
                                e
                            );
                            consumer.close().await;
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!("Consumer creation failed (attempt {}): {}", i + 1, e);
                }
            }
            sleep(Duration::from_millis(500)).await;
        }

        anyhow::bail!("Kafka did not become ready in time")
    }

    /// Get the Kafka bootstrap servers connection string.
    #[inline]
    pub fn bootstrap_servers(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Get host string.
    #[inline]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Get port number.
    #[inline]
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Create a topic with the specified configuration.
    ///
    /// Uses krafka to create the topic via the Kafka protocol.
    pub async fn create_topic(
        &self,
        name: &str,
        num_partitions: i32,
        replication_factor: i16,
    ) -> Result<()> {
        use krafka::admin::{AdminClient, NewTopic};

        let admin = AdminClient::builder()
            .bootstrap_servers(self.bootstrap_servers())
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create admin client: {}", e))?;

        let topic = NewTopic::new(name, num_partitions, replication_factor);
        let results = admin
            .create_topics(vec![topic], Duration::from_secs(30))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create topic {}: {}", name, e))?;

        for result in &results {
            if let Some(ref err) = result.error {
                anyhow::bail!("Failed to create topic {}: {}", result.name, err);
            }
        }

        info!(
            "Created Kafka topic '{}' with {} partitions",
            name, num_partitions
        );
        Ok(())
    }

    /// Produce messages to a topic.
    ///
    /// Returns the offsets of the produced messages.
    pub async fn produce_messages(
        &self,
        topic: &str,
        partition: i32,
        messages: Vec<(Option<Vec<u8>>, Vec<u8>)>,
    ) -> Result<Vec<i64>> {
        use krafka::producer::{Producer, ProducerRecord};

        let producer = Producer::builder()
            .bootstrap_servers(self.bootstrap_servers())
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create producer: {}", e))?;

        let mut offsets = Vec::with_capacity(messages.len());
        for (key, value) in messages {
            let record = ProducerRecord::new(topic, value)
                .with_key(key)
                .with_partition(partition);
            let metadata = producer
                .send_record(record)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to produce message: {}", e))?;
            offsets.push(metadata.offset);
        }

        producer.flush().await.ok();
        producer.close().await;

        info!(
            "Produced {} messages to {}/{}",
            offsets.len(),
            topic,
            partition
        );
        Ok(offsets)
    }

    /// Consume messages from a topic partition starting from an offset.
    pub async fn consume_messages(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        max_messages: usize,
    ) -> Result<Vec<krafka::consumer::ConsumerRecord>> {
        use krafka::consumer::Consumer;

        let consumer = Consumer::builder()
            .bootstrap_servers(self.bootstrap_servers())
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create consumer: {}", e))?;

        consumer
            .assign(topic, vec![partition])
            .await
            .map_err(|e| anyhow::anyhow!("Failed to assign partition: {}", e))?;

        consumer
            .seek(topic, partition, start_offset)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to seek to offset {}: {}", start_offset, e))?;

        let mut result = Vec::new();
        // Poll until we have enough records or timeout
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while result.len() < max_messages && tokio::time::Instant::now() < deadline {
            let records = consumer
                .poll(Duration::from_secs(1))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to poll records: {}", e))?;
            if records.is_empty() && !result.is_empty() {
                break;
            }
            result.extend(records);
        }

        result.truncate(max_messages);
        consumer.close().await;

        info!(
            "Consumed {} messages from {}/{} starting at offset {}",
            result.len(),
            topic,
            partition,
            start_offset
        );
        Ok(result)
    }

    /// Get the current high watermark for a topic partition.
    ///
    /// The high watermark is the offset of the last committed message + 1.
    pub async fn get_high_watermark(&self, topic: &str, partition: i32) -> Result<i64> {
        use krafka::consumer::Consumer;

        let consumer = Consumer::builder()
            .bootstrap_servers(self.bootstrap_servers())
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create consumer: {}", e))?;

        consumer
            .assign(topic, vec![partition])
            .await
            .map_err(|e| anyhow::anyhow!("Failed to assign partition: {}", e))?;

        consumer
            .seek_to_end(topic, partition)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to seek to end: {}", e))?;

        // Poll once to commit the seek position
        let _ = consumer.poll(Duration::from_millis(100)).await;

        let offset = consumer.position(topic, partition).await.unwrap_or(0);

        consumer.close().await;

        Ok(offset)
    }
}

// ============================================================================
// MQTT Test Container (Mosquitto)
// ============================================================================

/// An MQTT broker container for integration tests using testcontainers.
///
/// This fixture starts an Eclipse Mosquitto MQTT broker which is lightweight
/// and starts quickly.
///
/// # Example
///
/// ```rust,ignore
/// let mqtt = TestMqtt::start().await?;
/// println!("MQTT broker: {}", mqtt.broker_url());
/// mqtt.publish("test/topic", b"hello", 0).await?;
/// ```
pub struct TestMqtt {
    pub container: testcontainers::ContainerAsync<testcontainers_modules::mosquitto::Mosquitto>,
    pub host: String,
    pub port: u16,
}

impl TestMqtt {
    /// Start a Mosquitto MQTT broker container.
    pub async fn start() -> Result<Self> {
        use testcontainers::runners::AsyncRunner;
        use testcontainers_modules::mosquitto::Mosquitto;

        info!("Starting Mosquitto MQTT broker...");

        let container = Mosquitto::default().start().await?;

        let host = container.get_host().await?.to_string();

        // Retry port retrieval
        let mut port = None;
        for i in 0..20 {
            match container.get_host_port_ipv4(1883).await {
                Ok(p) => {
                    port = Some(p);
                    break;
                }
                Err(e) => {
                    tracing::debug!("Waiting for MQTT port exposure (attempt {}): {}", i + 1, e);
                    sleep(Duration::from_millis(100 * (i + 1) as u64)).await;
                }
            }
        }
        let port = port.ok_or_else(|| anyhow::anyhow!("MQTT port not exposed after retries"))?;

        // Wait for MQTT to be ready
        Self::wait_for_mqtt(&host, port).await?;

        info!("MQTT broker ready at {}:{}", host, port);

        Ok(Self {
            container,
            host,
            port,
        })
    }

    /// Wait for the MQTT broker to become ready.
    async fn wait_for_mqtt(host: &str, port: u16) -> Result<()> {
        use rumqttc::{AsyncClient, MqttOptions};

        let client_id = format!("test-probe-{}", uuid::Uuid::new_v4());
        let mut options = MqttOptions::new(client_id, host, port);
        options.set_keep_alive(Duration::from_secs(5));

        for i in 0..30 {
            let (client, mut eventloop) = AsyncClient::new(options.clone(), 10);

            // Try to connect
            let connect_result = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    match eventloop.poll().await {
                        Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(ack))) => {
                            if ack.code == rumqttc::ConnectReturnCode::Success {
                                return Ok(());
                            }
                            return Err(anyhow::anyhow!("ConnAck error: {:?}", ack.code));
                        }
                        Ok(_) => continue,
                        Err(e) => return Err(anyhow::anyhow!("Connection error: {}", e)),
                    }
                }
            })
            .await;

            let _ = client.disconnect().await;

            match connect_result {
                Ok(Ok(())) => {
                    info!("MQTT broker ready after {} attempts", i + 1);
                    return Ok(());
                }
                Ok(Err(e)) => {
                    tracing::debug!("MQTT not ready (attempt {}): {}", i + 1, e);
                }
                Err(_) => {
                    tracing::debug!("MQTT connection timeout (attempt {})", i + 1);
                }
            }

            sleep(Duration::from_millis(300)).await;
        }

        anyhow::bail!("MQTT broker did not become ready in time")
    }

    /// Get the MQTT broker URL.
    #[inline]
    pub fn broker_url(&self) -> String {
        format!("mqtt://{}:{}", self.host, self.port)
    }

    /// Get host and port tuple.
    #[inline]
    pub fn host_port(&self) -> (&str, u16) {
        (&self.host, self.port)
    }

    /// Get host string.
    #[inline]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Get port number.
    #[inline]
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Publish a message to a topic.
    pub async fn publish(&self, topic: &str, payload: &[u8], qos: u8) -> Result<()> {
        use rumqttc::{AsyncClient, MqttOptions, QoS};

        let client_id = format!("test-pub-{}", uuid::Uuid::new_v4());
        let mut options = MqttOptions::new(client_id, &self.host, self.port);
        options.set_keep_alive(Duration::from_secs(5));

        let qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            _ => QoS::ExactlyOnce,
        };

        let (client, mut eventloop) = AsyncClient::new(options, 10);

        // Wait for connection
        let connected = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match eventloop.poll().await {
                    Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(ack))) => {
                        if ack.code == rumqttc::ConnectReturnCode::Success {
                            return Ok(());
                        }
                        return Err(anyhow::anyhow!("ConnAck error: {:?}", ack.code));
                    }
                    Ok(_) => continue,
                    Err(e) => return Err(anyhow::anyhow!("Connection error: {}", e)),
                }
            }
        })
        .await;

        connected.map_err(|_| anyhow::anyhow!("Connection timeout"))??;

        // Publish the message
        client
            .publish(topic, qos, false, payload)
            .await
            .map_err(|e| anyhow::anyhow!("Publish error: {}", e))?;

        // Wait for publish acknowledgement if QoS > 0
        if qos != QoS::AtMostOnce {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match eventloop.poll().await {
                        Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_))) => return Ok(()),
                        Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubComp(_))) => return Ok(()),
                        Ok(_) => continue,
                        Err(e) => return Err(anyhow::anyhow!("Poll error: {}", e)),
                    }
                }
            })
            .await
            .map_err(|_| anyhow::anyhow!("Publish ack timeout"))??;
        }

        let _ = client.disconnect().await;

        info!("Published {} bytes to topic '{}'", payload.len(), topic);
        Ok(())
    }

    /// Subscribe to a topic and receive messages.
    ///
    /// Returns a receiver that will receive messages as they arrive.
    pub async fn subscribe(
        &self,
        topic: &str,
        qos: u8,
    ) -> Result<(
        rumqttc::AsyncClient,
        tokio::sync::mpsc::Receiver<(String, Vec<u8>)>,
    )> {
        use rumqttc::{AsyncClient, MqttOptions, QoS};

        let client_id = format!("test-sub-{}", uuid::Uuid::new_v4());
        let mut options = MqttOptions::new(client_id, &self.host, self.port);
        options.set_keep_alive(Duration::from_secs(30));

        let qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            _ => QoS::ExactlyOnce,
        };

        let (client, mut eventloop) = AsyncClient::new(options, 100);
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        // Wait for connection and subscribe
        let client_clone = client.clone();
        let topic_owned = topic.to_string();

        tokio::spawn(async move {
            let mut connected = false;

            loop {
                match eventloop.poll().await {
                    Ok(rumqttc::Event::Incoming(packet)) => {
                        match packet {
                            rumqttc::Packet::ConnAck(ack) => {
                                if ack.code == rumqttc::ConnectReturnCode::Success {
                                    connected = true;
                                    // Subscribe after connection
                                    if let Err(e) = client_clone.subscribe(&topic_owned, qos).await
                                    {
                                        tracing::error!("Subscribe error: {}", e);
                                        break;
                                    }
                                }
                            }
                            rumqttc::Packet::SubAck(_) => {
                                tracing::debug!(
                                    "Subscribed to '{}' (connected={})",
                                    topic_owned,
                                    connected
                                );
                            }
                            rumqttc::Packet::Publish(publish) => {
                                let topic = publish.topic.clone();
                                let payload = publish.payload.to_vec();
                                if tx.send((topic, payload)).await.is_err() {
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!("MQTT event loop error: {}", e);
                        break;
                    }
                }
            }
        });

        // Wait a bit for subscription to be established
        sleep(Duration::from_millis(100)).await;

        Ok((client, rx))
    }
}
