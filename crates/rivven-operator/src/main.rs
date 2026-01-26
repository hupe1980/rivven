//! Rivven Kubernetes Operator
//!
//! This operator manages RivvenCluster custom resources in Kubernetes,
//! automatically deploying and managing Rivven distributed streaming clusters.

mod controller;
mod crd;
mod error;
mod resources;

use anyhow::{Context, Result};
use clap::Parser;
use kube::Client;
use std::net::SocketAddr;
use tracing::{info, Level};
use tracing_subscriber::fmt::format::FmtSpan;

/// Rivven Kubernetes Operator
#[derive(Parser, Debug)]
#[command(name = "rivven-operator")]
#[command(about = "Kubernetes operator for Rivven distributed event streaming clusters")]
#[command(version)]
struct Args {
    /// Metrics server address
    #[arg(long, env = "METRICS_ADDR", default_value = "0.0.0.0:8080")]
    metrics_addr: SocketAddr,

    /// Health probe address
    #[arg(long, env = "HEALTH_ADDR", default_value = "0.0.0.0:8081")]
    health_addr: SocketAddr,

    /// Enable leader election for high availability
    #[arg(long, env = "LEADER_ELECTION", default_value = "true")]
    leader_election: bool,

    /// Namespace to watch (empty for cluster-wide)
    #[arg(long, env = "WATCH_NAMESPACE", default_value = "")]
    namespace: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    log_level: Level,

    /// Enable JSON log format
    #[arg(long, env = "LOG_JSON", default_value = "false")]
    log_json: bool,

    /// Print CRD YAML and exit
    #[arg(long)]
    print_crd: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Handle CRD printing
    if args.print_crd {
        print_crd()?;
        return Ok(());
    }

    // Initialize logging
    init_logging(&args)?;

    info!(
        version = env!("CARGO_PKG_VERSION"),
        namespace = if args.namespace.is_empty() {
            "all"
        } else {
            &args.namespace
        },
        "Starting Rivven Kubernetes Operator"
    );

    // Initialize metrics
    let metrics_addr = args.metrics_addr;
    tokio::spawn(async move {
        if let Err(e) = start_metrics_server(metrics_addr).await {
            tracing::error!(error = %e, "Metrics server failed");
        }
    });

    // Start health server
    let health_addr = args.health_addr;
    tokio::spawn(async move {
        if let Err(e) = start_health_server(health_addr).await {
            tracing::error!(error = %e, "Health server failed");
        }
    });

    // Create Kubernetes client
    let client = Client::try_default()
        .await
        .context("Failed to create Kubernetes client")?;

    // Parse namespace (empty string means cluster-wide)
    let namespace = if args.namespace.is_empty() {
        None
    } else {
        Some(args.namespace)
    };

    // Run the controller
    controller::run_controller(client, namespace)
        .await
        .context("Controller failed")?;

    Ok(())
}

/// Initialize logging subsystem
fn init_logging(args: &Args) -> Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(args.log_level)
        .with_span_events(FmtSpan::CLOSE)
        .with_target(true)
        .with_thread_ids(false)
        .with_line_number(false);

    if args.log_json {
        subscriber.json().init();
    } else {
        subscriber.init();
    }

    Ok(())
}

/// Start the Prometheus metrics server
async fn start_metrics_server(addr: SocketAddr) -> Result<()> {
    use metrics_exporter_prometheus::PrometheusBuilder;

    info!(address = %addr, "Starting metrics server");

    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .context("Failed to install Prometheus exporter")?;

    // Keep the server running
    std::future::pending::<()>().await;

    Ok(())
}

/// Start the health probe server
async fn start_health_server(addr: SocketAddr) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    info!(address = %addr, "Starting health server");

    let listener = TcpListener::bind(addr)
        .await
        .context("Failed to bind health server")?;

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut buf = [0; 1024];
            if socket.read(&mut buf).await.is_ok() {
                let response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
    }
}

/// Print the CRD YAML for installation
fn print_crd() -> Result<()> {
    use kube::CustomResourceExt;

    let crd = crd::RivvenCluster::crd();
    let yaml = serde_yaml::to_string(&crd)?;
    println!("{}", yaml);

    Ok(())
}

pub mod prelude {
    //! Re-exports for convenient usage
    pub use crate::controller::run_controller;
    pub use crate::crd::{RivvenCluster, RivvenClusterSpec, RivvenClusterStatus};
    pub use crate::error::{OperatorError, Result};
}
