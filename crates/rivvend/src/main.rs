//! Rivven Server - High-Performance Distributed Event Streaming
//!
//! Usage:
//!   # Standalone mode (default)
//!   rivvend
//!
//!   # Cluster mode - first node (bootstrap)
//!   rivvend --mode cluster --node-id node-1
//!
//!   # Cluster mode - joining nodes
//!   rivvend --mode cluster --node-id node-2 --seeds node-1:9093
//!
//!   # With custom configuration
//!   rivvend --mode cluster \
//!     --node-id node-1 \
//!     --bind 0.0.0.0:9092 \
//!     --cluster-bind 0.0.0.0:9093 \
//!     --data-dir /var/lib/rivven \
//!     --datacenter us-west-2 \
//!     --rack rack-a \
//!     --replication-factor 3

use clap::Parser;
use rivvend::{Cli, ClusterServer};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse CLI arguments
    let cli = Cli::parse();

    // Initialize tracing with configured log level
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| cli.log_level.clone().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Print startup banner
    print_banner(&cli);

    // Validate configuration
    if let Err(e) = cli.validate() {
        eprintln!("Configuration error: {}", e);
        std::process::exit(1);
    }

    // Create shutdown channel
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let shutdown_tx = Arc::new(shutdown_tx);

    // Create and start server
    let server = ClusterServer::new(cli).await?;
    let server_shutdown = server.get_shutdown_handle();

    // Handle shutdown signals
    let shutdown_tx_signal = shutdown_tx.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        tracing::info!("Shutdown signal received, initiating graceful shutdown...");
        let _ = shutdown_tx_signal.send(());
    });

    // Start server in background
    let server_handle = tokio::spawn(async move { server.start().await });

    // Wait for shutdown signal
    let mut shutdown_rx = shutdown_tx.subscribe();
    let _ = shutdown_rx.recv().await;

    // Trigger server shutdown
    tracing::info!("Triggering server shutdown...");
    server_shutdown.shutdown();

    // Wait for server to stop with timeout
    let shutdown_timeout = tokio::time::Duration::from_secs(30);
    match tokio::time::timeout(shutdown_timeout, server_handle).await {
        Ok(Ok(Ok(()))) => {
            tracing::info!("Server shut down gracefully");
        }
        Ok(Ok(Err(e))) => {
            tracing::error!("Server error during shutdown: {}", e);
        }
        Ok(Err(e)) => {
            tracing::error!("Server task panicked: {}", e);
        }
        Err(_) => {
            tracing::warn!(
                "Shutdown timed out after {:?}, forcing exit",
                shutdown_timeout
            );
        }
    }

    tracing::info!("Goodbye!");
    Ok(())
}

/// Wait for shutdown signals (Ctrl+C or SIGTERM)
async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("Received Ctrl+C");
        }
        _ = terminate => {
            tracing::info!("Received SIGTERM");
        }
    }
}

fn print_banner(cli: &Cli) {
    let mode = if cli.is_cluster_mode() {
        "cluster"
    } else {
        "standalone"
    };
    let node_id = cli.node_id.as_deref().unwrap_or("auto");

    eprintln!(
        r#"
 ____  _                     
|  _ \(_)_____   _____ _ __  
| |_) | \ \ / / / / _ \ '_ \ 
|  _ <| |\ V / V /  __/ | | |
|_| \_\_| \_/\_/ \___|_| |_|

High-Performance Distributed Event Streaming Platform

  Mode:         {}
  Node ID:      {}
  Bind:         {}
  Data Dir:     {}
  Persistence:  {}

"#,
        mode,
        node_id,
        cli.bind,
        cli.data_dir.display(),
        cli.persistence
    );

    if cli.is_cluster_mode() {
        let seeds = if cli.seeds.is_empty() {
            "none (bootstrapping new cluster)".to_string()
        } else {
            cli.seeds.join(", ")
        };
        let rack = cli.rack.as_deref().unwrap_or("default");
        eprintln!(
            "  Cluster:      {}\n  \
             Rack:         {}\n  \
             Seeds:        {}\n  \
             Replication:  {}\n  \
             Min ISR:      {}\n",
            cli.cluster_name, rack, seeds, cli.replication_factor, cli.min_isr
        );
    }
}
