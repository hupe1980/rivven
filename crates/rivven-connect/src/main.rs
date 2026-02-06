//! rivven-connect - Connector runtime for Rivven
//!
//! # Architecture
//!
//! Sources read from external systems and publish to broker topics.
//! Sinks consume from broker topics and write to external systems.
//! The broker provides durability, replay, decoupling, and fan-out.
//!
//! ```text
//! ┌─────────────┐     ┌──────────────┐     ┌─────────────┐
//! │   Source    │────▶│    Broker    │────▶│    Sink     │
//! │ (postgres)  │     │   Topics     │     │   (s3)      │
//! └─────────────┘     └──────────────┘     └─────────────┘
//! ```
//!
//! # Usage
//!
//! ```bash
//! # Run all connectors
//! rivven-connect -c connect.yaml
//!
//! # Run only sources
//! rivven-connect -c connect.yaml sources
//!
//! # Run only sinks  
//! rivven-connect -c connect.yaml sinks
//!
//! # Validate configuration
//! rivven-connect -c connect.yaml validate
//! ```

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

// Use the library modules instead of declaring them
use rivven_connect::{
    broker_client, config, connectors, error, health, metrics, sink_runner, source_runner,
};

use config::ConnectConfig;
use error::ConnectorStatus;
use health::{ConnectorHealth, HealthState, SharedHealthState};

#[derive(Parser)]
#[command(name = "rivven-connect")]
#[command(version, about = "Connector runtime for Rivven streaming platform")]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, default_value = "connect.yaml")]
    config: PathBuf,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run all enabled sources and sinks (default)
    Run,
    /// Run only source connectors
    Sources,
    /// Run only sink connectors
    Sinks,
    /// Validate configuration file
    Validate,
    /// Check connectivity to broker and configured sources/sinks
    Check {
        /// Check a specific source by name
        #[arg(long)]
        source: Option<String>,
        /// Check a specific sink by name
        #[arg(long)]
        sink: Option<String>,
    },
    /// Discover available schemas from a source
    Discover {
        /// Source name to discover schemas from
        source: String,
        /// Output format (json, yaml, table)
        #[arg(long, default_value = "table")]
        format: String,
    },
    /// Show connector status (requires running health endpoint)
    Status {
        /// Health endpoint URL (default: from config)
        #[arg(long)]
        url: Option<String>,
    },
    /// List available connector types
    Connectors,
    /// Show config schema for a connector type
    Schema {
        /// Connector type (e.g., postgres-cdc, stdout)
        connector: String,
        /// Output format (json, yaml)
        #[arg(long, default_value = "json")]
        format: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_logging(cli.verbose);

    // Commands that don't need config
    match &cli.command {
        Some(Commands::Connectors) => return list_connectors(),
        Some(Commands::Schema { connector, format }) => return show_schema(connector, format),
        _ => {}
    }

    let config = ConnectConfig::from_file(&cli.config)
        .with_context(|| format!("Failed to load config from {}", cli.config.display()))?;

    match cli.command.unwrap_or(Commands::Run) {
        Commands::Run => run_all(config).await,
        Commands::Sources => run_sources_only(config).await,
        Commands::Sinks => run_sinks_only(config).await,
        Commands::Validate => validate_config(config),
        Commands::Check { source, sink } => check_connectivity(config, source, sink).await,
        Commands::Discover { source, format } => discover_source(config, source, format).await,
        Commands::Status { url } => show_status(config, url).await,
        Commands::Connectors | Commands::Schema { .. } => unreachable!(), // handled above
    }
}

fn init_logging(verbose: bool) {
    let filter = if verbose {
        EnvFilter::new("debug")
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_target(true))
        .init();
}

async fn run_all(config: ConnectConfig) -> Result<()> {
    info!("Starting rivven-connect");
    info!("Bootstrap servers: {:?}", config.broker.bootstrap_servers);

    let config = Arc::new(config);
    let (shutdown_tx, _) = broadcast::channel::<()>(16);

    // Initialize health state
    let health_state: SharedHealthState = Arc::new(tokio::sync::RwLock::new(HealthState {
        started_at: Some(Instant::now()),
        ..Default::default()
    }));

    // Start health check server if enabled
    if config.settings.health.enabled {
        let health_config = config.settings.health.clone();
        let health_state_clone = health_state.clone();
        tokio::spawn(async move {
            if let Err(e) = health::start_health_server(health_config, health_state_clone).await {
                error!("Health server failed: {}", e);
            }
        });
    }

    // Start metrics server if enabled
    if config.settings.metrics.enabled {
        let metrics_config = config.settings.metrics.clone();
        let metrics_state: metrics::SharedMetricsState =
            Arc::new(tokio::sync::RwLock::new(metrics::MetricsState {
                started_at: Some(Instant::now()),
                ..Default::default()
            }));
        tokio::spawn(async move {
            if let Err(e) = metrics::start_metrics_server(metrics_config, metrics_state).await {
                error!("Metrics server failed: {}", e);
            }
        });
    }

    let mut tasks = Vec::new();

    // Start sources
    for (name, source_config) in config.enabled_sources() {
        if !source_config.enabled {
            continue;
        }

        info!("Starting source: {} ({})", name, source_config.connector);

        let config = config.clone();
        let name = name.clone();
        let source_config = source_config.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let health_state = health_state.clone();

        // Initialize health entry
        {
            let mut state = health_state.write().await;
            state.sources.insert(
                name.clone(),
                ConnectorHealth {
                    status: ConnectorStatus::Starting,
                    events_processed: 0,
                    errors_count: 0,
                    last_error: None,
                },
            );
        }

        let task_name = name.clone();
        tasks.push(tokio::spawn(async move {
            let result =
                source_runner::run_source(&name, &source_config, &config, &mut shutdown_rx).await;

            // Update health state
            {
                let mut state = health_state.write().await;
                if let Some(health) = state.sources.get_mut(&name) {
                    health.status = match &result {
                        Ok(()) => ConnectorStatus::Stopped,
                        Err(e) if e.is_shutdown() => ConnectorStatus::Stopped,
                        Err(e) => {
                            health.last_error = Some(e.to_string());
                            ConnectorStatus::Failed
                        }
                    };
                }
            }

            if let Err(e) = &result {
                if !e.is_shutdown() {
                    error!("Source '{}' failed: {}", task_name, e);
                }
            }
            result
        }));
    }

    // Start sinks
    for (name, sink_config) in config.enabled_sinks() {
        if !sink_config.enabled {
            continue;
        }

        info!("Starting sink: {} ({})", name, sink_config.connector);

        let config = config.clone();
        let name = name.clone();
        let sink_config = sink_config.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let health_state = health_state.clone();

        // Initialize health entry
        {
            let mut state = health_state.write().await;
            state.sinks.insert(
                name.clone(),
                ConnectorHealth {
                    status: ConnectorStatus::Starting,
                    events_processed: 0,
                    errors_count: 0,
                    last_error: None,
                },
            );
        }

        let task_name = name.clone();
        tasks.push(tokio::spawn(async move {
            let result =
                sink_runner::run_sink(&name, &sink_config, &config, &mut shutdown_rx).await;

            // Update health state
            {
                let mut state = health_state.write().await;
                if let Some(health) = state.sinks.get_mut(&name) {
                    health.status = match &result {
                        Ok(()) => ConnectorStatus::Stopped,
                        Err(e) if e.is_shutdown() => ConnectorStatus::Stopped,
                        Err(e) => {
                            health.last_error = Some(e.to_string());
                            ConnectorStatus::Failed
                        }
                    };
                }
            }

            if let Err(e) = &result {
                if !e.is_shutdown() {
                    error!("Sink '{}' failed: {}", task_name, e);
                }
            }
            result
        }));
    }

    if tasks.is_empty() {
        warn!("No enabled sources or sinks found in configuration");
        return Ok(());
    }

    info!("Running {} connector(s)", tasks.len());

    // Wait for shutdown signal or connector failure
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal (Ctrl+C)");
        }
        _ = wait_for_fatal_error(&mut tasks) => {
            error!("A connector failed fatally");
        }
    }

    // Initiate graceful shutdown
    info!("Initiating graceful shutdown...");
    let _ = shutdown_tx.send(());

    // Wait for tasks with timeout
    let shutdown_timeout = tokio::time::Duration::from_secs(10);
    match tokio::time::timeout(shutdown_timeout, futures::future::join_all(tasks)).await {
        Ok(results) => {
            let failed = results.iter().filter(|r| r.is_err()).count();
            if failed > 0 {
                warn!("{} connector(s) had errors during shutdown", failed);
            }
        }
        Err(_) => {
            warn!("Shutdown timeout reached, some connectors may not have stopped cleanly");
        }
    }

    info!("Shutdown complete");
    Ok(())
}

/// Wait for a fatal error from any task
async fn wait_for_fatal_error(_tasks: &mut [tokio::task::JoinHandle<error::Result<()>>]) {
    // This is a placeholder - in production you'd want more sophisticated
    // failure handling (restart policies, circuit breakers, etc.)
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    }
}

async fn run_sources_only(config: ConnectConfig) -> Result<()> {
    info!("Starting rivven-connect (sources only)");

    let config = Arc::new(config);
    let (shutdown_tx, _) = broadcast::channel::<()>(16);
    let mut tasks = Vec::new();

    for (name, source_config) in config.enabled_sources() {
        info!("Starting source: {}", name);
        let config = config.clone();
        let name = name.clone();
        let source_config = source_config.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();

        tasks.push(tokio::spawn(async move {
            source_runner::run_source(&name, &source_config, &config, &mut shutdown_rx).await
        }));
    }

    if tasks.is_empty() {
        warn!("No enabled sources found");
        return Ok(());
    }

    info!("Running {} source(s)", tasks.len());

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal");
            let _ = shutdown_tx.send(());
        }
        _ = futures::future::join_all(&mut tasks) => {}
    }

    Ok(())
}

async fn run_sinks_only(config: ConnectConfig) -> Result<()> {
    info!("Starting rivven-connect (sinks only)");

    let config = Arc::new(config);
    let (shutdown_tx, _) = broadcast::channel::<()>(16);
    let mut tasks = Vec::new();

    for (name, sink_config) in config.enabled_sinks() {
        info!("Starting sink: {}", name);
        let config = config.clone();
        let name = name.clone();
        let sink_config = sink_config.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();

        tasks.push(tokio::spawn(async move {
            sink_runner::run_sink(&name, &sink_config, &config, &mut shutdown_rx).await
        }));
    }

    if tasks.is_empty() {
        warn!("No enabled sinks found");
        return Ok(());
    }

    info!("Running {} sink(s)", tasks.len());

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal");
            let _ = shutdown_tx.send(());
        }
        _ = futures::future::join_all(&mut tasks) => {}
    }

    Ok(())
}

fn validate_config(config: ConnectConfig) -> Result<()> {
    println!("✓ Configuration valid!\n");

    println!("Broker:");
    println!("  Bootstrap servers:");
    for server in &config.broker.bootstrap_servers {
        println!("    - {}", server);
    }
    println!(
        "  Metadata refresh: {}ms",
        config.broker.metadata_refresh_ms
    );
    println!(
        "  Connection timeout: {}ms",
        config.broker.connection_timeout_ms
    );
    println!("  Request timeout: {}ms", config.broker.request_timeout_ms);
    println!(
        "  TLS: {}",
        if config.broker.tls.enabled {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!();

    println!("Topic Settings:");
    println!(
        "  Auto-create: {}",
        if config.settings.topic.auto_create {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!(
        "  Default partitions: {}",
        config.settings.topic.default_partitions
    );
    println!(
        "  Default replication: {}",
        config.settings.topic.default_replication_factor
    );
    println!();

    println!("Retry Policy:");
    println!("  Max retries: {}", config.settings.retry.max_retries);
    println!(
        "  Initial backoff: {}ms",
        config.settings.retry.initial_backoff_ms
    );
    println!("  Max backoff: {}ms", config.settings.retry.max_backoff_ms);
    println!();

    println!("Endpoints:");
    if config.settings.health.enabled {
        println!(
            "  Health:  http://0.0.0.0:{}{}",
            config.settings.health.port, config.settings.health.path
        );
    } else {
        println!("  Health:  disabled");
    }
    if config.settings.metrics.enabled {
        println!(
            "  Metrics: http://0.0.0.0:{}/metrics",
            config.settings.metrics.port
        );
    } else {
        println!("  Metrics: disabled");
    }
    println!();

    let enabled_sources: Vec<_> = config.enabled_sources().collect();
    let disabled_sources = config.sources.len() - enabled_sources.len();

    println!(
        "Sources ({} enabled, {} disabled):",
        enabled_sources.len(),
        disabled_sources
    );
    for (name, source) in &config.sources {
        let status = if source.enabled { "✓" } else { "○" };
        let partitions = source
            .topic_config
            .as_ref()
            .and_then(|tc| tc.partitions)
            .unwrap_or(config.settings.topic.default_partitions);
        println!(
            "  {} {} ({}) → topic: {} ({} partitions)",
            status, name, source.connector, source.topic, partitions
        );
    }
    println!();

    let enabled_sinks: Vec<_> = config.enabled_sinks().collect();
    let disabled_sinks = config.sinks.len() - enabled_sinks.len();

    println!(
        "Sinks ({} enabled, {} disabled):",
        enabled_sinks.len(),
        disabled_sinks
    );
    for (name, sink) in &config.sinks {
        let status = if sink.enabled { "✓" } else { "○" };
        println!(
            "  {} {} ({}) ← topics: {:?}",
            status, name, sink.connector, sink.topics
        );
    }

    Ok(())
}

async fn show_status(config: ConnectConfig, url: Option<String>) -> Result<()> {
    let health_url = url.unwrap_or_else(|| {
        if config.settings.health.enabled {
            format!(
                "http://localhost:{}{}",
                config.settings.health.port, config.settings.health.path
            )
        } else {
            "http://localhost:8080/health".to_string()
        }
    });

    println!("Fetching status from: {}", health_url);
    println!();

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .context("Failed to create HTTP client")?;

    match client.get(&health_url).send().await {
        Ok(response) => {
            let status_code = response.status();
            let body: serde_json::Value = response
                .json()
                .await
                .unwrap_or_else(|_| serde_json::json!({"error": "Failed to parse response"}));

            println!(
                "Health Status: {}",
                if status_code.is_success() {
                    "✓ OK"
                } else {
                    "✗ UNHEALTHY"
                }
            );
            println!();

            if let Some(status) = body.get("status") {
                println!("Overall: {}", status);
            }
            if let Some(uptime) = body.get("uptime_seconds") {
                let uptime_secs = uptime.as_f64().unwrap_or(0.0);
                let hours = (uptime_secs / 3600.0) as u64;
                let mins = ((uptime_secs % 3600.0) / 60.0) as u64;
                let secs = (uptime_secs % 60.0) as u64;
                println!("Uptime: {}h {}m {}s", hours, mins, secs);
            }

            if let Some(sources) = body.get("sources").and_then(|s| s.as_object()) {
                println!("\nSources:");
                for (name, info) in sources {
                    let status = info
                        .get("status")
                        .and_then(|s| s.as_str())
                        .unwrap_or("unknown");
                    let events = info
                        .get("events_processed")
                        .and_then(|e| e.as_u64())
                        .unwrap_or(0);
                    let errors = info
                        .get("errors_count")
                        .and_then(|e| e.as_u64())
                        .unwrap_or(0);
                    let status_icon = match status {
                        "running" => "✓",
                        "failed" => "✗",
                        "unhealthy" => "!",
                        _ => "○",
                    };
                    println!(
                        "  {} {} - events: {}, errors: {}",
                        status_icon, name, events, errors
                    );
                    if let Some(error) = info.get("last_error").and_then(|e| e.as_str()) {
                        println!("      └─ Error: {}", error);
                    }
                }
            }

            if let Some(sinks) = body.get("sinks").and_then(|s| s.as_object()) {
                println!("\nSinks:");
                for (name, info) in sinks {
                    let status = info
                        .get("status")
                        .and_then(|s| s.as_str())
                        .unwrap_or("unknown");
                    let events = info
                        .get("events_processed")
                        .and_then(|e| e.as_u64())
                        .unwrap_or(0);
                    let errors = info
                        .get("errors_count")
                        .and_then(|e| e.as_u64())
                        .unwrap_or(0);
                    let status_icon = match status {
                        "running" => "✓",
                        "failed" => "✗",
                        "unhealthy" => "!",
                        _ => "○",
                    };
                    println!(
                        "  {} {} - events: {}, errors: {}",
                        status_icon, name, events, errors
                    );
                    if let Some(error) = info.get("last_error").and_then(|e| e.as_str()) {
                        println!("      └─ Error: {}", error);
                    }
                }
            }

            Ok(())
        }
        Err(e) => {
            eprintln!("✗ Failed to connect to health endpoint: {}", e);
            eprintln!("\nMake sure rivven-connect is running with health endpoint enabled:");
            eprintln!("  settings:");
            eprintln!("    health:");
            eprintln!("      enabled: true");
            std::process::exit(1);
        }
    }
}

/// Check connectivity to broker and optionally to specific sources/sinks
async fn check_connectivity(
    config: ConnectConfig,
    source_name: Option<String>,
    sink_name: Option<String>,
) -> Result<()> {
    println!("Running connectivity checks...\n");
    let mut all_passed = true;

    // Check broker connectivity
    print!("Broker ({:?})... ", config.broker.bootstrap_servers);
    match check_broker(&config).await {
        Ok(()) => println!("✓ connected"),
        Err(e) => {
            println!("✗ failed: {}", e);
            all_passed = false;
        }
    }

    // Check specific source if requested
    if let Some(ref name) = source_name {
        if let Some(source) = config.sources.get(name) {
            print!("Source '{}' ({})... ", name, source.connector);
            match check_source_config(name, source).await {
                Ok(msg) => println!("✓ {}", msg),
                Err(e) => {
                    println!("✗ {}", e);
                    all_passed = false;
                }
            }
        } else {
            println!("✗ Source '{}' not found in configuration", name);
            all_passed = false;
        }
    }

    // Check specific sink if requested
    if let Some(ref name) = sink_name {
        if let Some(sink) = config.sinks.get(name) {
            print!("Sink '{}' ({})... ", name, sink.connector);
            match check_sink_config(name, sink).await {
                Ok(msg) => println!("✓ {}", msg),
                Err(e) => {
                    println!("✗ {}", e);
                    all_passed = false;
                }
            }
        } else {
            println!("✗ Sink '{}' not found in configuration", name);
            all_passed = false;
        }
    }

    // If no specific source/sink, check all configured ones
    if source_name.is_none() && sink_name.is_none() {
        // Check all sources
        for (name, source) in &config.sources {
            if !source.enabled {
                println!("Source '{}' (disabled) - skipped", name);
                continue;
            }
            print!("Source '{}' ({})... ", name, source.connector);
            match check_source_config(name, source).await {
                Ok(msg) => println!("✓ {}", msg),
                Err(e) => {
                    println!("✗ {}", e);
                    all_passed = false;
                }
            }
        }

        // Check all sinks
        for (name, sink) in &config.sinks {
            if !sink.enabled {
                println!("Sink '{}' (disabled) - skipped", name);
                continue;
            }
            print!("Sink '{}' ({})... ", name, sink.connector);
            match check_sink_config(name, sink).await {
                Ok(msg) => println!("✓ {}", msg),
                Err(e) => {
                    println!("✗ {}", e);
                    all_passed = false;
                }
            }
        }
    }

    println!();
    if all_passed {
        println!("All checks passed! ✓");
        Ok(())
    } else {
        anyhow::bail!("Some checks failed");
    }
}

async fn check_broker(config: &ConnectConfig) -> Result<()> {
    use broker_client::BrokerClient;
    use std::sync::Arc;

    let broker = Arc::new(BrokerClient::new(
        config.broker.clone(),
        config.settings.retry.clone(),
    ));

    // Try to connect with a short timeout
    tokio::time::timeout(std::time::Duration::from_secs(5), broker.connect())
        .await
        .map_err(|_| anyhow::anyhow!("connection timeout"))?
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    Ok(())
}

/// Default timeout for check operations
const CHECK_TIMEOUT_SECS: u64 = 30;

async fn check_source_config(_name: &str, source: &config::SourceConfig) -> Result<String> {
    use connectors::cdc::{PostgresCdcConfig, PostgresCdcSource};
    use rivven_connect::Source;
    use std::time::Duration;
    use validator::Validate;

    match source.connector.as_str() {
        "postgres-cdc" => {
            #[cfg(feature = "postgres")]
            {
                // Parse and validate config using SDK types
                let pg_config: PostgresCdcConfig = serde_yaml::from_value(source.config.clone())
                    .map_err(|e| anyhow::anyhow!("invalid config: {}", e))?;

                // Run validation
                pg_config
                    .validate()
                    .map_err(|e| anyhow::anyhow!("validation failed: {}", e))?;

                // Use SDK connector check method with timeout
                let connector = PostgresCdcSource::new();
                let check_future = connector.check(&pg_config);

                let check_result =
                    tokio::time::timeout(Duration::from_secs(CHECK_TIMEOUT_SECS), check_future)
                        .await
                        .map_err(|_| {
                            anyhow::anyhow!("check timed out after {}s", CHECK_TIMEOUT_SECS)
                        })?
                        .map_err(|e| anyhow::anyhow!("check failed: {}", e))?;

                if check_result.is_success() {
                    Ok(check_result
                        .message
                        .unwrap_or_else(|| "connected".to_string()))
                } else {
                    Err(anyhow::anyhow!(
                        "{}",
                        check_result
                            .message
                            .unwrap_or_else(|| "check failed".to_string())
                    ))
                }
            }
            #[cfg(not(feature = "postgres"))]
            {
                Err(anyhow::anyhow!("PostgreSQL support not compiled in"))
            }
        }
        "http" => {
            // Basic validation only
            if source.config.is_null() {
                return Err(anyhow::anyhow!("missing 'url' in config"));
            }
            Ok("config valid".to_string())
        }
        other => Ok(format!("connector '{}' - no check implemented", other)),
    }
}

async fn check_sink_config(_name: &str, sink: &config::SinkConfig) -> Result<String> {
    use connectors::stdout::{StdoutSink, StdoutSinkConfig};
    use rivven_connect::Sink;

    match sink.connector.as_str() {
        "stdout" => {
            // Parse and validate config using SDK types
            let stdout_config: StdoutSinkConfig =
                serde_yaml::from_value(sink.config.clone()).unwrap_or_default();

            // Use SDK connector check method
            let connector = StdoutSink::new();
            let check_result = connector
                .check(&stdout_config)
                .await
                .map_err(|e| anyhow::anyhow!("check failed: {}", e))?;

            if check_result.is_success() {
                Ok(check_result
                    .message
                    .unwrap_or_else(|| "available".to_string()))
            } else {
                Err(anyhow::anyhow!(
                    "{}",
                    check_result
                        .message
                        .unwrap_or_else(|| "check failed".to_string())
                ))
            }
        }
        #[cfg(feature = "http")]
        "http" | "http-webhook" => {
            // Use the library's check implementation
            use connectors::http::{HttpWebhookConfig, HttpWebhookSink};
            let http_config: HttpWebhookConfig = serde_yaml::from_value(sink.config.clone())
                .map_err(|e| anyhow::anyhow!("invalid config: {}", e))?;

            let connector = HttpWebhookSink::new();
            let check_result = connector
                .check(&http_config)
                .await
                .map_err(|e| anyhow::anyhow!("check failed: {}", e))?;

            if check_result.is_success() {
                Ok(check_result
                    .message
                    .unwrap_or_else(|| "URL valid".to_string()))
            } else {
                Err(anyhow::anyhow!(
                    "{}",
                    check_result
                        .message
                        .unwrap_or_else(|| "check failed".to_string())
                ))
            }
        }
        // For external sinks (snowflake, s3), use registry lookup
        // These are enabled via feature flags (e.g., features = ["snowflake", "s3"])
        other => Ok(format!(
            "connector '{}' - check not available (built-in connectors only)",
            other
        )),
    }
}

/// Timeout for discover operations (seconds)
const DISCOVER_TIMEOUT_SECS: u64 = 60;

/// Discover available streams/tables from a source
async fn discover_source(config: ConnectConfig, source_name: String, format: String) -> Result<()> {
    use connectors::cdc::{PostgresCdcConfig, PostgresCdcSource};
    use rivven_connect::Source;
    use validator::Validate;

    let source = config
        .sources
        .get(&source_name)
        .ok_or_else(|| anyhow::anyhow!("Source '{}' not found in configuration", source_name))?;

    println!(
        "Discovering schemas from source: {} ({})",
        source_name, source.connector
    );
    println!();

    match source.connector.as_str() {
        "postgres-cdc" => {
            #[cfg(feature = "postgres")]
            {
                // Parse and validate config using SDK types
                let pg_config: PostgresCdcConfig = serde_yaml::from_value(source.config.clone())
                    .map_err(|e| anyhow::anyhow!("Invalid postgres config: {}", e))?;

                pg_config
                    .validate()
                    .map_err(|e| anyhow::anyhow!("Config validation failed: {}", e))?;

                // Use SDK connector discover method with timeout
                let connector = PostgresCdcSource::new();
                let discover_future = connector.discover(&pg_config);
                let catalog = tokio::time::timeout(
                    std::time::Duration::from_secs(DISCOVER_TIMEOUT_SECS),
                    discover_future,
                )
                .await
                .map_err(|_| anyhow::anyhow!(
                    "Discover timed out after {} seconds. Check network connectivity and database accessibility.",
                    DISCOVER_TIMEOUT_SECS
                ))?
                .map_err(|e| anyhow::anyhow!("Discover failed: {}", e))?;

                // Format output
                match format.as_str() {
                    "json" => {
                        println!("{}", serde_json::to_string_pretty(&catalog)?);
                    }
                    "yaml" => {
                        println!("{}", serde_yaml::to_string(&catalog)?);
                    }
                    _ => {
                        // Pretty table format
                        println!("Found {} streams:\n", catalog.streams.len());
                        for stream in &catalog.streams {
                            let namespace = stream.namespace.as_deref().unwrap_or("public");
                            println!("  {}.{}", namespace, stream.name);

                            // Show columns if available in JSON schema
                            if let Some(props) = stream.json_schema.get("properties") {
                                if let Some(obj) = props.as_object() {
                                    for (col_name, col_schema) in obj {
                                        let col_type = col_schema
                                            .get("type")
                                            .and_then(|t| t.as_str())
                                            .unwrap_or("unknown");
                                        println!("    - {} ({})", col_name, col_type);
                                    }
                                }
                            }
                            println!();
                        }
                    }
                }
                Ok(())
            }
            #[cfg(not(feature = "postgres"))]
            {
                anyhow::bail!("PostgreSQL support not compiled in");
            }
        }
        other => {
            anyhow::bail!("Discover not implemented for connector type: {}", other);
        }
    }
}

/// Show config schema for a connector type
fn show_schema(connector: &str, format: &str) -> Result<()> {
    use connectors::cdc::PostgresCdcConfig;
    use connectors::stdout::StdoutSinkConfig;
    use schemars::schema_for;

    let schema = match connector {
        "postgres-cdc" => {
            let schema = schema_for!(PostgresCdcConfig);
            serde_json::to_value(&schema)?
        }
        "stdout" => {
            let schema = schema_for!(StdoutSinkConfig);
            serde_json::to_value(&schema)?
        }
        unknown => {
            anyhow::bail!("Unknown connector type: '{}'\nUse 'rivven-connect connectors' to list available types", unknown);
        }
    };

    match format {
        "yaml" => println!("{}", serde_yaml::to_string(&schema)?),
        _ => println!("{}", serde_json::to_string_pretty(&schema)?),
    }

    Ok(())
}

/// List available connector types
fn list_connectors() -> Result<()> {
    use crate::connectors::{create_connector_inventory, ConnectorCategory, ConnectorType};

    let inventory = create_connector_inventory();

    println!("╭─────────────────────────────────────────────────────────────────────╮");
    println!("│               Rivven Connect - Connector Catalog                     │");
    println!("╰─────────────────────────────────────────────────────────────────────╯\n");

    println!(
        "📊 Total: {} connectors ({} sources, {} sinks)\n",
        inventory.total_count(),
        inventory.source_count(),
        inventory.sink_count()
    );

    // Group by parent category
    let categories = [
        (
            "📁 DATABASE",
            vec![
                ConnectorCategory::DatabaseCdc,
                ConnectorCategory::DatabaseBatch,
                ConnectorCategory::DatabaseNosql,
            ],
        ),
        (
            "📨 MESSAGING",
            vec![
                ConnectorCategory::MessagingKafka,
                ConnectorCategory::MessagingMqtt,
                ConnectorCategory::MessagingCloud,
            ],
        ),
        (
            "📦 STORAGE",
            vec![
                ConnectorCategory::StorageObject,
                ConnectorCategory::StorageFile,
            ],
        ),
        ("🏢 WAREHOUSE", vec![ConnectorCategory::Warehouse]),
        (
            "🤖 AI/ML",
            vec![ConnectorCategory::AiLlm, ConnectorCategory::AiVector],
        ),
        ("🌐 HTTP/API", vec![ConnectorCategory::HttpApi]),
        (
            "🔧 UTILITY",
            vec![
                ConnectorCategory::UtilityGenerate,
                ConnectorCategory::UtilityDebug,
            ],
        ),
    ];

    for (parent_name, cats) in categories {
        let mut connectors: Vec<_> = cats
            .iter()
            .flat_map(|cat| inventory.by_category(*cat))
            .collect();

        if connectors.is_empty() {
            continue;
        }

        connectors.sort_by_key(|m| &m.name);
        connectors.dedup_by_key(|m| &m.name);

        println!("{}", parent_name);
        println!("─────────────────────────────────────────────────────────────────────");
        println!("{:<18} {:<10} Description", "Name", "Type");
        println!("─────────────────────────────────────────────────────────────────────");

        for meta in connectors {
            let types: Vec<_> = meta
                .types
                .iter()
                .map(|t| match t {
                    ConnectorType::Source => "Source",
                    ConnectorType::Sink => "Sink",
                    ConnectorType::Processor => "Proc",
                    ConnectorType::Cache => "Cache",
                    ConnectorType::Scanner => "Scan",
                })
                .collect();
            let types_str = types.join(",");

            // Truncate description if too long
            let desc = if meta.description.len() > 45 {
                format!("{}...", &meta.description[..42])
            } else {
                meta.description.clone()
            };

            println!("{:<18} {:<10} {}", meta.name, types_str, desc);
        }
        println!();
    }

    // Show feature flag status
    println!("🏷️  FEATURE FLAGS");
    println!("─────────────────────────────────────────────────────────────────────");

    let features = [
        (
            "postgres",
            cfg!(feature = "postgres"),
            "PostgreSQL CDC source",
        ),
        ("mysql", cfg!(feature = "mysql"), "MySQL/MariaDB CDC source"),
        ("http", cfg!(feature = "http"), "HTTP webhook sink"),
        ("kafka", cfg!(feature = "kafka"), "Kafka source & sink"),
        ("mqtt", cfg!(feature = "mqtt"), "MQTT source"),
        ("sqs", cfg!(feature = "sqs"), "AWS SQS source"),
        ("pubsub", cfg!(feature = "pubsub"), "Google Pub/Sub source"),
        ("s3", cfg!(feature = "s3"), "Amazon S3 sink"),
        ("gcs", cfg!(feature = "gcs"), "Google Cloud Storage sink"),
        ("azure", cfg!(feature = "azure"), "Azure Blob Storage sink"),
        ("snowflake", cfg!(feature = "snowflake"), "Snowflake sink"),
        (
            "bigquery",
            cfg!(feature = "bigquery"),
            "Google BigQuery sink",
        ),
        (
            "redshift",
            cfg!(feature = "redshift"),
            "Amazon Redshift sink",
        ),
    ];

    for (name, enabled, desc) in features {
        let status = if enabled { "✓" } else { "✗" };
        println!("  {} {:<14} {}", status, name, desc);
    }

    println!();
    println!("💡 TIP: Use 'rivven-connect schema <connector>' to see config options");
    println!("💡 TIP: Use 'rivven-connect discover <source>' to discover streams");

    Ok(())
}
