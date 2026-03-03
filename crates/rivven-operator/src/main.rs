//! Rivven Kubernetes Operator
//!
//! This operator manages RivvenCluster custom resources in Kubernetes,
//! automatically deploying and managing Rivven distributed streaming clusters.

mod cluster_client;
mod connect_controller;
mod controller;
#[allow(dead_code)] // CRD spec types are used by K8s API schema/serde, not constructed in Rust
mod crd;
mod error;
mod resources;
mod schema_registry_controller;
mod topic_controller;

use anyhow::{Context, Result};
use clap::Parser;
use k8s_openapi::api::coordination::v1::Lease;
use kube::api::{Api, PostParams};
use kube::Client;
use std::net::SocketAddr;
use tracing::{info, warn, Level};
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
        leader_election = args.leader_election,
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

    // If leader election is enabled, acquire a Lease before starting controllers.
    // This prevents multiple operator replicas from reconciling simultaneously,
    // which would cause conflicts (OPER-C1 fix).
    if args.leader_election {
        acquire_leader_lease(&client).await?;
        info!("Leader election: this instance is the leader");
    } else {
        warn!("Leader election DISABLED — only run a single replica to avoid conflicts");
    }

    // Run all controllers concurrently
    let cluster_handle = tokio::spawn({
        let client = client.clone();
        let namespace = namespace.clone();
        async move {
            controller::run_controller(client, namespace)
                .await
                .context("RivvenCluster controller failed")
        }
    });

    let connect_handle = tokio::spawn({
        let client = client.clone();
        let namespace = namespace.clone();
        async move {
            connect_controller::run_connect_controller(client, namespace)
                .await
                .context("RivvenConnect controller failed")
        }
    });

    let topic_handle = tokio::spawn({
        let client = client.clone();
        let namespace = namespace.clone();
        async move {
            topic_controller::run_topic_controller(client, namespace)
                .await
                .context("RivvenTopic controller failed")
        }
    });

    let schema_handle = tokio::spawn({
        let client = client.clone();
        let namespace = namespace.clone();
        async move {
            schema_registry_controller::run_schema_registry_controller(client, namespace)
                .await
                .context("RivvenSchemaRegistry controller failed")
        }
    });

    // Wait for any controller to finish (they run forever, so this means one failed)
    tokio::select! {
        res = cluster_handle => {
            res.context("Cluster controller task panicked")??;
        }
        res = connect_handle => {
            res.context("Connect controller task panicked")??;
        }
        res = topic_handle => {
            res.context("Topic controller task panicked")??;
        }
        res = schema_handle => {
            res.context("Schema registry controller task panicked")??;
        }
    }

    Ok(())
}

/// Acquire a Kubernetes Lease for leader election.
///
/// Blocks until this instance becomes the leader. Uses a Lease object in the
/// `kube-system` namespace (or the pod's own namespace via the downward API).
/// The Lease is renewed every 10s with a 15s duration.
///
/// This prevents multiple operator replicas from running controllers
/// simultaneously, avoiding reconciliation conflicts.
async fn acquire_leader_lease(client: &Client) -> Result<()> {
    use chrono::Utc;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;

    let lease_name = "rivven-operator-leader";
    // Try the pod namespace from the downward API, fall back to kube-system
    let lease_ns = std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "kube-system".to_string());
    let identity = std::env::var("POD_NAME")
        .unwrap_or_else(|_| format!("rivven-operator-{}", std::process::id()));

    let leases: Api<Lease> = Api::namespaced(client.clone(), &lease_ns);

    let lease_duration_secs = 15;
    let renew_interval = std::time::Duration::from_secs(10);

    info!(
        lease = lease_name,
        namespace = lease_ns,
        identity = identity,
        "Attempting to acquire leader lease"
    );

    loop {
        let now = Utc::now();
        let micro_now = MicroTime(now);

        match leases.get(lease_name).await {
            Ok(existing) => {
                let spec = existing.spec.as_ref();
                let holder = spec.and_then(|s| s.holder_identity.as_deref());
                let renew_time = spec.and_then(|s| s.renew_time.as_ref());
                let duration = spec.and_then(|s| s.lease_duration_seconds);

                // Check if existing lease is expired
                let expired = match (renew_time, duration) {
                    (Some(MicroTime(t)), Some(d)) => {
                        now.signed_duration_since(*t).num_seconds() > d as i64
                    }
                    _ => true, // No renew time means expired
                };

                if holder == Some(identity.as_str()) || expired {
                    // We own it or it's expired — take/renew
                    let patch = serde_json::json!({
                        "spec": {
                            "holderIdentity": identity,
                            "leaseDurationSeconds": lease_duration_secs,
                            "renewTime": micro_now,
                            "acquireTime": if holder == Some(identity.as_str()) {
                                spec.and_then(|s| s.acquire_time.clone())
                                    .unwrap_or_else(|| micro_now.clone())
                            } else {
                                micro_now.clone()
                            }
                        }
                    });
                    let params = kube::api::PatchParams::apply("rivven-operator");
                    leases
                        .patch(lease_name, &params, &kube::api::Patch::Merge(&patch))
                        .await
                        .context("Failed to update leader lease")?;

                    if expired && holder != Some(identity.as_str()) {
                        info!("Acquired leader lease (previous holder expired)");
                    }
                    // Start background renewal with lease-loss detection.
                    // If renewal fails repeatedly (>= lease_duration_secs),
                    // the process exits to avoid split-brain.
                    let leases_bg = leases.clone();
                    let identity_bg = identity.clone();
                    let max_consecutive_failures = (lease_duration_secs as u64)
                        .div_ceil(renew_interval.as_secs())
                        .max(2);
                    tokio::spawn(async move {
                        let mut consecutive_failures: u64 = 0;
                        loop {
                            tokio::time::sleep(renew_interval).await;
                            let now = Utc::now();
                            let patch = serde_json::json!({
                                "spec": {
                                    "holderIdentity": identity_bg,
                                    "renewTime": MicroTime(now),
                                    "leaseDurationSeconds": lease_duration_secs,
                                }
                            });
                            let params = kube::api::PatchParams::apply("rivven-operator");
                            match leases_bg
                                .patch(lease_name, &params, &kube::api::Patch::Merge(&patch))
                                .await
                            {
                                Ok(_) => {
                                    consecutive_failures = 0;
                                }
                                Err(e) => {
                                    consecutive_failures += 1;
                                    tracing::error!(
                                        error = %e,
                                        consecutive_failures,
                                        "Failed to renew leader lease"
                                    );
                                    if consecutive_failures >= max_consecutive_failures {
                                        tracing::error!(
                                            "Lost leader lease after {} consecutive renewal failures — exiting to prevent split-brain",
                                            consecutive_failures
                                        );
                                        std::process::exit(1);
                                    }
                                }
                            }
                        }
                    });
                    return Ok(());
                }
                // Someone else holds it — wait and retry
                info!(
                    holder = holder.unwrap_or("unknown"),
                    "Lease held by another instance, waiting..."
                );
            }
            Err(kube::Error::Api(ae)) if ae.code == 404 => {
                // Lease doesn't exist — create it
                let lease = Lease {
                    metadata: kube::api::ObjectMeta {
                        name: Some(lease_name.to_string()),
                        namespace: Some(lease_ns.clone()),
                        ..Default::default()
                    },
                    spec: Some(k8s_openapi::api::coordination::v1::LeaseSpec {
                        holder_identity: Some(identity.clone()),
                        lease_duration_seconds: Some(lease_duration_secs),
                        acquire_time: Some(micro_now.clone()),
                        renew_time: Some(micro_now),
                        ..Default::default()
                    }),
                };
                match leases.create(&PostParams::default(), &lease).await {
                    Ok(_) => {
                        info!("Created and acquired leader lease");
                        // Start background renewal with lease-loss detection
                        let leases_bg = leases.clone();
                        let identity_bg = identity.clone();
                        let max_consecutive_failures = (lease_duration_secs as u64)
                            .div_ceil(renew_interval.as_secs())
                            .max(2);
                        tokio::spawn(async move {
                            let mut consecutive_failures: u64 = 0;
                            loop {
                                tokio::time::sleep(renew_interval).await;
                                let now = Utc::now();
                                let patch = serde_json::json!({
                                    "spec": {
                                        "holderIdentity": identity_bg,
                                        "renewTime": MicroTime(now),
                                        "leaseDurationSeconds": lease_duration_secs,
                                    }
                                });
                                let params = kube::api::PatchParams::apply("rivven-operator");
                                match leases_bg
                                    .patch(lease_name, &params, &kube::api::Patch::Merge(&patch))
                                    .await
                                {
                                    Ok(_) => {
                                        consecutive_failures = 0;
                                    }
                                    Err(e) => {
                                        consecutive_failures += 1;
                                        tracing::error!(
                                            error = %e,
                                            consecutive_failures,
                                            "Failed to renew leader lease"
                                        );
                                        if consecutive_failures >= max_consecutive_failures {
                                            tracing::error!(
                                                "Lost leader lease after {} consecutive renewal failures — exiting to prevent split-brain",
                                                consecutive_failures
                                            );
                                            std::process::exit(1);
                                        }
                                    }
                                }
                            }
                        });
                        return Ok(());
                    }
                    Err(kube::Error::Api(ae)) if ae.code == 409 => {
                        // Another instance created it first — retry
                        info!("Lease creation conflict, retrying...");
                    }
                    Err(e) => return Err(e).context("Failed to create leader lease"),
                }
            }
            Err(e) => return Err(e).context("Failed to get leader lease"),
        }

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
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
                if let Err(e) = socket.write_all(response.as_bytes()).await {
                    tracing::debug!(error = %e, "Health check response write failed");
                }
            }
        });
    }
}

/// Print all CRD YAMLs for installation
fn print_crd() -> Result<()> {
    use kube::CustomResourceExt;

    let crds = [
        crd::RivvenCluster::crd(),
        crd::RivvenConnect::crd(),
        crd::RivvenTopic::crd(),
        crd::RivvenSchemaRegistry::crd(),
    ];

    for (i, crd) in crds.iter().enumerate() {
        if i > 0 {
            println!("---");
        }
        let yaml = serde_yaml::to_string(crd)?;
        print!("{}", yaml);
    }

    Ok(())
}

pub mod prelude {
    //! Re-exports for convenient usage
    pub use crate::connect_controller::run_connect_controller;
    pub use crate::controller::run_controller;
    pub use crate::crd::{
        RivvenCluster, RivvenClusterSpec, RivvenClusterStatus, RivvenConnect, RivvenConnectSpec,
        RivvenConnectStatus, RivvenSchemaRegistry, RivvenTopic,
    };
    pub use crate::error::{OperatorError, Result};
    pub use crate::schema_registry_controller::run_schema_registry_controller;
    pub use crate::topic_controller::run_topic_controller;
}
