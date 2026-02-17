//! RivvenConnect Controller
//!
//! This module implements the Kubernetes controller for managing RivvenConnect
//! custom resources. It watches for changes and reconciles connector pipelines.

use crate::crd::{
    ClusterReference, ConnectCondition, ConnectPhase, ConnectorStatus, RivvenConnect,
    RivvenConnectSpec, RivvenConnectStatus,
};
use crate::error::{OperatorError, Result};
use chrono::Utc;
use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::finalizer::{finalizer, Event as FinalizerEvent};
use kube::runtime::watcher::Config;
use kube::{Client, Resource, ResourceExt};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};
use validator::Validate;

/// Finalizer name for cleanup operations
pub const CONNECT_FINALIZER: &str = "rivven.hupe1980.github.io/connect-finalizer";

/// Default requeue interval for successful reconciliations
const DEFAULT_REQUEUE_SECONDS: u64 = 60; // 1 minute

/// Requeue interval for error cases
const ERROR_REQUEUE_SECONDS: u64 = 30;

/// Context passed to the connect controller
pub struct ConnectControllerContext {
    /// Kubernetes client
    pub client: Client,
    /// Metrics recorder
    pub metrics: Option<ConnectControllerMetrics>,
}

/// Metrics for the connect controller
#[derive(Clone)]
pub struct ConnectControllerMetrics {
    /// Counter for reconciliation attempts
    pub reconciliations: metrics::Counter,
    /// Counter for reconciliation errors
    pub errors: metrics::Counter,
    /// Histogram for reconciliation duration
    pub duration: metrics::Histogram,
}

impl ConnectControllerMetrics {
    /// Create new connect controller metrics
    pub fn new() -> Self {
        Self {
            reconciliations: metrics::counter!("rivven_connect_reconciliations_total"),
            errors: metrics::counter!("rivven_connect_reconciliation_errors_total"),
            duration: metrics::histogram!("rivven_connect_reconciliation_duration_seconds"),
        }
    }
}

impl Default for ConnectControllerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Start the RivvenConnect controller
pub async fn run_connect_controller(client: Client, namespace: Option<String>) -> Result<()> {
    let connects: Api<RivvenConnect> = match &namespace {
        Some(ns) => Api::namespaced(client.clone(), ns),
        None => Api::all(client.clone()),
    };

    let ctx = Arc::new(ConnectControllerContext {
        client: client.clone(),
        metrics: Some(ConnectControllerMetrics::new()),
    });

    info!(
        namespace = namespace.as_deref().unwrap_or("all"),
        "Starting RivvenConnect controller"
    );

    // Watch related resources for changes
    let deployments = match &namespace {
        Some(ns) => Api::<Deployment>::namespaced(client.clone(), ns),
        None => Api::<Deployment>::all(client.clone()),
    };

    let configmaps = match &namespace {
        Some(ns) => Api::<ConfigMap>::namespaced(client.clone(), ns),
        None => Api::<ConfigMap>::all(client.clone()),
    };

    Controller::new(connects.clone(), Config::default())
        .owns(deployments, Config::default())
        .owns(configmaps, Config::default())
        .run(reconcile_connect, connect_error_policy, ctx)
        .for_each(|result| async move {
            match result {
                Ok((obj, action)) => {
                    debug!(
                        name = obj.name,
                        namespace = obj.namespace,
                        ?action,
                        "Connect reconciliation completed"
                    );
                }
                Err(e) => {
                    error!(error = %e, "Connect reconciliation failed");
                }
            }
        })
        .await;

    Ok(())
}

/// Main reconciliation function for RivvenConnect
#[instrument(skip(connect, ctx), fields(name = %connect.name_any(), namespace = connect.namespace()))]
async fn reconcile_connect(
    connect: Arc<RivvenConnect>,
    ctx: Arc<ConnectControllerContext>,
) -> Result<Action> {
    let start = std::time::Instant::now();

    if let Some(ref metrics) = ctx.metrics {
        metrics.reconciliations.increment(1);
    }

    let namespace = connect.namespace().unwrap_or_else(|| "default".to_string());
    let connects: Api<RivvenConnect> = Api::namespaced(ctx.client.clone(), &namespace);

    let result = finalizer(&connects, CONNECT_FINALIZER, connect, |event| async {
        match event {
            FinalizerEvent::Apply(connect) => apply_connect(connect, ctx.clone()).await,
            FinalizerEvent::Cleanup(connect) => cleanup_connect(connect, ctx.clone()).await,
        }
    })
    .await;

    if let Some(ref metrics) = ctx.metrics {
        metrics.duration.record(start.elapsed().as_secs_f64());
    }

    result.map_err(|e| {
        if let Some(ref metrics) = ctx.metrics {
            metrics.errors.increment(1);
        }
        OperatorError::ReconcileFailed(e.to_string())
    })
}

/// Apply (create/update) the connect resources
#[instrument(skip(connect, ctx))]
async fn apply_connect(
    connect: Arc<RivvenConnect>,
    ctx: Arc<ConnectControllerContext>,
) -> Result<Action> {
    let name = connect.name_any();
    let namespace = connect.namespace().unwrap_or_else(|| "default".to_string());

    info!(name = %name, namespace = %namespace, "Reconciling RivvenConnect");

    // Validate the connect spec
    if let Err(errors) = connect.spec.validate() {
        let error_messages: Vec<String> = errors
            .field_errors()
            .iter()
            .flat_map(|(field, errs)| {
                errs.iter()
                    .map(move |e| format!("{}: {:?}", field, e.message))
            })
            .collect();
        let error_msg = error_messages.join("; ");
        warn!(name = %name, errors = %error_msg, "Connect spec validation failed");

        // Update status to failed
        update_connect_status(
            &ctx.client,
            &namespace,
            &name,
            build_failed_status(&connect, &error_msg),
        )
        .await?;

        return Err(OperatorError::InvalidConfig(error_msg));
    }

    // Verify cluster reference exists
    verify_cluster_ref(&ctx.client, &namespace, &connect.spec.cluster_ref).await?;

    // Build and apply ConfigMap with pipeline configuration
    let configmap = build_connect_configmap(&connect)?;
    apply_connect_configmap(&ctx.client, &namespace, configmap).await?;

    // Build and apply Deployment
    let deployment = build_connect_deployment(&connect)?;
    let deploy_status = apply_connect_deployment(&ctx.client, &namespace, deployment).await?;

    // Build and apply headless Service for connect workers
    let service = build_connect_service(&connect)?;
    apply_connect_service(&ctx.client, &namespace, service).await?;

    // Update connect status
    let status = build_connect_status(&connect, deploy_status);
    update_connect_status(&ctx.client, &namespace, &name, status).await?;

    info!(name = %name, "Connect reconciliation complete");

    Ok(Action::requeue(Duration::from_secs(
        DEFAULT_REQUEUE_SECONDS,
    )))
}

/// Verify the referenced RivvenCluster exists
async fn verify_cluster_ref(
    client: &Client,
    namespace: &str,
    cluster_ref: &ClusterReference,
) -> Result<()> {
    let cluster_ns = cluster_ref.namespace.as_deref().unwrap_or(namespace);
    let clusters: Api<crate::crd::RivvenCluster> = Api::namespaced(client.clone(), cluster_ns);

    match clusters.get(&cluster_ref.name).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(ae)) if ae.code == 404 => Err(OperatorError::ClusterNotFound(
            format!("{}/{}", cluster_ns, cluster_ref.name),
        )),
        Err(e) => Err(OperatorError::from(e)),
    }
}

/// Build ConfigMap containing the connect pipeline configuration
fn build_connect_configmap(connect: &RivvenConnect) -> Result<ConfigMap> {
    let name = connect.name_any();
    let namespace = connect.namespace().unwrap_or_else(|| "default".to_string());

    // Build pipeline YAML configuration
    let pipeline_config = build_pipeline_yaml(&connect.spec)?;

    let mut labels = BTreeMap::new();
    labels.insert(
        "app.kubernetes.io/name".to_string(),
        "rivven-connect".to_string(),
    );
    labels.insert("app.kubernetes.io/instance".to_string(), name.clone());
    labels.insert(
        "app.kubernetes.io/component".to_string(),
        "connect".to_string(),
    );
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        "rivven-operator".to_string(),
    );

    let mut data = BTreeMap::new();
    data.insert("pipeline.yaml".to_string(), pipeline_config);

    Ok(ConfigMap {
        metadata: ObjectMeta {
            name: Some(format!("rivven-connect-{}", name)),
            namespace: Some(namespace),
            labels: Some(labels),
            owner_references: Some(vec![connect.controller_owner_ref(&()).ok_or_else(
                || {
                    OperatorError::InvalidConfig(
                        "Failed to generate owner reference for ConfigMap".into(),
                    )
                },
            )?]),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    })
}

/// Build the pipeline YAML from the spec
fn build_pipeline_yaml(spec: &RivvenConnectSpec) -> Result<String> {
    use std::fmt::Write;

    let mut yaml = String::new();

    writeln!(yaml, "version: \"1.0\"").map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
    writeln!(yaml).map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;

    // Broker configuration
    writeln!(yaml, "broker:").map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
    writeln!(yaml, "  bootstrap_servers:")
        .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;

    // Use cluster service discovery
    let cluster_ns = spec.cluster_ref.namespace.as_deref().unwrap_or("default");
    let cluster_svc = format!(
        "rivven-{}.{}.svc.cluster.local:9092",
        spec.cluster_ref.name, cluster_ns
    );
    writeln!(yaml, "    - {}", cluster_svc)
        .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
    writeln!(yaml).map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;

    // Global settings
    if !spec.sources.is_empty() || !spec.sinks.is_empty() {
        writeln!(yaml, "settings:").map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
        writeln!(yaml, "  topic:").map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
        writeln!(yaml, "    auto_create: {}", spec.settings.topic.auto_create)
            .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
        writeln!(
            yaml,
            "    default_partitions: {}",
            spec.settings.topic.default_partitions
        )
        .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
        writeln!(yaml).map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
    }

    // Sources
    if !spec.sources.is_empty() {
        writeln!(yaml, "sources:").map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
        for source in &spec.sources {
            writeln!(yaml, "  {}:", source.name)
                .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
            writeln!(yaml, "    connector: {}", source.connector)
                .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
            writeln!(yaml, "    topic: {}", source.topic)
                .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
            writeln!(yaml, "    enabled: {}", source.enabled)
                .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;

            // Write config - merge topic_routing into config for CDC connectors
            // topic_routing is CDC-specific and must be in the connector config section
            let mut config = source.config.clone();
            if let Some(ref routing) = source.topic_routing {
                if let serde_json::Value::Object(ref mut map) = config {
                    map.insert("topic_routing".to_string(), serde_json::json!(routing));
                } else if config.is_null() {
                    config = serde_json::json!({"topic_routing": routing});
                }
            }

            if !config.is_null() {
                writeln!(yaml, "    config:")
                    .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
                // Serialize JSON config to YAML
                let config_str = serde_json::to_string_pretty(&config)
                    .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
                for line in config_str.lines() {
                    writeln!(yaml, "      {}", line)
                        .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
                }
            }
        }
        writeln!(yaml).map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
    }

    // Sinks
    if !spec.sinks.is_empty() {
        writeln!(yaml, "sinks:").map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
        for sink in &spec.sinks {
            writeln!(yaml, "  {}:", sink.name)
                .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
            writeln!(yaml, "    connector: {}", sink.connector)
                .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
            writeln!(yaml, "    topics: {:?}", sink.topics)
                .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
            writeln!(yaml, "    consumer_group: {}", sink.consumer_group)
                .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
            writeln!(yaml, "    enabled: {}", sink.enabled)
                .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;

            // Write config if not null/empty
            if !sink.config.is_null() {
                writeln!(yaml, "    config:")
                    .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
                let config_str = serde_json::to_string_pretty(&sink.config)
                    .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
                for line in config_str.lines() {
                    writeln!(yaml, "      {}", line)
                        .map_err(|e| OperatorError::InvalidConfig(e.to_string()))?;
                }
            }
        }
    }

    Ok(yaml)
}

/// Build the Deployment for connect workers
fn build_connect_deployment(connect: &RivvenConnect) -> Result<Deployment> {
    let name = connect.name_any();
    let namespace = connect.namespace().unwrap_or_else(|| "default".to_string());
    let spec = &connect.spec;

    // Immutable selector labels (must not include user pod_labels)
    let selector_labels: BTreeMap<String, String> = [
        (
            "app.kubernetes.io/name".to_string(),
            "rivven-connect".to_string(),
        ),
        ("app.kubernetes.io/instance".to_string(), name.clone()),
    ]
    .into();

    let mut labels = BTreeMap::new();
    labels.insert(
        "app.kubernetes.io/name".to_string(),
        "rivven-connect".to_string(),
    );
    labels.insert("app.kubernetes.io/instance".to_string(), name.clone());
    labels.insert(
        "app.kubernetes.io/component".to_string(),
        "connect".to_string(),
    );
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        "rivven-operator".to_string(),
    );

    // Merge user labels (only into template labels, not selector)
    for (k, v) in &spec.pod_labels {
        if !k.starts_with("app.kubernetes.io/") {
            labels.insert(k.clone(), v.clone());
        }
    }

    let image = spec
        .image
        .clone()
        .unwrap_or_else(|| format!("ghcr.io/hupe1980/rivven-connect:{}", spec.version));

    // Build container
    let container = k8s_openapi::api::core::v1::Container {
        name: "connect".to_string(),
        image: Some(image),
        image_pull_policy: Some(spec.image_pull_policy.clone()),
        args: Some(vec![
            "run".to_string(),
            "--config".to_string(),
            "/config/pipeline.yaml".to_string(),
        ]),
        env: Some(spec.env.clone()),
        volume_mounts: Some(vec![
            k8s_openapi::api::core::v1::VolumeMount {
                name: "config".to_string(),
                mount_path: "/config".to_string(),
                read_only: Some(true),
                ..Default::default()
            },
            k8s_openapi::api::core::v1::VolumeMount {
                name: "data".to_string(),
                mount_path: "/data".to_string(),
                ..Default::default()
            },
        ]),
        resources: spec
            .resources
            .as_ref()
            .map(|r| serde_json::from_value(r.clone()))
            .transpose()
            .map_err(|e| {
                OperatorError::InvalidConfig(format!("invalid resources config: {}", e))
            })?,
        security_context: {
            let custom = spec
                .container_security_context
                .as_ref()
                .map(|sc| serde_json::from_value(sc.clone()))
                .transpose()
                .map_err(|e| {
                    OperatorError::InvalidConfig(format!(
                        "invalid container security context: {}",
                        e
                    ))
                })?;
            Some(
                custom.unwrap_or_else(|| k8s_openapi::api::core::v1::SecurityContext {
                    allow_privilege_escalation: Some(false),
                    read_only_root_filesystem: Some(true),
                    run_as_non_root: Some(true),
                    run_as_user: Some(1000),
                    run_as_group: Some(1000),
                    capabilities: Some(k8s_openapi::api::core::v1::Capabilities {
                        drop: Some(vec!["ALL".to_string()]),
                        ..Default::default()
                    }),
                    seccomp_profile: Some(k8s_openapi::api::core::v1::SeccompProfile {
                        type_: "RuntimeDefault".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            )
        },
        liveness_probe: Some(k8s_openapi::api::core::v1::Probe {
            http_get: Some(k8s_openapi::api::core::v1::HTTPGetAction {
                path: Some("/health".to_string()),
                port: k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(8080),
                ..Default::default()
            }),
            initial_delay_seconds: Some(30),
            period_seconds: Some(10),
            ..Default::default()
        }),
        readiness_probe: Some(k8s_openapi::api::core::v1::Probe {
            http_get: Some(k8s_openapi::api::core::v1::HTTPGetAction {
                path: Some("/ready".to_string()),
                port: k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(8080),
                ..Default::default()
            }),
            initial_delay_seconds: Some(10),
            period_seconds: Some(5),
            ..Default::default()
        }),
        ports: Some(vec![
            k8s_openapi::api::core::v1::ContainerPort {
                name: Some("http".to_string()),
                container_port: 8080,
                ..Default::default()
            },
            k8s_openapi::api::core::v1::ContainerPort {
                name: Some("metrics".to_string()),
                container_port: 9090,
                ..Default::default()
            },
        ]),
        ..Default::default()
    };

    // Build volumes
    let volumes = vec![
        k8s_openapi::api::core::v1::Volume {
            name: "config".to_string(),
            config_map: Some(k8s_openapi::api::core::v1::ConfigMapVolumeSource {
                name: format!("rivven-connect-{}", name),
                ..Default::default()
            }),
            ..Default::default()
        },
        k8s_openapi::api::core::v1::Volume {
            name: "data".to_string(),
            empty_dir: Some(k8s_openapi::api::core::v1::EmptyDirVolumeSource::default()),
            ..Default::default()
        },
    ];

    // Build image pull secrets
    let image_pull_secrets: Option<Vec<_>> = if spec.image_pull_secrets.is_empty() {
        None
    } else {
        Some(
            spec.image_pull_secrets
                .iter()
                .map(|s| k8s_openapi::api::core::v1::LocalObjectReference { name: s.clone() })
                .collect(),
        )
    };

    let pod_spec = k8s_openapi::api::core::v1::PodSpec {
        containers: vec![container],
        volumes: Some(volumes),
        image_pull_secrets,
        service_account_name: spec.service_account.clone(),
        node_selector: if spec.node_selector.is_empty() {
            None
        } else {
            Some(spec.node_selector.clone())
        },
        tolerations: if spec.tolerations.is_empty() {
            None
        } else {
            Some(spec.tolerations.clone())
        },
        affinity: spec
            .affinity
            .as_ref()
            .map(|a| serde_json::from_value(a.clone()))
            .transpose()
            .map_err(|e| OperatorError::InvalidConfig(format!("invalid affinity config: {}", e)))?,
        security_context: {
            let custom = spec
                .security_context
                .as_ref()
                .map(|sc| serde_json::from_value(sc.clone()))
                .transpose()
                .map_err(|e| {
                    OperatorError::InvalidConfig(format!("invalid pod security context: {}", e))
                })?;
            Some(
                custom.unwrap_or_else(|| k8s_openapi::api::core::v1::PodSecurityContext {
                    run_as_non_root: Some(true),
                    run_as_user: Some(1000),
                    run_as_group: Some(1000),
                    fs_group: Some(1000),
                    seccomp_profile: Some(k8s_openapi::api::core::v1::SeccompProfile {
                        type_: "RuntimeDefault".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            )
        },
        automount_service_account_token: Some(false),
        ..Default::default()
    };

    Ok(Deployment {
        metadata: ObjectMeta {
            name: Some(format!("rivven-connect-{}", name)),
            namespace: Some(namespace),
            labels: Some(labels.clone()),
            owner_references: Some(vec![connect.controller_owner_ref(&()).ok_or_else(
                || {
                    OperatorError::InvalidConfig(
                        "Failed to generate owner reference for Deployment".into(),
                    )
                },
            )?]),
            ..Default::default()
        },
        spec: Some(k8s_openapi::api::apps::v1::DeploymentSpec {
            replicas: Some(spec.replicas),
            selector: k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                match_labels: Some(selector_labels),
                ..Default::default()
            },
            template: k8s_openapi::api::core::v1::PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    annotations: Some(spec.pod_annotations.clone()),
                    ..Default::default()
                }),
                spec: Some(pod_spec),
            },
            ..Default::default()
        }),
        ..Default::default()
    })
}

/// Build the Service for connect workers
fn build_connect_service(connect: &RivvenConnect) -> Result<Service> {
    let name = connect.name_any();
    let namespace = connect.namespace().unwrap_or_else(|| "default".to_string());

    // Selector labels must match the Deployment selector (immutable 2-label set)
    let selector_labels: BTreeMap<String, String> = [
        (
            "app.kubernetes.io/name".to_string(),
            "rivven-connect".to_string(),
        ),
        ("app.kubernetes.io/instance".to_string(), name.clone()),
    ]
    .into();

    let mut labels = selector_labels.clone();
    labels.insert(
        "app.kubernetes.io/component".to_string(),
        "connect".to_string(),
    );
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        "rivven-operator".to_string(),
    );

    Ok(Service {
        metadata: ObjectMeta {
            name: Some(format!("rivven-connect-{}", name)),
            namespace: Some(namespace),
            labels: Some(labels),
            owner_references: Some(vec![connect.controller_owner_ref(&()).ok_or_else(
                || {
                    OperatorError::InvalidConfig(
                        "Failed to generate owner reference for Service".into(),
                    )
                },
            )?]),
            ..Default::default()
        },
        spec: Some(k8s_openapi::api::core::v1::ServiceSpec {
            selector: Some(selector_labels),
            ports: Some(vec![
                k8s_openapi::api::core::v1::ServicePort {
                    name: Some("http".to_string()),
                    port: 8080,
                    target_port: Some(
                        k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(8080),
                    ),
                    ..Default::default()
                },
                k8s_openapi::api::core::v1::ServicePort {
                    name: Some("metrics".to_string()),
                    port: 9090,
                    target_port: Some(
                        k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(9090),
                    ),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    })
}

/// Apply ConfigMap using server-side apply
async fn apply_connect_configmap(client: &Client, namespace: &str, cm: ConfigMap) -> Result<()> {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let name =
        cm.metadata.name.as_ref().ok_or_else(|| {
            OperatorError::InvalidConfig("ConfigMap missing metadata.name".into())
        })?;

    debug!(name = %name, "Applying Connect ConfigMap");

    let patch_params = PatchParams::apply("rivven-operator").force();
    api.patch(name, &patch_params, &Patch::Apply(&cm))
        .await
        .map_err(OperatorError::from)?;

    Ok(())
}

/// Apply Deployment using server-side apply
async fn apply_connect_deployment(
    client: &Client,
    namespace: &str,
    deployment: Deployment,
) -> Result<Option<k8s_openapi::api::apps::v1::DeploymentStatus>> {
    let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let name =
        deployment.metadata.name.as_ref().ok_or_else(|| {
            OperatorError::InvalidConfig("Deployment missing metadata.name".into())
        })?;

    debug!(name = %name, "Applying Connect Deployment");

    let patch_params = PatchParams::apply("rivven-operator").force();
    let result = api
        .patch(name, &patch_params, &Patch::Apply(&deployment))
        .await
        .map_err(OperatorError::from)?;

    Ok(result.status)
}

/// Apply Service using server-side apply
async fn apply_connect_service(client: &Client, namespace: &str, svc: Service) -> Result<()> {
    let api: Api<Service> = Api::namespaced(client.clone(), namespace);
    let name = svc
        .metadata
        .name
        .as_ref()
        .ok_or_else(|| OperatorError::InvalidConfig("Service missing metadata.name".into()))?;

    debug!(name = %name, "Applying Connect Service");

    let patch_params = PatchParams::apply("rivven-operator").force();
    api.patch(name, &patch_params, &Patch::Apply(&svc))
        .await
        .map_err(OperatorError::from)?;

    Ok(())
}

/// Build connect status from deployment status
fn build_connect_status(
    connect: &RivvenConnect,
    deploy_status: Option<k8s_openapi::api::apps::v1::DeploymentStatus>,
) -> RivvenConnectStatus {
    let now = Utc::now().to_rfc3339();
    let spec = &connect.spec;

    let (replicas, ready_replicas, updated_replicas) = deploy_status
        .map(|s| {
            (
                s.replicas.unwrap_or(0),
                s.ready_replicas.unwrap_or(0),
                s.updated_replicas.unwrap_or(0),
            )
        })
        .unwrap_or((0, 0, 0));

    let desired_replicas = spec.replicas;

    // Determine phase
    let phase = if ready_replicas == 0 {
        ConnectPhase::Pending
    } else if ready_replicas < desired_replicas {
        if updated_replicas < desired_replicas {
            ConnectPhase::Starting
        } else {
            ConnectPhase::Degraded
        }
    } else {
        ConnectPhase::Running
    };

    // Count configured connectors
    let sources_total = spec.sources.len() as i32;
    let sinks_total = spec.sinks.len() as i32;

    // Estimate running connectors (simplified - in production, query the connect workers)
    let sources_running = if phase == ConnectPhase::Running {
        spec.sources.iter().filter(|s| s.enabled).count() as i32
    } else {
        0
    };
    let sinks_running = if phase == ConnectPhase::Running {
        spec.sinks.iter().filter(|s| s.enabled).count() as i32
    } else {
        0
    };

    // Build conditions
    let mut conditions = vec![];

    conditions.push(ConnectCondition {
        condition_type: "Ready".to_string(),
        status: if ready_replicas >= desired_replicas {
            "True".to_string()
        } else {
            "False".to_string()
        },
        reason: Some(format!(
            "{}/{} replicas ready",
            ready_replicas, desired_replicas
        )),
        message: None,
        last_transition_time: Some(now.clone()),
    });

    conditions.push(ConnectCondition {
        condition_type: "BrokerConnected".to_string(),
        status: if phase == ConnectPhase::Running {
            "True".to_string()
        } else {
            "Unknown".to_string()
        },
        reason: Some("ClusterRefValid".to_string()),
        message: None,
        last_transition_time: Some(now.clone()),
    });

    conditions.push(ConnectCondition {
        condition_type: "SourcesHealthy".to_string(),
        status: if sources_running == sources_total && sources_total > 0 {
            "True".to_string()
        } else if sources_running > 0 {
            "Partial".to_string()
        } else if sources_total == 0 {
            "N/A".to_string()
        } else {
            "False".to_string()
        },
        reason: Some(format!(
            "{}/{} sources running",
            sources_running, sources_total
        )),
        message: None,
        last_transition_time: Some(now.clone()),
    });

    conditions.push(ConnectCondition {
        condition_type: "SinksHealthy".to_string(),
        status: if sinks_running == sinks_total && sinks_total > 0 {
            "True".to_string()
        } else if sinks_running > 0 {
            "Partial".to_string()
        } else if sinks_total == 0 {
            "N/A".to_string()
        } else {
            "False".to_string()
        },
        reason: Some(format!("{}/{} sinks running", sinks_running, sinks_total)),
        message: None,
        last_transition_time: Some(now.clone()),
    });

    // Build connector statuses
    let mut connector_statuses = Vec::new();

    for source in &spec.sources {
        connector_statuses.push(ConnectorStatus {
            name: source.name.clone(),
            connector_type: "source".to_string(),
            kind: source.connector.clone(),
            state: if source.enabled && phase == ConnectPhase::Running {
                "running".to_string()
            } else if !source.enabled {
                "disabled".to_string()
            } else {
                "pending".to_string()
            },
            events_processed: 0, // Would need to query metrics
            last_error: None,
            last_success_time: None,
        });
    }

    for sink in &spec.sinks {
        connector_statuses.push(ConnectorStatus {
            name: sink.name.clone(),
            connector_type: "sink".to_string(),
            kind: sink.connector.clone(),
            state: if sink.enabled && phase == ConnectPhase::Running {
                "running".to_string()
            } else if !sink.enabled {
                "disabled".to_string()
            } else {
                "pending".to_string()
            },
            events_processed: 0,
            last_error: None,
            last_success_time: None,
        });
    }

    RivvenConnectStatus {
        phase,
        replicas,
        ready_replicas,
        sources_running,
        sinks_running,
        sources_total,
        sinks_total,
        observed_generation: connect.metadata.generation.unwrap_or(0),
        conditions,
        connector_statuses,
        last_updated: Some(now),
        message: None,
    }
}

/// Build a failed status
fn build_failed_status(connect: &RivvenConnect, error_msg: &str) -> RivvenConnectStatus {
    let now = Utc::now().to_rfc3339();

    RivvenConnectStatus {
        phase: ConnectPhase::Failed,
        replicas: 0,
        ready_replicas: 0,
        sources_running: 0,
        sinks_running: 0,
        sources_total: connect.spec.sources.len() as i32,
        sinks_total: connect.spec.sinks.len() as i32,
        observed_generation: connect.metadata.generation.unwrap_or(0),
        conditions: vec![ConnectCondition {
            condition_type: "Ready".to_string(),
            status: "False".to_string(),
            reason: Some("ValidationFailed".to_string()),
            message: Some(error_msg.to_string()),
            last_transition_time: Some(now.clone()),
        }],
        connector_statuses: vec![],
        last_updated: Some(now),
        message: Some(error_msg.to_string()),
    }
}

/// Update the connect status subresource
async fn update_connect_status(
    client: &Client,
    namespace: &str,
    name: &str,
    status: RivvenConnectStatus,
) -> Result<()> {
    let api: Api<RivvenConnect> = Api::namespaced(client.clone(), namespace);

    debug!(name = %name, phase = ?status.phase, "Updating connect status");

    let patch = serde_json::json!({
        "status": status
    });

    let patch_params = PatchParams::default();
    api.patch_status(name, &patch_params, &Patch::Merge(&patch))
        .await
        .map_err(OperatorError::from)?;

    Ok(())
}

/// Cleanup resources when connect is deleted
#[instrument(skip(connect, _ctx))]
async fn cleanup_connect(
    connect: Arc<RivvenConnect>,
    _ctx: Arc<ConnectControllerContext>,
) -> Result<Action> {
    let name = connect.name_any();
    let namespace = connect.namespace().unwrap_or_else(|| "default".to_string());

    info!(name = %name, namespace = %namespace, "Cleaning up RivvenConnect resources");

    // Resources with owner references will be garbage collected automatically
    // Additional cleanup could include:
    // 1. Graceful connector shutdown
    // 2. Offset commit for sinks
    // 3. External resource cleanup

    info!(name = %name, "Connect cleanup complete");

    Ok(Action::await_change())
}

/// Error policy for the connect controller
fn connect_error_policy(
    _connect: Arc<RivvenConnect>,
    error: &OperatorError,
    _ctx: Arc<ConnectControllerContext>,
) -> Action {
    warn!(
        error = %error,
        "Connect reconciliation error, will retry"
    );

    let delay = error
        .requeue_delay()
        .unwrap_or_else(|| Duration::from_secs(ERROR_REQUEUE_SECONDS));

    Action::requeue(delay)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        ConnectConfigSpec, ConnectTlsSpec, GlobalConnectSettings, RateLimitSpec, SinkConnectorSpec,
        SourceConnectorSpec, SourceTopicConfigSpec,
    };

    fn create_test_connect() -> RivvenConnect {
        RivvenConnect {
            metadata: ObjectMeta {
                name: Some("test-connect".to_string()),
                namespace: Some("default".to_string()),
                uid: Some("test-uid".to_string()),
                generation: Some(1),
                ..Default::default()
            },
            spec: RivvenConnectSpec {
                cluster_ref: ClusterReference {
                    name: "test-cluster".to_string(),
                    namespace: None,
                },
                replicas: 2,
                version: "0.0.1".to_string(),
                image: None,
                image_pull_policy: "IfNotPresent".to_string(),
                image_pull_secrets: vec![],
                resources: None,
                config: ConnectConfigSpec::default(),
                sources: vec![SourceConnectorSpec {
                    name: "test-source".to_string(),
                    connector: "datagen".to_string(),
                    topic: "test-topic".to_string(),
                    topic_routing: None,
                    enabled: true,
                    config: serde_json::Value::Null,
                    config_secret_ref: None,
                    topic_config: SourceTopicConfigSpec::default(),
                }],
                sinks: vec![SinkConnectorSpec {
                    name: "test-sink".to_string(),
                    connector: "stdout".to_string(),
                    topics: vec!["test-topic".to_string()],
                    consumer_group: "test-group".to_string(),
                    enabled: true,
                    start_offset: "latest".to_string(),
                    config: serde_json::Value::Null,
                    config_secret_ref: None,
                    rate_limit: RateLimitSpec::default(),
                }],
                settings: GlobalConnectSettings::default(),
                tls: ConnectTlsSpec::default(),
                pod_annotations: BTreeMap::new(),
                pod_labels: BTreeMap::new(),
                env: vec![],
                node_selector: BTreeMap::new(),
                tolerations: vec![],
                affinity: None,
                service_account: None,
                security_context: None,
                container_security_context: None,
            },
            status: None,
        }
    }

    #[test]
    fn test_build_connect_status_pending() {
        let connect = create_test_connect();
        let status = build_connect_status(&connect, None);

        assert_eq!(status.phase, ConnectPhase::Pending);
        assert_eq!(status.replicas, 0);
        assert_eq!(status.ready_replicas, 0);
        assert_eq!(status.sources_total, 1);
        assert_eq!(status.sinks_total, 1);
    }

    #[test]
    fn test_build_connect_status_running() {
        let connect = create_test_connect();
        let deploy_status = k8s_openapi::api::apps::v1::DeploymentStatus {
            replicas: Some(2),
            ready_replicas: Some(2),
            updated_replicas: Some(2),
            ..Default::default()
        };

        let status = build_connect_status(&connect, Some(deploy_status));

        assert_eq!(status.phase, ConnectPhase::Running);
        assert_eq!(status.replicas, 2);
        assert_eq!(status.ready_replicas, 2);
        assert_eq!(status.sources_running, 1);
        assert_eq!(status.sinks_running, 1);
    }

    #[test]
    fn test_build_connect_status_degraded() {
        let connect = create_test_connect();
        let deploy_status = k8s_openapi::api::apps::v1::DeploymentStatus {
            replicas: Some(2),
            ready_replicas: Some(1),
            updated_replicas: Some(2),
            ..Default::default()
        };

        let status = build_connect_status(&connect, Some(deploy_status));

        assert_eq!(status.phase, ConnectPhase::Degraded);
    }

    #[test]
    fn test_build_failed_status() {
        let connect = create_test_connect();
        let status = build_failed_status(&connect, "Test error");

        assert_eq!(status.phase, ConnectPhase::Failed);
        assert_eq!(status.message, Some("Test error".to_string()));
        assert_eq!(status.conditions.len(), 1);
        assert_eq!(status.conditions[0].status, "False");
    }

    #[test]
    fn test_build_pipeline_yaml() {
        let connect = create_test_connect();
        let yaml = build_pipeline_yaml(&connect.spec).unwrap();

        assert!(yaml.contains("version: \"1.0\""));
        assert!(yaml.contains("bootstrap_servers:"));
        assert!(yaml.contains("rivven-test-cluster"));
        assert!(yaml.contains("sources:"));
        assert!(yaml.contains("test-source:"));
        assert!(yaml.contains("sinks:"));
        assert!(yaml.contains("test-sink:"));
    }

    #[test]
    fn test_build_connect_configmap() {
        let connect = create_test_connect();
        let cm = build_connect_configmap(&connect).unwrap();

        assert_eq!(
            cm.metadata.name,
            Some("rivven-connect-test-connect".to_string())
        );
        assert!(cm.data.unwrap().contains_key("pipeline.yaml"));
    }

    #[test]
    fn test_build_connect_deployment() {
        let connect = create_test_connect();
        let deployment = build_connect_deployment(&connect).unwrap();

        assert_eq!(
            deployment.metadata.name,
            Some("rivven-connect-test-connect".to_string())
        );
        assert_eq!(deployment.spec.as_ref().unwrap().replicas, Some(2));
    }

    #[test]
    fn test_build_connect_service() {
        let connect = create_test_connect();
        let service = build_connect_service(&connect).unwrap();

        assert_eq!(
            service.metadata.name,
            Some("rivven-connect-test-connect".to_string())
        );
        let ports = service.spec.as_ref().unwrap().ports.as_ref().unwrap();
        assert_eq!(ports.len(), 2);
    }

    #[test]
    fn test_connector_statuses() {
        let connect = create_test_connect();
        let deploy_status = k8s_openapi::api::apps::v1::DeploymentStatus {
            replicas: Some(2),
            ready_replicas: Some(2),
            updated_replicas: Some(2),
            ..Default::default()
        };

        let status = build_connect_status(&connect, Some(deploy_status));

        assert_eq!(status.connector_statuses.len(), 2);

        let source_status = status
            .connector_statuses
            .iter()
            .find(|s| s.name == "test-source")
            .unwrap();
        assert_eq!(source_status.connector_type, "source");
        assert_eq!(source_status.state, "running");

        let sink_status = status
            .connector_statuses
            .iter()
            .find(|s| s.name == "test-sink")
            .unwrap();
        assert_eq!(sink_status.connector_type, "sink");
        assert_eq!(sink_status.state, "running");
    }

    #[test]
    fn test_conditions() {
        let connect = create_test_connect();
        let deploy_status = k8s_openapi::api::apps::v1::DeploymentStatus {
            replicas: Some(2),
            ready_replicas: Some(2),
            updated_replicas: Some(2),
            ..Default::default()
        };

        let status = build_connect_status(&connect, Some(deploy_status));

        assert_eq!(status.conditions.len(), 4);

        let ready_cond = status
            .conditions
            .iter()
            .find(|c| c.condition_type == "Ready")
            .unwrap();
        assert_eq!(ready_cond.status, "True");

        let broker_cond = status
            .conditions
            .iter()
            .find(|c| c.condition_type == "BrokerConnected")
            .unwrap();
        assert_eq!(broker_cond.status, "True");
    }
}
