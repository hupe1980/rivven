//! RivvenSchemaRegistry Controller
//!
//! This module implements the Kubernetes controller for managing RivvenSchemaRegistry
//! custom resources. It watches for changes and reconciles Schema Registry deployments.

use crate::crd::{
    ClusterReference, RivvenSchemaRegistry, RivvenSchemaRegistrySpec, RivvenSchemaRegistryStatus,
    SchemaRegistryCondition, SchemaRegistryPhase,
};
use crate::error::{OperatorError, Result};
use chrono::Utc;
use futures::StreamExt;
use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec};
use k8s_openapi::api::core::v1::{
    ConfigMap, Container, ContainerPort, EnvVar, EnvVarSource, HTTPGetAction, ObjectFieldSelector,
    PodSpec, PodTemplateSpec, Probe, Service, ServicePort, ServiceSpec, VolumeMount,
};
use k8s_openapi::api::policy::v1::{PodDisruptionBudget, PodDisruptionBudgetSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
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
pub const SCHEMA_REGISTRY_FINALIZER: &str = "rivven.hupe1980.github.io/schema-registry-finalizer";

/// Default requeue interval for successful reconciliations
const DEFAULT_REQUEUE_SECONDS: u64 = 60; // 1 minute

/// Requeue interval for error cases
const ERROR_REQUEUE_SECONDS: u64 = 30;

/// Context passed to the schema registry controller
pub struct SchemaRegistryControllerContext {
    /// Kubernetes client
    pub client: Client,
    /// Metrics recorder
    pub metrics: Option<SchemaRegistryControllerMetrics>,
}

/// Metrics for the schema registry controller
#[derive(Clone)]
pub struct SchemaRegistryControllerMetrics {
    /// Counter for reconciliation attempts
    pub reconciliations: metrics::Counter,
    /// Counter for reconciliation errors
    pub errors: metrics::Counter,
    /// Histogram for reconciliation duration
    pub duration: metrics::Histogram,
}

impl SchemaRegistryControllerMetrics {
    /// Create new schema registry controller metrics
    pub fn new() -> Self {
        Self {
            reconciliations: metrics::counter!("rivven_schema_registry_reconciliations_total"),
            errors: metrics::counter!("rivven_schema_registry_reconciliation_errors_total"),
            duration: metrics::histogram!("rivven_schema_registry_reconciliation_duration_seconds"),
        }
    }
}

impl Default for SchemaRegistryControllerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Start the RivvenSchemaRegistry controller
pub async fn run_schema_registry_controller(
    client: Client,
    namespace: Option<String>,
) -> Result<()> {
    let registries: Api<RivvenSchemaRegistry> = match &namespace {
        Some(ns) => Api::namespaced(client.clone(), ns),
        None => Api::all(client.clone()),
    };

    let ctx = Arc::new(SchemaRegistryControllerContext {
        client: client.clone(),
        metrics: Some(SchemaRegistryControllerMetrics::new()),
    });

    info!(
        namespace = namespace.as_deref().unwrap_or("all"),
        "Starting RivvenSchemaRegistry controller"
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

    Controller::new(registries.clone(), Config::default())
        .owns(deployments, Config::default())
        .owns(configmaps, Config::default())
        .run(reconcile_schema_registry, schema_registry_error_policy, ctx)
        .for_each(|result| async move {
            match result {
                Ok((obj, action)) => {
                    debug!(
                        name = obj.name,
                        namespace = obj.namespace,
                        ?action,
                        "Schema registry reconciliation completed"
                    );
                }
                Err(e) => {
                    error!(error = %e, "Schema registry reconciliation failed");
                }
            }
        })
        .await;

    Ok(())
}

/// Main reconciliation function for RivvenSchemaRegistry
#[instrument(skip(registry, ctx), fields(name = %registry.name_any(), namespace = registry.namespace()))]
async fn reconcile_schema_registry(
    registry: Arc<RivvenSchemaRegistry>,
    ctx: Arc<SchemaRegistryControllerContext>,
) -> Result<Action> {
    let start = std::time::Instant::now();

    if let Some(ref metrics) = ctx.metrics {
        metrics.reconciliations.increment(1);
    }

    let namespace = registry
        .namespace()
        .unwrap_or_else(|| "default".to_string());
    let registries: Api<RivvenSchemaRegistry> = Api::namespaced(ctx.client.clone(), &namespace);

    let result = finalizer(
        &registries,
        SCHEMA_REGISTRY_FINALIZER,
        registry,
        |event| async {
            match event {
                FinalizerEvent::Apply(registry) => {
                    apply_schema_registry(registry, ctx.clone()).await
                }
                FinalizerEvent::Cleanup(registry) => {
                    cleanup_schema_registry(registry, ctx.clone()).await
                }
            }
        },
    )
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

/// Apply (create/update) the schema registry resources
#[instrument(skip(registry, ctx))]
async fn apply_schema_registry(
    registry: Arc<RivvenSchemaRegistry>,
    ctx: Arc<SchemaRegistryControllerContext>,
) -> Result<Action> {
    let name = registry.name_any();
    let namespace = registry
        .namespace()
        .unwrap_or_else(|| "default".to_string());

    info!(name = %name, namespace = %namespace, "Reconciling RivvenSchemaRegistry");

    // Validate the registry spec
    if let Err(errors) = registry.spec.validate() {
        let error_messages: Vec<String> = errors
            .field_errors()
            .iter()
            .flat_map(|(field, errs)| {
                errs.iter()
                    .map(move |e| format!("{}: {:?}", field, e.message))
            })
            .collect();
        let error_msg = error_messages.join("; ");
        warn!(name = %name, errors = %error_msg, "Schema registry spec validation failed");

        // Update status to failed
        update_schema_registry_status(
            &ctx.client,
            &namespace,
            &name,
            build_failed_status(&registry, &error_msg),
        )
        .await?;

        return Err(OperatorError::InvalidConfig(error_msg));
    }

    // Verify cluster reference exists
    verify_cluster_ref(&ctx.client, &namespace, &registry.spec.cluster_ref).await?;

    // Build and apply ConfigMap with registry configuration
    let configmap = build_registry_configmap(&registry)?;
    apply_registry_configmap(&ctx.client, &namespace, configmap).await?;

    // Build and apply Deployment
    let deployment = build_registry_deployment(&registry)?;
    let deployment_status = apply_registry_deployment(&ctx.client, &namespace, deployment).await?;

    // Build and apply Service
    let service = build_registry_service(&registry)?;
    apply_registry_service(&ctx.client, &namespace, service).await?;

    // Apply PDB if enabled
    if registry.spec.pod_disruption_budget.enabled {
        let pdb = build_registry_pdb(&registry)?;
        apply_registry_pdb(&ctx.client, &namespace, pdb).await?;
    }

    // Update status
    let status = build_running_status(&registry, &deployment_status);
    update_schema_registry_status(&ctx.client, &namespace, &name, status).await?;

    Ok(Action::requeue(Duration::from_secs(
        DEFAULT_REQUEUE_SECONDS,
    )))
}

/// Cleanup the schema registry resources
#[instrument(skip(registry, _ctx))]
async fn cleanup_schema_registry(
    registry: Arc<RivvenSchemaRegistry>,
    _ctx: Arc<SchemaRegistryControllerContext>,
) -> Result<Action> {
    let name = registry.name_any();
    let namespace = registry
        .namespace()
        .unwrap_or_else(|| "default".to_string());

    info!(name = %name, namespace = %namespace, "Cleaning up RivvenSchemaRegistry");

    // Resources with owner references will be garbage collected automatically
    // Additional cleanup logic can be added here if needed

    Ok(Action::await_change())
}

/// Error policy for the controller
fn schema_registry_error_policy(
    _registry: Arc<RivvenSchemaRegistry>,
    error: &OperatorError,
    _ctx: Arc<SchemaRegistryControllerContext>,
) -> Action {
    warn!(error = %error, "Schema registry reconciliation error, will retry");
    Action::requeue(Duration::from_secs(ERROR_REQUEUE_SECONDS))
}

/// Verify that the cluster reference exists
async fn verify_cluster_ref(
    client: &Client,
    namespace: &str,
    cluster_ref: &ClusterReference,
) -> Result<()> {
    use crate::crd::RivvenCluster;

    let cluster_ns = cluster_ref.namespace.as_deref().unwrap_or(namespace);
    let clusters: Api<RivvenCluster> = Api::namespaced(client.clone(), cluster_ns);

    clusters
        .get(&cluster_ref.name)
        .await
        .map_err(|e| OperatorError::ClusterNotFound(format!("{}: {}", cluster_ref.name, e)))?;

    Ok(())
}

/// Build the ConfigMap for the schema registry
fn build_registry_configmap(registry: &RivvenSchemaRegistry) -> Result<ConfigMap> {
    let name = registry.name_any();
    let namespace = registry
        .namespace()
        .unwrap_or_else(|| "default".to_string());
    let resource_name = format!("rivven-schema-registry-{}", name);

    let spec = &registry.spec;
    let labels = spec.get_labels(&name);

    // Build configuration YAML
    let config = build_registry_config_yaml(spec)?;

    Ok(ConfigMap {
        metadata: ObjectMeta {
            name: Some(format!("{}-config", resource_name)),
            namespace: Some(namespace),
            labels: Some(labels),
            owner_references: Some(vec![owner_reference(registry)]),
            ..Default::default()
        },
        data: Some(BTreeMap::from([("config.yaml".to_string(), config)])),
        ..Default::default()
    })
}

/// Build the registry configuration YAML
fn build_registry_config_yaml(spec: &RivvenSchemaRegistrySpec) -> Result<String> {
    let mut config = String::new();

    // Server configuration
    config.push_str(&format!(
        "server:\n  port: {}\n  bind_address: \"{}\"\n  timeout_seconds: {}\n  max_request_size: {}\n",
        spec.server.port,
        spec.server.bind_address,
        spec.server.timeout_seconds,
        spec.server.max_request_size,
    ));

    // Storage configuration
    config.push_str(&format!(
        "\nstorage:\n  mode: \"{}\"\n  topic: \"{}\"\n  replication_factor: {}\n  partitions: {}\n  normalize: {}\n",
        spec.storage.mode,
        spec.storage.topic,
        spec.storage.replication_factor,
        spec.storage.partitions,
        spec.storage.normalize,
    ));

    // Compatibility configuration
    config.push_str(&format!(
        "\ncompatibility:\n  default_level: \"{}\"\n  allow_overrides: {}\n",
        spec.compatibility.default_level, spec.compatibility.allow_overrides,
    ));

    // Schema format configuration
    config.push_str(&format!(
        "\nschemas:\n  avro: {}\n  json_schema: {}\n  protobuf: {}\n  strict_validation: {}\n",
        spec.schemas.avro,
        spec.schemas.json_schema,
        spec.schemas.protobuf,
        spec.schemas.strict_validation,
    ));

    // Contexts configuration
    if spec.contexts.enabled {
        config.push_str(&format!(
            "\ncontexts:\n  enabled: true\n  max_contexts: {}\n",
            spec.contexts.max_contexts,
        ));
    }

    // Metrics configuration
    if spec.metrics.enabled {
        config.push_str(&format!(
            "\nmetrics:\n  enabled: true\n  port: {}\n  path: \"{}\"\n",
            spec.metrics.port, spec.metrics.path,
        ));
    }

    Ok(config)
}

/// Build the Deployment for the schema registry
fn build_registry_deployment(registry: &RivvenSchemaRegistry) -> Result<Deployment> {
    let name = registry.name_any();
    let namespace = registry
        .namespace()
        .unwrap_or_else(|| "default".to_string());
    let resource_name = format!("rivven-schema-registry-{}", name);

    let spec = &registry.spec;
    let labels = spec.get_labels(&name);
    let selector_labels = spec.get_selector_labels(&name);

    // Build container
    let container = build_registry_container(spec, &resource_name);

    // Build pod labels
    let mut pod_labels = selector_labels.clone();
    pod_labels.extend(spec.pod_labels.clone());

    // Build pod annotations
    let mut pod_annotations = BTreeMap::new();
    if spec.metrics.enabled {
        pod_annotations.insert("prometheus.io/scrape".to_string(), "true".to_string());
        pod_annotations.insert(
            "prometheus.io/port".to_string(),
            spec.metrics.port.to_string(),
        );
        pod_annotations.insert("prometheus.io/path".to_string(), spec.metrics.path.clone());
    }
    pod_annotations.extend(spec.pod_annotations.clone());

    let pod_spec = PodSpec {
        containers: vec![container],
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
        service_account_name: spec.service_account.clone(),
        image_pull_secrets: if spec.image_pull_secrets.is_empty() {
            None
        } else {
            Some(
                spec.image_pull_secrets
                    .iter()
                    .map(|s| k8s_openapi::api::core::v1::LocalObjectReference { name: s.clone() })
                    .collect(),
            )
        },
        // Disable service account token auto-mounting for security
        automount_service_account_token: Some(false),
        ..Default::default()
    };

    Ok(Deployment {
        metadata: ObjectMeta {
            name: Some(resource_name.clone()),
            namespace: Some(namespace),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_reference(registry)]),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(spec.replicas),
            selector: LabelSelector {
                match_labels: Some(selector_labels),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(pod_labels),
                    annotations: Some(pod_annotations),
                    ..Default::default()
                }),
                spec: Some(pod_spec),
            },
            ..Default::default()
        }),
        ..Default::default()
    })
}

/// Build the container for the schema registry
fn build_registry_container(spec: &RivvenSchemaRegistrySpec, _resource_name: &str) -> Container {
    // Build environment variables
    let mut env = vec![
        EnvVar {
            name: "RIVVEN_SCHEMA_PORT".to_string(),
            value: Some(spec.server.port.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "RIVVEN_SCHEMA_BIND_ADDRESS".to_string(),
            value: Some(spec.server.bind_address.clone()),
            ..Default::default()
        },
        EnvVar {
            name: "RIVVEN_SCHEMA_STORAGE_MODE".to_string(),
            value: Some(spec.storage.mode.clone()),
            ..Default::default()
        },
        EnvVar {
            name: "RIVVEN_SCHEMA_STORAGE_TOPIC".to_string(),
            value: Some(spec.storage.topic.clone()),
            ..Default::default()
        },
        EnvVar {
            name: "RIVVEN_SCHEMA_COMPATIBILITY".to_string(),
            value: Some(spec.compatibility.default_level.clone()),
            ..Default::default()
        },
        // Broker connection (from cluster reference)
        EnvVar {
            name: "RIVVEN_BROKERS".to_string(),
            value: Some(format!(
                "rivven-{}-headless.{}.svc.cluster.local:9092",
                spec.cluster_ref.name,
                spec.cluster_ref.namespace.as_deref().unwrap_or("default")
            )),
            ..Default::default()
        },
        // Pod info
        EnvVar {
            name: "POD_NAME".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    field_path: "metadata.name".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "POD_NAMESPACE".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    field_path: "metadata.namespace".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
    ];

    // Add custom env vars
    for e in &spec.env {
        if !env.iter().any(|existing| existing.name == e.name) {
            env.push(e.clone());
        }
    }

    // Build probes
    let liveness_probe = if spec.liveness_probe.enabled {
        Some(Probe {
            http_get: Some(HTTPGetAction {
                path: Some("/health/live".to_string()),
                port: IntOrString::Int(spec.server.port),
                ..Default::default()
            }),
            initial_delay_seconds: Some(spec.liveness_probe.initial_delay_seconds),
            period_seconds: Some(spec.liveness_probe.period_seconds),
            timeout_seconds: Some(spec.liveness_probe.timeout_seconds),
            success_threshold: Some(spec.liveness_probe.success_threshold),
            failure_threshold: Some(spec.liveness_probe.failure_threshold),
            ..Default::default()
        })
    } else {
        None
    };

    let readiness_probe = if spec.readiness_probe.enabled {
        Some(Probe {
            http_get: Some(HTTPGetAction {
                path: Some("/health/ready".to_string()),
                port: IntOrString::Int(spec.server.port),
                ..Default::default()
            }),
            initial_delay_seconds: Some(spec.readiness_probe.initial_delay_seconds),
            period_seconds: Some(spec.readiness_probe.period_seconds),
            timeout_seconds: Some(spec.readiness_probe.timeout_seconds),
            success_threshold: Some(spec.readiness_probe.success_threshold),
            failure_threshold: Some(spec.readiness_probe.failure_threshold),
            ..Default::default()
        })
    } else {
        None
    };

    // Container ports
    let mut ports = vec![ContainerPort {
        name: Some("http".to_string()),
        container_port: spec.server.port,
        protocol: Some("TCP".to_string()),
        ..Default::default()
    }];

    if spec.metrics.enabled && spec.metrics.port != spec.server.port {
        ports.push(ContainerPort {
            name: Some("metrics".to_string()),
            container_port: spec.metrics.port,
            protocol: Some("TCP".to_string()),
            ..Default::default()
        });
    }

    Container {
        name: "rivven-schema-registry".to_string(),
        image: Some(spec.get_image()),
        image_pull_policy: Some(spec.image_pull_policy.clone()),
        command: Some(vec!["rivven-schema".to_string()]),
        args: Some(vec![
            "serve".to_string(),
            "--config".to_string(),
            "/etc/rivven-schema/config.yaml".to_string(),
        ]),
        env: Some(env),
        ports: Some(ports),
        liveness_probe,
        readiness_probe,
        volume_mounts: Some(vec![VolumeMount {
            name: "config".to_string(),
            mount_path: "/etc/rivven-schema".to_string(),
            read_only: Some(true),
            ..Default::default()
        }]),
        // Secure defaults
        security_context: Some(k8s_openapi::api::core::v1::SecurityContext {
            run_as_non_root: Some(true),
            run_as_user: Some(1000),
            run_as_group: Some(1000),
            read_only_root_filesystem: Some(true),
            allow_privilege_escalation: Some(false),
            capabilities: Some(k8s_openapi::api::core::v1::Capabilities {
                drop: Some(vec!["ALL".to_string()]),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Build the Service for the schema registry
fn build_registry_service(registry: &RivvenSchemaRegistry) -> Result<Service> {
    let name = registry.name_any();
    let namespace = registry
        .namespace()
        .unwrap_or_else(|| "default".to_string());
    let resource_name = format!("rivven-schema-registry-{}", name);

    let spec = &registry.spec;
    let labels = spec.get_labels(&name);
    let selector_labels = spec.get_selector_labels(&name);

    let mut ports = vec![ServicePort {
        name: Some("http".to_string()),
        port: spec.server.port,
        target_port: Some(IntOrString::Int(spec.server.port)),
        protocol: Some("TCP".to_string()),
        ..Default::default()
    }];

    if spec.metrics.enabled && spec.metrics.port != spec.server.port {
        ports.push(ServicePort {
            name: Some("metrics".to_string()),
            port: spec.metrics.port,
            target_port: Some(IntOrString::Int(spec.metrics.port)),
            protocol: Some("TCP".to_string()),
            ..Default::default()
        });
    }

    Ok(Service {
        metadata: ObjectMeta {
            name: Some(resource_name),
            namespace: Some(namespace),
            labels: Some(labels),
            owner_references: Some(vec![owner_reference(registry)]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(selector_labels),
            ports: Some(ports),
            type_: Some("ClusterIP".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    })
}

/// Build the PodDisruptionBudget for the schema registry
fn build_registry_pdb(registry: &RivvenSchemaRegistry) -> Result<PodDisruptionBudget> {
    let name = registry.name_any();
    let namespace = registry
        .namespace()
        .unwrap_or_else(|| "default".to_string());
    let resource_name = format!("rivven-schema-registry-{}", name);

    let spec = &registry.spec;
    let labels = spec.get_labels(&name);
    let selector_labels = spec.get_selector_labels(&name);

    let pdb_spec = &spec.pod_disruption_budget;

    Ok(PodDisruptionBudget {
        metadata: ObjectMeta {
            name: Some(resource_name),
            namespace: Some(namespace),
            labels: Some(labels),
            owner_references: Some(vec![owner_reference(registry)]),
            ..Default::default()
        },
        spec: Some(PodDisruptionBudgetSpec {
            selector: Some(LabelSelector {
                match_labels: Some(selector_labels),
                ..Default::default()
            }),
            min_available: pdb_spec
                .min_available
                .as_ref()
                .map(|v| IntOrString::String(v.clone())),
            max_unavailable: pdb_spec
                .max_unavailable
                .as_ref()
                .map(|v| IntOrString::String(v.clone())),
            ..Default::default()
        }),
        ..Default::default()
    })
}

/// Create owner reference for managed resources
fn owner_reference(registry: &RivvenSchemaRegistry) -> OwnerReference {
    OwnerReference {
        api_version: "rivven.hupe1980.github.io/v1alpha1".to_string(),
        kind: "RivvenSchemaRegistry".to_string(),
        name: registry.name_any(),
        uid: registry.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Verify the operator still owns a resource before force-applying.
fn verify_ownership<K: Resource>(existing: &K) -> Result<()> {
    let labels = existing.meta().labels.as_ref();
    let managed_by = labels.and_then(|l| l.get("app.kubernetes.io/managed-by"));
    match managed_by {
        Some(manager) if manager != "rivven-operator" => {
            let name = existing.meta().name.as_deref().unwrap_or("<unknown>");
            Err(OperatorError::InvalidConfig(format!(
                "resource '{}' is managed by '{}', not rivven-operator; \
                 refusing to force-apply to avoid ownership conflict",
                name, manager
            )))
        }
        _ => Ok(()),
    }
}

/// Apply ConfigMap to cluster
async fn apply_registry_configmap(
    client: &Client,
    namespace: &str,
    configmap: ConfigMap,
) -> Result<()> {
    let configmaps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let name = configmap.metadata.name.clone().unwrap_or_default();

    // Verify ownership before force-applying
    if let Ok(existing) = configmaps.get(&name).await {
        verify_ownership(&existing)?;
    }

    let patch = Patch::Apply(&configmap);
    let params = PatchParams::apply("rivven-operator").force();

    configmaps
        .patch(&name, &params, &patch)
        .await
        .map_err(|e| OperatorError::ReconcileFailed(format!("ConfigMap: {}", e)))?;

    debug!(name = %name, "Applied schema registry ConfigMap");
    Ok(())
}

/// Apply Deployment to cluster
async fn apply_registry_deployment(
    client: &Client,
    namespace: &str,
    deployment: Deployment,
) -> Result<(i32, i32)> {
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let name = deployment.metadata.name.clone().unwrap_or_default();

    // Verify ownership before force-applying
    if let Ok(existing) = deployments.get(&name).await {
        verify_ownership(&existing)?;
    }

    let patch = Patch::Apply(&deployment);
    let params = PatchParams::apply("rivven-operator").force();

    let result = deployments
        .patch(&name, &params, &patch)
        .await
        .map_err(|e| OperatorError::ReconcileFailed(format!("Deployment: {}", e)))?;

    let status = result.status.as_ref();
    let replicas = status.and_then(|s| s.replicas).unwrap_or(0);
    let ready_replicas = status.and_then(|s| s.ready_replicas).unwrap_or(0);

    debug!(name = %name, replicas = replicas, ready = ready_replicas, "Applied schema registry Deployment");
    Ok((replicas, ready_replicas))
}

/// Apply Service to cluster
async fn apply_registry_service(client: &Client, namespace: &str, service: Service) -> Result<()> {
    let services: Api<Service> = Api::namespaced(client.clone(), namespace);
    let name = service.metadata.name.clone().unwrap_or_default();

    // Verify ownership before force-applying
    if let Ok(existing) = services.get(&name).await {
        verify_ownership(&existing)?;
    }

    let patch = Patch::Apply(&service);
    let params = PatchParams::apply("rivven-operator").force();

    services
        .patch(&name, &params, &patch)
        .await
        .map_err(|e| OperatorError::ReconcileFailed(format!("Service: {}", e)))?;

    debug!(name = %name, "Applied schema registry Service");
    Ok(())
}

/// Apply PodDisruptionBudget to cluster
async fn apply_registry_pdb(
    client: &Client,
    namespace: &str,
    pdb: PodDisruptionBudget,
) -> Result<()> {
    let pdbs: Api<PodDisruptionBudget> = Api::namespaced(client.clone(), namespace);
    let name = pdb.metadata.name.clone().unwrap_or_default();

    // Verify ownership before force-applying
    if let Ok(existing) = pdbs.get(&name).await {
        verify_ownership(&existing)?;
    }

    let patch = Patch::Apply(&pdb);
    let params = PatchParams::apply("rivven-operator").force();

    pdbs.patch(&name, &params, &patch)
        .await
        .map_err(|e| OperatorError::ReconcileFailed(format!("PodDisruptionBudget: {}", e)))?;

    debug!(name = %name, "Applied schema registry PodDisruptionBudget");
    Ok(())
}

/// Update the schema registry status
async fn update_schema_registry_status(
    client: &Client,
    namespace: &str,
    name: &str,
    status: RivvenSchemaRegistryStatus,
) -> Result<()> {
    let registries: Api<RivvenSchemaRegistry> = Api::namespaced(client.clone(), namespace);

    let patch = serde_json::json!({
        "status": status
    });

    registries
        .patch_status(
            name,
            &PatchParams::apply("rivven-operator"),
            &Patch::Merge(&patch),
        )
        .await
        .map_err(|e| OperatorError::ReconcileFailed(format!("Status update: {}", e)))?;

    debug!(name = %name, phase = ?status.phase, "Updated schema registry status");
    Ok(())
}

/// Build status for a failed registry
fn build_failed_status(registry: &RivvenSchemaRegistry, error: &str) -> RivvenSchemaRegistryStatus {
    RivvenSchemaRegistryStatus {
        phase: SchemaRegistryPhase::Failed,
        replicas: 0,
        ready_replicas: 0,
        schemas_registered: 0,
        subjects_count: 0,
        contexts_count: if registry.spec.contexts.enabled { 1 } else { 0 },
        observed_generation: registry.metadata.generation.unwrap_or(0),
        conditions: vec![SchemaRegistryCondition {
            condition_type: "Ready".to_string(),
            status: "False".to_string(),
            reason: Some("ValidationFailed".to_string()),
            message: Some(error.to_string()),
            last_transition_time: Some(Utc::now().to_rfc3339()),
        }],
        endpoints: vec![],
        storage_status: None,
        external_sync_status: None,
        last_sync_time: None,
        last_updated: Some(Utc::now().to_rfc3339()),
        message: Some(error.to_string()),
    }
}

/// Build status for a running registry
fn build_running_status(
    registry: &RivvenSchemaRegistry,
    deployment_status: &(i32, i32),
) -> RivvenSchemaRegistryStatus {
    let (replicas, ready_replicas) = *deployment_status;
    let name = registry.name_any();
    let namespace = registry
        .namespace()
        .unwrap_or_else(|| "default".to_string());

    let phase = if ready_replicas == replicas && replicas > 0 {
        SchemaRegistryPhase::Running
    } else if ready_replicas > 0 {
        SchemaRegistryPhase::Degraded
    } else {
        SchemaRegistryPhase::Provisioning
    };

    let endpoint = format!(
        "http://rivven-schema-registry-{}.{}.svc.cluster.local:{}",
        name, namespace, registry.spec.server.port
    );

    RivvenSchemaRegistryStatus {
        phase,
        replicas,
        ready_replicas,
        schemas_registered: 0, // Updated by registry itself
        subjects_count: 0,     // Updated by registry itself
        contexts_count: if registry.spec.contexts.enabled { 1 } else { 0 },
        observed_generation: registry.metadata.generation.unwrap_or(0),
        conditions: vec![
            SchemaRegistryCondition {
                condition_type: "Ready".to_string(),
                status: if ready_replicas == replicas && replicas > 0 {
                    "True"
                } else {
                    "False"
                }
                .to_string(),
                reason: Some(
                    if ready_replicas == replicas && replicas > 0 {
                        "AllReplicasReady"
                    } else {
                        "ReplicasNotReady"
                    }
                    .to_string(),
                ),
                message: Some(format!("{}/{} replicas ready", ready_replicas, replicas)),
                last_transition_time: Some(Utc::now().to_rfc3339()),
            },
            SchemaRegistryCondition {
                condition_type: "StorageHealthy".to_string(),
                status: "Unknown".to_string(),
                reason: Some("Pending".to_string()),
                message: Some("Storage health check pending".to_string()),
                last_transition_time: Some(Utc::now().to_rfc3339()),
            },
        ],
        endpoints: vec![endpoint],
        storage_status: Some(registry.spec.storage.mode.clone()),
        external_sync_status: if registry.spec.external.enabled {
            Some("Pending".to_string())
        } else {
            None
        },
        last_sync_time: None,
        last_updated: Some(Utc::now().to_rfc3339()),
        message: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        ClusterReference, ExternalRegistrySpec, PdbSpec, ProbeSpec, SchemaCompatibilitySpec,
        SchemaContextsSpec, SchemaFormatSpec, SchemaRegistryAuthSpec, SchemaRegistryMetricsSpec,
        SchemaRegistryServerSpec, SchemaRegistryStorageSpec, SchemaRegistryTlsSpec,
        SchemaValidationSpec,
    };

    fn create_test_registry() -> RivvenSchemaRegistry {
        RivvenSchemaRegistry {
            metadata: ObjectMeta {
                name: Some("test-registry".to_string()),
                namespace: Some("default".to_string()),
                uid: Some("test-uid".to_string()),
                generation: Some(1),
                ..Default::default()
            },
            spec: RivvenSchemaRegistrySpec {
                cluster_ref: ClusterReference {
                    name: "test-cluster".to_string(),
                    namespace: None,
                },
                replicas: 3,
                version: "0.0.1".to_string(),
                image: None,
                image_pull_policy: "IfNotPresent".to_string(),
                image_pull_secrets: vec![],
                resources: None,
                server: SchemaRegistryServerSpec::default(),
                storage: SchemaRegistryStorageSpec::default(),
                compatibility: SchemaCompatibilitySpec::default(),
                schemas: SchemaFormatSpec::default(),
                contexts: SchemaContextsSpec::default(),
                validation: SchemaValidationSpec::default(),
                auth: SchemaRegistryAuthSpec::default(),
                tls: SchemaRegistryTlsSpec::default(),
                metrics: SchemaRegistryMetricsSpec::default(),
                external: ExternalRegistrySpec::default(),
                pod_annotations: BTreeMap::new(),
                pod_labels: BTreeMap::new(),
                env: vec![],
                node_selector: BTreeMap::new(),
                tolerations: vec![],
                affinity: None,
                service_account: None,
                security_context: None,
                container_security_context: None,
                liveness_probe: ProbeSpec::default(),
                readiness_probe: ProbeSpec::default(),
                pod_disruption_budget: PdbSpec::default(),
            },
            status: None,
        }
    }

    #[test]
    fn test_build_registry_configmap() {
        let registry = create_test_registry();
        let configmap = build_registry_configmap(&registry).unwrap();

        assert_eq!(
            configmap.metadata.name,
            Some("rivven-schema-registry-test-registry-config".to_string())
        );
        assert!(configmap.data.is_some());
        let data = configmap.data.unwrap();
        assert!(data.contains_key("config.yaml"));
    }

    #[test]
    fn test_build_registry_deployment() {
        let registry = create_test_registry();
        let deployment = build_registry_deployment(&registry).unwrap();

        assert_eq!(
            deployment.metadata.name,
            Some("rivven-schema-registry-test-registry".to_string())
        );
        assert_eq!(deployment.spec.as_ref().unwrap().replicas, Some(3));
    }

    #[test]
    fn test_build_registry_service() {
        let registry = create_test_registry();
        let service = build_registry_service(&registry).unwrap();

        assert_eq!(
            service.metadata.name,
            Some("rivven-schema-registry-test-registry".to_string())
        );

        let ports = service.spec.as_ref().unwrap().ports.as_ref().unwrap();
        assert_eq!(ports.len(), 2); // http + metrics
        assert_eq!(ports[0].port, 8081);
    }

    #[test]
    fn test_build_registry_pdb() {
        let registry = create_test_registry();
        let pdb = build_registry_pdb(&registry).unwrap();

        assert_eq!(
            pdb.metadata.name,
            Some("rivven-schema-registry-test-registry".to_string())
        );
    }

    #[test]
    fn test_build_failed_status() {
        let registry = create_test_registry();
        let status = build_failed_status(&registry, "Test error");

        assert_eq!(status.phase, SchemaRegistryPhase::Failed);
        assert_eq!(status.message, Some("Test error".to_string()));
        assert_eq!(status.conditions.len(), 1);
        assert_eq!(status.conditions[0].status, "False");
    }

    #[test]
    fn test_build_running_status() {
        let registry = create_test_registry();
        let status = build_running_status(&registry, &(3, 3));

        assert_eq!(status.phase, SchemaRegistryPhase::Running);
        assert_eq!(status.replicas, 3);
        assert_eq!(status.ready_replicas, 3);
        assert!(!status.endpoints.is_empty());
    }

    #[test]
    fn test_build_running_status_degraded() {
        let registry = create_test_registry();
        let status = build_running_status(&registry, &(3, 2));

        assert_eq!(status.phase, SchemaRegistryPhase::Degraded);
        assert_eq!(status.replicas, 3);
        assert_eq!(status.ready_replicas, 2);
    }

    #[test]
    fn test_build_running_status_provisioning() {
        let registry = create_test_registry();
        let status = build_running_status(&registry, &(3, 0));

        assert_eq!(status.phase, SchemaRegistryPhase::Provisioning);
        assert_eq!(status.ready_replicas, 0);
    }

    #[test]
    fn test_spec_get_image_default() {
        let registry = create_test_registry();
        assert_eq!(
            registry.spec.get_image(),
            "ghcr.io/hupe1980/rivven-schema:0.0.1"
        );
    }

    #[test]
    fn test_spec_get_image_custom() {
        let mut registry = create_test_registry();
        registry.spec.image = Some("custom/registry:latest".to_string());
        assert_eq!(registry.spec.get_image(), "custom/registry:latest");
    }

    #[test]
    fn test_spec_get_labels() {
        let registry = create_test_registry();
        let labels = registry.spec.get_labels("test-registry");

        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"rivven-schema-registry".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/instance"),
            Some(&"test-registry".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/component"),
            Some(&"schema-registry".to_string())
        );
    }

    #[test]
    fn test_spec_get_selector_labels() {
        let registry = create_test_registry();
        let labels = registry.spec.get_selector_labels("test-registry");

        assert_eq!(labels.len(), 2);
        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"rivven-schema-registry".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/instance"),
            Some(&"test-registry".to_string())
        );
    }

    #[test]
    fn test_schema_registry_phase_default() {
        let phase = SchemaRegistryPhase::default();
        assert_eq!(phase, SchemaRegistryPhase::Pending);
    }

    #[test]
    fn test_server_spec_default() {
        let spec = SchemaRegistryServerSpec::default();
        assert_eq!(spec.port, 8081);
        assert_eq!(spec.bind_address, "0.0.0.0");
        assert_eq!(spec.timeout_seconds, 30);
    }

    #[test]
    fn test_storage_spec_default() {
        let spec = SchemaRegistryStorageSpec::default();
        assert_eq!(spec.mode, "broker");
        assert_eq!(spec.topic, "_schemas");
        assert_eq!(spec.replication_factor, 3);
    }

    #[test]
    fn test_compatibility_spec_default() {
        let spec = SchemaCompatibilitySpec::default();
        assert_eq!(spec.default_level, "BACKWARD");
        assert!(spec.allow_overrides);
    }

    #[test]
    fn test_schema_format_spec_default() {
        let spec = SchemaFormatSpec::default();
        assert!(spec.avro);
        assert!(spec.json_schema);
        assert!(spec.protobuf);
    }

    #[test]
    fn test_metrics_spec_default() {
        let spec = SchemaRegistryMetricsSpec::default();
        assert!(spec.enabled);
        assert_eq!(spec.port, 9090);
        assert_eq!(spec.path, "/metrics");
    }

    #[test]
    fn test_build_registry_config_yaml() {
        let registry = create_test_registry();
        let config = build_registry_config_yaml(&registry.spec).unwrap();

        assert!(config.contains("port: 8081"));
        assert!(config.contains("mode: \"broker\""));
        assert!(config.contains("default_level: \"BACKWARD\""));
        assert!(config.contains("avro: true"));
    }

    #[test]
    fn test_controller_metrics_new() {
        let _metrics = SchemaRegistryControllerMetrics::new();
        // Test passes if it doesn't panic during construction
    }
}
