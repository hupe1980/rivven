//! RivvenCluster Controller
//!
//! This module implements the Kubernetes controller pattern for managing
//! RivvenCluster custom resources. It watches for changes and reconciles
//! the actual cluster state to match the desired specification.

use crate::crd::{ClusterCondition, ClusterPhase, RivvenCluster, RivvenClusterStatus};
use crate::error::{OperatorError, Result};
use crate::resources::ResourceBuilder;
use chrono::Utc;
use futures::StreamExt;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim, Service};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::finalizer::{finalizer, Event as FinalizerEvent};
use kube::runtime::watcher::Config;
use kube::{Client, Resource, ResourceExt};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};
use validator::Validate;

/// Finalizer name for cleanup operations
pub const FINALIZER_NAME: &str = "rivven.hupe1980.github.io/cluster-finalizer";

/// Default requeue interval for successful reconciliations
const DEFAULT_REQUEUE_SECONDS: u64 = 300; // 5 minutes

/// Requeue interval for error cases (base for exponential backoff)
const ERROR_REQUEUE_SECONDS: u64 = 30;

/// Maximum requeue delay for error backoff
const MAX_ERROR_REQUEUE_SECONDS: u64 = 600;

/// Context passed to the controller
pub struct ControllerContext {
    /// Kubernetes client
    pub client: Client,
    /// Metrics recorder (optional)
    pub metrics: Option<ControllerMetrics>,
    /// Per-cluster error retry counts for exponential backoff
    pub error_counts: dashmap::DashMap<String, u32>,
}

/// Metrics for the controller
#[derive(Clone)]
pub struct ControllerMetrics {
    /// Counter for reconciliation attempts
    pub reconciliations: metrics::Counter,
    /// Counter for reconciliation errors
    pub errors: metrics::Counter,
    /// Histogram for reconciliation duration
    pub duration: metrics::Histogram,
}

impl ControllerMetrics {
    /// Create new controller metrics
    pub fn new() -> Self {
        Self {
            reconciliations: metrics::counter!("rivven_operator_reconciliations_total"),
            errors: metrics::counter!("rivven_operator_reconciliation_errors_total"),
            duration: metrics::histogram!("rivven_operator_reconciliation_duration_seconds"),
        }
    }
}

impl Default for ControllerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Start the RivvenCluster controller
pub async fn run_controller(client: Client, namespace: Option<String>) -> Result<()> {
    let clusters: Api<RivvenCluster> = match &namespace {
        Some(ns) => Api::namespaced(client.clone(), ns),
        None => Api::all(client.clone()),
    };

    let ctx = Arc::new(ControllerContext {
        client: client.clone(),
        metrics: Some(ControllerMetrics::new()),
        error_counts: dashmap::DashMap::new(),
    });

    info!(
        namespace = namespace.as_deref().unwrap_or("all"),
        "Starting RivvenCluster controller"
    );

    // Watch related resources for changes
    let statefulsets = match &namespace {
        Some(ns) => Api::<StatefulSet>::namespaced(client.clone(), ns),
        None => Api::<StatefulSet>::all(client.clone()),
    };

    let services = match &namespace {
        Some(ns) => Api::<Service>::namespaced(client.clone(), ns),
        None => Api::<Service>::all(client.clone()),
    };

    Controller::new(clusters.clone(), Config::default())
        .owns(statefulsets, Config::default())
        .owns(services, Config::default())
        .run(reconcile, error_policy, ctx)
        .for_each(|result| async move {
            match result {
                Ok((obj, action)) => {
                    debug!(
                        name = obj.name,
                        namespace = obj.namespace,
                        ?action,
                        "Reconciliation completed"
                    );
                }
                Err(e) => {
                    error!(error = %e, "Reconciliation failed");
                }
            }
        })
        .await;

    Ok(())
}

/// Main reconciliation function
#[instrument(skip(cluster, ctx), fields(name = %cluster.name_any(), namespace = cluster.namespace()))]
async fn reconcile(cluster: Arc<RivvenCluster>, ctx: Arc<ControllerContext>) -> Result<Action> {
    let start = std::time::Instant::now();

    if let Some(ref metrics) = ctx.metrics {
        metrics.reconciliations.increment(1);
    }

    let namespace = cluster.namespace().unwrap_or_else(|| "default".to_string());
    let cluster_name = cluster.name_any();
    let clusters: Api<RivvenCluster> = Api::namespaced(ctx.client.clone(), &namespace);

    let result = finalizer(&clusters, FINALIZER_NAME, cluster, |event| async {
        match event {
            FinalizerEvent::Apply(cluster) => apply_cluster(cluster, ctx.clone()).await,
            FinalizerEvent::Cleanup(cluster) => cleanup_cluster(cluster, ctx.clone()).await,
        }
    })
    .await;

    if let Some(ref metrics) = ctx.metrics {
        metrics.duration.record(start.elapsed().as_secs_f64());
    }

    // Reset error backoff counter on success
    if result.is_ok() {
        ctx.error_counts.remove(&cluster_name);
    }

    result.map_err(|e| {
        if let Some(ref metrics) = ctx.metrics {
            metrics.errors.increment(1);
        }
        OperatorError::ReconcileFailed(e.to_string())
    })
}

/// Apply (create/update) the cluster resources
#[instrument(skip(cluster, ctx))]
async fn apply_cluster(cluster: Arc<RivvenCluster>, ctx: Arc<ControllerContext>) -> Result<Action> {
    let name = cluster.name_any();
    let namespace = cluster.namespace().unwrap_or_else(|| "default".to_string());

    info!(name = %name, namespace = %namespace, "Reconciling RivvenCluster");

    // Validate the cluster spec before reconciliation
    if let Err(errors) = cluster.spec.validate() {
        let error_messages: Vec<String> = errors
            .field_errors()
            .iter()
            .flat_map(|(field, errs)| {
                errs.iter()
                    .map(move |e| format!("{}: {:?}", field, e.message))
            })
            .collect();
        let error_msg = error_messages.join("; ");
        warn!(name = %name, errors = %error_msg, "Cluster spec validation failed");
        return Err(OperatorError::InvalidConfig(error_msg));
    }

    // Additional security validations
    validate_cluster_security(&cluster)?;

    // Build all resources
    let builder = ResourceBuilder::new(&cluster)?;

    // Apply ConfigMap
    let configmap = builder.build_configmap()?;
    apply_configmap(&ctx.client, &namespace, configmap).await?;

    // Apply headless service
    let headless_svc = builder.build_headless_service();
    apply_service(&ctx.client, &namespace, headless_svc).await?;

    // Apply client service
    let client_svc = builder.build_client_service();
    apply_service(&ctx.client, &namespace, client_svc).await?;

    // Apply StatefulSet
    let statefulset = builder.build_statefulset();
    let sts_status = apply_statefulset(&ctx.client, &namespace, statefulset).await?;

    // Apply PDB if enabled
    if let Some(pdb) = builder.build_pdb() {
        apply_pdb(&ctx.client, &namespace, pdb).await?;
    }

    // Update cluster status
    let status = build_status(&cluster, sts_status);
    update_status(&ctx.client, &namespace, &name, status).await?;

    info!(name = %name, "Reconciliation complete");

    Ok(Action::requeue(Duration::from_secs(
        DEFAULT_REQUEUE_SECONDS,
    )))
}

/// Validate cluster for security best practices
fn validate_cluster_security(cluster: &RivvenCluster) -> Result<()> {
    let spec = &cluster.spec;

    // Validate TLS configuration consistency
    if spec.tls.enabled && spec.tls.cert_secret_name.is_none() {
        return Err(OperatorError::InvalidConfig(
            "TLS is enabled but no certificate secret is specified".to_string(),
        ));
    }

    if spec.tls.mtls_enabled && spec.tls.ca_secret_name.is_none() {
        return Err(OperatorError::InvalidConfig(
            "mTLS is enabled but no CA secret is specified".to_string(),
        ));
    }

    // Validate replication factor vs replicas
    if spec.config.default_replication_factor > spec.replicas {
        return Err(OperatorError::InvalidConfig(format!(
            "Replication factor ({}) cannot exceed replica count ({})",
            spec.config.default_replication_factor, spec.replicas
        )));
    }

    // Warn if running with single replica in production
    if spec.replicas == 1 {
        warn!(
            cluster = cluster.name_any(),
            "Running with single replica - not recommended for production"
        );
    }

    // Validate Raft timing constraints (heartbeat should be < election timeout / 3)
    let min_election_timeout = spec.config.raft_heartbeat_interval_ms * 3;
    if spec.config.raft_election_timeout_ms < min_election_timeout {
        return Err(OperatorError::InvalidConfig(format!(
            "Raft election timeout ({}) should be at least 3x heartbeat interval ({})",
            spec.config.raft_election_timeout_ms, spec.config.raft_heartbeat_interval_ms
        )));
    }

    Ok(())
}

/// Cleanup resources when cluster is deleted
#[instrument(skip(cluster, ctx))]
async fn cleanup_cluster(
    cluster: Arc<RivvenCluster>,
    ctx: Arc<ControllerContext>,
) -> Result<Action> {
    let name = cluster.name_any();
    let namespace = cluster.namespace().unwrap_or_else(|| "default".to_string());

    info!(name = %name, namespace = %namespace, "Cleaning up RivvenCluster resources");

    // Resources with owner references (StatefulSet, Service, ConfigMap, PDB)
    // are garbage-collected automatically by Kubernetes when the CR is deleted.
    // PVCs created by StatefulSet volumeClaimTemplates do NOT get owner refs
    // and must be deleted explicitly.

    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(ctx.client.clone(), &namespace);
    let lp = ListParams::default().labels(&format!("app.kubernetes.io/instance={}", name));

    match pvcs.list(&lp).await {
        Ok(pvc_list) => {
            for pvc in pvc_list.items {
                if let Some(pvc_name) = pvc.metadata.name.as_deref() {
                    info!(name = %name, pvc = %pvc_name, "Deleting PVC");
                    if let Err(e) = pvcs.delete(pvc_name, &DeleteParams::default()).await {
                        warn!(
                            name = %name,
                            pvc = %pvc_name,
                            error = %e,
                            "Failed to delete PVC (may have already been removed)"
                        );
                    }
                }
            }
        }
        Err(e) => {
            warn!(
                name = %name,
                error = %e,
                "Failed to list PVCs for cleanup"
            );
        }
    }

    info!(name = %name, "Cleanup complete");

    Ok(Action::await_change())
}

/// Verify the operator still owns a resource before force-applying.
///
/// Checks whether an existing resource was created by the rivven-operator by
/// inspecting the `app.kubernetes.io/managed-by` label. If the resource
/// exists and is managed by a different controller (e.g. Helm, another
/// operator), the apply is rejected to prevent silently hijacking ownership.
/// If the resource does not yet exist (404), ownership is trivially valid.
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

/// Apply a ConfigMap using server-side apply.
///
/// # Force-apply and field ownership
///
/// Uses `PatchParams::apply("rivven-operator").force()` which takes
/// ownership of **all** fields in the patch, even if they were previously
/// managed by another field manager (e.g. kubectl, Helm, or another
/// controller). Before force-applying, verifies the operator owns the
/// resource via the `app.kubernetes.io/managed-by` label to prevent
/// silently hijacking resources managed by other controllers.
async fn apply_configmap(client: &Client, namespace: &str, cm: ConfigMap) -> Result<()> {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let name =
        cm.metadata.name.as_ref().ok_or_else(|| {
            OperatorError::InvalidConfig("ConfigMap missing metadata.name".into())
        })?;

    debug!(name = %name, "Applying ConfigMap");

    // Verify ownership before force-applying
    if let Ok(existing) = api.get(name).await {
        verify_ownership(&existing)?;
    }

    let patch_params = PatchParams::apply("rivven-operator").force();
    api.patch(name, &patch_params, &Patch::Apply(&cm))
        .await
        .map_err(OperatorError::from)?;

    Ok(())
}

/// Apply a Service using server-side apply
async fn apply_service(client: &Client, namespace: &str, svc: Service) -> Result<()> {
    let api: Api<Service> = Api::namespaced(client.clone(), namespace);
    let name = svc
        .metadata
        .name
        .as_ref()
        .ok_or_else(|| OperatorError::InvalidConfig("Service missing metadata.name".into()))?;

    debug!(name = %name, "Applying Service");

    // Verify ownership before force-applying
    if let Ok(existing) = api.get(name).await {
        verify_ownership(&existing)?;
    }

    let patch_params = PatchParams::apply("rivven-operator").force();
    api.patch(name, &patch_params, &Patch::Apply(&svc))
        .await
        .map_err(OperatorError::from)?;

    Ok(())
}

/// Apply a StatefulSet using server-side apply
async fn apply_statefulset(
    client: &Client,
    namespace: &str,
    sts: StatefulSet,
) -> Result<Option<k8s_openapi::api::apps::v1::StatefulSetStatus>> {
    let api: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
    let name =
        sts.metadata.name.as_ref().ok_or_else(|| {
            OperatorError::InvalidConfig("StatefulSet missing metadata.name".into())
        })?;

    debug!(name = %name, "Applying StatefulSet");

    // Verify ownership before force-applying
    if let Ok(existing) = api.get(name).await {
        verify_ownership(&existing)?;
    }

    let patch_params = PatchParams::apply("rivven-operator").force();
    let result = api
        .patch(name, &patch_params, &Patch::Apply(&sts))
        .await
        .map_err(OperatorError::from)?;

    Ok(result.status)
}

/// Apply a PodDisruptionBudget using server-side apply
async fn apply_pdb(client: &Client, namespace: &str, pdb: PodDisruptionBudget) -> Result<()> {
    let api: Api<PodDisruptionBudget> = Api::namespaced(client.clone(), namespace);
    let name = pdb
        .metadata
        .name
        .as_ref()
        .ok_or_else(|| OperatorError::InvalidConfig("PDB missing metadata.name".into()))?;

    debug!(name = %name, "Applying PodDisruptionBudget");

    // Verify ownership before force-applying
    if let Ok(existing) = api.get(name).await {
        verify_ownership(&existing)?;
    }

    let patch_params = PatchParams::apply("rivven-operator").force();
    api.patch(name, &patch_params, &Patch::Apply(&pdb))
        .await
        .map_err(OperatorError::from)?;

    Ok(())
}

/// Build cluster status from StatefulSet status
fn build_status(
    cluster: &RivvenCluster,
    sts_status: Option<k8s_openapi::api::apps::v1::StatefulSetStatus>,
) -> RivvenClusterStatus {
    let now = Utc::now().to_rfc3339();

    let (replicas, ready_replicas, updated_replicas) = sts_status
        .map(|s| {
            (
                s.replicas,
                s.ready_replicas.unwrap_or(0),
                s.updated_replicas.unwrap_or(0),
            )
        })
        .unwrap_or((0, 0, 0));

    let desired_replicas = cluster.spec.replicas;

    // Determine phase based on state
    let phase = if ready_replicas == 0 {
        ClusterPhase::Provisioning
    } else if ready_replicas < desired_replicas {
        if updated_replicas < desired_replicas {
            ClusterPhase::Updating
        } else {
            ClusterPhase::Degraded
        }
    } else if ready_replicas == desired_replicas {
        ClusterPhase::Running
    } else {
        ClusterPhase::Degraded
    };

    // Build conditions
    let mut conditions = vec![];

    // Ready condition
    conditions.push(ClusterCondition {
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

    // Available condition
    conditions.push(ClusterCondition {
        condition_type: "Available".to_string(),
        status: if ready_replicas > 0 {
            "True".to_string()
        } else {
            "False".to_string()
        },
        reason: Some(
            if ready_replicas > 0 {
                "AtLeastOneReplicaReady"
            } else {
                "NoReplicasReady"
            }
            .to_string(),
        ),
        message: None,
        last_transition_time: Some(now.clone()),
    });

    // Build broker endpoints
    let name = cluster
        .metadata
        .name
        .as_ref()
        .map(|n| format!("rivven-{}", n));
    let namespace = cluster
        .metadata
        .namespace
        .as_ref()
        .cloned()
        .unwrap_or_else(|| "default".to_string());

    let broker_endpoints: Vec<String> = (0..ready_replicas)
        .map(|i| {
            format!(
                "{}-{}.{}-headless.{}.svc.cluster.local:9092",
                name.as_deref().unwrap_or("rivven"),
                i,
                name.as_deref().unwrap_or("rivven"),
                namespace
            )
        })
        .collect();

    RivvenClusterStatus {
        phase,
        replicas,
        ready_replicas,
        updated_replicas,
        observed_generation: cluster.metadata.generation.unwrap_or(0),
        conditions,
        broker_endpoints,
        leader: None, // Would need to query the cluster to determine leader
        last_updated: Some(now),
        message: None,
    }
}

/// Update the cluster status subresource
async fn update_status(
    client: &Client,
    namespace: &str,
    name: &str,
    status: RivvenClusterStatus,
) -> Result<()> {
    let api: Api<RivvenCluster> = Api::namespaced(client.clone(), namespace);

    debug!(name = %name, phase = ?status.phase, "Updating cluster status");

    let patch = serde_json::json!({
        "status": status
    });

    let patch_params = PatchParams::default();
    api.patch_status(name, &patch_params, &Patch::Merge(&patch))
        .await
        .map_err(OperatorError::from)?;

    Ok(())
}

/// Error policy for the controller — exponential backoff with jitter.
fn error_policy(
    cluster: Arc<RivvenCluster>,
    error: &OperatorError,
    ctx: Arc<ControllerContext>,
) -> Action {
    let key = cluster.name_any();
    let retries = {
        let mut entry = ctx.error_counts.entry(key.clone()).or_insert(0);
        *entry += 1;
        *entry
    };

    // Use the error's suggested delay OR exponential backoff:
    // 30s → 60s → 120s → 240s → 480s → 600s (capped)
    let delay = error.requeue_delay().unwrap_or_else(|| {
        let base = Duration::from_secs(ERROR_REQUEUE_SECONDS);
        let backoff = base * 2u32.saturating_pow((retries - 1).min(5));
        backoff.min(Duration::from_secs(MAX_ERROR_REQUEUE_SECONDS))
    });

    warn!(
        error = %error,
        retry = retries,
        delay_secs = delay.as_secs(),
        "Reconciliation error for '{}', will retry",
        key
    );

    Action::requeue(delay)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        BrokerConfig, MetricsSpec, PdbSpec, ProbeSpec, RivvenClusterSpec, StorageSpec, TlsSpec,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;

    fn create_test_cluster() -> RivvenCluster {
        RivvenCluster {
            metadata: ObjectMeta {
                name: Some("test-cluster".to_string()),
                namespace: Some("default".to_string()),
                uid: Some("test-uid".to_string()),
                generation: Some(1),
                ..Default::default()
            },
            spec: RivvenClusterSpec {
                replicas: 3,
                version: "0.0.1".to_string(),
                image: None,
                image_pull_policy: "IfNotPresent".to_string(),
                image_pull_secrets: vec![],
                storage: StorageSpec::default(),
                resources: None,
                config: BrokerConfig::default(),
                tls: TlsSpec::default(),
                metrics: MetricsSpec::default(),
                affinity: None,
                node_selector: BTreeMap::new(),
                tolerations: vec![],
                pod_disruption_budget: PdbSpec::default(),
                service_account: None,
                pod_annotations: BTreeMap::new(),
                pod_labels: BTreeMap::new(),
                env: vec![],
                liveness_probe: ProbeSpec::default(),
                readiness_probe: ProbeSpec::default(),
                security_context: None,
                container_security_context: None,
            },
            status: None,
        }
    }

    #[test]
    fn test_build_status_provisioning() {
        let cluster = create_test_cluster();
        let status = build_status(&cluster, None);

        assert_eq!(status.phase, ClusterPhase::Provisioning);
        assert_eq!(status.replicas, 0);
        assert_eq!(status.ready_replicas, 0);
    }

    #[test]
    fn test_build_status_running() {
        let cluster = create_test_cluster();
        let sts_status = k8s_openapi::api::apps::v1::StatefulSetStatus {
            replicas: 3,
            ready_replicas: Some(3),
            updated_replicas: Some(3),
            ..Default::default()
        };

        let status = build_status(&cluster, Some(sts_status));

        assert_eq!(status.phase, ClusterPhase::Running);
        assert_eq!(status.replicas, 3);
        assert_eq!(status.ready_replicas, 3);
    }

    #[test]
    fn test_build_status_degraded() {
        let cluster = create_test_cluster();
        let sts_status = k8s_openapi::api::apps::v1::StatefulSetStatus {
            replicas: 3,
            ready_replicas: Some(2),
            updated_replicas: Some(3),
            ..Default::default()
        };

        let status = build_status(&cluster, Some(sts_status));

        assert_eq!(status.phase, ClusterPhase::Degraded);
    }

    #[test]
    fn test_build_status_updating() {
        let cluster = create_test_cluster();
        let sts_status = k8s_openapi::api::apps::v1::StatefulSetStatus {
            replicas: 3,
            ready_replicas: Some(2),
            updated_replicas: Some(1),
            ..Default::default()
        };

        let status = build_status(&cluster, Some(sts_status));

        assert_eq!(status.phase, ClusterPhase::Updating);
    }

    #[test]
    fn test_broker_endpoints() {
        let cluster = create_test_cluster();
        let sts_status = k8s_openapi::api::apps::v1::StatefulSetStatus {
            replicas: 3,
            ready_replicas: Some(3),
            updated_replicas: Some(3),
            ..Default::default()
        };

        let status = build_status(&cluster, Some(sts_status));

        assert_eq!(status.broker_endpoints.len(), 3);
        assert!(status.broker_endpoints[0].contains("rivven-test-cluster-0"));
    }

    #[test]
    fn test_conditions() {
        let cluster = create_test_cluster();
        let sts_status = k8s_openapi::api::apps::v1::StatefulSetStatus {
            replicas: 3,
            ready_replicas: Some(3),
            updated_replicas: Some(3),
            ..Default::default()
        };

        let status = build_status(&cluster, Some(sts_status));

        assert_eq!(status.conditions.len(), 2);

        let ready_cond = status
            .conditions
            .iter()
            .find(|c| c.condition_type == "Ready")
            .unwrap();
        assert_eq!(ready_cond.status, "True");

        let available_cond = status
            .conditions
            .iter()
            .find(|c| c.condition_type == "Available")
            .unwrap();
        assert_eq!(available_cond.status, "True");
    }
}
