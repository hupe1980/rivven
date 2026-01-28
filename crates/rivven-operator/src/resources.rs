//! Kubernetes Resource Builders
//!
//! This module generates Kubernetes manifests (StatefulSet, Service, ConfigMap, etc.)
//! from RivvenCluster specifications.

use crate::crd::{RivvenCluster, RivvenClusterSpec};
use crate::error::{OperatorError, Result};
use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::{
    ConfigMap, Container, ContainerPort, EnvVar, EnvVarSource, HTTPGetAction, ObjectFieldSelector,
    PersistentVolumeClaim, PersistentVolumeClaimSpec, PodSpec, PodTemplateSpec, Probe, Service,
    ServicePort, ServiceSpec, VolumeMount,
};
use k8s_openapi::api::policy::v1::{PodDisruptionBudget, PodDisruptionBudgetSpec};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use std::collections::BTreeMap;

/// Builder for generating Kubernetes resources from a RivvenCluster
pub struct ResourceBuilder<'a> {
    cluster: &'a RivvenCluster,
    name: String,
    namespace: String,
}

impl<'a> ResourceBuilder<'a> {
    /// Create a new resource builder
    pub fn new(cluster: &'a RivvenCluster) -> Result<Self> {
        let name =
            cluster.metadata.name.clone().ok_or_else(|| {
                OperatorError::InvalidConfig("cluster name is required".to_string())
            })?;

        let namespace = cluster
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        Ok(Self {
            cluster,
            name,
            namespace,
        })
    }

    /// Get the resource name prefix
    fn resource_name(&self) -> String {
        format!("rivven-{}", self.name)
    }

    /// Get owner reference for managed resources
    fn owner_reference(&self) -> OwnerReference {
        OwnerReference {
            api_version: "rivven.io/v1alpha1".to_string(),
            kind: "RivvenCluster".to_string(),
            name: self.name.clone(),
            uid: self.cluster.metadata.uid.clone().unwrap_or_default(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }
    }

    /// Build the StatefulSet for the Rivven cluster
    pub fn build_statefulset(&self) -> StatefulSet {
        let spec = &self.cluster.spec;
        let name = self.resource_name();
        let labels = spec.get_labels(&self.name);
        let selector_labels = spec.get_selector_labels(&self.name);

        // Build container
        let container = self.build_container(spec);

        // Build PVC template
        let pvc_template = self.build_pvc_template(spec);

        // Build pod template
        let mut pod_labels = selector_labels.clone();
        pod_labels.extend(spec.pod_labels.clone());

        let mut pod_annotations = BTreeMap::new();
        if spec.metrics.enabled {
            pod_annotations.insert("prometheus.io/scrape".to_string(), "true".to_string());
            pod_annotations.insert(
                "prometheus.io/port".to_string(),
                spec.metrics.port.to_string(),
            );
        }
        pod_annotations.extend(spec.pod_annotations.clone());

        // Apply secure defaults for pod security context if not specified
        let pod_security_context = spec.security_context.clone().or_else(|| {
            Some(k8s_openapi::api::core::v1::PodSecurityContext {
                // Run as non-root by default
                run_as_non_root: Some(true),
                // Use rivven user (UID 1000)
                run_as_user: Some(1000),
                run_as_group: Some(1000),
                fs_group: Some(1000),
                // Use RuntimeDefault seccomp profile
                seccomp_profile: Some(k8s_openapi::api::core::v1::SeccompProfile {
                    type_: "RuntimeDefault".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            })
        });

        let pod_spec = PodSpec {
            containers: vec![container],
            affinity: spec.affinity.clone(),
            security_context: pod_security_context,
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
                        .map(|s| k8s_openapi::api::core::v1::LocalObjectReference {
                            name: s.clone(),
                        })
                        .collect(),
                )
            },
            // Disable service account token auto-mounting for security
            automount_service_account_token: Some(false),
            ..Default::default()
        };

        StatefulSet {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                namespace: Some(self.namespace.clone()),
                labels: Some(labels.clone()),
                owner_references: Some(vec![self.owner_reference()]),
                ..Default::default()
            },
            spec: Some(StatefulSetSpec {
                service_name: format!("{}-headless", name),
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
                volume_claim_templates: Some(vec![pvc_template]),
                pod_management_policy: Some("Parallel".to_string()),
                update_strategy: Some(k8s_openapi::api::apps::v1::StatefulSetUpdateStrategy {
                    type_: Some("RollingUpdate".to_string()),
                    rolling_update: Some(
                        k8s_openapi::api::apps::v1::RollingUpdateStatefulSetStrategy {
                            max_unavailable: Some(IntOrString::Int(1)),
                            partition: Some(0),
                        },
                    ),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Build the main container for the broker pod
    fn build_container(&self, spec: &RivvenClusterSpec) -> Container {
        let name = self.resource_name();

        // Build environment variables
        let mut env = vec![
            EnvVar {
                name: "RIVVEN_DATA_DIR".to_string(),
                value: Some("/data".to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "RIVVEN_BIND_ADDRESS".to_string(),
                value: Some("0.0.0.0".to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "RIVVEN_PORT".to_string(),
                value: Some("9092".to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "RIVVEN_CLUSTER_NAME".to_string(),
                value: Some(self.name.clone()),
                ..Default::default()
            },
            EnvVar {
                name: "RIVVEN_POD_NAME".to_string(),
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
                name: "RIVVEN_POD_NAMESPACE".to_string(),
                value_from: Some(EnvVarSource {
                    field_ref: Some(ObjectFieldSelector {
                        field_path: "metadata.namespace".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            EnvVar {
                name: "RIVVEN_SERVICE_NAME".to_string(),
                value: Some(format!("{}-headless", name)),
                ..Default::default()
            },
            // Broker config
            EnvVar {
                name: "RIVVEN_DEFAULT_PARTITIONS".to_string(),
                value: Some(spec.config.default_partitions.to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "RIVVEN_DEFAULT_REPLICATION_FACTOR".to_string(),
                value: Some(spec.config.default_replication_factor.to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "RIVVEN_COMPRESSION".to_string(),
                value: Some(spec.config.compression_type.clone()),
                ..Default::default()
            },
        ];

        // Add metrics config
        if spec.metrics.enabled {
            env.push(EnvVar {
                name: "RIVVEN_METRICS_ENABLED".to_string(),
                value: Some("true".to_string()),
                ..Default::default()
            });
            env.push(EnvVar {
                name: "RIVVEN_METRICS_PORT".to_string(),
                value: Some(spec.metrics.port.to_string()),
                ..Default::default()
            });
        }

        // Add TLS config
        if spec.tls.enabled {
            env.push(EnvVar {
                name: "RIVVEN_TLS_ENABLED".to_string(),
                value: Some("true".to_string()),
                ..Default::default()
            });
            if let Some(ref cert_secret) = spec.tls.cert_secret_name {
                env.push(EnvVar {
                    name: "RIVVEN_TLS_CERT_PATH".to_string(),
                    value: Some("/tls/tls.crt".to_string()),
                    ..Default::default()
                });
                env.push(EnvVar {
                    name: "RIVVEN_TLS_KEY_PATH".to_string(),
                    value: Some("/tls/tls.key".to_string()),
                    ..Default::default()
                });
                // Note: Volume mount for TLS secret would be added here
                let _ = cert_secret; // Suppress unused warning
            }
        }

        // Add user-defined env vars
        env.extend(spec.env.clone());

        // Build ports
        let mut ports = vec![ContainerPort {
            name: Some("broker".to_string()),
            container_port: 9092,
            protocol: Some("TCP".to_string()),
            ..Default::default()
        }];

        if spec.metrics.enabled {
            ports.push(ContainerPort {
                name: Some("metrics".to_string()),
                container_port: spec.metrics.port,
                protocol: Some("TCP".to_string()),
                ..Default::default()
            });
        }

        // Build probes
        let liveness_probe = if spec.liveness_probe.enabled {
            Some(Probe {
                http_get: Some(HTTPGetAction {
                    path: Some("/health".to_string()),
                    port: IntOrString::Int(9092),
                    scheme: Some("HTTP".to_string()),
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
                    path: Some("/ready".to_string()),
                    port: IntOrString::Int(9092),
                    scheme: Some("HTTP".to_string()),
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

        // Build volume mounts
        let volume_mounts = vec![VolumeMount {
            name: "data".to_string(),
            mount_path: "/data".to_string(),
            ..Default::default()
        }];

        // Apply secure defaults for container security context if not specified
        let container_security_context = spec.container_security_context.clone().or_else(|| {
            Some(k8s_openapi::api::core::v1::SecurityContext {
                // Prevent privilege escalation
                allow_privilege_escalation: Some(false),
                // Read-only root filesystem (data volume is writable)
                read_only_root_filesystem: Some(true),
                // Run as non-root
                run_as_non_root: Some(true),
                run_as_user: Some(1000),
                run_as_group: Some(1000),
                // Drop all capabilities (Rivven doesn't need any)
                capabilities: Some(k8s_openapi::api::core::v1::Capabilities {
                    drop: Some(vec!["ALL".to_string()]),
                    ..Default::default()
                }),
                // Use RuntimeDefault seccomp profile
                seccomp_profile: Some(k8s_openapi::api::core::v1::SeccompProfile {
                    type_: "RuntimeDefault".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            })
        });

        Container {
            name: "rivven".to_string(),
            image: Some(spec.get_image()),
            image_pull_policy: Some(spec.image_pull_policy.clone()),
            command: Some(vec!["rivvend".to_string()]),
            args: Some(vec![
                "--config".to_string(),
                "/etc/rivven/config.yaml".to_string(),
            ]),
            env: Some(env),
            ports: Some(ports),
            resources: spec.resources.clone(),
            liveness_probe,
            readiness_probe,
            volume_mounts: Some(volume_mounts),
            security_context: container_security_context,
            ..Default::default()
        }
    }

    /// Build PVC template for StatefulSet
    fn build_pvc_template(&self, spec: &RivvenClusterSpec) -> PersistentVolumeClaim {
        let mut resources = BTreeMap::new();
        resources.insert("storage".to_string(), Quantity(spec.storage.size.clone()));

        PersistentVolumeClaim {
            metadata: ObjectMeta {
                name: Some("data".to_string()),
                ..Default::default()
            },
            spec: Some(PersistentVolumeClaimSpec {
                access_modes: Some(spec.storage.access_modes.clone()),
                storage_class_name: spec.storage.storage_class_name.clone(),
                resources: Some(k8s_openapi::api::core::v1::VolumeResourceRequirements {
                    requests: Some(resources),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Build the headless service for pod discovery
    pub fn build_headless_service(&self) -> Service {
        let spec = &self.cluster.spec;
        let name = format!("{}-headless", self.resource_name());
        let labels = spec.get_labels(&self.name);
        let selector_labels = spec.get_selector_labels(&self.name);

        Service {
            metadata: ObjectMeta {
                name: Some(name),
                namespace: Some(self.namespace.clone()),
                labels: Some(labels),
                owner_references: Some(vec![self.owner_reference()]),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                cluster_ip: Some("None".to_string()),
                selector: Some(selector_labels),
                ports: Some(vec![
                    ServicePort {
                        name: Some("broker".to_string()),
                        port: 9092,
                        target_port: Some(IntOrString::Int(9092)),
                        protocol: Some("TCP".to_string()),
                        ..Default::default()
                    },
                    ServicePort {
                        name: Some("raft".to_string()),
                        port: 9093,
                        target_port: Some(IntOrString::Int(9093)),
                        protocol: Some("TCP".to_string()),
                        ..Default::default()
                    },
                ]),
                publish_not_ready_addresses: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Build the client-facing service
    pub fn build_client_service(&self) -> Service {
        let spec = &self.cluster.spec;
        let name = self.resource_name();
        let labels = spec.get_labels(&self.name);
        let selector_labels = spec.get_selector_labels(&self.name);

        let mut ports = vec![ServicePort {
            name: Some("broker".to_string()),
            port: 9092,
            target_port: Some(IntOrString::Int(9092)),
            protocol: Some("TCP".to_string()),
            ..Default::default()
        }];

        if spec.metrics.enabled {
            ports.push(ServicePort {
                name: Some("metrics".to_string()),
                port: spec.metrics.port,
                target_port: Some(IntOrString::Int(spec.metrics.port)),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            });
        }

        Service {
            metadata: ObjectMeta {
                name: Some(name),
                namespace: Some(self.namespace.clone()),
                labels: Some(labels),
                owner_references: Some(vec![self.owner_reference()]),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                type_: Some("ClusterIP".to_string()),
                selector: Some(selector_labels),
                ports: Some(ports),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Build ConfigMap for broker configuration
    pub fn build_configmap(&self) -> ConfigMap {
        let spec = &self.cluster.spec;
        let name = format!("{}-config", self.resource_name());
        let labels = spec.get_labels(&self.name);

        // Build configuration YAML
        let mut config = BTreeMap::new();
        config.insert(
            "default_partitions".to_string(),
            spec.config.default_partitions.to_string(),
        );
        config.insert(
            "default_replication_factor".to_string(),
            spec.config.default_replication_factor.to_string(),
        );
        config.insert(
            "log_retention_hours".to_string(),
            spec.config.log_retention_hours.to_string(),
        );
        config.insert(
            "log_segment_bytes".to_string(),
            spec.config.log_segment_bytes.to_string(),
        );
        config.insert(
            "max_message_bytes".to_string(),
            spec.config.max_message_bytes.to_string(),
        );
        config.insert(
            "auto_create_topics".to_string(),
            spec.config.auto_create_topics.to_string(),
        );
        config.insert(
            "compression_enabled".to_string(),
            spec.config.compression_enabled.to_string(),
        );
        config.insert(
            "compression_type".to_string(),
            spec.config.compression_type.clone(),
        );
        config.insert(
            "raft_election_timeout_ms".to_string(),
            spec.config.raft_election_timeout_ms.to_string(),
        );
        config.insert(
            "raft_heartbeat_interval_ms".to_string(),
            spec.config.raft_heartbeat_interval_ms.to_string(),
        );

        // Add raw overrides
        for (k, v) in &spec.config.raw {
            config.insert(k.clone(), v.clone());
        }

        let config_yaml = serde_yaml::to_string(&config).unwrap_or_default();

        let mut data = BTreeMap::new();
        data.insert("config.yaml".to_string(), config_yaml);

        ConfigMap {
            metadata: ObjectMeta {
                name: Some(name),
                namespace: Some(self.namespace.clone()),
                labels: Some(labels),
                owner_references: Some(vec![self.owner_reference()]),
                ..Default::default()
            },
            data: Some(data),
            ..Default::default()
        }
    }

    /// Build PodDisruptionBudget
    pub fn build_pdb(&self) -> Option<PodDisruptionBudget> {
        let spec = &self.cluster.spec;

        if !spec.pod_disruption_budget.enabled {
            return None;
        }

        let name = format!("{}-pdb", self.resource_name());
        let labels = spec.get_labels(&self.name);
        let selector_labels = spec.get_selector_labels(&self.name);

        Some(PodDisruptionBudget {
            metadata: ObjectMeta {
                name: Some(name),
                namespace: Some(self.namespace.clone()),
                labels: Some(labels),
                owner_references: Some(vec![self.owner_reference()]),
                ..Default::default()
            },
            spec: Some(PodDisruptionBudgetSpec {
                selector: Some(LabelSelector {
                    match_labels: Some(selector_labels),
                    ..Default::default()
                }),
                min_available: spec
                    .pod_disruption_budget
                    .min_available
                    .as_ref()
                    .map(|v| IntOrString::String(v.clone())),
                max_unavailable: spec
                    .pod_disruption_budget
                    .max_unavailable
                    .as_ref()
                    .map(|v| IntOrString::String(v.clone())),
                ..Default::default()
            }),
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{BrokerConfig, MetricsSpec, PdbSpec, ProbeSpec, StorageSpec, TlsSpec};

    fn create_test_cluster(name: &str) -> RivvenCluster {
        RivvenCluster {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some("default".to_string()),
                uid: Some("test-uid-123".to_string()),
                ..Default::default()
            },
            spec: RivvenClusterSpec {
                replicas: 3,
                version: "0.1.0".to_string(),
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
    fn test_build_statefulset() {
        let cluster = create_test_cluster("my-cluster");
        let builder = ResourceBuilder::new(&cluster).unwrap();
        let sts = builder.build_statefulset();

        assert_eq!(sts.metadata.name, Some("rivven-my-cluster".to_string()));
        assert_eq!(sts.spec.as_ref().unwrap().replicas, Some(3));
        assert_eq!(
            sts.spec.as_ref().unwrap().service_name,
            "rivven-my-cluster-headless"
        );
    }

    #[test]
    fn test_build_headless_service() {
        let cluster = create_test_cluster("my-cluster");
        let builder = ResourceBuilder::new(&cluster).unwrap();
        let svc = builder.build_headless_service();

        assert_eq!(
            svc.metadata.name,
            Some("rivven-my-cluster-headless".to_string())
        );
        assert_eq!(
            svc.spec.as_ref().unwrap().cluster_ip,
            Some("None".to_string())
        );
    }

    #[test]
    fn test_build_client_service() {
        let cluster = create_test_cluster("my-cluster");
        let builder = ResourceBuilder::new(&cluster).unwrap();
        let svc = builder.build_client_service();

        assert_eq!(svc.metadata.name, Some("rivven-my-cluster".to_string()));
        assert_eq!(
            svc.spec.as_ref().unwrap().type_,
            Some("ClusterIP".to_string())
        );
    }

    #[test]
    fn test_build_configmap() {
        let cluster = create_test_cluster("my-cluster");
        let builder = ResourceBuilder::new(&cluster).unwrap();
        let cm = builder.build_configmap();

        assert_eq!(
            cm.metadata.name,
            Some("rivven-my-cluster-config".to_string())
        );
        assert!(cm.data.is_some());
        assert!(cm.data.as_ref().unwrap().contains_key("config.yaml"));
    }

    #[test]
    fn test_build_pdb() {
        let cluster = create_test_cluster("my-cluster");
        let builder = ResourceBuilder::new(&cluster).unwrap();
        let pdb = builder.build_pdb();

        assert!(pdb.is_some());
        let pdb = pdb.unwrap();
        assert_eq!(pdb.metadata.name, Some("rivven-my-cluster-pdb".to_string()));
    }

    #[test]
    fn test_owner_references() {
        let cluster = create_test_cluster("my-cluster");
        let builder = ResourceBuilder::new(&cluster).unwrap();
        let sts = builder.build_statefulset();

        let owner_refs = sts.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "RivvenCluster");
        assert_eq!(owner_refs[0].name, "my-cluster");
    }

    #[test]
    fn test_custom_labels() {
        let mut cluster = create_test_cluster("my-cluster");
        cluster
            .spec
            .pod_labels
            .insert("custom".to_string(), "label".to_string());

        let builder = ResourceBuilder::new(&cluster).unwrap();
        let sts = builder.build_statefulset();

        let pod_labels = sts
            .spec
            .as_ref()
            .unwrap()
            .template
            .metadata
            .as_ref()
            .unwrap()
            .labels
            .as_ref()
            .unwrap();

        assert_eq!(pod_labels.get("custom"), Some(&"label".to_string()));
    }
}
