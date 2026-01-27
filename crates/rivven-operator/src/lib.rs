//! # Rivven Kubernetes Operator
//!
//! Production-grade Kubernetes operator for deploying and managing Rivven clusters and connectors.
//!
//! This crate provides the core functionality for the Rivven Kubernetes operator,
//! enabling declarative management of Rivven clusters using Custom Resource Definitions (CRDs).
//!
//! ## Features
//!
//! - **Custom Resource Definitions**: `RivvenCluster` and `RivvenConnect` CRDs for declarative management
//! - **Automated Reconciliation**: Continuous state management with eventual consistency
//! - **StatefulSet Management**: Ordered deployment, scaling, and rolling updates
//! - **Service Discovery**: Automatic headless service for broker discovery
//! - **Configuration Management**: ConfigMaps for broker and connector configuration
//! - **Security**: Pod security contexts, TLS support, and secure defaults
//! - **Observability**: Prometheus-compatible operator metrics
//! - **High Availability**: PodDisruptionBudget support for safe upgrades
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use rivven_operator::prelude::*;
//! use kube::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Create Kubernetes client from default config
//!     let client = Client::try_default().await?;
//!     
//!     // Run the operator controller
//!     run_controller(client, None).await
//! }
//! ```
//!
//! ## Architecture
//!
//! The operator follows the standard Kubernetes controller pattern:
//!
//! 1. **Watch**: Monitor RivvenCluster and RivvenConnect resources for changes
//! 2. **Reconcile**: Compare desired state (CRD spec) with actual state (K8s resources)
//! 3. **Act**: Create, update, or delete resources to match desired state
//! 4. **Status**: Update CRD status with current cluster state
//!
//! ## Modules
//!
//! - [`crd`] - Custom Resource Definition types with validation
//! - [`controller`] - Reconciliation logic and controller setup
//! - [`resources`] - Kubernetes resource builders (StatefulSet, Service, ConfigMap)
//! - [`error`] - Error types for operator operations
//!
//! ## Custom Resource Definitions
//!
//! ### RivvenCluster
//!
//! Manages Rivven broker clusters with StatefulSets:
//!
//! ```yaml
//! apiVersion: rivven.io/v1alpha1
//! kind: RivvenCluster
//! metadata:
//!   name: production
//! spec:
//!   replicas: 3
//!   version: "0.1.0"
//!   storage:
//!     size: 100Gi
//!   config:
//!     defaultPartitions: 3
//!     defaultReplicationFactor: 2
//! ```
//!
//! ### RivvenConnect
//!
//! Manages connector pipelines for CDC and data integration:
//!
//! ```yaml
//! apiVersion: rivven.io/v1alpha1
//! kind: RivvenConnect
//! metadata:
//!   name: cdc-pipeline
//! spec:
//!   clusterRef:
//!     name: production
//!   sources:
//!     - name: postgres-cdc
//!       connector: postgres-cdc
//!       topic: cdc.events
//!   sinks:
//!     - name: s3-archive
//!       connector: s3
//!       topics: ["cdc.*"]
//! ```
//!
//! ### RivvenTopic
//!
//! Manages topics declaratively for GitOps workflows:
//!
//! ```yaml
//! apiVersion: rivven.io/v1alpha1
//! kind: RivvenTopic
//! metadata:
//!   name: orders-events
//! spec:
//!   clusterRef:
//!     name: production
//!   partitions: 12
//!   replicationFactor: 3
//!   config:
//!     retentionMs: 604800000
//!     cleanupPolicy: delete
//!     compressionType: lz4
//!   acls:
//!     - principal: "user:order-service"
//!       operations: ["Read", "Write"]
//! ```
//!
//! ## Security
//!
//! The operator applies secure defaults:
//!
//! - **Non-root containers**: `runAsNonRoot: true`
//! - **Read-only filesystem**: `readOnlyRootFilesystem: true`
//! - **Dropped capabilities**: All capabilities dropped
//! - **Seccomp profiles**: RuntimeDefault seccomp profile
//! - **TLS support**: Optional TLS for broker communication
//!
//! ## Metrics
//!
//! The operator exposes Prometheus metrics:
//!
//! - `rivven_operator_reconcile_total` - Total reconciliation attempts
//! - `rivven_operator_reconcile_errors_total` - Reconciliation errors
//! - `rivven_operator_reconcile_duration_seconds` - Reconciliation latency
//!
//! ## Feature Flags
//!
//! This crate does not have optional features - all functionality is included
//! by default for simplicity.

pub mod connect_controller;
pub mod controller;
pub mod crd;
pub mod error;
pub mod resources;
pub mod topic_controller;

pub mod prelude {
    //! Re-exports for convenient usage
    pub use crate::connect_controller::{
        run_connect_controller, ConnectControllerContext, ConnectControllerMetrics,
    };
    pub use crate::controller::{run_controller, ControllerContext, ControllerMetrics};
    pub use crate::topic_controller::{
        run_topic_controller, TopicControllerContext, TopicControllerMetrics,
    };
    // RivvenCluster CRD types
    pub use crate::crd::{
        BrokerConfig, ClusterCondition, ClusterPhase, MetricsSpec, PdbSpec, ProbeSpec,
        RivvenCluster, RivvenClusterSpec, RivvenClusterStatus, ServiceMonitorSpec, StorageSpec,
        TlsSpec,
    };
    // RivvenConnect CRD types
    pub use crate::crd::{
        ClusterReference, ConnectCondition, ConnectConfigSpec, ConnectMetricsSpec, ConnectPhase,
        ConnectTlsSpec, ConnectorStatus, GlobalConnectSettings, HealthConfigSpec, RateLimitSpec,
        RetryConfigSpec, RivvenConnect, RivvenConnectSpec, RivvenConnectStatus, SinkConnectorSpec,
        SourceConnectorSpec, SourceTopicConfigSpec, TableSpec, TopicSettingsSpec,
    };
    // RivvenTopic CRD types
    pub use crate::crd::{
        PartitionInfo, RivvenTopic, RivvenTopicSpec, RivvenTopicStatus, TopicAcl, TopicCondition,
        TopicConfig,
    };
    pub use crate::error::{OperatorError, Result};
    pub use crate::resources::ResourceBuilder;
}
