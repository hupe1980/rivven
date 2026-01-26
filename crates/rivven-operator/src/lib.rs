//! Rivven Kubernetes Operator Library
//!
//! This library provides the core functionality for the Rivven Kubernetes operator.
//! It can be used to build custom operators or extend the default behavior.
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_operator::prelude::*;
//! use kube::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let client = Client::try_default().await?;
//!     run_controller(client, None).await
//! }
//! ```

pub mod controller;
pub mod crd;
pub mod error;
pub mod resources;

pub mod prelude {
    //! Re-exports for convenient usage
    pub use crate::controller::{run_controller, ControllerContext, ControllerMetrics};
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
    pub use crate::error::{OperatorError, Result};
    pub use crate::resources::ResourceBuilder;
}
