//! Rivven Server Daemon (rivvend)
//!
//! High-performance distributed event streaming server with support
//! for both standalone and clustered deployment modes.
//!
//! ## Deployment Modes
//!
//! - **Standalone**: Single-node mode, zero configuration required
//! - **Cluster**: Multi-node mode with SWIM membership, Raft consensus,
//!   and ISR replication
//!
//! ## Quick Start
//!
//! ```bash
//! # Standalone mode
//! rivvend
//!
//! # Cluster mode
//! rivvend --mode cluster --node-id node-1 --seeds node-2:9093
//! ```

pub mod auth_handler;
pub mod cli;
pub mod cluster_server;
pub mod framing;
pub mod handler;
pub mod partitioner;
pub mod protocol;
pub mod raft_api;
pub mod rate_limiter;
pub mod secure_server;

#[cfg(feature = "dashboard")]
pub mod dashboard;

#[cfg(test)]
mod protocol_tests;

pub use auth_handler::{AuthenticatedHandler, ConnectionAuth};
pub use cli::{Cli, DeploymentMode};
pub use cluster_server::{ClusterServer, RequestRouter, ServerStats, ServerStatus, ShutdownHandle};
pub use partitioner::{StickyPartitioner, StickyPartitionerConfig};
pub use raft_api::{create_raft_router, start_raft_api_server, RaftApiState};
pub use rate_limiter::{
    ConnectionGuard, ConnectionResult, RateLimitConfig, RateLimiter, RequestResult,
};
pub use secure_server::{
    ConnectionSecurityContext, SecureServer, SecureServerBuilder, SecureServerConfig,
};

#[cfg(feature = "dashboard")]
pub use dashboard::{create_dashboard_router, DashboardData, DashboardState};

#[cfg(feature = "dashboard")]
pub use raft_api::{start_raft_api_server_with_dashboard, DashboardConfig};
