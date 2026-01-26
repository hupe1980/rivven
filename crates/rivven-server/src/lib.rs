//! Rivven Server
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
//! rivven-server
//!
//! # Cluster mode
//! rivven-server --mode cluster --node-id node-1 --seeds node-2:9093
//! ```

pub mod cli;
pub mod cluster_server;
pub mod handler;
pub mod partitioner;
pub mod protocol;
pub mod raft_api;
pub mod rate_limiter;
pub mod server;
pub mod auth_handler;
pub mod secure_server;

#[cfg(feature = "dashboard")]
pub mod dashboard;

#[cfg(test)]
mod protocol_tests;

pub use cli::{Cli, DeploymentMode};
pub use cluster_server::{ClusterServer, RequestRouter, ServerStatus, ShutdownHandle, ServerStats};
pub use raft_api::{RaftApiState, create_raft_router, start_raft_api_server};
pub use partitioner::{StickyPartitioner, StickyPartitionerConfig};
pub use rate_limiter::{RateLimiter, RateLimitConfig, ConnectionGuard, ConnectionResult, RequestResult};
pub use server::Server;
pub use auth_handler::{AuthenticatedHandler, ConnectionAuth};
pub use secure_server::{SecureServer, SecureServerConfig, SecureServerBuilder, ConnectionSecurityContext};

#[cfg(feature = "dashboard")]
pub use dashboard::{create_dashboard_router, DashboardState, DashboardData};

#[cfg(feature = "dashboard")]
pub use raft_api::{start_raft_api_server_with_dashboard, DashboardConfig};

