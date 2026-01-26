//! Distributed connector types and error handling

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

/// Node identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn generate() -> Self {
        Self(format!("node-{}", uuid::Uuid::new_v4()))
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Task identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId(pub String);

impl TaskId {
    pub fn new(connector: &str, task_num: u32) -> Self {
        Self(format!("{}-{}", connector, task_num))
    }

    /// Get the connector name from task ID
    pub fn connector_name(&self) -> &str {
        self.0
            .rsplit_once('-')
            .map(|(name, _)| name)
            .unwrap_or(&self.0)
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Connector identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectorId(pub String);

impl ConnectorId {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl std::fmt::Display for ConnectorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for ConnectorId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Connector execution mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ConnectorMode {
    /// Only one instance runs at a time (with failover)
    /// Used for: CDC connectors that can't be partitioned
    Singleton,

    /// Multiple instances with automatic task partitioning
    /// Used for: S3 sink, Elasticsearch sink
    #[default]
    Scalable,

    /// One task per configured partition
    /// Used for: Connectors with explicit parallelism config
    Partitioned,
}

impl std::fmt::Display for ConnectorMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorMode::Singleton => write!(f, "singleton"),
            ConnectorMode::Scalable => write!(f, "scalable"),
            ConnectorMode::Partitioned => write!(f, "partitioned"),
        }
    }
}

/// Connector configuration for distributed mode
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfig {
    /// Connector name (unique identifier)
    pub name: ConnectorId,

    /// Connector type (e.g., "postgres-cdc", "s3")
    pub connector_type: String,

    /// Execution mode
    #[serde(default)]
    pub mode: ConnectorMode,

    /// Maximum number of tasks (for scalable mode)
    #[serde(default = "default_max_tasks")]
    pub max_tasks: u32,

    /// Connector-specific configuration
    pub config: serde_json::Value,

    /// Topics involved
    pub topics: Vec<String>,

    /// Priority (higher = more important for scheduling)
    #[serde(default)]
    pub priority: i32,

    /// Resource requirements
    #[serde(default)]
    pub resources: ResourceRequirements,

    /// Failover configuration
    #[serde(default)]
    pub failover: FailoverConfig,
}

fn default_max_tasks() -> u32 {
    8
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        Self {
            name: ConnectorId::new("unnamed"),
            connector_type: "unknown".to_string(),
            mode: ConnectorMode::default(),
            max_tasks: default_max_tasks(),
            config: serde_json::Value::Null,
            topics: Vec::new(),
            priority: 0,
            resources: ResourceRequirements::default(),
            failover: FailoverConfig::default(),
        }
    }
}

/// Resource requirements for a connector
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceRequirements {
    /// Memory in MB (0 = no limit)
    #[serde(default)]
    pub memory_mb: u32,

    /// CPU cores (0 = no limit)
    #[serde(default)]
    pub cpu_cores: f32,

    /// Prefer nodes with specific labels
    #[serde(default)]
    pub node_labels: HashMap<String, String>,
}

/// Failover configuration for singleton connectors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverConfig {
    /// Enable automatic failover
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Heartbeat interval in milliseconds
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_ms: u64,

    /// Time before considering a node dead
    #[serde(default = "default_failure_timeout")]
    pub failure_timeout_ms: u64,

    /// Minimum time between failovers
    #[serde(default = "default_min_failover_interval")]
    pub min_failover_interval_ms: u64,
}

fn default_true() -> bool {
    true
}

fn default_heartbeat_interval() -> u64 {
    1000 // 1 second
}

fn default_failure_timeout() -> u64 {
    10000 // 10 seconds
}

fn default_min_failover_interval() -> u64 {
    30000 // 30 seconds
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            heartbeat_interval_ms: default_heartbeat_interval(),
            failure_timeout_ms: default_failure_timeout(),
            min_failover_interval_ms: default_min_failover_interval(),
        }
    }
}

/// Distributed connector error types
#[derive(Debug, Error)]
pub enum DistributedError {
    #[error("Not the leader: current leader is {0}")]
    NotLeader(NodeId),

    #[error("No leader available")]
    NoLeader,

    #[error("Node not found: {0}")]
    NodeNotFound(NodeId),

    #[error("Connector not found: {0}")]
    ConnectorNotFound(ConnectorId),

    #[error("Task not found: {0}")]
    TaskNotFound(TaskId),

    #[error("Connector already exists: {0}")]
    ConnectorExists(ConnectorId),

    #[error("Insufficient resources: {0}")]
    InsufficientResources(String),

    #[error("Coordination error: {0}")]
    CoordinationError(String),

    #[error("Communication error: {0}")]
    CommunicationError(String),

    #[error("Timeout waiting for {0}")]
    Timeout(String),

    #[error("Invalid state transition: {0}")]
    InvalidStateTransition(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Broker error: {0}")]
    BrokerError(String),
}

impl From<serde_json::Error> for DistributedError {
    fn from(err: serde_json::Error) -> Self {
        DistributedError::SerializationError(err.to_string())
    }
}

pub type DistributedResult<T> = Result<T, DistributedError>;

/// Generation number for leader election
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct Generation(pub u64);

impl Generation {
    pub fn new(gen: u64) -> Self {
        Self(gen)
    }

    pub fn increment(&mut self) -> Self {
        self.0 += 1;
        *self
    }
}

impl std::fmt::Display for Generation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Epoch for connector assignments (incremented on rebalance)
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct Epoch(pub u64);

impl Epoch {
    pub fn new(epoch: u64) -> Self {
        Self(epoch)
    }

    pub fn increment(&mut self) -> Self {
        self.0 += 1;
        *self
    }
}

impl std::fmt::Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
