//! Cluster and topic metadata types

use serde::{Deserialize, Serialize};

/// Broker/node information for metadata discovery
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrokerInfo {
    /// Node ID
    pub node_id: String,
    /// Host for client connections
    pub host: String,
    /// Port for client connections
    pub port: u16,
    /// Optional rack ID for rack-aware placement
    pub rack: Option<String>,
}

impl BrokerInfo {
    /// Create a new broker info
    pub fn new(node_id: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        Self {
            node_id: node_id.into(),
            host: host.into(),
            port,
            rack: None,
        }
    }

    /// Set rack ID
    pub fn with_rack(mut self, rack: impl Into<String>) -> Self {
        self.rack = Some(rack.into());
        self
    }

    /// Get the address string (host:port)
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Topic metadata for cluster discovery
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicMetadata {
    /// Topic name
    pub name: String,
    /// Is the topic internal (e.g., __consumer_offsets, _schemas)
    pub is_internal: bool,
    /// Partition metadata
    pub partitions: Vec<PartitionMetadata>,
}

impl TopicMetadata {
    /// Create a new topic metadata
    pub fn new(name: impl Into<String>, partitions: Vec<PartitionMetadata>) -> Self {
        let name = name.into();
        let is_internal = name.starts_with('_');
        Self {
            name,
            is_internal,
            partitions,
        }
    }

    /// Get partition count
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Check if topic has any offline partitions
    pub fn has_offline_partitions(&self) -> bool {
        self.partitions.iter().any(|p| p.offline)
    }

    /// Get the leader node ID for a partition
    pub fn partition_leader(&self, partition: u32) -> Option<&str> {
        self.partitions
            .iter()
            .find(|p| p.partition == partition)
            .and_then(|p| p.leader.as_deref())
    }
}

/// Partition metadata for cluster discovery
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionMetadata {
    /// Partition ID
    pub partition: u32,
    /// Leader node ID (None if no leader)
    pub leader: Option<String>,
    /// Replica node IDs
    pub replicas: Vec<String>,
    /// ISR (in-sync replica) node IDs
    pub isr: Vec<String>,
    /// Is offline (no leader available)
    pub offline: bool,
}

impl PartitionMetadata {
    /// Create a new partition metadata
    pub fn new(partition: u32) -> Self {
        Self {
            partition,
            leader: None,
            replicas: Vec::new(),
            isr: Vec::new(),
            offline: true,
        }
    }

    /// Set the leader
    pub fn with_leader(mut self, leader: impl Into<String>) -> Self {
        self.leader = Some(leader.into());
        self.offline = false;
        self
    }

    /// Add replicas
    pub fn with_replicas(mut self, replicas: Vec<String>) -> Self {
        self.replicas = replicas;
        self
    }

    /// Add ISR
    pub fn with_isr(mut self, isr: Vec<String>) -> Self {
        self.isr = isr;
        self
    }

    /// Check if partition is under-replicated
    pub fn is_under_replicated(&self) -> bool {
        self.isr.len() < self.replicas.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_info() {
        let broker = BrokerInfo::new("node1", "localhost", 9092).with_rack("rack1");

        assert_eq!(broker.node_id, "node1");
        assert_eq!(broker.address(), "localhost:9092");
        assert_eq!(broker.rack, Some("rack1".to_string()));
    }

    #[test]
    fn test_topic_metadata_internal() {
        let topic = TopicMetadata::new("_schemas", vec![]);
        assert!(topic.is_internal);

        let topic = TopicMetadata::new("events", vec![]);
        assert!(!topic.is_internal);
    }

    #[test]
    fn test_partition_metadata() {
        let partition = PartitionMetadata::new(0)
            .with_leader("node1")
            .with_replicas(vec!["node1".to_string(), "node2".to_string()])
            .with_isr(vec!["node1".to_string()]);

        assert!(!partition.offline);
        assert!(partition.is_under_replicated());
        assert_eq!(partition.leader, Some("node1".to_string()));
    }
}
