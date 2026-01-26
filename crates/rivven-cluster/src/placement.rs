//! Partition placement using consistent hashing with rack awareness
//!
//! This module implements partition placement strategies:
//! - Consistent hashing for even distribution
//! - Rack awareness for fault tolerance
//! - Load balancing based on partition counts
//!
//! Goal: Minimize data movement during cluster changes while
//! maintaining balanced partition distribution.

use crate::error::{ClusterError, Result};
use crate::node::{Node, NodeId, NodeCapabilities};
use hashring::HashRing;
use std::collections::{HashMap, HashSet};

/// Partition placement strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PlacementStrategy {
    /// Round-robin across nodes (simple, predictable)
    RoundRobin,
    /// Consistent hashing (minimal reassignment on changes)
    #[default]
    ConsistentHash,
    /// Least-loaded node first
    LeastLoaded,
    /// Rack-aware placement (spread replicas across racks)
    RackAware,
}

/// Partition placement configuration
#[derive(Debug, Clone)]
pub struct PlacementConfig {
    /// Primary placement strategy
    pub strategy: PlacementStrategy,
    /// Enable rack awareness (spread replicas across racks)
    pub rack_aware: bool,
    /// Virtual nodes per physical node for consistent hashing
    pub virtual_nodes: usize,
    /// Maximum partitions per node (0 = unlimited)
    pub max_partitions_per_node: usize,
}

impl Default for PlacementConfig {
    fn default() -> Self {
        Self {
            strategy: PlacementStrategy::ConsistentHash,
            rack_aware: true,
            virtual_nodes: 150, // Good balance for even distribution
            max_partitions_per_node: 0,
        }
    }
}

/// Partition placer for assigning partitions to nodes
pub struct PartitionPlacer {
    config: PlacementConfig,
    /// Consistent hash ring
    ring: HashRing<NodeId>,
    /// Node information cache
    nodes: HashMap<NodeId, NodePlacementInfo>,
    /// Rack to nodes mapping
    racks: HashMap<String, Vec<NodeId>>,
}

/// Node information for placement decisions
#[derive(Debug, Clone)]
struct NodePlacementInfo {
    id: NodeId,
    rack: Option<String>,
    capabilities: NodeCapabilities,
    leader_count: u32,
    replica_count: u32,
    /// Higher weight = more partitions assigned (reserved for future weighted placement)
    #[allow(dead_code)]
    weight: u32,
}

impl PartitionPlacer {
    /// Create a new partition placer
    pub fn new(config: PlacementConfig) -> Self {
        Self {
            config,
            ring: HashRing::new(),
            nodes: HashMap::new(),
            racks: HashMap::new(),
        }
    }
    
    /// Add a node to the placer
    pub fn add_node(&mut self, node: &Node) {
        let info = NodePlacementInfo {
            id: node.info.id.clone(),
            rack: node.info.rack.clone(),
            capabilities: node.info.capabilities,
            leader_count: node.partition_leader_count,
            replica_count: node.partition_replica_count,
            weight: 100, // Default weight
        };
        
        // Add to consistent hash ring with virtual nodes
        for i in 0..self.config.virtual_nodes {
            let vnode = format!("{}#{}", node.info.id, i);
            self.ring.add(vnode);
        }
        
        // Track rack membership
        if let Some(rack) = &node.info.rack {
            self.racks
                .entry(rack.clone())
                .or_default()
                .push(node.info.id.clone());
        }
        
        self.nodes.insert(node.info.id.clone(), info);
    }
    
    /// Remove a node from the placer
    pub fn remove_node(&mut self, node_id: &NodeId) {
        // Remove from hash ring
        for i in 0..self.config.virtual_nodes {
            let vnode = format!("{}#{}", node_id, i);
            self.ring.remove(&vnode);
        }
        
        // Remove from rack tracking
        if let Some(info) = self.nodes.get(node_id) {
            if let Some(rack) = &info.rack {
                if let Some(rack_nodes) = self.racks.get_mut(rack) {
                    rack_nodes.retain(|n| n != node_id);
                }
            }
        }
        
        self.nodes.remove(node_id);
    }
    
    /// Update node load statistics
    pub fn update_node_load(&mut self, node_id: &NodeId, leader_count: u32, replica_count: u32) {
        if let Some(info) = self.nodes.get_mut(node_id) {
            info.leader_count = leader_count;
            info.replica_count = replica_count;
        }
    }
    
    /// Assign replicas for a new partition
    pub fn assign_partition(
        &self,
        topic: &str,
        partition: u32,
        replication_factor: u16,
    ) -> Result<Vec<NodeId>> {
        let eligible_nodes: Vec<_> = self.nodes
            .values()
            .filter(|n| n.capabilities.replica_eligible)
            .collect();
        
        if eligible_nodes.len() < replication_factor as usize {
            return Err(ClusterError::InvalidReplicationFactor {
                factor: replication_factor,
                nodes: eligible_nodes.len(),
            });
        }
        
        match self.config.strategy {
            PlacementStrategy::ConsistentHash => {
                self.assign_consistent_hash(topic, partition, replication_factor)
            }
            PlacementStrategy::RoundRobin => {
                self.assign_round_robin(topic, partition, replication_factor)
            }
            PlacementStrategy::LeastLoaded => {
                self.assign_least_loaded(replication_factor)
            }
            PlacementStrategy::RackAware => {
                self.assign_rack_aware(topic, partition, replication_factor)
            }
        }
    }
    
    /// Consistent hash based assignment
    fn assign_consistent_hash(
        &self,
        topic: &str,
        partition: u32,
        replication_factor: u16,
    ) -> Result<Vec<NodeId>> {
        let key = format!("{}-{}", topic, partition);
        let mut replicas = Vec::with_capacity(replication_factor as usize);
        let mut seen_nodes = HashSet::new();
        let mut seen_racks = HashSet::new();
        
        // Get nodes from the ring, skipping duplicates
        let ring_nodes: Vec<_> = (0..self.config.virtual_nodes * self.nodes.len())
            .filter_map(|i| {
                let probe_key = format!("{}-{}", key, i);
                self.ring.get(&probe_key).map(|vnode| {
                    // Extract node ID from virtual node (format: "node_id#N")
                    vnode.split('#').next().unwrap_or(vnode).to_string()
                })
            })
            .collect();
        
        for node_id in ring_nodes {
            if replicas.len() >= replication_factor as usize {
                break;
            }
            
            if seen_nodes.contains(&node_id) {
                continue;
            }
            
            // Rack awareness: try to spread across racks
            if self.config.rack_aware {
                if let Some(info) = self.nodes.get(&node_id) {
                    if let Some(rack) = &info.rack {
                        // Skip if we already have a replica in this rack
                        // (unless we have no choice)
                        if seen_racks.contains(rack) && seen_racks.len() < self.racks.len() {
                            continue;
                        }
                        seen_racks.insert(rack.clone());
                    }
                }
            }
            
            seen_nodes.insert(node_id.clone());
            replicas.push(node_id);
        }
        
        // If rack awareness prevented us from getting enough replicas, relax the constraint
        if replicas.len() < replication_factor as usize {
            for node_id in self.nodes.keys() {
                if replicas.len() >= replication_factor as usize {
                    break;
                }
                if !seen_nodes.contains(node_id) {
                    replicas.push(node_id.clone());
                    seen_nodes.insert(node_id.clone());
                }
            }
        }
        
        if replicas.len() < replication_factor as usize {
            return Err(ClusterError::InvalidReplicationFactor {
                factor: replication_factor,
                nodes: self.nodes.len(),
            });
        }
        
        Ok(replicas)
    }
    
    /// Round-robin assignment
    fn assign_round_robin(
        &self,
        _topic: &str,
        partition: u32,
        replication_factor: u16,
    ) -> Result<Vec<NodeId>> {
        let eligible: Vec<_> = self.nodes
            .values()
            .filter(|n| n.capabilities.replica_eligible)
            .map(|n| n.id.clone())
            .collect();
        
        if eligible.len() < replication_factor as usize {
            return Err(ClusterError::InvalidReplicationFactor {
                factor: replication_factor,
                nodes: eligible.len(),
            });
        }
        
        let mut replicas = Vec::with_capacity(replication_factor as usize);
        let start = partition as usize % eligible.len();
        
        for i in 0..replication_factor as usize {
            let idx = (start + i) % eligible.len();
            replicas.push(eligible[idx].clone());
        }
        
        Ok(replicas)
    }
    
    /// Least-loaded assignment
    fn assign_least_loaded(&self, replication_factor: u16) -> Result<Vec<NodeId>> {
        let mut eligible: Vec<_> = self.nodes
            .values()
            .filter(|n| n.capabilities.replica_eligible)
            .collect();
        
        if eligible.len() < replication_factor as usize {
            return Err(ClusterError::InvalidReplicationFactor {
                factor: replication_factor,
                nodes: eligible.len(),
            });
        }
        
        // Sort by load (leader_count * 3 + replica_count)
        eligible.sort_by_key(|n| n.leader_count * 3 + n.replica_count);
        
        let replicas: Vec<_> = eligible
            .iter()
            .take(replication_factor as usize)
            .map(|n| n.id.clone())
            .collect();
        
        Ok(replicas)
    }
    
    /// Rack-aware assignment (explicit rack spreading)
    fn assign_rack_aware(
        &self,
        topic: &str,
        partition: u32,
        replication_factor: u16,
    ) -> Result<Vec<NodeId>> {
        let key = format!("{}-{}", topic, partition);
        let mut replicas = Vec::with_capacity(replication_factor as usize);
        let mut used_racks = HashSet::new();
        let mut used_nodes = HashSet::new();
        
        // Get sorted racks for deterministic ordering
        let mut rack_list: Vec<_> = self.racks.keys().cloned().collect();
        rack_list.sort();
        
        // First pass: one replica per rack
        for rack in &rack_list {
            if replicas.len() >= replication_factor as usize {
                break;
            }
            
            if let Some(rack_nodes) = self.racks.get(rack) {
                // Pick node using hash for consistency
                let idx = {
                    let hash = fxhash(&format!("{}-{}", key, rack));
                    hash as usize % rack_nodes.len()
                };
                
                let node_id = &rack_nodes[idx];
                if let Some(info) = self.nodes.get(node_id) {
                    if info.capabilities.replica_eligible && !used_nodes.contains(node_id) {
                        replicas.push(node_id.clone());
                        used_nodes.insert(node_id.clone());
                        used_racks.insert(rack.clone());
                    }
                }
            }
        }
        
        // Second pass: fill remaining from any rack
        if replicas.len() < replication_factor as usize {
            let mut remaining: Vec<_> = self.nodes
                .values()
                .filter(|n| n.capabilities.replica_eligible && !used_nodes.contains(&n.id))
                .collect();
            
            // Sort by load for balance
            remaining.sort_by_key(|n| n.leader_count * 3 + n.replica_count);
            
            for info in remaining {
                if replicas.len() >= replication_factor as usize {
                    break;
                }
                replicas.push(info.id.clone());
            }
        }
        
        if replicas.len() < replication_factor as usize {
            return Err(ClusterError::InvalidReplicationFactor {
                factor: replication_factor,
                nodes: self.nodes.len(),
            });
        }
        
        Ok(replicas)
    }
    
    /// Calculate partition reassignments when nodes change
    pub fn calculate_reassignments(
        &self,
        current_assignments: &HashMap<String, Vec<Vec<NodeId>>>,
        _added_nodes: &[NodeId],
        removed_nodes: &[NodeId],
    ) -> HashMap<String, Vec<(u32, Vec<NodeId>)>> {
        let mut reassignments = HashMap::new();
        let removed_set: HashSet<_> = removed_nodes.iter().cloned().collect();
        
        for (topic, partitions) in current_assignments {
            let mut topic_reassignments = Vec::new();
            
            for (partition_idx, current_replicas) in partitions.iter().enumerate() {
                // Check if any replica is on a removed node
                let has_removed = current_replicas.iter().any(|n| removed_set.contains(n));
                
                if has_removed {
                    // Need to reassign this partition
                    let replication_factor = current_replicas.len() as u16;
                    
                    if let Ok(new_replicas) = self.assign_partition(
                        topic,
                        partition_idx as u32,
                        replication_factor,
                    ) {
                        topic_reassignments.push((partition_idx as u32, new_replicas));
                    }
                }
            }
            
            if !topic_reassignments.is_empty() {
                reassignments.insert(topic.clone(), topic_reassignments);
            }
        }
        
        reassignments
    }
    
    /// Get partition distribution statistics
    pub fn get_distribution_stats(&self) -> DistributionStats {
        let mut leader_counts: Vec<u32> = self.nodes.values().map(|n| n.leader_count).collect();
        let mut replica_counts: Vec<u32> = self.nodes.values().map(|n| n.replica_count).collect();
        
        leader_counts.sort();
        replica_counts.sort();
        
        let leader_sum: u32 = leader_counts.iter().sum();
        let replica_sum: u32 = replica_counts.iter().sum();
        
        DistributionStats {
            node_count: self.nodes.len(),
            rack_count: self.racks.len(),
            total_leaders: leader_sum,
            total_replicas: replica_sum,
            leader_min: leader_counts.first().copied().unwrap_or(0),
            leader_max: leader_counts.last().copied().unwrap_or(0),
            leader_avg: if self.nodes.is_empty() { 0.0 } else { leader_sum as f64 / self.nodes.len() as f64 },
            replica_min: replica_counts.first().copied().unwrap_or(0),
            replica_max: replica_counts.last().copied().unwrap_or(0),
            replica_avg: if self.nodes.is_empty() { 0.0 } else { replica_sum as f64 / self.nodes.len() as f64 },
        }
    }
}

/// Partition distribution statistics
#[derive(Debug, Clone)]
pub struct DistributionStats {
    pub node_count: usize,
    pub rack_count: usize,
    pub total_leaders: u32,
    pub total_replicas: u32,
    pub leader_min: u32,
    pub leader_max: u32,
    pub leader_avg: f64,
    pub replica_min: u32,
    pub replica_max: u32,
    pub replica_avg: f64,
}

/// Simple FNV-1a hash for deterministic hashing
fn fxhash(s: &str) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    
    let mut hash = FNV_OFFSET;
    for byte in s.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeInfo;
    
    fn create_test_node(id: &str, rack: Option<&str>) -> Node {
        let info = NodeInfo::new(
            id,
            format!("127.0.0.1:{}", 9092 + id.chars().last().unwrap().to_digit(10).unwrap_or(0)).parse().unwrap(),
            format!("127.0.0.1:{}", 9093 + id.chars().last().unwrap().to_digit(10).unwrap_or(0)).parse().unwrap(),
        );
        let mut info = if let Some(r) = rack { info.with_rack(r) } else { info };
        Node::new(info)
    }
    
    #[test]
    fn test_consistent_hash_placement() {
        let mut placer = PartitionPlacer::new(PlacementConfig::default());
        
        for i in 1..=5 {
            let node = create_test_node(&format!("node-{}", i), Some(&format!("rack-{}", (i % 3) + 1)));
            placer.add_node(&node);
        }
        
        // Assign partitions
        let replicas = placer.assign_partition("test-topic", 0, 3).unwrap();
        assert_eq!(replicas.len(), 3);
        
        // Same partition should get same assignment (deterministic)
        let replicas2 = placer.assign_partition("test-topic", 0, 3).unwrap();
        assert_eq!(replicas, replicas2);
    }
    
    #[test]
    fn test_rack_awareness() {
        let mut config = PlacementConfig::default();
        config.rack_aware = true;
        let mut placer = PartitionPlacer::new(config);
        
        // 6 nodes across 3 racks
        for i in 1..=6 {
            let rack = format!("rack-{}", ((i - 1) / 2) + 1);
            let node = create_test_node(&format!("node-{}", i), Some(&rack));
            placer.add_node(&node);
        }
        
        // With RF=3, we should get replicas in different racks
        let replicas = placer.assign_partition("test", 0, 3).unwrap();
        assert_eq!(replicas.len(), 3);
        
        // Check rack distribution
        let mut racks = HashSet::new();
        for replica in &replicas {
            if let Some(info) = placer.nodes.get(replica) {
                if let Some(rack) = &info.rack {
                    racks.insert(rack.clone());
                }
            }
        }
        
        // Should be spread across 3 racks
        assert_eq!(racks.len(), 3);
    }
    
    #[test]
    fn test_replication_factor_validation() {
        let mut placer = PartitionPlacer::new(PlacementConfig::default());
        
        // Only 2 nodes
        placer.add_node(&create_test_node("node-1", None));
        placer.add_node(&create_test_node("node-2", None));
        
        // RF=3 should fail
        let result = placer.assign_partition("test", 0, 3);
        assert!(matches!(result, Err(ClusterError::InvalidReplicationFactor { .. })));
        
        // RF=2 should succeed
        let replicas = placer.assign_partition("test", 0, 2).unwrap();
        assert_eq!(replicas.len(), 2);
    }
}
