use crate::common::{Node, PartitionInfo, TopicPartition};
use std::collections::HashMap;

/// An immutable snapshot of the Kafka cluster metadata.
#[derive(Debug, Clone)]
pub struct Cluster {
    nodes: Vec<Node>,
    nodes_by_id: HashMap<i32, Node>,
    partitions_by_topic: HashMap<String, Vec<PartitionInfo>>,
    leaders: HashMap<TopicPartition, Node>,
}

impl Cluster {
    pub fn new(nodes: Vec<Node>, partitions: Vec<PartitionInfo>) -> Self {
        let nodes_by_id: HashMap<i32, Node> = nodes.iter().map(|n| (n.id, n.clone())).collect();
        let mut partitions_by_topic: HashMap<String, Vec<PartitionInfo>> = HashMap::new();
        let mut leaders: HashMap<TopicPartition, Node> = HashMap::new();

        for p in &partitions {
            partitions_by_topic
                .entry(p.topic.clone())
                .or_default()
                .push(p.clone());
            if let Some(leader) = &p.leader {
                leaders.insert(
                    TopicPartition::new(p.topic.clone(), p.partition),
                    leader.clone(),
                );
            }
        }

        Self {
            nodes,
            nodes_by_id,
            partitions_by_topic,
            leaders,
        }
    }

    pub fn empty() -> Self {
        Self::new(vec![], vec![])
    }

    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    pub fn node_by_id(&self, id: i32) -> Option<&Node> {
        self.nodes_by_id.get(&id)
    }

    pub fn leader_for(&self, tp: &TopicPartition) -> Option<&Node> {
        self.leaders.get(tp)
    }

    pub fn partitions_for_topic(&self, topic: &str) -> &[PartitionInfo] {
        self.partitions_by_topic
            .get(topic)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn topics(&self) -> impl Iterator<Item = &str> {
        self.partitions_by_topic.keys().map(|s| s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_leader_for() {
        let node = Node::new(0, "localhost", 9092);
        let pi = PartitionInfo {
            topic: "test".into(),
            partition: 0,
            leader: Some(node.clone()),
            replicas: vec![node.clone()],
            in_sync_replicas: vec![node.clone()],
        };
        let cluster = Cluster::new(vec![node.clone()], vec![pi]);
        let leader = cluster.leader_for(&TopicPartition::new("test", 0));
        assert_eq!(leader, Some(&node));
    }

    #[test]
    fn test_cluster_partitions_for_topic() {
        let node = Node::new(0, "localhost", 9092);
        let partitions: Vec<PartitionInfo> = (0..3)
            .map(|i| PartitionInfo {
                topic: "test".into(),
                partition: i,
                leader: Some(node.clone()),
                replicas: vec![node.clone()],
                in_sync_replicas: vec![node.clone()],
            })
            .collect();
        let cluster = Cluster::new(vec![node], partitions);
        assert_eq!(cluster.partitions_for_topic("test").len(), 3);
        assert_eq!(cluster.partitions_for_topic("unknown").len(), 0);
    }

    #[test]
    fn test_cluster_empty() {
        let cluster = Cluster::empty();
        assert!(cluster.nodes().is_empty());
    }

    #[test]
    fn test_cluster_node_by_id() {
        let nodes = vec![Node::new(0, "host1", 9092), Node::new(1, "host2", 9092)];
        let cluster = Cluster::new(nodes, vec![]);
        assert_eq!(cluster.node_by_id(0).unwrap().host, "host1");
        assert!(cluster.node_by_id(99).is_none());
    }
}
