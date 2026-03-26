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

    #[test]
    fn test_cluster_with_multiple_topics() {
        let node = Node::new(0, "localhost", 9092);
        let partitions = vec![
            PartitionInfo {
                topic: "topic-a".into(),
                partition: 0,
                leader: Some(node.clone()),
                replicas: vec![node.clone()],
                in_sync_replicas: vec![node.clone()],
            },
            PartitionInfo {
                topic: "topic-a".into(),
                partition: 1,
                leader: Some(node.clone()),
                replicas: vec![node.clone()],
                in_sync_replicas: vec![node.clone()],
            },
            PartitionInfo {
                topic: "topic-b".into(),
                partition: 0,
                leader: Some(node.clone()),
                replicas: vec![node.clone()],
                in_sync_replicas: vec![node.clone()],
            },
        ];
        let cluster = Cluster::new(vec![node], partitions);

        // Should have two topics
        let topics: Vec<&str> = cluster.topics().collect();
        assert_eq!(topics.len(), 2);
        assert!(topics.contains(&"topic-a"));
        assert!(topics.contains(&"topic-b"));

        assert_eq!(cluster.partitions_for_topic("topic-a").len(), 2);
        assert_eq!(cluster.partitions_for_topic("topic-b").len(), 1);
    }

    #[test]
    fn test_leader_for_unknown_partition() {
        let node = Node::new(0, "localhost", 9092);
        let pi = PartitionInfo {
            topic: "test".into(),
            partition: 0,
            leader: Some(node.clone()),
            replicas: vec![node.clone()],
            in_sync_replicas: vec![node.clone()],
        };
        let cluster = Cluster::new(vec![node], vec![pi]);

        // Unknown topic
        assert!(
            cluster
                .leader_for(&TopicPartition::new("nonexistent", 0))
                .is_none()
        );

        // Known topic, unknown partition
        assert!(
            cluster
                .leader_for(&TopicPartition::new("test", 99))
                .is_none()
        );
    }

    #[test]
    fn test_leader_for_no_leader_partition() {
        // A partition with no leader (leader = None) should return None from leader_for
        let node = Node::new(0, "localhost", 9092);
        let pi = PartitionInfo {
            topic: "test".into(),
            partition: 0,
            leader: None, // no leader assigned
            replicas: vec![node.clone()],
            in_sync_replicas: vec![],
        };
        let cluster = Cluster::new(vec![node], vec![pi]);
        assert!(
            cluster
                .leader_for(&TopicPartition::new("test", 0))
                .is_none()
        );
    }

    #[test]
    fn test_topics_iteration() {
        let node = Node::new(0, "localhost", 9092);
        let partitions = vec![
            PartitionInfo {
                topic: "alpha".into(),
                partition: 0,
                leader: Some(node.clone()),
                replicas: vec![node.clone()],
                in_sync_replicas: vec![node.clone()],
            },
            PartitionInfo {
                topic: "beta".into(),
                partition: 0,
                leader: Some(node.clone()),
                replicas: vec![node.clone()],
                in_sync_replicas: vec![node.clone()],
            },
            PartitionInfo {
                topic: "gamma".into(),
                partition: 0,
                leader: Some(node.clone()),
                replicas: vec![node.clone()],
                in_sync_replicas: vec![node.clone()],
            },
        ];
        let cluster = Cluster::new(vec![node], partitions);

        let mut topics: Vec<&str> = cluster.topics().collect();
        topics.sort();
        assert_eq!(topics, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn test_topics_empty_cluster() {
        let cluster = Cluster::empty();
        assert_eq!(cluster.topics().count(), 0);
    }

    #[test]
    fn test_partitions_for_topic_spread_across_brokers() {
        let node0 = Node::new(0, "broker0", 9092);
        let node1 = Node::new(1, "broker1", 9092);
        let node2 = Node::new(2, "broker2", 9092);

        let partitions = vec![
            PartitionInfo {
                topic: "spread-topic".into(),
                partition: 0,
                leader: Some(node0.clone()),
                replicas: vec![node0.clone(), node1.clone(), node2.clone()],
                in_sync_replicas: vec![node0.clone(), node1.clone(), node2.clone()],
            },
            PartitionInfo {
                topic: "spread-topic".into(),
                partition: 1,
                leader: Some(node1.clone()),
                replicas: vec![node1.clone(), node2.clone(), node0.clone()],
                in_sync_replicas: vec![node1.clone(), node2.clone()],
            },
            PartitionInfo {
                topic: "spread-topic".into(),
                partition: 2,
                leader: Some(node2.clone()),
                replicas: vec![node2.clone(), node0.clone(), node1.clone()],
                in_sync_replicas: vec![node2.clone(), node0.clone(), node1.clone()],
            },
        ];

        let cluster = Cluster::new(
            vec![node0.clone(), node1.clone(), node2.clone()],
            partitions,
        );

        let parts = cluster.partitions_for_topic("spread-topic");
        assert_eq!(parts.len(), 3);

        // Verify each partition has the right leader
        assert_eq!(parts[0].leader.as_ref().unwrap().id, 0);
        assert_eq!(parts[1].leader.as_ref().unwrap().id, 1);
        assert_eq!(parts[2].leader.as_ref().unwrap().id, 2);

        // Verify each partition has 3 replicas
        for p in parts {
            assert_eq!(p.replicas.len(), 3);
        }

        // Verify leader_for works for each partition
        assert_eq!(
            cluster
                .leader_for(&TopicPartition::new("spread-topic", 0))
                .unwrap()
                .id,
            0
        );
        assert_eq!(
            cluster
                .leader_for(&TopicPartition::new("spread-topic", 1))
                .unwrap()
                .id,
            1
        );
        assert_eq!(
            cluster
                .leader_for(&TopicPartition::new("spread-topic", 2))
                .unwrap()
                .id,
            2
        );
    }

    #[test]
    fn test_node_by_id_all_registered_nodes() {
        let node0 = Node::new(0, "broker0.example.com", 9092);
        let node1 = Node::new(1, "broker1.example.com", 9093);
        let node2 = Node::new(2, "broker2.example.com", 9094);

        let cluster = Cluster::new(vec![node0.clone(), node1.clone(), node2.clone()], vec![]);

        // Every registered node should be retrievable by id
        let found0 = cluster.node_by_id(0).unwrap();
        assert_eq!(found0.id, 0);
        assert_eq!(found0.host, "broker0.example.com");
        assert_eq!(found0.port, 9092);

        let found1 = cluster.node_by_id(1).unwrap();
        assert_eq!(found1.id, 1);
        assert_eq!(found1.host, "broker1.example.com");
        assert_eq!(found1.port, 9093);

        let found2 = cluster.node_by_id(2).unwrap();
        assert_eq!(found2.id, 2);
        assert_eq!(found2.host, "broker2.example.com");
        assert_eq!(found2.port, 9094);

        // Non-existent nodes return None
        assert!(cluster.node_by_id(-1).is_none());
        assert!(cluster.node_by_id(100).is_none());
    }

    #[test]
    fn test_partitions_for_unknown_topic_returns_empty() {
        // Mirrors Java: Cluster.partitionsForTopic() returns empty list for unknown topic
        let node = Node::new(0, "localhost", 9092);
        let pi = PartitionInfo {
            topic: "known".into(),
            partition: 0,
            leader: Some(node.clone()),
            replicas: vec![node.clone()],
            in_sync_replicas: vec![node.clone()],
        };
        let cluster = Cluster::new(vec![node], vec![pi]);

        let result = cluster.partitions_for_topic("totally-unknown");
        assert!(result.is_empty());
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_cluster_nodes_returns_all() {
        let nodes = vec![
            Node::new(0, "host0", 9092),
            Node::new(1, "host1", 9093),
            Node::new(2, "host2", 9094),
        ];
        let cluster = Cluster::new(nodes.clone(), vec![]);
        assert_eq!(cluster.nodes().len(), 3);
        assert_eq!(cluster.nodes()[0].id, 0);
        assert_eq!(cluster.nodes()[1].id, 1);
        assert_eq!(cluster.nodes()[2].id, 2);
    }

    #[test]
    fn test_cluster_mixed_leader_and_leaderless_partitions() {
        let node = Node::new(0, "localhost", 9092);
        let partitions = vec![
            PartitionInfo {
                topic: "test".into(),
                partition: 0,
                leader: Some(node.clone()),
                replicas: vec![node.clone()],
                in_sync_replicas: vec![node.clone()],
            },
            PartitionInfo {
                topic: "test".into(),
                partition: 1,
                leader: None, // leaderless
                replicas: vec![node.clone()],
                in_sync_replicas: vec![],
            },
        ];
        let cluster = Cluster::new(vec![node.clone()], partitions);

        // Partition 0 has a leader
        assert_eq!(
            cluster
                .leader_for(&TopicPartition::new("test", 0))
                .unwrap()
                .id,
            0
        );
        // Partition 1 has no leader
        assert!(
            cluster
                .leader_for(&TopicPartition::new("test", 1))
                .is_none()
        );

        // Both partitions are still in partitions_for_topic
        assert_eq!(cluster.partitions_for_topic("test").len(), 2);
    }
}
