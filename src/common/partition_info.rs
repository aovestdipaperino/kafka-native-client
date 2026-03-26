use super::Node;

/// Metadata about a single partition of a topic.
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub topic: String,
    pub partition: i32,
    pub leader: Option<Node>,
    pub replicas: Vec<Node>,
    pub in_sync_replicas: Vec<Node>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_info_stores_all_fields() {
        let leader = Node::new(0, "leader-host", 9092);
        let replica1 = Node::new(1, "replica1-host", 9093);
        let replica2 = Node::new(2, "replica2-host", 9094);
        let isr = vec![leader.clone(), replica1.clone()];
        let replicas = vec![leader.clone(), replica1.clone(), replica2.clone()];

        let pi = PartitionInfo {
            topic: "my-topic".to_string(),
            partition: 7,
            leader: Some(leader.clone()),
            replicas: replicas.clone(),
            in_sync_replicas: isr.clone(),
        };

        assert_eq!(pi.topic, "my-topic");
        assert_eq!(pi.partition, 7);
        assert_eq!(pi.leader.as_ref().unwrap().id, 0);
        assert_eq!(pi.leader.as_ref().unwrap().host, "leader-host");
        assert_eq!(pi.replicas.len(), 3);
        assert_eq!(pi.replicas[0].id, 0);
        assert_eq!(pi.replicas[1].id, 1);
        assert_eq!(pi.replicas[2].id, 2);
        assert_eq!(pi.in_sync_replicas.len(), 2);
        assert_eq!(pi.in_sync_replicas[0].id, 0);
        assert_eq!(pi.in_sync_replicas[1].id, 1);
    }

    #[test]
    fn test_partition_with_no_leader() {
        // Mirrors Java: PartitionInfo can have a null leader (e.g., during reassignment)
        let replica = Node::new(1, "replica-host", 9092);
        let pi = PartitionInfo {
            topic: "leaderless-topic".to_string(),
            partition: 0,
            leader: None,
            replicas: vec![replica.clone()],
            in_sync_replicas: vec![],
        };

        assert_eq!(pi.topic, "leaderless-topic");
        assert_eq!(pi.partition, 0);
        assert!(pi.leader.is_none());
        assert_eq!(pi.replicas.len(), 1);
        assert!(pi.in_sync_replicas.is_empty());
    }

    #[test]
    fn test_partition_with_empty_replicas() {
        // Edge case: no replicas at all (should not happen in practice, but the struct allows it)
        let pi = PartitionInfo {
            topic: "test".to_string(),
            partition: 0,
            leader: None,
            replicas: vec![],
            in_sync_replicas: vec![],
        };

        assert!(pi.replicas.is_empty());
        assert!(pi.in_sync_replicas.is_empty());
        assert!(pi.leader.is_none());
    }

    #[test]
    fn test_partition_info_clone() {
        let node = Node::new(0, "host", 9092);
        let pi = PartitionInfo {
            topic: "test".to_string(),
            partition: 3,
            leader: Some(node.clone()),
            replicas: vec![node.clone()],
            in_sync_replicas: vec![node.clone()],
        };

        let cloned = pi.clone();
        assert_eq!(cloned.topic, pi.topic);
        assert_eq!(cloned.partition, pi.partition);
        assert_eq!(cloned.leader, pi.leader);
        assert_eq!(cloned.replicas.len(), pi.replicas.len());
        assert_eq!(cloned.in_sync_replicas.len(), pi.in_sync_replicas.len());
    }

    #[test]
    fn test_partition_info_isr_subset_of_replicas() {
        // ISR should always be a subset of replicas in practice
        let n0 = Node::new(0, "h0", 9092);
        let n1 = Node::new(1, "h1", 9092);
        let n2 = Node::new(2, "h2", 9092);

        let pi = PartitionInfo {
            topic: "test".to_string(),
            partition: 0,
            leader: Some(n0.clone()),
            replicas: vec![n0.clone(), n1.clone(), n2.clone()],
            in_sync_replicas: vec![n0.clone(), n1.clone()], // n2 is out of sync
        };

        assert_eq!(pi.replicas.len(), 3);
        assert_eq!(pi.in_sync_replicas.len(), 2);
        // Verify ISR nodes are a subset of replicas by id
        for isr_node in &pi.in_sync_replicas {
            assert!(pi.replicas.iter().any(|r| r.id == isr_node.id));
        }
    }
}
