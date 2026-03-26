use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::sync::Notify;

use proto_kafka::messages::{MetadataRequest, MetadataResponse, TopicName};
use proto_kafka::messages::metadata_request::MetadataRequestTopic;
use proto_kafka::protocol::StrBytes;

use crate::common::{Cluster, Node, TopicPartition, PartitionInfo};
use crate::error::{KafkaError, Result};
use super::connection_manager::ConnectionManager;

pub struct Metadata {
    cluster: RwLock<Arc<Cluster>>,
    connection_manager: Arc<ConnectionManager>,
    topics: RwLock<HashSet<String>>,
    last_refresh_ms: AtomicI64,
    metadata_max_age_ms: u64,
    notify: Notify,
}

impl Metadata {
    pub fn new(connection_manager: Arc<ConnectionManager>, metadata_max_age_ms: u64) -> Self {
        Self {
            cluster: RwLock::new(Arc::new(Cluster::empty())),
            connection_manager,
            topics: RwLock::new(HashSet::new()),
            last_refresh_ms: AtomicI64::new(0),
            metadata_max_age_ms,
            notify: Notify::new(),
        }
    }

    /// Get the current cluster snapshot.
    pub fn cluster(&self) -> Arc<Cluster> {
        self.cluster.read().unwrap().clone()
    }

    /// Register a topic for metadata tracking.
    pub fn add_topic(&self, topic: &str) {
        self.topics.write().unwrap().insert(topic.to_string());
    }

    /// Get the leader for a topic partition, triggering a refresh if unknown.
    pub async fn leader_for(&self, tp: &TopicPartition) -> Result<Node> {
        // Check current cluster
        if let Some(leader) = self.cluster().leader_for(tp) {
            return Ok(leader.clone());
        }

        // Add topic and refresh
        self.add_topic(&tp.topic);
        self.fetch().await?;

        self.cluster().leader_for(tp).cloned().ok_or_else(|| {
            KafkaError::ConnectionFailed {
                broker_id: -1,
                message: format!("no leader for {tp}"),
            }
        })
    }

    /// Fetch metadata from the cluster.
    pub async fn fetch(&self) -> Result<()> {
        let topics: Vec<String> = self.topics.read().unwrap().iter().cloned().collect();

        // Build MetadataRequest
        let topic_list: Option<Vec<MetadataRequestTopic>> = if topics.is_empty() {
            None // Fetch all topics
        } else {
            Some(topics.iter().map(|t| {
                MetadataRequestTopic::default()
                    .with_name(Some(TopicName(StrBytes::from_string(t.clone()))))
            }).collect())
        };

        let request = MetadataRequest::default()
            .with_topics(topic_list)
            .with_allow_auto_topic_creation(false);

        // Send to any available broker (try broker -1 which is the bootstrap connection)
        let response: MetadataResponse = self.connection_manager.send(-1, request).await?;

        // Parse response into Cluster
        let cluster = Self::parse_metadata_response(&response);

        // Register discovered brokers with the connection manager
        for node in cluster.nodes() {
            self.connection_manager.register_broker(node.id, node.address());
        }

        // Update cached cluster
        *self.cluster.write().unwrap() = Arc::new(cluster);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        self.last_refresh_ms.store(now, Ordering::Relaxed);

        Ok(())
    }

    /// Signal that metadata should be refreshed.
    pub fn request_update(&self) {
        self.notify.notify_one();
    }

    /// Check if metadata needs refreshing based on age.
    pub fn needs_refresh(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let last = self.last_refresh_ms.load(Ordering::Relaxed);
        (now - last) > self.metadata_max_age_ms as i64
    }

    fn parse_metadata_response(response: &MetadataResponse) -> Cluster {
        let nodes: Vec<Node> = response.brokers.iter().map(|b| {
            Node::new(
                b.node_id.0,
                b.host.as_str(),
                u16::try_from(b.port).unwrap_or(9092),
            )
        }).collect();

        let partitions: Vec<PartitionInfo> = response.topics.iter().flat_map(|t| {
            let topic_name = t.name.as_ref().map(|n| n.0.to_string()).unwrap_or_default();
            let nodes_ref = &nodes;
            t.partitions.iter().map(move |p| {
                let leader = nodes_ref.iter().find(|n| n.id == p.leader_id.0).cloned();
                PartitionInfo {
                    topic: topic_name.clone(),
                    partition: p.partition_index,
                    leader,
                    replicas: p.replica_nodes.iter()
                        .filter_map(|id| nodes_ref.iter().find(|n| n.id == id.0).cloned())
                        .collect(),
                    in_sync_replicas: p.isr_nodes.iter()
                        .filter_map(|id| nodes_ref.iter().find(|n| n.id == id.0).cloned())
                        .collect(),
                }
            })
        }).collect();

        Cluster::new(nodes, partitions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto_kafka::messages::{MetadataResponse, BrokerId, TopicName};
    use proto_kafka::messages::metadata_response::{MetadataResponseBroker, MetadataResponseTopic, MetadataResponsePartition};

    #[test]
    fn test_parse_metadata_response() {
        let mut response = MetadataResponse::default();
        response.brokers = vec![
            MetadataResponseBroker::default()
                .with_node_id(BrokerId(0))
                .with_host(StrBytes::from_static_str("host1"))
                .with_port(9092),
            MetadataResponseBroker::default()
                .with_node_id(BrokerId(1))
                .with_host(StrBytes::from_static_str("host2"))
                .with_port(9093),
        ];
        response.topics = vec![
            MetadataResponseTopic::default()
                .with_name(Some(TopicName(StrBytes::from_static_str("test-topic"))))
                .with_partitions(vec![
                    MetadataResponsePartition::default()
                        .with_partition_index(0)
                        .with_leader_id(BrokerId(0))
                        .with_replica_nodes(vec![BrokerId(0), BrokerId(1)])
                        .with_isr_nodes(vec![BrokerId(0), BrokerId(1)]),
                    MetadataResponsePartition::default()
                        .with_partition_index(1)
                        .with_leader_id(BrokerId(1))
                        .with_replica_nodes(vec![BrokerId(0), BrokerId(1)])
                        .with_isr_nodes(vec![BrokerId(1)]),
                ]),
        ];

        let cluster = Metadata::parse_metadata_response(&response);

        assert_eq!(cluster.nodes().len(), 2);
        assert_eq!(cluster.partitions_for_topic("test-topic").len(), 2);

        let tp0 = TopicPartition::new("test-topic", 0);
        let leader0 = cluster.leader_for(&tp0).unwrap();
        assert_eq!(leader0.id, 0);
        assert_eq!(leader0.host, "host1");

        let tp1 = TopicPartition::new("test-topic", 1);
        let leader1 = cluster.leader_for(&tp1).unwrap();
        assert_eq!(leader1.id, 1);
    }

    #[test]
    fn test_add_topic() {
        let cm = Arc::new(ConnectionManager::new(vec![], String::new(), 5));
        let metadata = Metadata::new(cm, 300_000);
        metadata.add_topic("topic1");
        metadata.add_topic("topic2");
        assert_eq!(metadata.topics.read().unwrap().len(), 2);
    }
}
