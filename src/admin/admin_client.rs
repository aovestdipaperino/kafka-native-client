use std::sync::Arc;
use std::time::Duration;

use proto_kafka::error::ParseResponseErrorCode;
use proto_kafka::messages::create_topics_request::CreatableTopic;
use proto_kafka::messages::delete_topics_request::DeleteTopicState;
use proto_kafka::messages::{CreateTopicsRequest, DeleteTopicsRequest, TopicName};
use proto_kafka::protocol::StrBytes;

use crate::config::AdminConfig;
use crate::error::{KafkaError, Result};
use crate::network::connection_manager::ConnectionManager;
use crate::network::metadata::Metadata;

use super::types::{NewTopic, TopicResult};

/// A client for managing Kafka cluster resources (topics, configs, etc.).
pub struct AdminClient {
    connection_manager: Arc<ConnectionManager>,
    metadata: Arc<Metadata>,
    request_timeout_ms: u64,
}

impl AdminClient {
    /// Create a new AdminClient, connecting to the cluster.
    pub async fn new(config: AdminConfig) -> Result<Self> {
        let connection_manager = Arc::new(ConnectionManager::new(
            config.bootstrap_servers.clone(),
            config.client_id.clone(),
            5,
        ));
        connection_manager.bootstrap().await?;

        let metadata = Arc::new(Metadata::new(
            connection_manager.clone(),
            config.metadata_max_age_ms,
        ));
        metadata.fetch().await?;

        Ok(Self {
            connection_manager,
            metadata,
            request_timeout_ms: config.request_timeout_ms,
        })
    }

    /// Create topics in the cluster.
    pub async fn create_topics(&self, topics: &[NewTopic]) -> Result<Vec<TopicResult>> {
        let creatable_topics: Vec<CreatableTopic> = topics
            .iter()
            .map(|t| {
                CreatableTopic::default()
                    .with_name(TopicName(StrBytes::from_string(t.name.clone())))
                    .with_num_partitions(t.num_partitions)
                    .with_replication_factor(t.replication_factor)
            })
            .collect();

        let request = CreateTopicsRequest::default()
            .with_topics(creatable_topics)
            .with_timeout_ms(self.request_timeout_ms as i32);

        let response = self.connection_manager.send(-1, request).await?;

        let results = response
            .topics
            .iter()
            .map(|t| {
                let error = if t.error_code.is_err() {
                    t.error_code.err().map(KafkaError::Response)
                } else {
                    None
                };
                TopicResult {
                    name: t.name.0.to_string(),
                    error,
                }
            })
            .collect();

        Ok(results)
    }

    /// Delete topics from the cluster.
    pub async fn delete_topics(&self, topic_names: &[&str]) -> Result<Vec<TopicResult>> {
        let topics: Vec<DeleteTopicState> = topic_names
            .iter()
            .map(|name| {
                DeleteTopicState::default()
                    .with_name(Some(TopicName(StrBytes::from_string((*name).to_string()))))
            })
            .collect();

        let request = DeleteTopicsRequest::default()
            .with_topics(topics)
            .with_timeout_ms(self.request_timeout_ms as i32);

        let response = self.connection_manager.send(-1, request).await?;

        let results = response
            .responses
            .iter()
            .map(|t| {
                let error = if t.error_code.is_err() {
                    t.error_code.err().map(KafkaError::Response)
                } else {
                    None
                };
                TopicResult {
                    name: t.name.as_ref().map(|n| n.0.to_string()).unwrap_or_default(),
                    error,
                }
            })
            .collect();

        Ok(results)
    }

    /// Get metadata about the cluster.
    pub async fn describe_cluster(&self) -> Result<super::types::ClusterDescription> {
        self.metadata.fetch().await?;
        let cluster = self.metadata.cluster();

        Ok(super::types::ClusterDescription {
            cluster_id: String::new(),
            controller: cluster.node_by_id(0).cloned(),
            nodes: cluster.nodes().to_vec(),
        })
    }

    /// Close the admin client.
    pub async fn close(self, _timeout: Duration) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_topic() {
        let topic = NewTopic::new("test-topic", 3, 1).with_config("retention.ms", "86400000");
        assert_eq!(topic.name, "test-topic");
        assert_eq!(topic.num_partitions, 3);
        assert_eq!(topic.replication_factor, 1);
        assert_eq!(topic.configs.get("retention.ms").unwrap(), "86400000");
    }
}
