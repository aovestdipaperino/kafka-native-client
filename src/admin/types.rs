use std::collections::HashMap;

use crate::common::{Node, TopicPartition};
use crate::error::KafkaError;

/// Specification for a new topic to create.
pub struct NewTopic {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub configs: HashMap<String, String>,
}

impl NewTopic {
    pub fn new(name: impl Into<String>, num_partitions: i32, replication_factor: i16) -> Self {
        Self {
            name: name.into(),
            num_partitions,
            replication_factor,
            configs: HashMap::new(),
        }
    }

    pub fn with_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.insert(key.into(), value.into());
        self
    }
}

/// Result of a topic operation.
#[derive(Debug)]
pub struct TopicResult {
    pub name: String,
    pub error: Option<KafkaError>,
}

/// A topic listing from the cluster.
#[derive(Debug, Clone)]
pub struct TopicListing {
    pub name: String,
    pub is_internal: bool,
}

/// Detailed description of a topic.
#[derive(Debug, Clone)]
pub struct TopicDescription {
    pub name: String,
    pub is_internal: bool,
    pub partitions: Vec<TopicPartitionInfo>,
}

/// Info about a single partition within a topic.
#[derive(Debug, Clone)]
pub struct TopicPartitionInfo {
    pub partition: i32,
    pub leader: Option<Node>,
    pub replicas: Vec<Node>,
    pub isr: Vec<Node>,
}

/// Description of the cluster.
#[derive(Debug, Clone)]
pub struct ClusterDescription {
    pub cluster_id: String,
    pub controller: Option<Node>,
    pub nodes: Vec<Node>,
}

/// A consumer group listing.
#[derive(Debug, Clone)]
pub struct ConsumerGroupListing {
    pub group_id: String,
    pub state: String,
}

/// Detailed description of a consumer group.
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescription {
    pub group_id: String,
    pub state: String,
    pub members: Vec<MemberDescription>,
    pub coordinator: Option<Node>,
}

/// Description of a consumer group member.
#[derive(Debug, Clone)]
pub struct MemberDescription {
    pub member_id: String,
    pub client_id: String,
    pub host: String,
    pub assignment: Vec<TopicPartition>,
}

/// Identifies a resource for configuration operations.
#[derive(Debug, Clone)]
pub struct ConfigResource {
    pub resource_type: ResourceType,
    pub name: String,
}

/// Type of configurable resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceType {
    Topic = 2,
    Broker = 4,
}

/// A configuration entry.
#[derive(Debug, Clone)]
pub struct ConfigEntry {
    pub name: String,
    pub value: Option<String>,
    pub is_default: bool,
    pub is_read_only: bool,
}

/// Specification for which offsets to list.
#[derive(Debug, Clone)]
pub enum OffsetSpec {
    Latest,
    Earliest,
    Timestamp(i64),
}

/// Result of a list offsets operation.
#[derive(Debug, Clone)]
pub struct ListOffsetsResult {
    pub offset: i64,
    pub timestamp: i64,
}
