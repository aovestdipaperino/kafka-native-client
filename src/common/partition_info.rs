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
