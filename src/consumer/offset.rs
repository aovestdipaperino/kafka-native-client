/// Re-export OffsetResetStrategy from config (canonical definition).
pub use crate::config::consumer_config::OffsetResetStrategy;

/// An offset and associated metadata for a committed consumer position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetAndMetadata {
    pub offset: i64,
    pub metadata: Option<String>,
    pub leader_epoch: Option<i32>,
}

impl OffsetAndMetadata {
    pub fn new(offset: i64) -> Self {
        Self { offset, metadata: None, leader_epoch: None }
    }
}

/// An offset and timestamp returned by offsets_for_times.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetAndTimestamp {
    pub offset: i64,
    pub timestamp: i64,
    pub leader_epoch: Option<i32>,
}

/// Consumer group metadata.
#[derive(Debug, Clone)]
pub struct ConsumerGroupMetadata {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
}
