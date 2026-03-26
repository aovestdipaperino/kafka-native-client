use crate::common::RecordHeader;
use bytes::Bytes;

/// Timestamp type for consumer records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampType {
    Creation,
    LogAppend,
}

/// A record received from a Kafka partition.
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub timestamp_type: TimestampType,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
    pub leader_epoch: Option<i32>,
    pub serialized_key_size: i32,
    pub serialized_value_size: i32,
}
