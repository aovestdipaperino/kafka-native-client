use std::collections::{HashMap, HashSet};
use crate::common::TopicPartition;
use super::consumer_record::ConsumerRecord;

/// A batch of consumer records returned by poll().
pub struct ConsumerRecords {
    records: HashMap<TopicPartition, Vec<ConsumerRecord>>,
}

impl ConsumerRecords {
    pub fn new(records: HashMap<TopicPartition, Vec<ConsumerRecord>>) -> Self {
        Self { records }
    }

    pub fn empty() -> Self {
        Self { records: HashMap::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.records.values().all(|v| v.is_empty())
    }

    pub fn count(&self) -> usize {
        self.records.values().map(|v| v.len()).sum()
    }

    pub fn records_for(&self, tp: &TopicPartition) -> &[ConsumerRecord] {
        self.records.get(tp).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn partitions(&self) -> HashSet<&TopicPartition> {
        self.records.keys().collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = &ConsumerRecord> {
        self.records.values().flat_map(|v| v.iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::consumer_record::TimestampType;
    use bytes::Bytes;

    fn make_record(topic: &str, partition: i32, offset: i64) -> ConsumerRecord {
        ConsumerRecord {
            topic: topic.into(),
            partition,
            offset,
            timestamp: 1000,
            timestamp_type: TimestampType::Creation,
            key: None,
            value: Some(Bytes::from("hello")),
            headers: vec![],
            leader_epoch: None,
            serialized_key_size: -1,
            serialized_value_size: 5,
        }
    }

    #[test]
    fn test_consumer_records_count() {
        let tp0 = TopicPartition::new("test", 0);
        let tp1 = TopicPartition::new("test", 1);
        let mut map = HashMap::new();
        map.insert(tp0.clone(), vec![make_record("test", 0, 0), make_record("test", 0, 1)]);
        map.insert(tp1.clone(), vec![make_record("test", 1, 0)]);
        let records = ConsumerRecords::new(map);
        assert_eq!(records.count(), 3);
        assert!(!records.is_empty());
        assert_eq!(records.records_for(&tp0).len(), 2);
        assert_eq!(records.records_for(&tp1).len(), 1);
        assert_eq!(records.partitions().len(), 2);
    }

    #[test]
    fn test_empty_consumer_records() {
        let records = ConsumerRecords::empty();
        assert!(records.is_empty());
        assert_eq!(records.count(), 0);
    }

    #[test]
    fn test_iter() {
        let tp = TopicPartition::new("test", 0);
        let mut map = HashMap::new();
        map.insert(tp, vec![make_record("test", 0, 0), make_record("test", 0, 1)]);
        let records = ConsumerRecords::new(map);
        assert_eq!(records.iter().count(), 2);
    }
}
