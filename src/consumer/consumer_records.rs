use super::consumer_record::ConsumerRecord;
use crate::common::TopicPartition;
use std::collections::{HashMap, HashSet};

/// A batch of consumer records returned by poll().
pub struct ConsumerRecords {
    records: HashMap<TopicPartition, Vec<ConsumerRecord>>,
}

impl ConsumerRecords {
    pub fn new(records: HashMap<TopicPartition, Vec<ConsumerRecord>>) -> Self {
        Self { records }
    }

    pub fn empty() -> Self {
        Self {
            records: HashMap::new(),
        }
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
    use super::super::consumer_record::TimestampType;
    use super::*;
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
        map.insert(
            tp0.clone(),
            vec![make_record("test", 0, 0), make_record("test", 0, 1)],
        );
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
        map.insert(
            tp,
            vec![make_record("test", 0, 0), make_record("test", 0, 1)],
        );
        let records = ConsumerRecords::new(map);
        assert_eq!(records.iter().count(), 2);
    }

    /// records_for returns only the records belonging to the requested partition.
    #[test]
    fn test_records_for_partition_returns_only_matching() {
        let tp0 = TopicPartition::new("topic", 0);
        let tp1 = TopicPartition::new("topic", 1);
        let mut map = HashMap::new();
        map.insert(
            tp0.clone(),
            vec![
                make_record("topic", 0, 0),
                make_record("topic", 0, 1),
                make_record("topic", 0, 2),
            ],
        );
        map.insert(tp1.clone(), vec![make_record("topic", 1, 0)]);
        let records = ConsumerRecords::new(map);

        let for_tp0 = records.records_for(&tp0);
        assert_eq!(for_tp0.len(), 3);
        for r in for_tp0 {
            assert_eq!(r.topic, "topic");
            assert_eq!(r.partition, 0);
        }

        let for_tp1 = records.records_for(&tp1);
        assert_eq!(for_tp1.len(), 1);
        assert_eq!(for_tp1[0].partition, 1);
    }

    /// Iterating over records from multiple partitions yields all records.
    #[test]
    fn test_iter_multiple_partitions() {
        let tp_a0 = TopicPartition::new("a", 0);
        let tp_b0 = TopicPartition::new("b", 0);
        let tp_a1 = TopicPartition::new("a", 1);
        let mut map = HashMap::new();
        map.insert(tp_a0, vec![make_record("a", 0, 0), make_record("a", 0, 1)]);
        map.insert(tp_b0, vec![make_record("b", 0, 0)]);
        map.insert(tp_a1, vec![make_record("a", 1, 100)]);
        let records = ConsumerRecords::new(map);

        // Total count via iterator matches count()
        assert_eq!(records.iter().count(), records.count());
        assert_eq!(records.count(), 4);
        assert_eq!(records.partitions().len(), 3);
    }

    /// count returns the total number of records across all partitions.
    #[test]
    fn test_count_total_records() {
        let mut map = HashMap::new();
        map.insert(
            TopicPartition::new("t", 0),
            vec![make_record("t", 0, 0), make_record("t", 0, 1)],
        );
        map.insert(
            TopicPartition::new("t", 1),
            vec![
                make_record("t", 1, 0),
                make_record("t", 1, 1),
                make_record("t", 1, 2),
            ],
        );
        let records = ConsumerRecords::new(map);
        assert_eq!(records.count(), 5);
    }

    /// records_for on a partition not present returns an empty slice.
    #[test]
    fn test_records_for_nonexistent_partition_returns_empty() {
        let tp_exists = TopicPartition::new("topic", 0);
        let tp_missing = TopicPartition::new("topic", 99);
        let mut map = HashMap::new();
        map.insert(tp_exists, vec![make_record("topic", 0, 0)]);
        let records = ConsumerRecords::new(map);

        let result = records.records_for(&tp_missing);
        assert!(result.is_empty());
    }

    /// is_empty returns false when records are present.
    #[test]
    fn test_is_empty_on_non_empty_records() {
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("t", 0), vec![make_record("t", 0, 0)]);
        let records = ConsumerRecords::new(map);
        assert!(!records.is_empty());
    }

    /// Edge case: a map with partitions but zero records per partition counts as empty.
    #[test]
    fn test_is_empty_with_empty_vecs() {
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("t", 0), vec![]);
        let records = ConsumerRecords::new(map);
        assert!(records.is_empty());
        assert_eq!(records.count(), 0);
    }
}
