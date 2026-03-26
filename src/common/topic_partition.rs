use std::fmt;

/// A topic name and partition number.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_partition_eq() {
        let tp1 = TopicPartition::new("test", 0);
        let tp2 = TopicPartition::new("test", 0);
        assert_eq!(tp1, tp2);
    }

    #[test]
    fn test_topic_partition_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(TopicPartition::new("test", 0));
        set.insert(TopicPartition::new("test", 0));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_topic_partition_display() {
        let tp = TopicPartition::new("my-topic", 3);
        assert_eq!(format!("{tp}"), "my-topic-3");
    }

    #[test]
    fn test_different_topics_same_partition_not_equal() {
        let tp1 = TopicPartition::new("topic-a", 0);
        let tp2 = TopicPartition::new("topic-b", 0);
        assert_ne!(tp1, tp2);
    }

    #[test]
    fn test_same_topic_different_partition_not_equal() {
        let tp1 = TopicPartition::new("test", 0);
        let tp2 = TopicPartition::new("test", 1);
        assert_ne!(tp1, tp2);
    }

    #[test]
    fn test_ordering_in_hash_set() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(TopicPartition::new("a", 0));
        set.insert(TopicPartition::new("b", 0));
        set.insert(TopicPartition::new("a", 1));
        set.insert(TopicPartition::new("a", 0)); // duplicate
        assert_eq!(set.len(), 3);
        assert!(set.contains(&TopicPartition::new("a", 0)));
        assert!(set.contains(&TopicPartition::new("b", 0)));
        assert!(set.contains(&TopicPartition::new("a", 1)));
    }

    #[test]
    fn test_ordering_in_btree_set() {
        // TopicPartition doesn't impl Ord, but we can use it as HashMap key
        // and verify distinct entries are kept in a HashMap
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("z-topic", 2), "first");
        map.insert(TopicPartition::new("a-topic", 0), "second");
        map.insert(TopicPartition::new("z-topic", 2), "overwritten");
        assert_eq!(map.len(), 2);
        assert_eq!(map[&TopicPartition::new("z-topic", 2)], "overwritten");
        assert_eq!(map[&TopicPartition::new("a-topic", 0)], "second");
    }

    #[test]
    fn test_clone_preserves_equality() {
        let tp1 = TopicPartition::new("my-topic", 42);
        let tp2 = tp1.clone();
        assert_eq!(tp1, tp2);
        assert_eq!(tp1.topic, tp2.topic);
        assert_eq!(tp1.partition, tp2.partition);
    }

    #[test]
    fn test_clone_is_independent() {
        let tp1 = TopicPartition::new("my-topic", 42);
        let mut tp2 = tp1.clone();
        tp2.topic = "other-topic".to_string();
        tp2.partition = 99;
        // Original is unchanged
        assert_eq!(tp1.topic, "my-topic");
        assert_eq!(tp1.partition, 42);
        assert_ne!(tp1, tp2);
    }

    #[test]
    fn test_display_format_matches_java_to_string() {
        // Java TopicPartition.toString() returns "topic-partition"
        let tp = TopicPartition::new("orders", 5);
        assert_eq!(tp.to_string(), "orders-5");

        let tp_zero = TopicPartition::new("events", 0);
        assert_eq!(tp_zero.to_string(), "events-0");
    }

    #[test]
    fn test_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let tp1 = TopicPartition::new("test", 0);
        let tp2 = TopicPartition::new("test", 0);

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        tp1.hash(&mut h1);
        tp2.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    #[test]
    fn test_hash_differs_for_different_partitions() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let tp1 = TopicPartition::new("test", 0);
        let tp2 = TopicPartition::new("test", 1);

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        tp1.hash(&mut h1);
        tp2.hash(&mut h2);
        // Different topic partitions should (almost certainly) have different hashes
        assert_ne!(h1.finish(), h2.finish());
    }
}
