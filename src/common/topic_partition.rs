use std::fmt;

/// A topic name and partition number.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self { topic: topic.into(), partition }
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
}
