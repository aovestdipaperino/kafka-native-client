use crate::common::TopicPartition;

/// Callback interface for consumer group rebalance events.
pub trait ConsumerRebalanceListener: Send + Sync {
    /// Called after partitions are assigned to this consumer.
    fn on_partitions_assigned(&self, partitions: &[TopicPartition]);
    /// Called before partitions are revoked from this consumer.
    fn on_partitions_revoked(&self, partitions: &[TopicPartition]);
    /// Called when partitions are lost (session timeout).
    fn on_partitions_lost(&self, partitions: &[TopicPartition]) {
        self.on_partitions_revoked(partitions);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// A mock rebalance listener that records invocations for verification.
    struct MockRebalanceListener {
        assigned: Arc<Mutex<Vec<TopicPartition>>>,
        revoked: Arc<Mutex<Vec<TopicPartition>>>,
        lost: Arc<Mutex<Vec<TopicPartition>>>,
    }

    impl MockRebalanceListener {
        fn new() -> Self {
            Self {
                assigned: Arc::new(Mutex::new(Vec::new())),
                revoked: Arc::new(Mutex::new(Vec::new())),
                lost: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl ConsumerRebalanceListener for MockRebalanceListener {
        fn on_partitions_assigned(&self, partitions: &[TopicPartition]) {
            self.assigned.lock().unwrap().extend_from_slice(partitions);
        }

        fn on_partitions_revoked(&self, partitions: &[TopicPartition]) {
            self.revoked.lock().unwrap().extend_from_slice(partitions);
        }
    }

    /// on_partitions_assigned records the assigned partitions.
    #[test]
    fn test_on_partitions_assigned() {
        let listener = MockRebalanceListener::new();
        let partitions = vec![TopicPartition::new("t", 0), TopicPartition::new("t", 1)];
        listener.on_partitions_assigned(&partitions);

        let assigned = listener.assigned.lock().unwrap();
        assert_eq!(assigned.len(), 2);
        assert_eq!(assigned[0], TopicPartition::new("t", 0));
        assert_eq!(assigned[1], TopicPartition::new("t", 1));
    }

    /// on_partitions_revoked records the revoked partitions.
    #[test]
    fn test_on_partitions_revoked() {
        let listener = MockRebalanceListener::new();
        let partitions = vec![TopicPartition::new("orders", 3)];
        listener.on_partitions_revoked(&partitions);

        let revoked = listener.revoked.lock().unwrap();
        assert_eq!(revoked.len(), 1);
        assert_eq!(revoked[0], TopicPartition::new("orders", 3));
    }

    /// Default on_partitions_lost delegates to on_partitions_revoked.
    #[test]
    fn test_on_partitions_lost_defaults_to_revoked() {
        let listener = MockRebalanceListener::new();
        let partitions = vec![TopicPartition::new("a", 0), TopicPartition::new("b", 1)];
        // Call on_partitions_lost — should delegate to on_partitions_revoked
        listener.on_partitions_lost(&partitions);

        let revoked = listener.revoked.lock().unwrap();
        assert_eq!(revoked.len(), 2);
        assert_eq!(revoked[0], TopicPartition::new("a", 0));
        assert_eq!(revoked[1], TopicPartition::new("b", 1));

        // lost tracking is empty because we didn't override on_partitions_lost
        let lost = listener.lost.lock().unwrap();
        assert!(lost.is_empty());
    }

    /// A custom listener that overrides on_partitions_lost.
    struct CustomLostListener {
        revoked: Arc<Mutex<Vec<TopicPartition>>>,
        lost: Arc<Mutex<Vec<TopicPartition>>>,
    }

    impl ConsumerRebalanceListener for CustomLostListener {
        fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) {}

        fn on_partitions_revoked(&self, partitions: &[TopicPartition]) {
            self.revoked.lock().unwrap().extend_from_slice(partitions);
        }

        fn on_partitions_lost(&self, partitions: &[TopicPartition]) {
            self.lost.lock().unwrap().extend_from_slice(partitions);
        }
    }

    /// When on_partitions_lost is overridden, it does NOT delegate to revoked.
    #[test]
    fn test_custom_on_partitions_lost_does_not_delegate() {
        let revoked = Arc::new(Mutex::new(Vec::new()));
        let lost = Arc::new(Mutex::new(Vec::new()));
        let listener = CustomLostListener {
            revoked: revoked.clone(),
            lost: lost.clone(),
        };

        listener.on_partitions_lost(&[TopicPartition::new("x", 0)]);

        assert!(revoked.lock().unwrap().is_empty());
        assert_eq!(lost.lock().unwrap().len(), 1);
    }

    /// Calling callbacks with an empty slice is a no-op.
    #[test]
    fn test_callbacks_with_empty_partitions() {
        let listener = MockRebalanceListener::new();
        listener.on_partitions_assigned(&[]);
        listener.on_partitions_revoked(&[]);
        listener.on_partitions_lost(&[]);

        assert!(listener.assigned.lock().unwrap().is_empty());
        assert!(listener.revoked.lock().unwrap().is_empty());
        assert!(listener.lost.lock().unwrap().is_empty());
    }

    /// The trait is object-safe (can be used as Box<dyn ConsumerRebalanceListener>).
    #[test]
    fn test_trait_object_safety() {
        let listener: Box<dyn ConsumerRebalanceListener> = Box::new(MockRebalanceListener::new());
        listener.on_partitions_assigned(&[TopicPartition::new("t", 0)]);
        listener.on_partitions_revoked(&[TopicPartition::new("t", 0)]);
        listener.on_partitions_lost(&[TopicPartition::new("t", 0)]);
        // If this compiles and runs, the trait is object-safe
    }
}
