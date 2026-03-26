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
