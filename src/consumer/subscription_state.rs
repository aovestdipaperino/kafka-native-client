use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use crate::common::TopicPartition;
use crate::consumer::offset::{OffsetAndMetadata, OffsetResetStrategy};

/// How the consumer acquired its partition assignments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionType {
    /// Not subscribed to anything.
    None,
    /// Subscribed to topics — partitions assigned via group coordination.
    Topics,
    /// Manually assigned specific partitions.
    Manual,
}

/// Per-partition state tracked by the consumer.
#[derive(Debug, Clone)]
pub struct TopicPartitionState {
    pub position: Option<i64>,
    pub committed: Option<OffsetAndMetadata>,
    pub high_watermark: Option<i64>,
    pub log_start_offset: Option<i64>,
    pub paused: bool,
    pub reset_strategy: Option<OffsetResetStrategy>,
}

impl TopicPartitionState {
    fn new(reset_strategy: OffsetResetStrategy) -> Self {
        Self {
            position: None,
            committed: None,
            high_watermark: None,
            log_start_offset: None,
            paused: false,
            reset_strategy: Some(reset_strategy),
        }
    }
}

/// Tracks the consumer's subscriptions, partition assignments, and per-partition
/// state (fetch positions, committed offsets, pause status).
pub struct SubscriptionState {
    subscription_type: RwLock<SubscriptionType>,
    subscribed_topics: RwLock<HashSet<String>>,
    assigned_partitions: RwLock<HashSet<TopicPartition>>,
    partition_states: RwLock<HashMap<TopicPartition, TopicPartitionState>>,
    default_reset_strategy: OffsetResetStrategy,
}

impl SubscriptionState {
    pub fn new(default_reset_strategy: OffsetResetStrategy) -> Self {
        Self {
            subscription_type: RwLock::new(SubscriptionType::None),
            subscribed_topics: RwLock::new(HashSet::new()),
            assigned_partitions: RwLock::new(HashSet::new()),
            partition_states: RwLock::new(HashMap::new()),
            default_reset_strategy,
        }
    }

    /// Subscribe to a set of topics (group-managed assignment).
    pub fn subscribe(&self, topics: &[&str]) {
        let mut sub_type = self.subscription_type.write().unwrap();
        let mut subscribed = self.subscribed_topics.write().unwrap();

        *sub_type = SubscriptionType::Topics;
        subscribed.clear();
        for t in topics {
            subscribed.insert((*t).to_string());
        }
    }

    /// Manually assign specific partitions.
    pub fn assign(&self, partitions: &[TopicPartition]) {
        let mut sub_type = self.subscription_type.write().unwrap();
        let mut assigned = self.assigned_partitions.write().unwrap();
        let mut states = self.partition_states.write().unwrap();

        *sub_type = SubscriptionType::Manual;
        assigned.clear();
        states.clear();

        for tp in partitions {
            assigned.insert(tp.clone());
            states.insert(
                tp.clone(),
                TopicPartitionState::new(self.default_reset_strategy),
            );
        }
    }

    /// Clear all subscriptions and assignments.
    pub fn unsubscribe(&self) {
        *self.subscription_type.write().unwrap() = SubscriptionType::None;
        self.subscribed_topics.write().unwrap().clear();
        self.assigned_partitions.write().unwrap().clear();
        self.partition_states.write().unwrap().clear();
    }

    /// Called when group coordination assigns partitions to this consumer.
    pub fn assign_from_group(&self, partitions: &[TopicPartition]) {
        let mut assigned = self.assigned_partitions.write().unwrap();
        let mut states = self.partition_states.write().unwrap();

        assigned.clear();
        states.clear();

        for tp in partitions {
            assigned.insert(tp.clone());
            states.insert(
                tp.clone(),
                TopicPartitionState::new(self.default_reset_strategy),
            );
        }
    }

    pub fn subscription_type(&self) -> SubscriptionType {
        *self.subscription_type.read().unwrap()
    }

    pub fn subscribed_topics(&self) -> HashSet<String> {
        self.subscribed_topics.read().unwrap().clone()
    }

    pub fn assigned_partitions(&self) -> HashSet<TopicPartition> {
        self.assigned_partitions.read().unwrap().clone()
    }

    /// Set the fetch position for a partition.
    pub fn set_position(&self, tp: &TopicPartition, offset: i64) {
        if let Some(state) = self.partition_states.write().unwrap().get_mut(tp) {
            state.position = Some(offset);
            state.reset_strategy = None;
        }
    }

    /// Get the fetch position for a partition.
    pub fn position(&self, tp: &TopicPartition) -> Option<i64> {
        self.partition_states
            .read()
            .unwrap()
            .get(tp)
            .and_then(|s| s.position)
    }

    /// Set the committed offset for a partition.
    pub fn set_committed(&self, tp: &TopicPartition, offset: OffsetAndMetadata) {
        if let Some(state) = self.partition_states.write().unwrap().get_mut(tp) {
            state.committed = Some(offset);
        }
    }

    /// Get the committed offset for a partition.
    pub fn committed(&self, tp: &TopicPartition) -> Option<OffsetAndMetadata> {
        self.partition_states
            .read()
            .unwrap()
            .get(tp)
            .and_then(|s| s.committed.clone())
    }

    /// Seek to a specific offset for a partition.
    pub fn seek(&self, tp: &TopicPartition, offset: i64) {
        if let Some(state) = self.partition_states.write().unwrap().get_mut(tp) {
            state.position = Some(offset);
            state.reset_strategy = None;
        }
    }

    /// Mark partitions for reset to beginning.
    pub fn request_offset_reset(&self, tp: &TopicPartition, strategy: OffsetResetStrategy) {
        if let Some(state) = self.partition_states.write().unwrap().get_mut(tp) {
            state.reset_strategy = Some(strategy);
            state.position = None;
        }
    }

    /// Get partitions that need offset reset.
    pub fn partitions_needing_reset(&self) -> HashMap<TopicPartition, OffsetResetStrategy> {
        self.partition_states
            .read()
            .unwrap()
            .iter()
            .filter_map(|(tp, state)| state.reset_strategy.map(|strategy| (tp.clone(), strategy)))
            .collect()
    }

    /// Get partitions that have a valid fetch position.
    pub fn fetchable_partitions(&self) -> Vec<TopicPartition> {
        self.partition_states
            .read()
            .unwrap()
            .iter()
            .filter(|(_, state)| state.position.is_some() && !state.paused)
            .map(|(tp, _)| tp.clone())
            .collect()
    }

    /// Pause fetching from specific partitions.
    pub fn pause(&self, partitions: &[TopicPartition]) {
        let mut states = self.partition_states.write().unwrap();
        for tp in partitions {
            if let Some(state) = states.get_mut(tp) {
                state.paused = true;
            }
        }
    }

    /// Resume fetching from specific partitions.
    pub fn resume(&self, partitions: &[TopicPartition]) {
        let mut states = self.partition_states.write().unwrap();
        for tp in partitions {
            if let Some(state) = states.get_mut(tp) {
                state.paused = false;
            }
        }
    }

    /// Get paused partitions.
    pub fn paused_partitions(&self) -> HashSet<TopicPartition> {
        self.partition_states
            .read()
            .unwrap()
            .iter()
            .filter(|(_, state)| state.paused)
            .map(|(tp, _)| tp.clone())
            .collect()
    }

    /// Check if all assigned partitions have a fetch position.
    pub fn has_all_fetch_positions(&self) -> bool {
        let states = self.partition_states.read().unwrap();
        !states.is_empty() && states.values().all(|s| s.position.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscribe() {
        let state = SubscriptionState::new(OffsetResetStrategy::Latest);
        state.subscribe(&["topic1", "topic2"]);

        assert_eq!(state.subscription_type(), SubscriptionType::Topics);
        assert_eq!(state.subscribed_topics().len(), 2);
        assert!(state.subscribed_topics().contains("topic1"));
    }

    #[test]
    fn test_assign() {
        let state = SubscriptionState::new(OffsetResetStrategy::Latest);
        let partitions = vec![
            TopicPartition::new("test", 0),
            TopicPartition::new("test", 1),
        ];
        state.assign(&partitions);

        assert_eq!(state.subscription_type(), SubscriptionType::Manual);
        assert_eq!(state.assigned_partitions().len(), 2);
    }

    #[test]
    fn test_unsubscribe() {
        let state = SubscriptionState::new(OffsetResetStrategy::Latest);
        state.subscribe(&["topic1"]);
        state.unsubscribe();

        assert_eq!(state.subscription_type(), SubscriptionType::None);
        assert!(state.subscribed_topics().is_empty());
        assert!(state.assigned_partitions().is_empty());
    }

    #[test]
    fn test_position_tracking() {
        let state = SubscriptionState::new(OffsetResetStrategy::Latest);
        let tp = TopicPartition::new("test", 0);
        state.assign(&[tp.clone()]);

        assert!(state.position(&tp).is_none());

        state.set_position(&tp, 42);
        assert_eq!(state.position(&tp), Some(42));
    }

    #[test]
    fn test_seek() {
        let state = SubscriptionState::new(OffsetResetStrategy::Latest);
        let tp = TopicPartition::new("test", 0);
        state.assign(&[tp.clone()]);

        state.seek(&tp, 100);
        assert_eq!(state.position(&tp), Some(100));
    }

    #[test]
    fn test_fetchable_partitions() {
        let state = SubscriptionState::new(OffsetResetStrategy::Latest);
        let tp0 = TopicPartition::new("test", 0);
        let tp1 = TopicPartition::new("test", 1);
        state.assign(&[tp0.clone(), tp1.clone()]);

        // No positions yet
        assert!(state.fetchable_partitions().is_empty());

        state.set_position(&tp0, 0);
        state.set_position(&tp1, 0);
        assert_eq!(state.fetchable_partitions().len(), 2);

        state.pause(&[tp0.clone()]);
        assert_eq!(state.fetchable_partitions().len(), 1);
    }

    #[test]
    fn test_pause_resume() {
        let state = SubscriptionState::new(OffsetResetStrategy::Latest);
        let tp = TopicPartition::new("test", 0);
        state.assign(&[tp.clone()]);

        state.pause(&[tp.clone()]);
        assert!(state.paused_partitions().contains(&tp));

        state.resume(&[tp.clone()]);
        assert!(state.paused_partitions().is_empty());
    }

    #[test]
    fn test_partitions_needing_reset() {
        let state = SubscriptionState::new(OffsetResetStrategy::Earliest);
        let tp = TopicPartition::new("test", 0);
        state.assign(&[tp.clone()]);

        // Newly assigned partitions need reset
        let needing = state.partitions_needing_reset();
        assert_eq!(needing.len(), 1);
        assert_eq!(needing[&tp], OffsetResetStrategy::Earliest);

        // After setting position, no reset needed
        state.set_position(&tp, 0);
        assert!(state.partitions_needing_reset().is_empty());
    }

    #[test]
    fn test_has_all_fetch_positions() {
        let state = SubscriptionState::new(OffsetResetStrategy::Latest);
        let tp0 = TopicPartition::new("test", 0);
        let tp1 = TopicPartition::new("test", 1);
        state.assign(&[tp0.clone(), tp1.clone()]);

        assert!(!state.has_all_fetch_positions());

        state.set_position(&tp0, 0);
        assert!(!state.has_all_fetch_positions());

        state.set_position(&tp1, 0);
        assert!(state.has_all_fetch_positions());
    }

    /// Subscribing again replaces the previous set of topics.
    #[test]
    fn test_subscribe_replaces_previous_topics() {
        let state = SubscriptionState::new(OffsetResetStrategy::Latest);
        state.subscribe(&["topic-a", "topic-b"]);
        assert_eq!(state.subscribed_topics().len(), 2);

        state.subscribe(&["topic-c"]);
        let topics = state.subscribed_topics();
        assert_eq!(topics.len(), 1);
        assert!(topics.contains("topic-c"));
        assert!(!topics.contains("topic-a"));
    }

    /// Assigning again replaces the previous partition set.
    #[test]
    fn test_assign_replaces_previous_partitions() {
        let state = SubscriptionState::new(OffsetResetStrategy::Earliest);
        let tp0 = TopicPartition::new("t", 0);
        let tp1 = TopicPartition::new("t", 1);
        let tp2 = TopicPartition::new("t", 2);

        state.assign(&[tp0.clone(), tp1.clone()]);
        assert_eq!(state.assigned_partitions().len(), 2);

        state.assign(&[tp2.clone()]);
        let assigned = state.assigned_partitions();
        assert_eq!(assigned.len(), 1);
        assert!(assigned.contains(&tp2));
        assert!(!assigned.contains(&tp0));
    }

    /// Subscribe sets type to Topics; assign sets type to Manual.
    /// They use different subscription types.
    #[test]
    fn test_subscribe_then_assign_changes_type() {
        let state = SubscriptionState::new(OffsetResetStrategy::Latest);

        state.subscribe(&["topic1"]);
        assert_eq!(state.subscription_type(), SubscriptionType::Topics);

        state.assign(&[TopicPartition::new("topic1", 0)]);
        assert_eq!(state.subscription_type(), SubscriptionType::Manual);
    }

    /// Committed offset tracking: set and get committed offsets.
    #[test]
    fn test_committed_offset_tracking() {
        let state = SubscriptionState::new(OffsetResetStrategy::Latest);
        let tp = TopicPartition::new("test", 0);
        state.assign(&[tp.clone()]);

        // Initially no committed offset
        assert!(state.committed(&tp).is_none());

        let oam = OffsetAndMetadata {
            offset: 50,
            metadata: Some("test-metadata".into()),
            leader_epoch: Some(1),
        };
        state.set_committed(&tp, oam.clone());

        let committed = state.committed(&tp).unwrap();
        assert_eq!(committed.offset, 50);
        assert_eq!(committed.metadata.as_deref(), Some("test-metadata"));
        assert_eq!(committed.leader_epoch, Some(1));
    }

    /// assign_from_group clears previous state and sets new partitions.
    #[test]
    fn test_assign_from_group_clears_previous_state() {
        let state = SubscriptionState::new(OffsetResetStrategy::Earliest);
        let tp0 = TopicPartition::new("t", 0);
        let tp1 = TopicPartition::new("t", 1);
        let tp2 = TopicPartition::new("t", 2);

        // Set up initial assignment with positions
        state.subscribe(&["t"]);
        state.assign_from_group(&[tp0.clone(), tp1.clone()]);
        state.set_position(&tp0, 100);
        state.set_position(&tp1, 200);
        assert!(state.has_all_fetch_positions());

        // Re-assign from group with different partitions
        state.assign_from_group(&[tp2.clone()]);
        let assigned = state.assigned_partitions();
        assert_eq!(assigned.len(), 1);
        assert!(assigned.contains(&tp2));
        assert!(!assigned.contains(&tp0));
        assert!(!assigned.contains(&tp1));

        // Old positions are gone
        assert!(state.position(&tp0).is_none());
        assert!(state.position(&tp1).is_none());
        // New partition has no position yet
        assert!(state.position(&tp2).is_none());
        assert!(!state.has_all_fetch_positions());
    }

    /// Subscribe to multiple topics.
    #[test]
    fn test_multiple_topics_subscription() {
        let state = SubscriptionState::new(OffsetResetStrategy::Latest);
        state.subscribe(&["orders", "events", "metrics", "logs"]);
        let topics = state.subscribed_topics();
        assert_eq!(topics.len(), 4);
        assert!(topics.contains("orders"));
        assert!(topics.contains("events"));
        assert!(topics.contains("metrics"));
        assert!(topics.contains("logs"));
    }

    /// Position is None after assign until explicitly set.
    #[test]
    fn test_position_is_none_after_assign_until_set() {
        let state = SubscriptionState::new(OffsetResetStrategy::Latest);
        let tp0 = TopicPartition::new("t", 0);
        let tp1 = TopicPartition::new("t", 1);
        state.assign(&[tp0.clone(), tp1.clone()]);

        assert!(state.position(&tp0).is_none());
        assert!(state.position(&tp1).is_none());

        state.set_position(&tp0, 10);
        assert_eq!(state.position(&tp0), Some(10));
        assert!(state.position(&tp1).is_none()); // still None
    }

    /// has_all_fetch_positions is false when no partitions are assigned.
    #[test]
    fn test_has_all_fetch_positions_false_when_empty() {
        let state = SubscriptionState::new(OffsetResetStrategy::Latest);
        // No partitions assigned — empty states map
        assert!(!state.has_all_fetch_positions());
    }

    /// request_offset_reset clears position and sets reset strategy.
    #[test]
    fn test_request_offset_reset_clears_position() {
        let state = SubscriptionState::new(OffsetResetStrategy::Latest);
        let tp = TopicPartition::new("t", 0);
        state.assign(&[tp.clone()]);
        state.set_position(&tp, 42);
        assert_eq!(state.position(&tp), Some(42));

        state.request_offset_reset(&tp, OffsetResetStrategy::Earliest);
        assert!(state.position(&tp).is_none());

        let needing = state.partitions_needing_reset();
        assert_eq!(needing[&tp], OffsetResetStrategy::Earliest);
    }

    /// Committed offset for an unassigned partition returns None.
    #[test]
    fn test_committed_for_unassigned_partition() {
        let state = SubscriptionState::new(OffsetResetStrategy::Latest);
        let tp = TopicPartition::new("unknown", 0);
        assert!(state.committed(&tp).is_none());
    }
}
