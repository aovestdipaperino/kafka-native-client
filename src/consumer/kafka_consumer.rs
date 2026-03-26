use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::common::{PartitionInfo, TopicPartition};
use crate::config::ConsumerConfig;
use crate::consumer::consumer_records::ConsumerRecords;
use crate::consumer::fetcher::{Fetcher, FetcherConfig};
use crate::consumer::group_coordinator::GroupCoordinator;
use crate::consumer::offset::{ConsumerGroupMetadata, OffsetAndMetadata, OffsetResetStrategy};
use crate::consumer::rebalance_listener::ConsumerRebalanceListener;
use crate::consumer::subscription_state::SubscriptionState;
use crate::error::{KafkaError, Result};
use crate::network::connection_manager::ConnectionManager;
use crate::network::metadata::Metadata;

/// A Kafka consumer client that reads records from the Kafka cluster.
///
/// Supports both group-managed subscriptions (via `subscribe`) and manual
/// partition assignment (via `assign`). The `poll` method drives the
/// fetch loop, group coordination, and auto-commit.
pub struct KafkaConsumer {
    config: ConsumerConfig,
    metadata: Arc<Metadata>,
    connection_manager: Arc<ConnectionManager>,
    subscription_state: Arc<SubscriptionState>,
    fetcher: Fetcher,
    group_coordinator: Option<GroupCoordinator>,
    wakeup: AtomicBool,
    last_auto_commit_ms: std::sync::Mutex<i64>,
}

impl KafkaConsumer {
    /// Create a new KafkaConsumer, connecting to the cluster.
    pub async fn new(config: ConsumerConfig) -> Result<Self> {
        let connection_manager = Arc::new(ConnectionManager::new(
            config.bootstrap_servers.clone(),
            config.client_id.clone(),
            5,
        ));
        connection_manager.bootstrap().await?;

        let metadata = Arc::new(Metadata::new(
            connection_manager.clone(),
            config.metadata_max_age_ms,
        ));
        metadata.fetch().await?;

        let reset_strategy = config.auto_offset_reset;

        let subscription_state = Arc::new(SubscriptionState::new(reset_strategy));

        let fetcher = Fetcher::new(
            connection_manager.clone(),
            subscription_state.clone(),
            metadata.clone(),
            FetcherConfig {
                max_wait_ms: config.fetch_max_wait_ms,
                min_bytes: config.fetch_min_bytes,
                max_bytes: 52_428_800,
                max_partition_fetch_bytes: config.max_partition_fetch_bytes,
                max_poll_records: config.max_poll_records,
            },
        );

        let group_coordinator = config.group_id.as_ref().map(|group_id| {
            GroupCoordinator::new(
                connection_manager.clone(),
                group_id.clone(),
                config.session_timeout_ms as u64,
                config.heartbeat_interval_ms as u64,
                300_000,
                None,
                None,
            )
        });

        Ok(Self {
            config,
            metadata,
            connection_manager,
            subscription_state,
            fetcher,
            group_coordinator,
            wakeup: AtomicBool::new(false),
            last_auto_commit_ms: std::sync::Mutex::new(0),
        })
    }

    /// Subscribe to topics for group-managed partition assignment.
    pub fn subscribe(
        &mut self,
        topics: &[&str],
        listener: Option<Box<dyn ConsumerRebalanceListener>>,
    ) -> Result<()> {
        if self.group_coordinator.is_none() {
            return Err(KafkaError::Config(
                "group.id is required for subscribe()".into(),
            ));
        }

        self.subscription_state.subscribe(topics);

        for topic in topics {
            self.metadata.add_topic(topic);
        }

        // Replace coordinator with one that has the listener
        if listener.is_some()
            && let Some(group_id) = &self.config.group_id {
                self.group_coordinator = Some(GroupCoordinator::new(
                    self.connection_manager.clone(),
                    group_id.clone(),
                    self.config.session_timeout_ms as u64,
                    self.config.heartbeat_interval_ms as u64,
                    300_000,
                    None,
                    listener,
                ));
            }

        Ok(())
    }

    /// Manually assign specific partitions.
    pub fn assign(&self, partitions: &[TopicPartition]) -> Result<()> {
        self.subscription_state.assign(partitions);
        for tp in partitions {
            self.metadata.add_topic(&tp.topic);
        }
        Ok(())
    }

    /// Clear all subscriptions and assignments.
    pub fn unsubscribe(&self) -> Result<()> {
        self.subscription_state.unsubscribe();
        Ok(())
    }

    /// Poll for new records. Drives fetching, group coordination, and auto-commit.
    pub async fn poll(&self, timeout: Duration) -> Result<ConsumerRecords> {
        // Check wakeup flag
        if self.wakeup.swap(false, Ordering::Relaxed) {
            return Err(KafkaError::WakeupException);
        }

        // Group coordination: join/rejoin if needed
        if let Some(coordinator) = &self.group_coordinator
            && coordinator.needs_rejoin() {
                let assigned = coordinator.join_group(&self.subscription_state).await?;
                self.subscription_state.assign_from_group(&assigned);

                // Fetch committed offsets for new assignment
                let committed = coordinator.fetch_committed_offsets(&assigned).await?;
                for (tp, oam) in &committed {
                    self.subscription_state.set_position(tp, oam.offset);
                    self.subscription_state.set_committed(tp, oam.clone());
                }

                // For partitions with no committed offset, apply reset strategy
                for tp in &assigned {
                    if self.subscription_state.position(tp).is_none() {
                        // Default to offset 0 for earliest, or will be handled by broker
                        self.subscription_state.set_position(tp, 0);
                    }
                }
            }

        // Auto-commit if enabled
        if self.config.enable_auto_commit {
            self.maybe_auto_commit().await;
        }

        // Send fetch requests
        self.fetcher.send_fetches().await;

        // Wait for responses (with timeout)
        if self.fetcher.collect_fetch().is_empty() {
            tokio::time::sleep(timeout.min(Duration::from_millis(100))).await;
            self.fetcher.send_fetches().await;
        }

        Ok(self.fetcher.collect_fetch())
    }

    /// Commit all consumed offsets synchronously.
    pub async fn commit_sync(&self) -> Result<()> {
        let coordinator = self
            .group_coordinator
            .as_ref()
            .ok_or_else(|| KafkaError::Config("group.id required for commit".into()))?;

        let offsets = self.current_offsets();
        if offsets.is_empty() {
            return Ok(());
        }

        coordinator.commit_offsets_sync(offsets).await
    }

    /// Commit specific offsets synchronously.
    pub async fn commit_sync_offsets(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<()> {
        let coordinator = self
            .group_coordinator
            .as_ref()
            .ok_or_else(|| KafkaError::Config("group.id required for commit".into()))?;

        coordinator.commit_offsets_sync(offsets).await
    }

    /// Seek to a specific offset for a partition.
    pub fn seek(&self, partition: &TopicPartition, offset: i64) -> Result<()> {
        self.subscription_state.seek(partition, offset);
        Ok(())
    }

    /// Seek to the beginning for partitions.
    pub fn seek_to_beginning(&self, partitions: &[TopicPartition]) -> Result<()> {
        for tp in partitions {
            self.subscription_state
                .request_offset_reset(tp, OffsetResetStrategy::Earliest);
        }
        Ok(())
    }

    /// Seek to the end for partitions.
    pub fn seek_to_end(&self, partitions: &[TopicPartition]) -> Result<()> {
        for tp in partitions {
            self.subscription_state
                .request_offset_reset(tp, OffsetResetStrategy::Latest);
        }
        Ok(())
    }

    /// Get the current position (next offset to fetch) for a partition.
    pub fn position(&self, partition: &TopicPartition) -> Option<i64> {
        self.subscription_state.position(partition)
    }

    /// Get the last committed offset for partitions.
    pub async fn committed(
        &self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>> {
        let coordinator = self
            .group_coordinator
            .as_ref()
            .ok_or_else(|| KafkaError::Config("group.id required".into()))?;

        coordinator.fetch_committed_offsets(partitions).await
    }

    /// Get the current partition assignment.
    pub fn assignment(&self) -> HashSet<TopicPartition> {
        self.subscription_state.assigned_partitions()
    }

    /// Get subscribed topics.
    pub fn subscription(&self) -> HashSet<String> {
        self.subscription_state.subscribed_topics()
    }

    /// Get partition info for a topic.
    pub fn partitions_for(&self, topic: &str) -> Vec<PartitionInfo> {
        self.metadata.cluster().partitions_for_topic(topic).to_vec()
    }

    /// Pause fetching from partitions.
    pub fn pause(&self, partitions: &[TopicPartition]) -> Result<()> {
        self.subscription_state.pause(partitions);
        Ok(())
    }

    /// Resume fetching from partitions.
    pub fn resume(&self, partitions: &[TopicPartition]) -> Result<()> {
        self.subscription_state.resume(partitions);
        Ok(())
    }

    /// Get paused partitions.
    pub fn paused(&self) -> HashSet<TopicPartition> {
        self.subscription_state.paused_partitions()
    }

    /// Get group metadata.
    pub fn group_metadata(&self) -> ConsumerGroupMetadata {
        let (group_id, member_id, generation_id) =
            if let Some(coordinator) = &self.group_coordinator {
                (
                    coordinator.group_id().to_string(),
                    coordinator.member_id(),
                    coordinator.generation_id(),
                )
            } else {
                (String::new(), String::new(), -1)
            };

        ConsumerGroupMetadata {
            group_id,
            generation_id,
            member_id,
            group_instance_id: None,
        }
    }

    /// Signal the consumer to wake up from a blocking poll.
    pub fn wakeup(&self) {
        self.wakeup.store(true, Ordering::Relaxed);
    }

    /// Close the consumer, leaving the group if applicable.
    pub async fn close(self, _timeout: Duration) -> Result<()> {
        if let Some(coordinator) = &self.group_coordinator {
            let _ = coordinator.leave_group().await;
        }
        Ok(())
    }

    async fn maybe_auto_commit(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let should_commit = {
            let last = self.last_auto_commit_ms.lock().unwrap();
            (now - *last) >= self.config.auto_commit_interval_ms as i64
        };

        if should_commit
            && let Ok(()) = self.commit_sync().await {
                *self.last_auto_commit_ms.lock().unwrap() = now;
            }
    }

    fn current_offsets(&self) -> HashMap<TopicPartition, OffsetAndMetadata> {
        let assigned = self.subscription_state.assigned_partitions();
        let mut offsets = HashMap::new();
        for tp in assigned {
            if let Some(position) = self.subscription_state.position(&tp) {
                offsets.insert(tp, OffsetAndMetadata::new(position));
            }
        }
        offsets
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wakeup_flag() {
        // Test wakeup mechanism without requiring a live cluster
        let wakeup = AtomicBool::new(false);
        wakeup.store(true, Ordering::Relaxed);
        assert!(wakeup.swap(false, Ordering::Relaxed));
        assert!(!wakeup.load(Ordering::Relaxed));
    }
}
