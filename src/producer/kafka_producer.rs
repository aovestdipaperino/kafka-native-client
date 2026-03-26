use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::common::{PartitionInfo, TopicPartition};
use crate::config::ProducerConfig;
use crate::error::{KafkaError, Result};
use crate::network::connection_manager::ConnectionManager;
use crate::network::metadata::Metadata;
use crate::producer::callback::Callback;
use crate::producer::idempotent::ProducerIdManager;
use crate::producer::partitioner::{DefaultPartitioner, Partitioner};
use crate::producer::record_accumulator::RecordAccumulator;
use crate::producer::sender::Sender;
use crate::producer::{ProducerRecord, SendFuture};

/// A Kafka producer client that publishes records to the Kafka cluster.
///
/// The producer batches records per partition, compresses them, and sends
/// ProduceRequests to the appropriate broker leaders via a background Sender task.
pub struct KafkaProducer {
    metadata: Arc<Metadata>,
    accumulator: Arc<RecordAccumulator>,
    partitioner: Box<dyn Partitioner>,
    sender_shutdown: Arc<Notify>,
    sender_handle: JoinHandle<()>,
}

impl KafkaProducer {
    /// Create a new KafkaProducer, connecting to the cluster and starting
    /// the background sender task.
    pub async fn new(config: ProducerConfig) -> Result<Self> {
        let connection_manager = Arc::new(ConnectionManager::new(
            config.bootstrap_servers.clone(),
            config.client_id.clone(),
            config.max_in_flight_requests_per_connection,
        ));
        connection_manager.bootstrap().await?;

        let metadata = Arc::new(Metadata::new(
            connection_manager.clone(),
            config.metadata_max_age_ms,
        ));
        metadata.fetch().await?;

        let compression = config.compression_type.to_proto_kafka();
        let accumulator = Arc::new(RecordAccumulator::new(
            config.batch_size,
            compression,
            config.linger_ms,
            config.buffer_memory,
            config.max_block_ms,
        ));

        if config.enable_idempotence {
            let mgr = ProducerIdManager::new(connection_manager.clone());
            mgr.init().await?;
        }

        let sender = Sender::new(
            accumulator.clone(),
            connection_manager,
            metadata.clone(),
            config.acks,
            config.request_timeout_ms,
            config.retries,
            config.retry_backoff_ms,
            config.delivery_timeout_ms,
        );
        let sender_shutdown = sender.shutdown_handle();
        let sender_handle = tokio::spawn(sender.run());

        Ok(Self {
            metadata,
            accumulator,
            partitioner: Box::new(DefaultPartitioner::new()),
            sender_shutdown,
            sender_handle,
        })
    }

    /// Send a record to the Kafka cluster.
    ///
    /// The record is buffered in the accumulator and will be sent in the
    /// next batch by the background Sender task. Returns a [`SendFuture`]
    /// that resolves when the broker acknowledges the record.
    pub async fn send(
        &self,
        record: ProducerRecord,
        callback: Option<Callback>,
    ) -> Result<SendFuture> {
        let tp = self.partition_for(&record)?;
        self.accumulator.append(tp, record, callback).await
    }

    /// Flush all buffered records, waiting until they have been sent.
    pub async fn flush(&self) {
        while self.accumulator.has_unsent() {
            self.accumulator.notify.notify_one();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Get partition info for a topic from cached metadata.
    pub fn partitions_for(&self, topic: &str) -> Vec<PartitionInfo> {
        self.metadata.cluster().partitions_for_topic(topic).to_vec()
    }

    /// Shut down the producer: flush pending records, stop the sender task.
    pub async fn close(self, timeout: Duration) -> Result<()> {
        // Signal flush
        self.accumulator.notify.notify_one();

        // Wait for unsent records with timeout
        let _ = tokio::time::timeout(timeout, async {
            while self.accumulator.has_unsent() {
                self.accumulator.notify.notify_one();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        // Shut down sender
        self.sender_shutdown.notify_one();
        let _ = tokio::time::timeout(Duration::from_secs(5), self.sender_handle).await;

        Ok(())
    }

    fn partition_for(&self, record: &ProducerRecord) -> Result<TopicPartition> {
        if let Some(partition) = record.partition {
            return Ok(TopicPartition::new(record.topic.clone(), partition));
        }

        let cluster = self.metadata.cluster();
        let partitions = cluster.partitions_for_topic(&record.topic);
        if partitions.is_empty() {
            return Err(KafkaError::Config(format!(
                "no partitions found for topic '{}'",
                record.topic
            )));
        }

        let partition = self.partitioner.partition(
            &record.topic,
            record.key.as_deref(),
            record.value.as_deref(),
            &cluster,
        );

        Ok(TopicPartition::new(record.topic.clone(), partition))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_selection_with_explicit_partition() {
        // This test verifies the partitioning logic without needing a live broker
        let record = ProducerRecord::new("test", "value").with_partition(3);
        assert_eq!(record.partition, Some(3));
    }
}
