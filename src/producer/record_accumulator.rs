use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use indexmap::IndexMap;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};

use proto_kafka::protocol::StrBytes;
use proto_kafka::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record,
    TimestampType,
};

use crate::common::{Cluster, TopicPartition};
use crate::error::{KafkaError, Result};
use crate::producer::{Callback, ProducerRecord, RecordMetadata, SendFuture};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// An in-progress batch of records destined for a single TopicPartition.
pub struct ProducerBatch {
    pub tp: TopicPartition,
    pub records: Vec<Record>,
    pub callbacks: Vec<(oneshot::Sender<Result<RecordMetadata>>, Option<Callback>)>,
    pub created_ms: i64,
    pub record_count: usize,
    pub size_bytes: usize,
    /// Idempotent producer fields — set by Sender before building ProduceRequest.
    pub producer_id: Option<i64>,
    pub producer_epoch: Option<i16>,
    pub base_sequence: Option<i32>,
    /// Holds buffer memory permits until this batch is completed or failed.
    pub(crate) _permits: Vec<OwnedSemaphorePermit>,
}

impl ProducerBatch {
    fn new(tp: TopicPartition) -> Self {
        Self {
            tp,
            records: Vec::new(),
            callbacks: Vec::new(),
            created_ms: now_ms(),
            record_count: 0,
            size_bytes: 0,
            producer_id: None,
            producer_epoch: None,
            base_sequence: None,
            _permits: Vec::new(),
        }
    }

    /// Complete all callbacks with success metadata.
    /// Buffer memory permits are released when this batch is dropped.
    pub fn complete_all(self, base_offset: i64, timestamp: i64) {
        let tp = self.tp;
        for (i, (tx, cb)) in self.callbacks.into_iter().enumerate() {
            let meta = RecordMetadata {
                topic: tp.topic.clone(),
                partition: tp.partition,
                offset: base_offset + i as i64,
                timestamp,
                serialized_key_size: -1,
                serialized_value_size: -1,
            };
            if let Some(callback) = cb {
                callback(Ok(&meta));
            }
            let _ = tx.send(Ok(meta));
        }
    }

    /// Fail all callbacks with an error produced by `make_err`.
    /// Buffer memory permits are released when this batch is dropped.
    pub fn fail_all<F>(self, make_err: F)
    where
        F: Fn() -> KafkaError,
    {
        for (tx, cb) in self.callbacks {
            let err = make_err();
            if let Some(callback) = cb {
                callback(Err(&err));
            }
            let _ = tx.send(Err(err));
        }
    }
}

/// Accumulates records into per-partition batches for the producer.
///
/// Records are batched until the batch reaches `batch_size` bytes or `linger_ms`
/// elapses since the batch was created. A semaphore enforces total buffer memory
/// limits, blocking `append` calls when memory is exhausted.
pub struct RecordAccumulator {
    batches: Mutex<HashMap<TopicPartition, ProducerBatch>>,
    batch_size: usize,
    compression: Compression,
    linger_ms: u64,
    buffer_semaphore: Arc<Semaphore>,
    max_block_ms: u64,
    /// Notified when a new record is appended (wakes the Sender).
    pub notify: Arc<Notify>,
}

impl RecordAccumulator {
    pub fn new(
        batch_size: usize,
        compression: Compression,
        linger_ms: u64,
        buffer_memory: usize,
        max_block_ms: u64,
    ) -> Self {
        Self {
            batches: Mutex::new(HashMap::new()),
            batch_size,
            compression,
            linger_ms,
            buffer_semaphore: Arc::new(Semaphore::new(buffer_memory)),
            max_block_ms,
            notify: Arc::new(Notify::new()),
        }
    }

    /// Append a record to the batch for the given partition.
    ///
    /// Blocks (up to `max_block_ms`) if total buffer memory is exhausted.
    /// Returns a [`SendFuture`] that resolves when the broker acknowledges the record.
    pub async fn append(
        &self,
        tp: TopicPartition,
        record: ProducerRecord,
        callback: Option<Callback>,
    ) -> Result<SendFuture> {
        let size = record.estimated_size().max(1);

        // Acquire buffer memory — blocks if full
        let permit = tokio::time::timeout(
            std::time::Duration::from_millis(self.max_block_ms),
            self.buffer_semaphore
                .clone()
                .acquire_many_owned(size as u32),
        )
        .await
        .map_err(|_| KafkaError::BufferExhausted)?
        .map_err(|_| KafkaError::BufferExhausted)?;

        // Convert to proto_kafka Record
        let timestamp = record.timestamp.unwrap_or_else(now_ms);
        let headers: IndexMap<StrBytes, Option<Bytes>> = record
            .headers
            .iter()
            .map(|h| (StrBytes::from_string(h.key.clone()), h.value.clone()))
            .collect();

        let proto_record = Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: NO_SEQUENCE,
            timestamp,
            key: record.key,
            value: record.value,
            headers,
        };

        let (tx, rx) = oneshot::channel();
        let future = SendFuture::new(rx);

        {
            let mut batches = self.batches.lock().unwrap();
            let batch = batches
                .entry(tp.clone())
                .or_insert_with(|| ProducerBatch::new(tp));
            batch.size_bytes += size;
            batch.record_count += 1;
            batch.records.push(proto_record);
            batch.callbacks.push((tx, callback));
            batch._permits.push(permit);
        }

        self.notify.notify_one();
        Ok(future)
    }

    /// Returns the set of broker IDs that have at least one ready batch.
    ///
    /// A batch is ready when its size reaches `batch_size` or `linger_ms` has
    /// elapsed since the batch was created.
    pub fn ready(&self, cluster: &Cluster) -> HashSet<i32> {
        let now = now_ms();
        let batches = self.batches.lock().unwrap();
        let mut ready_nodes = HashSet::new();

        for (tp, batch) in batches.iter() {
            let size_ready = batch.size_bytes >= self.batch_size;
            let time_ready = (now - batch.created_ms) as u64 >= self.linger_ms;
            if (size_ready || time_ready)
                && let Some(leader) = cluster.leader_for(tp) {
                    ready_nodes.insert(leader.id);
                }
        }

        ready_nodes
    }

    /// Drain ready batches for the given broker IDs, grouped by broker.
    ///
    /// Batches are removed from the accumulator and ownership is transferred
    /// to the caller (typically the Sender) for building ProduceRequests.
    pub fn drain(
        &self,
        ready_nodes: &HashSet<i32>,
        cluster: &Cluster,
    ) -> HashMap<i32, Vec<ProducerBatch>> {
        let now = now_ms();
        let mut batches = self.batches.lock().unwrap();
        let mut result: HashMap<i32, Vec<ProducerBatch>> = HashMap::new();

        let tps: Vec<TopicPartition> = batches.keys().cloned().collect();
        for tp in tps {
            if let Some(batch) = batches.get(&tp) {
                let size_ready = batch.size_bytes >= self.batch_size;
                let time_ready = (now - batch.created_ms) as u64 >= self.linger_ms;
                if (size_ready || time_ready)
                    && let Some(leader) = cluster.leader_for(&tp)
                        && ready_nodes.contains(&leader.id) {
                            let batch = batches.remove(&tp).unwrap();
                            result.entry(leader.id).or_default().push(batch);
                        }
            }
        }

        result
    }

    /// Returns `true` if there are any unsent batches.
    pub fn has_unsent(&self) -> bool {
        !self.batches.lock().unwrap().is_empty()
    }

    /// The compression type configured for this accumulator.
    pub fn compression(&self) -> Compression {
        self.compression
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Node, PartitionInfo};

    fn test_cluster() -> Cluster {
        let node = Node::new(0, "localhost", 9092);
        let partitions = vec![
            PartitionInfo {
                topic: "test".into(),
                partition: 0,
                leader: Some(node.clone()),
                replicas: vec![node.clone()],
                in_sync_replicas: vec![node.clone()],
            },
            PartitionInfo {
                topic: "test".into(),
                partition: 1,
                leader: Some(node.clone()),
                replicas: vec![node.clone()],
                in_sync_replicas: vec![node.clone()],
            },
        ];
        Cluster::new(vec![node], partitions)
    }

    #[tokio::test]
    async fn test_append_batches_by_partition() {
        let acc = RecordAccumulator::new(16384, Compression::None, 5000, 1024 * 1024, 60000);
        let tp = TopicPartition::new("test", 0);

        acc.append(tp.clone(), ProducerRecord::new("test", "v1"), None)
            .await
            .unwrap();
        acc.append(tp.clone(), ProducerRecord::new("test", "v2"), None)
            .await
            .unwrap();

        let batches = acc.batches.lock().unwrap();
        let batch = batches.get(&tp).unwrap();
        assert_eq!(batch.record_count, 2);
        assert_eq!(batch.records.len(), 2);
    }

    #[tokio::test]
    async fn test_full_batch_is_ready() {
        let acc = RecordAccumulator::new(10, Compression::None, 60000, 1024 * 1024, 60000);
        let tp = TopicPartition::new("test", 0);
        let cluster = test_cluster();

        acc.append(tp.clone(), ProducerRecord::new("test", "hello-world"), None)
            .await
            .unwrap();

        let ready = acc.ready(&cluster);
        assert!(ready.contains(&0));
    }

    #[tokio::test]
    async fn test_linger_timeout_makes_ready() {
        let acc = RecordAccumulator::new(1024 * 1024, Compression::None, 0, 1024 * 1024, 60000);
        let tp = TopicPartition::new("test", 0);
        let cluster = test_cluster();

        acc.append(tp.clone(), ProducerRecord::new("test", "v1"), None)
            .await
            .unwrap();

        let ready = acc.ready(&cluster);
        assert!(ready.contains(&0));
    }

    #[tokio::test]
    async fn test_drain_removes_batches() {
        let acc = RecordAccumulator::new(10, Compression::None, 0, 1024 * 1024, 60000);
        let tp = TopicPartition::new("test", 0);
        let cluster = test_cluster();

        acc.append(tp.clone(), ProducerRecord::new("test", "hello"), None)
            .await
            .unwrap();

        let ready = acc.ready(&cluster);
        let drained = acc.drain(&ready, &cluster);

        assert!(drained.contains_key(&0));
        assert_eq!(drained[&0].len(), 1);
        assert_eq!(drained[&0][0].record_count, 1);
        assert!(!acc.has_unsent());
    }

    #[tokio::test]
    async fn test_buffer_memory_backpressure() {
        let acc = Arc::new(RecordAccumulator::new(
            16384,
            Compression::None,
            60000,
            10,
            100,
        ));
        let tp = TopicPartition::new("test", 0);

        // 5 bytes
        acc.append(tp.clone(), ProducerRecord::new("test", "hello"), None)
            .await
            .unwrap();
        // 5 bytes — total 10, at limit
        acc.append(tp.clone(), ProducerRecord::new("test", "world"), None)
            .await
            .unwrap();
        // 9 bytes — exceeds buffer, should time out
        let result = acc
            .append(tp.clone(), ProducerRecord::new("test", "overflow!"), None)
            .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), KafkaError::BufferExhausted));
    }

    #[tokio::test]
    async fn test_has_unsent() {
        let acc = RecordAccumulator::new(16384, Compression::None, 5000, 1024 * 1024, 60000);
        assert!(!acc.has_unsent());

        let tp = TopicPartition::new("test", 0);
        acc.append(tp, ProducerRecord::new("test", "v1"), None)
            .await
            .unwrap();
        assert!(acc.has_unsent());
    }

    #[tokio::test]
    async fn test_complete_all_resolves_futures() {
        let acc = RecordAccumulator::new(16384, Compression::None, 0, 1024 * 1024, 60000);
        let tp = TopicPartition::new("test", 0);
        let cluster = test_cluster();

        let f1 = acc
            .append(tp.clone(), ProducerRecord::new("test", "v1"), None)
            .await
            .unwrap();
        let f2 = acc
            .append(tp.clone(), ProducerRecord::new("test", "v2"), None)
            .await
            .unwrap();

        let ready = acc.ready(&cluster);
        let drained = acc.drain(&ready, &cluster);
        for batch in drained.into_values().flatten() {
            batch.complete_all(100, 999);
        }

        let m1 = f1.await.unwrap();
        assert_eq!(m1.offset, 100);
        assert_eq!(m1.timestamp, 999);

        let m2 = f2.await.unwrap();
        assert_eq!(m2.offset, 101);
    }

    #[tokio::test]
    async fn test_fail_all_errors_futures() {
        let acc = RecordAccumulator::new(16384, Compression::None, 0, 1024 * 1024, 60000);
        let tp = TopicPartition::new("test", 0);
        let cluster = test_cluster();

        let f1 = acc
            .append(tp.clone(), ProducerRecord::new("test", "v1"), None)
            .await
            .unwrap();

        let ready = acc.ready(&cluster);
        let drained = acc.drain(&ready, &cluster);
        for batch in drained.into_values().flatten() {
            batch.fail_all(|| KafkaError::Timeout);
        }

        let result = f1.await;
        assert!(result.is_err());
    }
}
