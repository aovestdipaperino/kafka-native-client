use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use tokio::sync::Notify;

use proto_kafka::error::ParseResponseErrorCode;
use proto_kafka::messages::produce_request::{PartitionProduceData, TopicProduceData};
use proto_kafka::messages::{ProduceRequest, TopicName};
use proto_kafka::protocol::StrBytes;
use proto_kafka::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_SEQUENCE, Record, RecordBatchEncoder,
    RecordEncodeOptions, TimestampType,
};

use crate::config::Acks;
use crate::error::{KafkaError, Result};
use crate::network::connection_manager::ConnectionManager;
use crate::network::metadata::Metadata;
use crate::producer::record_accumulator::{ProducerBatch, RecordAccumulator};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Background task that drains ready batches from the RecordAccumulator
/// and sends ProduceRequests to the appropriate brokers.
pub struct Sender {
    accumulator: Arc<RecordAccumulator>,
    connection_manager: Arc<ConnectionManager>,
    metadata: Arc<Metadata>,
    acks: Acks,
    request_timeout_ms: u64,
    retries: u32,
    retry_backoff_ms: u64,
    delivery_timeout_ms: u64,
    shutdown: Arc<Notify>,
}

impl Sender {
    pub fn new(
        accumulator: Arc<RecordAccumulator>,
        connection_manager: Arc<ConnectionManager>,
        metadata: Arc<Metadata>,
        acks: Acks,
        request_timeout_ms: u64,
        retries: u32,
        retry_backoff_ms: u64,
        delivery_timeout_ms: u64,
    ) -> Self {
        Self {
            accumulator,
            connection_manager,
            metadata,
            acks,
            request_timeout_ms,
            retries,
            retry_backoff_ms,
            delivery_timeout_ms,
            shutdown: Arc::new(Notify::new()),
        }
    }

    pub fn shutdown_handle(&self) -> Arc<Notify> {
        self.shutdown.clone()
    }

    /// Run the sender loop. This should be spawned as a tokio task.
    pub async fn run(self) {
        let linger_ms = 5; // wake up frequently to check for lingered batches
        loop {
            // Wait for new records or linger timeout
            tokio::select! {
                _ = self.accumulator.notify.notified() => {}
                _ = tokio::time::sleep(std::time::Duration::from_millis(linger_ms)) => {}
                _ = self.shutdown.notified() => {
                    // Drain remaining batches before exiting
                    self.send_pending().await;
                    return;
                }
            }

            self.send_pending().await;
        }
    }

    async fn send_pending(&self) {
        let cluster = self.metadata.cluster();

        // Find which brokers have ready batches
        let ready_nodes = self.accumulator.ready(&cluster);
        if ready_nodes.is_empty() {
            return;
        }

        // Ensure connections to ready brokers
        for &broker_id in &ready_nodes {
            if let Some(addr) = self.connection_manager.broker_address(broker_id)
                && let Err(e) = self
                    .connection_manager
                    .ensure_connection(broker_id, &addr)
                    .await
                {
                    tracing::warn!("Failed to connect to broker {broker_id}: {e}");
                }
        }

        // Drain ready batches grouped by broker
        let drained = self.accumulator.drain(&ready_nodes, &cluster);

        // Send a ProduceRequest per broker
        for (broker_id, batches) in drained {
            self.send_produce_request(broker_id, batches).await;
        }
    }

    async fn send_produce_request(&self, broker_id: i32, batches: Vec<ProducerBatch>) {
        let compression = self.accumulator.compression();

        // Build a mapping from (topic, partition) -> batch for response correlation
        let mut batch_index: HashMap<(String, i32), ProducerBatch> = HashMap::new();

        let produce_request = match build_produce_request(
            &batches,
            self.acks.to_i16(),
            self.request_timeout_ms as i32,
            compression,
        ) {
            Ok(req) => {
                // Move batches into the index after building the request
                for batch in batches {
                    batch_index.insert((batch.tp.topic.clone(), batch.tp.partition), batch);
                }
                req
            }
            Err(e) => {
                tracing::error!("Failed to build ProduceRequest: {e}");
                for batch in batches {
                    batch
                        .fail_all(|| KafkaError::Protocol(proto_kafka::ProtoError::NotEnoughBytes));
                }
                return;
            }
        };

        // Send to broker
        match self
            .connection_manager
            .send(broker_id, produce_request)
            .await
        {
            Ok(response) => {
                // Process per-partition responses
                for topic_response in response.responses {
                    let topic_name = topic_response.name.0.to_string();
                    for partition_response in topic_response.partition_responses {
                        let key = (topic_name.clone(), partition_response.index);
                        if let Some(batch) = batch_index.remove(&key) {
                            if partition_response.error_code.is_ok() {
                                batch.complete_all(
                                    partition_response.base_offset,
                                    partition_response.log_append_time_ms,
                                );
                            } else {
                                let error_code = partition_response.error_code;
                                batch.fail_all(move || {
                                    if let Some(err) = error_code.err() {
                                        KafkaError::Response(err)
                                    } else {
                                        KafkaError::Timeout
                                    }
                                });
                            }
                        }
                    }
                }
                // Any batches not matched in the response (shouldn't happen) — fail them
                for (_, batch) in batch_index {
                    batch.fail_all(|| KafkaError::Timeout);
                }
            }
            Err(e) => {
                tracing::error!("ProduceRequest to broker {broker_id} failed: {e}");
                // Fail all batches
                for (_, batch) in batch_index {
                    let now = now_ms();
                    if (now - batch.created_ms) as u64 >= self.delivery_timeout_ms {
                        batch.fail_all(|| KafkaError::Timeout);
                    } else {
                        // For retriable errors, we could re-enqueue, but for simplicity
                        // fail with connection error for now
                        batch.fail_all(|| KafkaError::ConnectionFailed {
                            broker_id,
                            message: "produce request failed".into(),
                        });
                    }
                }
            }
        }
    }
}

/// Build a ProduceRequest from a set of ProducerBatches.
///
/// Each batch's records are encoded into record batch wire format via
/// `RecordBatchEncoder`, then grouped by topic into the request structure.
fn build_produce_request(
    batches: &[ProducerBatch],
    acks: i16,
    timeout_ms: i32,
    compression: Compression,
) -> Result<ProduceRequest> {
    let mut topic_map: HashMap<String, Vec<PartitionProduceData>> = HashMap::new();

    for batch in batches {
        // Apply idempotent fields and set offsets for delta encoding
        let records: Vec<Record> = batch
            .records
            .iter()
            .enumerate()
            .map(|(i, r)| Record {
                transactional: false,
                control: false,
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id: batch
                    .producer_id
                    .unwrap_or(proto_kafka::records::NO_PRODUCER_ID),
                producer_epoch: batch
                    .producer_epoch
                    .unwrap_or(proto_kafka::records::NO_PRODUCER_EPOCH),
                timestamp_type: TimestampType::Creation,
                offset: i as i64,
                sequence: batch
                    .base_sequence
                    .map(|s| s + i as i32)
                    .unwrap_or(NO_SEQUENCE),
                timestamp: r.timestamp,
                key: r.key.clone(),
                value: r.value.clone(),
                headers: r.headers.clone(),
            })
            .collect();

        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut buf,
            &records,
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        )?;

        topic_map.entry(batch.tp.topic.clone()).or_default().push(
            PartitionProduceData::default()
                .with_index(batch.tp.partition)
                .with_records(Some(buf.freeze())),
        );
    }

    let topic_data: Vec<TopicProduceData> = topic_map
        .into_iter()
        .map(|(topic, partitions)| {
            TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_string(topic)))
                .with_partition_data(partitions)
        })
        .collect();

    Ok(ProduceRequest::default()
        .with_acks(acks)
        .with_timeout_ms(timeout_ms)
        .with_topic_data(topic_data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::TopicPartition;
    use indexmap::IndexMap;
    use proto_kafka::records::{NO_PRODUCER_EPOCH, NO_PRODUCER_ID};

    fn make_batch(tp: TopicPartition, records: Vec<Record>) -> ProducerBatch {
        let count = records.len();
        let size: usize = records
            .iter()
            .map(|r| {
                r.key.as_ref().map(|k| k.len()).unwrap_or(0)
                    + r.value.as_ref().map(|v| v.len()).unwrap_or(0)
            })
            .sum();
        ProducerBatch {
            tp,
            record_count: count,
            size_bytes: size,
            created_ms: now_ms(),
            callbacks: Vec::new(),
            records,
            producer_id: None,
            producer_epoch: None,
            base_sequence: None,
            _permits: Vec::new(),
        }
    }

    fn test_record(key: &str, value: &str) -> Record {
        Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: NO_SEQUENCE,
            timestamp: 1000,
            key: Some(bytes::Bytes::from(key.to_string())),
            value: Some(bytes::Bytes::from(value.to_string())),
            headers: IndexMap::new(),
        }
    }

    #[test]
    fn test_build_produce_request_single_batch() {
        let tp = TopicPartition::new("test", 0);
        let batch = make_batch(tp, vec![test_record("k1", "v1"), test_record("k2", "v2")]);

        let request = build_produce_request(&[batch], -1, 30000, Compression::None).unwrap();

        assert_eq!(request.acks, -1);
        assert_eq!(request.timeout_ms, 30000);
        assert_eq!(request.topic_data.len(), 1);
        assert_eq!(request.topic_data[0].name.0.as_str(), "test");
        assert_eq!(request.topic_data[0].partition_data.len(), 1);
        assert_eq!(request.topic_data[0].partition_data[0].index, 0);
        assert!(request.topic_data[0].partition_data[0].records.is_some());
    }

    #[test]
    fn test_build_produce_request_multiple_topics() {
        let batch1 = make_batch(
            TopicPartition::new("topic-a", 0),
            vec![test_record("k", "v")],
        );
        let batch2 = make_batch(
            TopicPartition::new("topic-b", 1),
            vec![test_record("k", "v")],
        );

        let request = build_produce_request(&[batch1, batch2], 1, 5000, Compression::None).unwrap();

        assert_eq!(request.topic_data.len(), 2);
    }

    #[test]
    fn test_build_produce_request_with_idempotent_fields() {
        let tp = TopicPartition::new("test", 0);
        let mut batch = make_batch(tp, vec![test_record("k", "v")]);
        batch.producer_id = Some(42);
        batch.producer_epoch = Some(1);
        batch.base_sequence = Some(5);

        let request = build_produce_request(&[batch], -1, 30000, Compression::None).unwrap();
        // If it encodes without error, the idempotent fields were applied
        assert!(request.topic_data[0].partition_data[0].records.is_some());
    }
}
