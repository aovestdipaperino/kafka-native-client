use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use proto_kafka::error::ParseResponseErrorCode;
use proto_kafka::messages::fetch_request::{FetchPartition, FetchTopic};
use proto_kafka::messages::{FetchRequest, TopicName};
use proto_kafka::protocol::StrBytes;
use proto_kafka::records::RecordBatchDecoder;

use crate::common::{RecordHeader, TopicPartition};
use crate::consumer::consumer_record::{ConsumerRecord, TimestampType};
use crate::consumer::consumer_records::ConsumerRecords;
use crate::consumer::subscription_state::SubscriptionState;
use crate::network::connection_manager::ConnectionManager;
use crate::network::metadata::Metadata;

/// Configuration for the Fetcher.
#[derive(Debug, Clone)]
pub struct FetcherConfig {
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub max_partition_fetch_bytes: i32,
    pub max_poll_records: i32,
}

impl Default for FetcherConfig {
    fn default() -> Self {
        Self {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 52_428_800,
            max_partition_fetch_bytes: 1_048_576,
            max_poll_records: 500,
        }
    }
}

/// A completed fetch result for a single partition.
struct CompletedFetch {
    tp: TopicPartition,
    records: Vec<ConsumerRecord>,
    high_watermark: i64,
}

/// Builds FetchRequests, sends them to brokers, and collects responses
/// into ConsumerRecords for the poll loop.
pub struct Fetcher {
    connection_manager: Arc<ConnectionManager>,
    subscription_state: Arc<SubscriptionState>,
    metadata: Arc<Metadata>,
    config: FetcherConfig,
    completed_fetches: Mutex<VecDeque<CompletedFetch>>,
}

impl Fetcher {
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        subscription_state: Arc<SubscriptionState>,
        metadata: Arc<Metadata>,
        config: FetcherConfig,
    ) -> Self {
        Self {
            connection_manager,
            subscription_state,
            metadata,
            config,
            completed_fetches: Mutex::new(VecDeque::new()),
        }
    }

    /// Build and send FetchRequests for all fetchable partitions.
    /// Returns the number of fetch requests sent.
    pub async fn send_fetches(&self) -> usize {
        let fetchable = self.subscription_state.fetchable_partitions();
        if fetchable.is_empty() {
            return 0;
        }

        let cluster = self.metadata.cluster();

        // Group partitions by leader broker
        let mut by_broker: HashMap<i32, Vec<(&TopicPartition, i64)>> = HashMap::new();
        for tp in &fetchable {
            if let Some(leader) = cluster.leader_for(tp)
                && let Some(position) = self.subscription_state.position(tp)
            {
                by_broker.entry(leader.id).or_default().push((tp, position));
            }
        }

        let mut fetch_count = 0;
        for (broker_id, partitions) in by_broker {
            let request = self.build_fetch_request(&partitions);

            // Ensure connection
            if let Some(addr) = self.connection_manager.broker_address(broker_id) {
                let _ = self
                    .connection_manager
                    .ensure_connection(broker_id, &addr)
                    .await;
            }

            match self.connection_manager.send(broker_id, request).await {
                Ok(response) => {
                    self.handle_fetch_response(response);
                    fetch_count += 1;
                }
                Err(e) => {
                    tracing::warn!("Fetch from broker {broker_id} failed: {e}");
                }
            }
        }

        fetch_count
    }

    /// Collect completed fetches into ConsumerRecords, up to max_poll_records.
    pub fn collect_fetch(&self) -> ConsumerRecords {
        let mut completed = self.completed_fetches.lock().unwrap();
        let mut records_by_partition: HashMap<TopicPartition, Vec<ConsumerRecord>> = HashMap::new();
        let mut total = 0;

        while let Some(fetch) = completed.pop_front() {
            for record in fetch.records {
                if total >= self.config.max_poll_records as usize {
                    break;
                }
                // Advance the consumer's position
                self.subscription_state
                    .set_position(&fetch.tp, record.offset + 1);
                records_by_partition
                    .entry(fetch.tp.clone())
                    .or_default()
                    .push(record);
                total += 1;
            }
            if total >= self.config.max_poll_records as usize {
                break;
            }
        }

        ConsumerRecords::new(records_by_partition)
    }

    fn build_fetch_request(&self, partitions: &[(&TopicPartition, i64)]) -> FetchRequest {
        let mut topic_map: HashMap<&str, Vec<FetchPartition>> = HashMap::new();

        for (tp, offset) in partitions {
            topic_map.entry(&tp.topic).or_default().push(
                FetchPartition::default()
                    .with_partition(tp.partition)
                    .with_fetch_offset(*offset)
                    .with_partition_max_bytes(self.config.max_partition_fetch_bytes),
            );
        }

        let topics: Vec<FetchTopic> = topic_map
            .into_iter()
            .map(|(topic, parts)| {
                FetchTopic::default()
                    .with_topic(TopicName(StrBytes::from_string(topic.to_string())))
                    .with_partitions(parts)
            })
            .collect();

        FetchRequest::default()
            .with_max_wait_ms(self.config.max_wait_ms)
            .with_min_bytes(self.config.min_bytes)
            .with_max_bytes(self.config.max_bytes)
            .with_topics(topics)
    }

    fn handle_fetch_response(&self, response: proto_kafka::messages::FetchResponse) {
        let mut completed = self.completed_fetches.lock().unwrap();

        for topic_response in response.responses {
            let topic_name = topic_response.topic.0.to_string();

            for partition_data in topic_response.partitions {
                if partition_data.error_code.is_err() {
                    tracing::warn!(
                        "Fetch error for {}-{}: {:?}",
                        topic_name,
                        partition_data.partition_index,
                        partition_data.error_code.err()
                    );
                    continue;
                }

                let tp = TopicPartition::new(topic_name.clone(), partition_data.partition_index);

                let records = if let Some(mut record_data) = partition_data.records {
                    decode_records(&tp, &mut record_data)
                } else {
                    Vec::new()
                };

                completed.push_back(CompletedFetch {
                    tp,
                    records,
                    high_watermark: partition_data.high_watermark,
                });
            }
        }
    }
}

fn decode_records(tp: &TopicPartition, data: &mut Bytes) -> Vec<ConsumerRecord> {
    let record_set = match RecordBatchDecoder::decode(data) {
        Ok(rs) => rs,
        Err(e) => {
            tracing::error!("Failed to decode records for {tp}: {e}");
            return Vec::new();
        }
    };

    record_set
        .records
        .into_iter()
        .map(|r| {
            let headers: Vec<RecordHeader> = r
                .headers
                .iter()
                .map(
                    |(k, v): (&proto_kafka::protocol::StrBytes, &Option<Bytes>)| RecordHeader {
                        key: k.to_string(),
                        value: v.clone(),
                    },
                )
                .collect();

            let key_size = r.key.as_ref().map(|k: &Bytes| k.len() as i32).unwrap_or(-1);
            let value_size = r
                .value
                .as_ref()
                .map(|v: &Bytes| v.len() as i32)
                .unwrap_or(-1);

            let timestamp_type = match r.timestamp_type {
                proto_kafka::records::TimestampType::Creation => TimestampType::Creation,
                proto_kafka::records::TimestampType::LogAppend => TimestampType::LogAppend,
            };

            ConsumerRecord {
                topic: tp.topic.clone(),
                partition: tp.partition,
                offset: r.offset,
                timestamp: r.timestamp,
                timestamp_type,
                key: r.key,
                value: r.value,
                headers,
                leader_epoch: Some(r.partition_leader_epoch),
                serialized_key_size: key_size,
                serialized_value_size: value_size,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::offset::OffsetResetStrategy;

    #[test]
    fn test_fetcher_config_defaults() {
        let config = FetcherConfig::default();
        assert_eq!(config.max_wait_ms, 500);
        assert_eq!(config.max_poll_records, 500);
    }

    #[test]
    fn test_collect_fetch_empty() {
        let cm = Arc::new(ConnectionManager::new(vec![], String::new(), 5));
        let sub = Arc::new(SubscriptionState::new(OffsetResetStrategy::Latest));
        let meta = Arc::new(Metadata::new(cm.clone(), 300_000));
        let fetcher = Fetcher::new(cm, sub, meta, FetcherConfig::default());

        let records = fetcher.collect_fetch();
        assert!(records.is_empty());
    }

    #[test]
    fn test_decode_records_empty() {
        let tp = TopicPartition::new("test", 0);
        let mut data = Bytes::new();
        let records = decode_records(&tp, &mut data);
        assert!(records.is_empty());
    }
}
