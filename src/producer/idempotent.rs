use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use proto_kafka::error::ParseResponseErrorCode;
use proto_kafka::messages::{InitProducerIdRequest, ProducerId};

use crate::common::TopicPartition;
use crate::error::{KafkaError, Result};
use crate::network::connection_manager::ConnectionManager;

/// Manages producer ID, epoch, and per-partition sequence numbers
/// for idempotent producing.
pub struct ProducerIdManager {
    producer_id: RwLock<i64>,
    producer_epoch: RwLock<i16>,
    sequence_numbers: Mutex<HashMap<TopicPartition, i32>>,
    connection_manager: Arc<ConnectionManager>,
}

impl ProducerIdManager {
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            producer_id: RwLock::new(-1),
            producer_epoch: RwLock::new(-1),
            sequence_numbers: Mutex::new(HashMap::new()),
            connection_manager,
        }
    }

    /// Send InitProducerIdRequest to obtain a producer ID and epoch.
    pub async fn init(&self) -> Result<()> {
        let request = InitProducerIdRequest::default()
            .with_transactional_id(None)
            .with_transaction_timeout_ms(60_000)
            .with_producer_id(ProducerId(-1))
            .with_producer_epoch(-1);

        let response = self.connection_manager.send(-1, request).await?;

        if response.error_code.is_err() {
            return Err(KafkaError::Response(response.error_code.err().unwrap()));
        }

        *self.producer_id.write().unwrap() = response.producer_id.0;
        *self.producer_epoch.write().unwrap() = response.producer_epoch;

        Ok(())
    }

    /// Get the next sequence number for a partition and advance by `record_count`.
    pub fn next_sequence(&self, tp: &TopicPartition, record_count: i32) -> i32 {
        let mut sequences = self.sequence_numbers.lock().unwrap();
        let seq = sequences.entry(tp.clone()).or_insert(0);
        let current = *seq;
        *seq += record_count;
        current
    }

    pub fn producer_id(&self) -> i64 {
        *self.producer_id.read().unwrap()
    }

    pub fn producer_epoch(&self) -> i16 {
        *self.producer_epoch.read().unwrap()
    }

    /// Reset the sequence number for a partition (e.g., after an OutOfOrderSequence error).
    pub fn reset_sequence(&self, tp: &TopicPartition) {
        self.sequence_numbers.lock().unwrap().remove(tp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence_numbers_increment() {
        let cm = Arc::new(ConnectionManager::new(vec![], String::new(), 5));
        let mgr = ProducerIdManager::new(cm);
        let tp = TopicPartition::new("test", 0);

        assert_eq!(mgr.next_sequence(&tp, 3), 0);
        assert_eq!(mgr.next_sequence(&tp, 2), 3);
        assert_eq!(mgr.next_sequence(&tp, 1), 5);
    }

    #[test]
    fn test_sequence_numbers_per_partition() {
        let cm = Arc::new(ConnectionManager::new(vec![], String::new(), 5));
        let mgr = ProducerIdManager::new(cm);
        let tp0 = TopicPartition::new("test", 0);
        let tp1 = TopicPartition::new("test", 1);

        assert_eq!(mgr.next_sequence(&tp0, 5), 0);
        assert_eq!(mgr.next_sequence(&tp1, 3), 0);
        assert_eq!(mgr.next_sequence(&tp0, 2), 5);
        assert_eq!(mgr.next_sequence(&tp1, 1), 3);
    }

    #[test]
    fn test_reset_sequence() {
        let cm = Arc::new(ConnectionManager::new(vec![], String::new(), 5));
        let mgr = ProducerIdManager::new(cm);
        let tp = TopicPartition::new("test", 0);

        mgr.next_sequence(&tp, 5);
        mgr.reset_sequence(&tp);
        assert_eq!(mgr.next_sequence(&tp, 1), 0);
    }

    #[test]
    fn test_initial_state() {
        let cm = Arc::new(ConnectionManager::new(vec![], String::new(), 5));
        let mgr = ProducerIdManager::new(cm);
        assert_eq!(mgr.producer_id(), -1);
        assert_eq!(mgr.producer_epoch(), -1);
    }
}
