use bytes::Bytes;
use crate::common::RecordHeader;

/// A record to be sent to Kafka.
pub struct ProducerRecord {
    pub topic: String,
    pub partition: Option<i32>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
    pub timestamp: Option<i64>,
}

impl ProducerRecord {
    pub fn new(topic: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            topic: topic.into(),
            partition: None,
            key: None,
            value: Some(value.into()),
            headers: vec![],
            timestamp: None,
        }
    }

    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self { self.key = Some(key.into()); self }
    pub fn with_partition(mut self, partition: i32) -> Self { self.partition = Some(partition); self }
    pub fn with_timestamp(mut self, ts: i64) -> Self { self.timestamp = Some(ts); self }
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push(RecordHeader::new(key, value));
        self
    }

    pub fn estimated_size(&self) -> usize {
        self.key.as_ref().map(|k| k.len()).unwrap_or(0)
            + self.value.as_ref().map(|v| v.len()).unwrap_or(0)
            + self.headers.iter().map(|h| h.key.len() + h.value.as_ref().map(|v| v.len()).unwrap_or(0)).sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_record_builder() {
        let record = ProducerRecord::new("test", "hello")
            .with_key("key1")
            .with_partition(3)
            .with_timestamp(12345)
            .with_header("h1", "v1");
        assert_eq!(record.topic, "test");
        assert_eq!(record.partition, Some(3));
        assert_eq!(record.timestamp, Some(12345));
        assert_eq!(record.headers.len(), 1);
    }

    #[test]
    fn test_estimated_size() {
        let record = ProducerRecord::new("test", "hello").with_key("key");
        assert_eq!(record.estimated_size(), 8); // 3 (key) + 5 (value)
    }
}
