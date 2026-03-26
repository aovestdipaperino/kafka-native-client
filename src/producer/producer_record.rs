use crate::common::RecordHeader;
use bytes::Bytes;

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

    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }
    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }
    pub fn with_timestamp(mut self, ts: i64) -> Self {
        self.timestamp = Some(ts);
        self
    }
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push(RecordHeader::new(key, value));
        self
    }

    pub fn estimated_size(&self) -> usize {
        self.key.as_ref().map(|k| k.len()).unwrap_or(0)
            + self.value.as_ref().map(|v| v.len()).unwrap_or(0)
            + self
                .headers
                .iter()
                .map(|h| h.key.len() + h.value.as_ref().map(|v| v.len()).unwrap_or(0))
                .sum::<usize>()
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

    #[test]
    fn test_record_with_no_key() {
        let record = ProducerRecord::new("topic", "value");
        assert!(record.key.is_none());
        assert_eq!(record.value.as_deref(), Some(b"value".as_slice()));
        assert_eq!(record.topic, "topic");
    }

    #[test]
    fn test_record_with_no_value() {
        // Construct a record with None value explicitly
        let record = ProducerRecord {
            topic: "topic".into(),
            partition: None,
            key: Some(bytes::Bytes::from("key")),
            value: None,
            headers: vec![],
            timestamp: None,
        };
        assert!(record.value.is_none());
        assert_eq!(record.key.as_deref(), Some(b"key".as_slice()));
    }

    #[test]
    fn test_record_with_empty_string_value() {
        let record = ProducerRecord::new("topic", "");
        assert_eq!(record.value.as_deref(), Some(b"".as_slice()));
        assert_eq!(record.estimated_size(), 0); // empty string has len 0
    }

    #[test]
    fn test_estimated_size_with_headers() {
        let record = ProducerRecord::new("topic", "val")
            .with_key("k")
            .with_header("h1", "hv1")
            .with_header("header-two", "value-two");
        // key=1, value=3, header1: key=2 + value=3 = 5, header2: key=10 + value=9 = 19
        assert_eq!(record.estimated_size(), 1 + 3 + (2 + 3) + (10 + 9));
    }

    #[test]
    fn test_estimated_size_no_key_no_value() {
        let record = ProducerRecord {
            topic: "topic".into(),
            partition: None,
            key: None,
            value: None,
            headers: vec![],
            timestamp: None,
        };
        assert_eq!(record.estimated_size(), 0);
    }

    #[test]
    fn test_multiple_headers_accumulate_size() {
        let record = ProducerRecord::new("topic", "")
            .with_header("a", "1")
            .with_header("b", "22")
            .with_header("c", "333");
        // value="" (0), no key (0)
        // header "a"/"1" = 1+1=2, "b"/"22" = 1+2=3, "c"/"333" = 1+3=4 => total 9
        assert_eq!(record.estimated_size(), 2 + 3 + 4);
        assert_eq!(record.headers.len(), 3);
    }

    #[test]
    fn test_builder_pattern_chaining_preserves_all_fields() {
        let record = ProducerRecord::new("my-topic", "payload")
            .with_key("my-key")
            .with_partition(7)
            .with_timestamp(9999)
            .with_header("h1", "v1")
            .with_header("h2", "v2");

        assert_eq!(record.topic, "my-topic");
        assert_eq!(record.key.as_deref(), Some(b"my-key".as_slice()));
        assert_eq!(record.value.as_deref(), Some(b"payload".as_slice()));
        assert_eq!(record.partition, Some(7));
        assert_eq!(record.timestamp, Some(9999));
        assert_eq!(record.headers.len(), 2);
        assert_eq!(record.headers[0].key, "h1");
        assert_eq!(record.headers[0].value.as_deref(), Some(b"v1".as_slice()));
        assert_eq!(record.headers[1].key, "h2");
        assert_eq!(record.headers[1].value.as_deref(), Some(b"v2".as_slice()));
    }
}
