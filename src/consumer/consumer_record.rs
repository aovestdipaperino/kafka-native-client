use crate::common::RecordHeader;
use bytes::Bytes;

/// Timestamp type for consumer records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampType {
    Creation,
    LogAppend,
}

/// A record received from a Kafka partition.
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub timestamp_type: TimestampType,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
    pub leader_epoch: Option<i32>,
    pub serialized_key_size: i32,
    pub serialized_value_size: i32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::RecordHeader;
    use bytes::Bytes;

    /// Java `ConsumerRecord.NULL_SIZE` is -1, indicating null key/value.
    const NULL_SIZE: i32 = -1;

    /// Create a fully-populated ConsumerRecord and verify every field.
    #[test]
    fn test_all_fields_set() {
        let key = Bytes::from("my-key");
        let value = Bytes::from("my-value");
        let headers = vec![
            RecordHeader::new("h1", Bytes::from("v1")),
            RecordHeader::new("h2", Bytes::from("v2")),
        ];

        let record = ConsumerRecord {
            topic: "orders".into(),
            partition: 3,
            offset: 42,
            timestamp: 1_700_000_000,
            timestamp_type: TimestampType::Creation,
            key: Some(key.clone()),
            value: Some(value.clone()),
            headers: headers.clone(),
            leader_epoch: Some(7),
            serialized_key_size: key.len() as i32,
            serialized_value_size: value.len() as i32,
        };

        assert_eq!(record.topic, "orders");
        assert_eq!(record.partition, 3);
        assert_eq!(record.offset, 42);
        assert_eq!(record.timestamp, 1_700_000_000);
        assert_eq!(record.timestamp_type, TimestampType::Creation);
        assert_eq!(record.key.as_ref().unwrap(), &key);
        assert_eq!(record.value.as_ref().unwrap(), &value);
        assert_eq!(record.headers.len(), 2);
        assert_eq!(record.headers[0].key, "h1");
        assert_eq!(record.headers[1].key, "h2");
        assert_eq!(record.leader_epoch, Some(7));
        assert_eq!(record.serialized_key_size, 6);
        assert_eq!(record.serialized_value_size, 8);
    }

    /// A record with None key and None value mirrors Java null key/value behavior.
    #[test]
    fn test_none_key_and_none_value() {
        let record = ConsumerRecord {
            topic: "events".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            timestamp_type: TimestampType::LogAppend,
            key: None,
            value: None,
            headers: vec![],
            leader_epoch: None,
            serialized_key_size: NULL_SIZE,
            serialized_value_size: NULL_SIZE,
        };

        assert!(record.key.is_none());
        assert!(record.value.is_none());
        assert_eq!(record.serialized_key_size, -1);
        assert_eq!(record.serialized_value_size, -1);
        assert_eq!(record.timestamp_type, TimestampType::LogAppend);
    }

    /// A record with an empty headers vec.
    #[test]
    fn test_empty_headers() {
        let record = ConsumerRecord {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            timestamp_type: TimestampType::Creation,
            key: None,
            value: None,
            headers: vec![],
            leader_epoch: None,
            serialized_key_size: NULL_SIZE,
            serialized_value_size: NULL_SIZE,
        };

        assert!(record.headers.is_empty());
    }

    /// Clone must preserve all fields identically.
    #[test]
    fn test_clone_preserves_all_fields() {
        let original = ConsumerRecord {
            topic: "clone-test".into(),
            partition: 5,
            offset: 999,
            timestamp: 123_456_789,
            timestamp_type: TimestampType::LogAppend,
            key: Some(Bytes::from("k")),
            value: Some(Bytes::from("v")),
            headers: vec![RecordHeader::new("hdr", Bytes::from("val"))],
            leader_epoch: Some(2),
            serialized_key_size: 1,
            serialized_value_size: 1,
        };

        let cloned = original.clone();

        assert_eq!(cloned.topic, original.topic);
        assert_eq!(cloned.partition, original.partition);
        assert_eq!(cloned.offset, original.offset);
        assert_eq!(cloned.timestamp, original.timestamp);
        assert_eq!(cloned.timestamp_type, original.timestamp_type);
        assert_eq!(cloned.key, original.key);
        assert_eq!(cloned.value, original.value);
        assert_eq!(cloned.headers.len(), original.headers.len());
        assert_eq!(cloned.headers[0].key, original.headers[0].key);
        assert_eq!(cloned.headers[0].value, original.headers[0].value);
        assert_eq!(cloned.leader_epoch, original.leader_epoch);
        assert_eq!(cloned.serialized_key_size, original.serialized_key_size);
        assert_eq!(cloned.serialized_value_size, original.serialized_value_size);
    }

    /// Serialized sizes of -1 mean null, matching Java `ConsumerRecord.NULL_SIZE`.
    #[test]
    fn test_null_size_semantics() {
        let record = ConsumerRecord {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            timestamp_type: TimestampType::Creation,
            key: None,
            value: None,
            headers: vec![],
            leader_epoch: None,
            serialized_key_size: NULL_SIZE,
            serialized_value_size: NULL_SIZE,
        };

        // In the Java client, NULL_SIZE == -1 signals a null key/value.
        assert_eq!(record.serialized_key_size, -1);
        assert_eq!(record.serialized_value_size, -1);
        assert!(record.key.is_none());
        assert!(record.value.is_none());

        // A record with a present but empty value should have size 0, not -1.
        let record_empty_val = ConsumerRecord {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            timestamp_type: TimestampType::Creation,
            key: None,
            value: Some(Bytes::new()),
            headers: vec![],
            leader_epoch: None,
            serialized_key_size: NULL_SIZE,
            serialized_value_size: 0,
        };
        assert!(record_empty_val.value.is_some());
        assert_eq!(record_empty_val.serialized_value_size, 0);
    }

    /// TimestampType variants are distinct.
    #[test]
    fn test_timestamp_type_variants() {
        assert_ne!(TimestampType::Creation, TimestampType::LogAppend);
        let creation = TimestampType::Creation;
        let creation_copy = creation;
        assert_eq!(creation, creation_copy);
    }
}
