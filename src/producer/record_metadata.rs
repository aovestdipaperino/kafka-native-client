/// Metadata returned after a record is successfully sent.
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub serialized_key_size: i32,
    pub serialized_value_size: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_stores_all_fields() {
        let meta = RecordMetadata {
            topic: "orders".into(),
            partition: 3,
            offset: 42,
            timestamp: 1700000000000,
            serialized_key_size: 10,
            serialized_value_size: 256,
        };
        assert_eq!(meta.topic, "orders");
        assert_eq!(meta.partition, 3);
        assert_eq!(meta.offset, 42);
        assert_eq!(meta.timestamp, 1700000000000);
        assert_eq!(meta.serialized_key_size, 10);
        assert_eq!(meta.serialized_value_size, 256);
    }

    #[test]
    fn test_metadata_with_negative_offset() {
        // In Kafka, offset -1 indicates no offset was assigned (error case
        // or acks=0 where broker doesn't return an offset).
        let meta = RecordMetadata {
            topic: "test".into(),
            partition: 0,
            offset: -1,
            timestamp: -1,
            serialized_key_size: -1,
            serialized_value_size: -1,
        };
        assert_eq!(meta.offset, -1);
        assert_eq!(meta.timestamp, -1);
        assert_eq!(meta.serialized_key_size, -1);
        assert_eq!(meta.serialized_value_size, -1);
    }

    #[test]
    fn test_metadata_clone() {
        let meta = RecordMetadata {
            topic: "events".into(),
            partition: 2,
            offset: 100,
            timestamp: 5000,
            serialized_key_size: 4,
            serialized_value_size: 128,
        };
        let cloned = meta.clone();
        assert_eq!(cloned.topic, meta.topic);
        assert_eq!(cloned.partition, meta.partition);
        assert_eq!(cloned.offset, meta.offset);
        assert_eq!(cloned.timestamp, meta.timestamp);
        assert_eq!(cloned.serialized_key_size, meta.serialized_key_size);
        assert_eq!(cloned.serialized_value_size, meta.serialized_value_size);
    }
}
