use bytes::Bytes;

/// A key-value header attached to a Kafka record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    pub key: String,
    pub value: Option<Bytes>,
}

impl RecordHeader {
    pub fn new(key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            value: Some(value.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_with_key_and_value() {
        let header = RecordHeader::new("content-type", "application/json");
        assert_eq!(header.key, "content-type");
        assert_eq!(header.value, Some(Bytes::from("application/json")));
    }

    #[test]
    fn test_header_with_empty_key() {
        let header = RecordHeader::new("", "some-value");
        assert_eq!(header.key, "");
        assert_eq!(header.value, Some(Bytes::from("some-value")));
    }

    #[test]
    fn test_header_with_none_value() {
        let header = RecordHeader {
            key: "tombstone-header".to_string(),
            value: None,
        };
        assert_eq!(header.key, "tombstone-header");
        assert!(header.value.is_none());
    }

    #[test]
    fn test_header_equality_same() {
        let h1 = RecordHeader::new("key", "value");
        let h2 = RecordHeader::new("key", "value");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_header_not_equal_different_key() {
        let h1 = RecordHeader::new("key-a", "value");
        let h2 = RecordHeader::new("key-b", "value");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_header_not_equal_different_value() {
        let h1 = RecordHeader::new("key", "value-a");
        let h2 = RecordHeader::new("key", "value-b");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_header_not_equal_some_vs_none() {
        let h1 = RecordHeader::new("key", "value");
        let h2 = RecordHeader {
            key: "key".to_string(),
            value: None,
        };
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_header_equality_both_none_value() {
        let h1 = RecordHeader {
            key: "key".to_string(),
            value: None,
        };
        let h2 = RecordHeader {
            key: "key".to_string(),
            value: None,
        };
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_header_clone() {
        let original = RecordHeader::new("trace-id", "abc123");
        let cloned = original.clone();
        assert_eq!(original, cloned);
        assert_eq!(cloned.key, "trace-id");
        assert_eq!(cloned.value, Some(Bytes::from("abc123")));
    }

    #[test]
    fn test_header_with_empty_value() {
        let header = RecordHeader::new("empty", "");
        assert_eq!(header.key, "empty");
        assert_eq!(header.value, Some(Bytes::from("")));
        // Empty value is not None
        assert!(header.value.is_some());
    }

    #[test]
    fn test_header_with_binary_value() {
        let binary_data: Vec<u8> = vec![0x00, 0xFF, 0x42, 0x01];
        let header = RecordHeader::new("binary-header", Bytes::from(binary_data.clone()));
        assert_eq!(header.key, "binary-header");
        assert_eq!(header.value.unwrap().as_ref(), &binary_data[..]);
    }
}
