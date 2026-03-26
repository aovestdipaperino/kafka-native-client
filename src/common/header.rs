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
