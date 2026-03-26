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
