/// All errors produced by the kafka-native-client.
#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    /// Wire-level Kafka error from broker response.
    #[error("kafka response error: {0}")]
    Response(proto_kafka::ResponseError),

    /// Protocol encoding/decoding failure.
    #[error("protocol error: {0}")]
    Protocol(proto_kafka::ProtoError),

    /// Network I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Invalid configuration.
    #[error("config error: {0}")]
    Config(String),

    /// Serialization/deserialization failure.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Operation timed out.
    #[error("operation timed out")]
    Timeout,

    /// Consumer wakeup signal.
    #[error("wakeup")]
    WakeupException,

    /// Record too large for configured limits.
    #[error("record too large: {size} bytes exceeds limit of {limit} bytes")]
    RecordTooLarge { size: usize, limit: usize },

    /// Producer accumulator memory exhausted.
    #[error("buffer memory exhausted")]
    BufferExhausted,

    /// Broker connection failure.
    #[error("connection to broker {broker_id} failed: {message}")]
    ConnectionFailed { broker_id: i32, message: String },

    /// Group coordinator error.
    #[error("group coordinator error: {0}")]
    GroupCoordinator(String),

    /// Producer fenced (idempotent producer epoch conflict).
    #[error("producer fenced")]
    ProducerFenced,
}

impl KafkaError {
    /// Returns true if this error is retriable.
    pub fn is_retriable(&self) -> bool {
        match self {
            KafkaError::Response(e) => e.is_retriable(),
            KafkaError::Timeout => true,
            KafkaError::ConnectionFailed { .. } => true,
            _ => false,
        }
    }
}

impl From<proto_kafka::ResponseError> for KafkaError {
    fn from(e: proto_kafka::ResponseError) -> Self {
        KafkaError::Response(e)
    }
}

impl From<proto_kafka::ProtoError> for KafkaError {
    fn from(e: proto_kafka::ProtoError) -> Self {
        KafkaError::Protocol(e)
    }
}

/// Result alias for kafka-native-client operations.
pub type Result<T> = std::result::Result<T, KafkaError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let kafka_err: KafkaError = io_err.into();
        assert!(matches!(kafka_err, KafkaError::Io(_)));
    }

    #[test]
    fn test_from_response_error() {
        let resp_err = proto_kafka::ResponseError::UnknownTopicOrPartition;
        let kafka_err: KafkaError = resp_err.into();
        assert!(matches!(kafka_err, KafkaError::Response(_)));
        assert!(kafka_err.is_retriable());
    }

    #[test]
    fn test_from_proto_error() {
        let proto_err = proto_kafka::ProtoError::NotEnoughBytes;
        let kafka_err: KafkaError = proto_err.into();
        assert!(matches!(kafka_err, KafkaError::Protocol(_)));
    }

    #[test]
    fn test_non_retriable() {
        let err = KafkaError::Config("bad".into());
        assert!(!err.is_retriable());
    }

    #[test]
    fn test_timeout_is_retriable() {
        assert!(KafkaError::Timeout.is_retriable());
    }
}
