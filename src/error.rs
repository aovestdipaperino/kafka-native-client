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

    #[test]
    fn test_is_retriable_connection_failed() {
        let err = KafkaError::ConnectionFailed {
            broker_id: 1,
            message: "refused".into(),
        };
        assert!(err.is_retriable());
    }

    #[test]
    fn test_buffer_exhausted_is_not_retriable() {
        assert!(!KafkaError::BufferExhausted.is_retriable());
    }

    #[test]
    fn test_producer_fenced_is_not_retriable() {
        assert!(!KafkaError::ProducerFenced.is_retriable());
    }

    #[test]
    fn test_wakeup_exception_is_not_retriable() {
        assert!(!KafkaError::WakeupException.is_retriable());
    }

    #[test]
    fn test_serialization_is_not_retriable() {
        let err = KafkaError::Serialization("bad data".into());
        assert!(!err.is_retriable());
    }

    #[test]
    fn test_config_is_not_retriable() {
        let err = KafkaError::Config("invalid".into());
        assert!(!err.is_retriable());
    }

    #[test]
    fn test_group_coordinator_is_not_retriable() {
        let err = KafkaError::GroupCoordinator("not available".into());
        assert!(!err.is_retriable());
    }

    #[test]
    fn test_record_too_large_is_not_retriable() {
        let err = KafkaError::RecordTooLarge {
            size: 2_000_000,
            limit: 1_048_576,
        };
        assert!(!err.is_retriable());
    }

    #[test]
    fn test_display_config_error() {
        let err = KafkaError::Config("bad config".into());
        let msg = format!("{err}");
        assert_eq!(msg, "config error: bad config");
    }

    #[test]
    fn test_display_timeout() {
        let err = KafkaError::Timeout;
        let msg = format!("{err}");
        assert_eq!(msg, "operation timed out");
    }

    #[test]
    fn test_display_wakeup() {
        let err = KafkaError::WakeupException;
        let msg = format!("{err}");
        assert_eq!(msg, "wakeup");
    }

    #[test]
    fn test_display_buffer_exhausted() {
        let err = KafkaError::BufferExhausted;
        let msg = format!("{err}");
        assert_eq!(msg, "buffer memory exhausted");
    }

    #[test]
    fn test_display_producer_fenced() {
        let err = KafkaError::ProducerFenced;
        let msg = format!("{err}");
        assert_eq!(msg, "producer fenced");
    }

    #[test]
    fn test_display_connection_failed() {
        let err = KafkaError::ConnectionFailed {
            broker_id: 42,
            message: "connection refused".into(),
        };
        let msg = format!("{err}");
        assert!(msg.contains("42"));
        assert!(msg.contains("connection refused"));
    }

    #[test]
    fn test_display_serialization() {
        let err = KafkaError::Serialization("invalid utf-8".into());
        let msg = format!("{err}");
        assert_eq!(msg, "serialization error: invalid utf-8");
    }

    #[test]
    fn test_display_group_coordinator() {
        let err = KafkaError::GroupCoordinator("not available".into());
        let msg = format!("{err}");
        assert_eq!(msg, "group coordinator error: not available");
    }

    #[test]
    fn test_record_too_large_includes_sizes_in_message() {
        let err = KafkaError::RecordTooLarge {
            size: 2_000_000,
            limit: 1_048_576,
        };
        let msg = format!("{err}");
        assert!(msg.contains("2000000"), "should contain the actual size");
        assert!(msg.contains("1048576"), "should contain the limit");
    }

    #[test]
    fn test_from_io_error_preserves_error_kind() {
        let io_err =
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused");
        let kafka_err: KafkaError = io_err.into();
        match kafka_err {
            KafkaError::Io(ref e) => {
                assert_eq!(e.kind(), std::io::ErrorKind::ConnectionRefused);
            }
            _ => panic!("expected Io variant"),
        }
    }
}
