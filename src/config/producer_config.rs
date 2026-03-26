use std::collections::HashMap;

use super::common_config::{Acks, CommonConfig};
use crate::{KafkaError, Result};

/// Compression type for produced records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl CompressionType {
    pub fn to_proto_kafka(self) -> proto_kafka::records::Compression {
        match self {
            CompressionType::None => proto_kafka::records::Compression::None,
            CompressionType::Gzip => proto_kafka::records::Compression::Gzip,
            CompressionType::Snappy => proto_kafka::records::Compression::Snappy,
            CompressionType::Lz4 => proto_kafka::records::Compression::Lz4,
            CompressionType::Zstd => proto_kafka::records::Compression::Zstd,
        }
    }

    fn parse(s: &str) -> std::result::Result<Self, String> {
        match s {
            "none" => Ok(Self::None),
            "gzip" => Ok(Self::Gzip),
            "snappy" => Ok(Self::Snappy),
            "lz4" => Ok(Self::Lz4),
            "zstd" => Ok(Self::Zstd),
            other => Err(format!("invalid compression.type: {other}")),
        }
    }
}

/// Configuration for the Kafka producer client.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    pub bootstrap_servers: Vec<String>,
    pub client_id: String,
    pub acks: Acks,
    pub batch_size: usize,
    pub linger_ms: u64,
    pub buffer_memory: usize,
    pub max_block_ms: u64,
    pub max_request_size: usize,
    pub delivery_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub retries: u32,
    pub retry_backoff_ms: u64,
    pub max_in_flight_requests_per_connection: usize,
    pub compression_type: CompressionType,
    pub enable_idempotence: bool,
    pub metadata_max_age_ms: u64,
}

impl ProducerConfig {
    /// Create a new builder.
    pub fn builder() -> ProducerConfigBuilder {
        ProducerConfigBuilder::default()
    }

    /// Return the common subset of configuration.
    pub fn common(&self) -> CommonConfig {
        CommonConfig {
            bootstrap_servers: self.bootstrap_servers.clone(),
            client_id: self.client_id.clone(),
            request_timeout_ms: self.request_timeout_ms,
            metadata_max_age_ms: self.metadata_max_age_ms,
        }
    }
}

/// Builder for [`ProducerConfig`].
#[derive(Default)]
pub struct ProducerConfigBuilder {
    bootstrap_servers: Option<Vec<String>>,
    client_id: Option<String>,
    acks: Option<Acks>,
    batch_size: Option<usize>,
    linger_ms: Option<u64>,
    buffer_memory: Option<usize>,
    max_block_ms: Option<u64>,
    max_request_size: Option<usize>,
    delivery_timeout_ms: Option<u64>,
    request_timeout_ms: Option<u64>,
    retries: Option<u32>,
    retry_backoff_ms: Option<u64>,
    max_in_flight_requests_per_connection: Option<usize>,
    compression_type: Option<CompressionType>,
    enable_idempotence: Option<bool>,
    metadata_max_age_ms: Option<u64>,
}

impl ProducerConfigBuilder {
    pub fn bootstrap_servers(mut self, servers: Vec<String>) -> Self {
        self.bootstrap_servers = Some(servers);
        self
    }

    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = Some(id.into());
        self
    }

    pub fn acks(mut self, acks: Acks) -> Self {
        self.acks = Some(acks);
        self
    }

    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = Some(size);
        self
    }

    pub fn linger_ms(mut self, ms: u64) -> Self {
        self.linger_ms = Some(ms);
        self
    }

    pub fn buffer_memory(mut self, bytes: usize) -> Self {
        self.buffer_memory = Some(bytes);
        self
    }

    pub fn max_block_ms(mut self, ms: u64) -> Self {
        self.max_block_ms = Some(ms);
        self
    }

    pub fn max_request_size(mut self, size: usize) -> Self {
        self.max_request_size = Some(size);
        self
    }

    pub fn delivery_timeout_ms(mut self, ms: u64) -> Self {
        self.delivery_timeout_ms = Some(ms);
        self
    }

    pub fn request_timeout_ms(mut self, ms: u64) -> Self {
        self.request_timeout_ms = Some(ms);
        self
    }

    pub fn retries(mut self, retries: u32) -> Self {
        self.retries = Some(retries);
        self
    }

    pub fn retry_backoff_ms(mut self, ms: u64) -> Self {
        self.retry_backoff_ms = Some(ms);
        self
    }

    pub fn max_in_flight_requests_per_connection(mut self, n: usize) -> Self {
        self.max_in_flight_requests_per_connection = Some(n);
        self
    }

    pub fn compression_type(mut self, ct: CompressionType) -> Self {
        self.compression_type = Some(ct);
        self
    }

    pub fn enable_idempotence(mut self, enable: bool) -> Self {
        self.enable_idempotence = Some(enable);
        self
    }

    pub fn metadata_max_age_ms(mut self, ms: u64) -> Self {
        self.metadata_max_age_ms = Some(ms);
        self
    }

    pub fn build(self) -> Result<ProducerConfig> {
        let bootstrap_servers = self
            .bootstrap_servers
            .ok_or_else(|| KafkaError::Config("bootstrap.servers is required".into()))?;

        if bootstrap_servers.is_empty() {
            return Err(KafkaError::Config(
                "bootstrap.servers must not be empty".into(),
            ));
        }

        let linger_ms = self.linger_ms.unwrap_or(5);
        let request_timeout_ms = self.request_timeout_ms.unwrap_or(30_000);
        let delivery_timeout_ms = self.delivery_timeout_ms.unwrap_or(120_000);

        if delivery_timeout_ms < request_timeout_ms + linger_ms {
            return Err(KafkaError::Config(format!(
                "delivery.timeout.ms ({delivery_timeout_ms}) must be >= request.timeout.ms ({request_timeout_ms}) + linger.ms ({linger_ms})"
            )));
        }

        Ok(ProducerConfig {
            bootstrap_servers,
            client_id: self.client_id.unwrap_or_default(),
            acks: self.acks.unwrap_or(Acks::All),
            batch_size: self.batch_size.unwrap_or(16_384),
            linger_ms,
            buffer_memory: self.buffer_memory.unwrap_or(33_554_432),
            max_block_ms: self.max_block_ms.unwrap_or(60_000),
            max_request_size: self.max_request_size.unwrap_or(1_048_576),
            delivery_timeout_ms,
            request_timeout_ms,
            retries: self.retries.unwrap_or(i32::MAX as u32),
            retry_backoff_ms: self.retry_backoff_ms.unwrap_or(100),
            max_in_flight_requests_per_connection: self
                .max_in_flight_requests_per_connection
                .unwrap_or(5),
            compression_type: self.compression_type.unwrap_or(CompressionType::None),
            enable_idempotence: self.enable_idempotence.unwrap_or(true),
            metadata_max_age_ms: self.metadata_max_age_ms.unwrap_or(300_000),
        })
    }
}

impl TryFrom<HashMap<String, String>> for ProducerConfig {
    type Error = KafkaError;

    fn try_from(props: HashMap<String, String>) -> Result<Self> {
        let mut builder = ProducerConfigBuilder::default();

        for (key, value) in &props {
            match key.as_str() {
                "bootstrap.servers" => {
                    let servers: Vec<String> =
                        value.split(',').map(|s| s.trim().to_owned()).collect();
                    builder = builder.bootstrap_servers(servers);
                }
                "client.id" => {
                    builder = builder.client_id(value.clone());
                }
                "acks" => {
                    let acks = Acks::parse(value).map_err(KafkaError::Config)?;
                    builder = builder.acks(acks);
                }
                "batch.size" => {
                    let v: usize = value
                        .parse()
                        .map_err(|_| KafkaError::Config(format!("invalid batch.size: {value}")))?;
                    builder = builder.batch_size(v);
                }
                "linger.ms" => {
                    let v: u64 = value
                        .parse()
                        .map_err(|_| KafkaError::Config(format!("invalid linger.ms: {value}")))?;
                    builder = builder.linger_ms(v);
                }
                "buffer.memory" => {
                    let v: usize = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid buffer.memory: {value}"))
                    })?;
                    builder = builder.buffer_memory(v);
                }
                "max.block.ms" => {
                    let v: u64 = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid max.block.ms: {value}"))
                    })?;
                    builder = builder.max_block_ms(v);
                }
                "max.request.size" => {
                    let v: usize = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid max.request.size: {value}"))
                    })?;
                    builder = builder.max_request_size(v);
                }
                "delivery.timeout.ms" => {
                    let v: u64 = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid delivery.timeout.ms: {value}"))
                    })?;
                    builder = builder.delivery_timeout_ms(v);
                }
                "request.timeout.ms" => {
                    let v: u64 = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid request.timeout.ms: {value}"))
                    })?;
                    builder = builder.request_timeout_ms(v);
                }
                "retries" => {
                    let v: u32 = value
                        .parse()
                        .map_err(|_| KafkaError::Config(format!("invalid retries: {value}")))?;
                    builder = builder.retries(v);
                }
                "retry.backoff.ms" => {
                    let v: u64 = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid retry.backoff.ms: {value}"))
                    })?;
                    builder = builder.retry_backoff_ms(v);
                }
                "max.in.flight.requests.per.connection" => {
                    let v: usize = value.parse().map_err(|_| {
                        KafkaError::Config(format!(
                            "invalid max.in.flight.requests.per.connection: {value}"
                        ))
                    })?;
                    builder = builder.max_in_flight_requests_per_connection(v);
                }
                "compression.type" => {
                    let ct = CompressionType::parse(value).map_err(KafkaError::Config)?;
                    builder = builder.compression_type(ct);
                }
                "enable.idempotence" => {
                    let v: bool = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid enable.idempotence: {value}"))
                    })?;
                    builder = builder.enable_idempotence(v);
                }
                "metadata.max.age.ms" => {
                    let v: u64 = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid metadata.max.age.ms: {value}"))
                    })?;
                    builder = builder.metadata_max_age_ms(v);
                }
                _ => {} // ignore unknown keys
            }
        }

        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_defaults() {
        let config = ProducerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".into()])
            .build()
            .unwrap();
        assert_eq!(config.acks, Acks::All);
        assert_eq!(config.batch_size, 16384);
        assert_eq!(config.linger_ms, 5);
        assert_eq!(config.buffer_memory, 33_554_432);
        assert_eq!(config.max_request_size, 1_048_576);
        assert!(config.enable_idempotence);
    }

    #[test]
    fn test_builder_missing_bootstrap() {
        let result = ProducerConfig::builder().build();
        assert!(result.is_err());
    }

    #[test]
    fn test_from_hashmap() {
        let mut props = std::collections::HashMap::new();
        props.insert("bootstrap.servers".into(), "host1:9092,host2:9092".into());
        props.insert("acks".into(), "1".into());
        props.insert("batch.size".into(), "32768".into());
        let config = ProducerConfig::try_from(props).unwrap();
        assert_eq!(config.acks, Acks::One);
        assert_eq!(config.batch_size, 32768);
        assert_eq!(config.bootstrap_servers, vec!["host1:9092", "host2:9092"]);
    }

    #[test]
    fn test_delivery_timeout_validation() {
        let result = ProducerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".into()])
            .delivery_timeout_ms(100)
            .request_timeout_ms(50)
            .linger_ms(60)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_all_builder_setters() {
        let config = ProducerConfig::builder()
            .bootstrap_servers(vec!["a:9092".into(), "b:9092".into()])
            .client_id("my-producer")
            .acks(Acks::One)
            .batch_size(8192)
            .linger_ms(10)
            .buffer_memory(1_000_000)
            .max_block_ms(5000)
            .max_request_size(512_000)
            .delivery_timeout_ms(60_000)
            .request_timeout_ms(15_000)
            .retries(3)
            .retry_backoff_ms(200)
            .max_in_flight_requests_per_connection(1)
            .compression_type(CompressionType::Zstd)
            .enable_idempotence(false)
            .metadata_max_age_ms(60_000)
            .build()
            .unwrap();
        assert_eq!(config.bootstrap_servers, vec!["a:9092", "b:9092"]);
        assert_eq!(config.client_id, "my-producer");
        assert_eq!(config.acks, Acks::One);
        assert_eq!(config.batch_size, 8192);
        assert_eq!(config.linger_ms, 10);
        assert_eq!(config.buffer_memory, 1_000_000);
        assert_eq!(config.max_block_ms, 5000);
        assert_eq!(config.max_request_size, 512_000);
        assert_eq!(config.delivery_timeout_ms, 60_000);
        assert_eq!(config.request_timeout_ms, 15_000);
        assert_eq!(config.retries, 3);
        assert_eq!(config.retry_backoff_ms, 200);
        assert_eq!(config.max_in_flight_requests_per_connection, 1);
        assert_eq!(config.compression_type, CompressionType::Zstd);
        assert!(!config.enable_idempotence);
        assert_eq!(config.metadata_max_age_ms, 60_000);
    }

    #[test]
    fn test_compression_type_to_proto_kafka() {
        assert!(matches!(
            CompressionType::None.to_proto_kafka(),
            proto_kafka::records::Compression::None
        ));
        assert!(matches!(
            CompressionType::Gzip.to_proto_kafka(),
            proto_kafka::records::Compression::Gzip
        ));
        assert!(matches!(
            CompressionType::Snappy.to_proto_kafka(),
            proto_kafka::records::Compression::Snappy
        ));
        assert!(matches!(
            CompressionType::Lz4.to_proto_kafka(),
            proto_kafka::records::Compression::Lz4
        ));
        assert!(matches!(
            CompressionType::Zstd.to_proto_kafka(),
            proto_kafka::records::Compression::Zstd
        ));
    }

    #[test]
    fn test_compression_type_parse_all_valid() {
        assert_eq!(
            CompressionType::parse("none").unwrap(),
            CompressionType::None
        );
        assert_eq!(
            CompressionType::parse("gzip").unwrap(),
            CompressionType::Gzip
        );
        assert_eq!(
            CompressionType::parse("snappy").unwrap(),
            CompressionType::Snappy
        );
        assert_eq!(CompressionType::parse("lz4").unwrap(), CompressionType::Lz4);
        assert_eq!(
            CompressionType::parse("zstd").unwrap(),
            CompressionType::Zstd
        );
    }

    #[test]
    fn test_compression_type_parse_invalid() {
        let result = CompressionType::parse("brotli");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid compression.type"));
    }

    #[test]
    fn test_empty_bootstrap_servers_returns_error() {
        let result = ProducerConfig::builder().bootstrap_servers(vec![]).build();
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("must not be empty"));
    }

    #[test]
    fn test_hashmap_conversion_all_known_keys() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".into(), "h1:9092,h2:9092".into());
        props.insert("client.id".into(), "test-client".into());
        props.insert("acks".into(), "all".into());
        props.insert("batch.size".into(), "32768".into());
        props.insert("linger.ms".into(), "10".into());
        props.insert("buffer.memory".into(), "67108864".into());
        props.insert("max.block.ms".into(), "120000".into());
        props.insert("max.request.size".into(), "2097152".into());
        props.insert("delivery.timeout.ms".into(), "240000".into());
        props.insert("request.timeout.ms".into(), "60000".into());
        props.insert("retries".into(), "5".into());
        props.insert("retry.backoff.ms".into(), "500".into());
        props.insert("max.in.flight.requests.per.connection".into(), "3".into());
        props.insert("compression.type".into(), "lz4".into());
        props.insert("enable.idempotence".into(), "false".into());
        props.insert("metadata.max.age.ms".into(), "60000".into());
        let config = ProducerConfig::try_from(props).unwrap();
        assert_eq!(config.bootstrap_servers, vec!["h1:9092", "h2:9092"]);
        assert_eq!(config.client_id, "test-client");
        assert_eq!(config.acks, Acks::All);
        assert_eq!(config.batch_size, 32768);
        assert_eq!(config.linger_ms, 10);
        assert_eq!(config.buffer_memory, 67_108_864);
        assert_eq!(config.max_block_ms, 120_000);
        assert_eq!(config.max_request_size, 2_097_152);
        assert_eq!(config.delivery_timeout_ms, 240_000);
        assert_eq!(config.request_timeout_ms, 60_000);
        assert_eq!(config.retries, 5);
        assert_eq!(config.retry_backoff_ms, 500);
        assert_eq!(config.max_in_flight_requests_per_connection, 3);
        assert_eq!(config.compression_type, CompressionType::Lz4);
        assert!(!config.enable_idempotence);
        assert_eq!(config.metadata_max_age_ms, 60_000);
    }

    #[test]
    fn test_hashmap_with_unknown_keys_are_ignored() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".into(), "localhost:9092".into());
        props.insert("some.unknown.key".into(), "whatever".into());
        props.insert("another.unknown".into(), "value".into());
        let config = ProducerConfig::try_from(props).unwrap();
        assert_eq!(config.bootstrap_servers, vec!["localhost:9092"]);
        // Defaults should still apply for non-overridden fields
        assert_eq!(config.acks, Acks::All);
    }

    #[test]
    fn test_delivery_timeout_exactly_equal_to_request_plus_linger() {
        // delivery_timeout_ms == request_timeout_ms + linger_ms should succeed
        let config = ProducerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".into()])
            .delivery_timeout_ms(130)
            .request_timeout_ms(100)
            .linger_ms(30)
            .build();
        assert!(config.is_ok());
        let c = config.unwrap();
        assert_eq!(c.delivery_timeout_ms, 130);
        assert_eq!(c.request_timeout_ms, 100);
        assert_eq!(c.linger_ms, 30);
    }

    #[test]
    fn test_default_enable_idempotence_is_true() {
        let config = ProducerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".into()])
            .build()
            .unwrap();
        assert!(config.enable_idempotence);
    }
}
