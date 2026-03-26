use std::collections::HashMap;

use super::common_config::CommonConfig;
use crate::{KafkaError, Result};

/// Strategy for resetting the consumer offset when no committed offset exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetResetStrategy {
    Latest,
    Earliest,
    None,
}

impl OffsetResetStrategy {
    pub fn parse(s: &str) -> std::result::Result<Self, String> {
        match s {
            "latest" => Ok(Self::Latest),
            "earliest" => Ok(Self::Earliest),
            "none" => Ok(Self::None),
            other => Err(format!("invalid auto.offset.reset: {other}")),
        }
    }
}

/// Configuration for the Kafka consumer client.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub bootstrap_servers: Vec<String>,
    pub group_id: Option<String>,
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub auto_offset_reset: OffsetResetStrategy,
    pub enable_auto_commit: bool,
    pub auto_commit_interval_ms: u64,
    pub fetch_min_bytes: i32,
    pub fetch_max_bytes: i32,
    pub fetch_max_wait_ms: i32,
    pub max_poll_records: i32,
    pub max_partition_fetch_bytes: i32,
    pub session_timeout_ms: i32,
    pub heartbeat_interval_ms: i32,
    pub request_timeout_ms: u64,
    pub metadata_max_age_ms: u64,
}

impl ConsumerConfig {
    /// Create a new builder.
    pub fn builder() -> ConsumerConfigBuilder {
        ConsumerConfigBuilder::default()
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

/// Builder for [`ConsumerConfig`].
#[derive(Default)]
pub struct ConsumerConfigBuilder {
    bootstrap_servers: Option<Vec<String>>,
    group_id: Option<String>,
    group_instance_id: Option<String>,
    client_id: Option<String>,
    auto_offset_reset: Option<OffsetResetStrategy>,
    enable_auto_commit: Option<bool>,
    auto_commit_interval_ms: Option<u64>,
    fetch_min_bytes: Option<i32>,
    fetch_max_bytes: Option<i32>,
    fetch_max_wait_ms: Option<i32>,
    max_poll_records: Option<i32>,
    max_partition_fetch_bytes: Option<i32>,
    session_timeout_ms: Option<i32>,
    heartbeat_interval_ms: Option<i32>,
    request_timeout_ms: Option<u64>,
    metadata_max_age_ms: Option<u64>,
}

impl ConsumerConfigBuilder {
    pub fn bootstrap_servers(mut self, servers: Vec<String>) -> Self {
        self.bootstrap_servers = Some(servers);
        self
    }

    pub fn group_id(mut self, id: impl Into<String>) -> Self {
        self.group_id = Some(id.into());
        self
    }

    pub fn group_instance_id(mut self, id: impl Into<String>) -> Self {
        self.group_instance_id = Some(id.into());
        self
    }

    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = Some(id.into());
        self
    }

    pub fn auto_offset_reset(mut self, strategy: OffsetResetStrategy) -> Self {
        self.auto_offset_reset = Some(strategy);
        self
    }

    pub fn enable_auto_commit(mut self, enable: bool) -> Self {
        self.enable_auto_commit = Some(enable);
        self
    }

    pub fn auto_commit_interval_ms(mut self, ms: u64) -> Self {
        self.auto_commit_interval_ms = Some(ms);
        self
    }

    pub fn fetch_min_bytes(mut self, bytes: i32) -> Self {
        self.fetch_min_bytes = Some(bytes);
        self
    }

    pub fn fetch_max_bytes(mut self, bytes: i32) -> Self {
        self.fetch_max_bytes = Some(bytes);
        self
    }

    pub fn fetch_max_wait_ms(mut self, ms: i32) -> Self {
        self.fetch_max_wait_ms = Some(ms);
        self
    }

    pub fn max_poll_records(mut self, n: i32) -> Self {
        self.max_poll_records = Some(n);
        self
    }

    pub fn max_partition_fetch_bytes(mut self, bytes: i32) -> Self {
        self.max_partition_fetch_bytes = Some(bytes);
        self
    }

    pub fn session_timeout_ms(mut self, ms: i32) -> Self {
        self.session_timeout_ms = Some(ms);
        self
    }

    pub fn heartbeat_interval_ms(mut self, ms: i32) -> Self {
        self.heartbeat_interval_ms = Some(ms);
        self
    }

    pub fn request_timeout_ms(mut self, ms: u64) -> Self {
        self.request_timeout_ms = Some(ms);
        self
    }

    pub fn metadata_max_age_ms(mut self, ms: u64) -> Self {
        self.metadata_max_age_ms = Some(ms);
        self
    }

    pub fn build(self) -> Result<ConsumerConfig> {
        let bootstrap_servers = self
            .bootstrap_servers
            .ok_or_else(|| KafkaError::Config("bootstrap.servers is required".into()))?;

        if bootstrap_servers.is_empty() {
            return Err(KafkaError::Config(
                "bootstrap.servers must not be empty".into(),
            ));
        }

        let enable_auto_commit = self.enable_auto_commit.unwrap_or(true);

        if enable_auto_commit && self.group_id.is_none() {
            return Err(KafkaError::Config(
                "group.id is required when enable.auto.commit is true".into(),
            ));
        }

        Ok(ConsumerConfig {
            bootstrap_servers,
            group_id: self.group_id,
            group_instance_id: self.group_instance_id,
            client_id: self.client_id.unwrap_or_default(),
            auto_offset_reset: self
                .auto_offset_reset
                .unwrap_or(OffsetResetStrategy::Latest),
            enable_auto_commit,
            auto_commit_interval_ms: self.auto_commit_interval_ms.unwrap_or(5_000),
            fetch_min_bytes: self.fetch_min_bytes.unwrap_or(1),
            fetch_max_bytes: self.fetch_max_bytes.unwrap_or(52_428_800),
            fetch_max_wait_ms: self.fetch_max_wait_ms.unwrap_or(500),
            max_poll_records: self.max_poll_records.unwrap_or(500),
            max_partition_fetch_bytes: self.max_partition_fetch_bytes.unwrap_or(1_048_576),
            session_timeout_ms: self.session_timeout_ms.unwrap_or(45_000),
            heartbeat_interval_ms: self.heartbeat_interval_ms.unwrap_or(3_000),
            request_timeout_ms: self.request_timeout_ms.unwrap_or(30_000),
            metadata_max_age_ms: self.metadata_max_age_ms.unwrap_or(300_000),
        })
    }
}

impl TryFrom<HashMap<String, String>> for ConsumerConfig {
    type Error = KafkaError;

    fn try_from(props: HashMap<String, String>) -> Result<Self> {
        let mut builder = ConsumerConfigBuilder::default();

        for (key, value) in &props {
            match key.as_str() {
                "bootstrap.servers" => {
                    let servers: Vec<String> =
                        value.split(',').map(|s| s.trim().to_owned()).collect();
                    builder = builder.bootstrap_servers(servers);
                }
                "group.id" => {
                    builder = builder.group_id(value.clone());
                }
                "group.instance.id" => {
                    builder = builder.group_instance_id(value.clone());
                }
                "client.id" => {
                    builder = builder.client_id(value.clone());
                }
                "auto.offset.reset" => {
                    let strategy = OffsetResetStrategy::parse(value).map_err(KafkaError::Config)?;
                    builder = builder.auto_offset_reset(strategy);
                }
                "enable.auto.commit" => {
                    let v: bool = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid enable.auto.commit: {value}"))
                    })?;
                    builder = builder.enable_auto_commit(v);
                }
                "auto.commit.interval.ms" => {
                    let v: u64 = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid auto.commit.interval.ms: {value}"))
                    })?;
                    builder = builder.auto_commit_interval_ms(v);
                }
                "fetch.min.bytes" => {
                    let v: i32 = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid fetch.min.bytes: {value}"))
                    })?;
                    builder = builder.fetch_min_bytes(v);
                }
                "fetch.max.bytes" => {
                    let v: i32 = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid fetch.max.bytes: {value}"))
                    })?;
                    builder = builder.fetch_max_bytes(v);
                }
                "fetch.max.wait.ms" => {
                    let v: i32 = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid fetch.max.wait.ms: {value}"))
                    })?;
                    builder = builder.fetch_max_wait_ms(v);
                }
                "max.poll.records" => {
                    let v: i32 = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid max.poll.records: {value}"))
                    })?;
                    builder = builder.max_poll_records(v);
                }
                "max.partition.fetch.bytes" => {
                    let v: i32 = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid max.partition.fetch.bytes: {value}"))
                    })?;
                    builder = builder.max_partition_fetch_bytes(v);
                }
                "session.timeout.ms" => {
                    let v: i32 = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid session.timeout.ms: {value}"))
                    })?;
                    builder = builder.session_timeout_ms(v);
                }
                "heartbeat.interval.ms" => {
                    let v: i32 = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid heartbeat.interval.ms: {value}"))
                    })?;
                    builder = builder.heartbeat_interval_ms(v);
                }
                "request.timeout.ms" => {
                    let v: u64 = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid request.timeout.ms: {value}"))
                    })?;
                    builder = builder.request_timeout_ms(v);
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
    fn test_consumer_defaults() {
        let config = ConsumerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".into()])
            .group_id("test-group")
            .build()
            .unwrap();
        assert_eq!(config.auto_offset_reset, OffsetResetStrategy::Latest);
        assert!(config.enable_auto_commit);
        assert_eq!(config.max_poll_records, 500);
    }

    #[test]
    fn test_auto_commit_requires_group_id() {
        let result = ConsumerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".into()])
            .enable_auto_commit(true)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_consumer_from_hashmap() {
        let mut props = std::collections::HashMap::new();
        props.insert("bootstrap.servers".into(), "localhost:9092".into());
        props.insert("group.id".into(), "my-group".into());
        props.insert("auto.offset.reset".into(), "earliest".into());
        let config = ConsumerConfig::try_from(props).unwrap();
        assert_eq!(config.auto_offset_reset, OffsetResetStrategy::Earliest);
        assert_eq!(config.group_id, Some("my-group".into()));
    }

    #[test]
    fn test_all_builder_setters() {
        let config = ConsumerConfig::builder()
            .bootstrap_servers(vec!["a:9092".into(), "b:9092".into()])
            .group_id("my-group")
            .group_instance_id("instance-1")
            .client_id("my-consumer")
            .auto_offset_reset(OffsetResetStrategy::Earliest)
            .enable_auto_commit(true)
            .auto_commit_interval_ms(10_000)
            .fetch_min_bytes(512)
            .fetch_max_bytes(10_000_000)
            .fetch_max_wait_ms(1000)
            .max_poll_records(100)
            .max_partition_fetch_bytes(512_000)
            .session_timeout_ms(30_000)
            .heartbeat_interval_ms(5_000)
            .request_timeout_ms(20_000)
            .metadata_max_age_ms(60_000)
            .build()
            .unwrap();
        assert_eq!(config.bootstrap_servers, vec!["a:9092", "b:9092"]);
        assert_eq!(config.group_id, Some("my-group".into()));
        assert_eq!(config.group_instance_id, Some("instance-1".into()));
        assert_eq!(config.client_id, "my-consumer");
        assert_eq!(config.auto_offset_reset, OffsetResetStrategy::Earliest);
        assert!(config.enable_auto_commit);
        assert_eq!(config.auto_commit_interval_ms, 10_000);
        assert_eq!(config.fetch_min_bytes, 512);
        assert_eq!(config.fetch_max_bytes, 10_000_000);
        assert_eq!(config.fetch_max_wait_ms, 1000);
        assert_eq!(config.max_poll_records, 100);
        assert_eq!(config.max_partition_fetch_bytes, 512_000);
        assert_eq!(config.session_timeout_ms, 30_000);
        assert_eq!(config.heartbeat_interval_ms, 5_000);
        assert_eq!(config.request_timeout_ms, 20_000);
        assert_eq!(config.metadata_max_age_ms, 60_000);
    }

    #[test]
    fn test_offset_reset_strategy_parse_all_variants() {
        assert_eq!(
            OffsetResetStrategy::parse("latest").unwrap(),
            OffsetResetStrategy::Latest
        );
        assert_eq!(
            OffsetResetStrategy::parse("earliest").unwrap(),
            OffsetResetStrategy::Earliest
        );
        assert_eq!(
            OffsetResetStrategy::parse("none").unwrap(),
            OffsetResetStrategy::None
        );
    }

    #[test]
    fn test_offset_reset_strategy_parse_invalid() {
        let result = OffsetResetStrategy::parse("beginning");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid auto.offset.reset"));
    }

    #[test]
    fn test_default_auto_offset_reset_is_latest() {
        let config = ConsumerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".into()])
            .group_id("g")
            .build()
            .unwrap();
        assert_eq!(config.auto_offset_reset, OffsetResetStrategy::Latest);
    }

    #[test]
    fn test_default_enable_auto_commit_is_true() {
        let config = ConsumerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".into()])
            .group_id("g")
            .build()
            .unwrap();
        assert!(config.enable_auto_commit);
    }

    #[test]
    fn test_hashmap_conversion_all_known_keys() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".into(), "h1:9092,h2:9092".into());
        props.insert("group.id".into(), "test-group".into());
        props.insert("group.instance.id".into(), "inst-1".into());
        props.insert("client.id".into(), "test-client".into());
        props.insert("auto.offset.reset".into(), "none".into());
        props.insert("enable.auto.commit".into(), "false".into());
        props.insert("auto.commit.interval.ms".into(), "10000".into());
        props.insert("fetch.min.bytes".into(), "256".into());
        props.insert("fetch.max.bytes".into(), "1000000".into());
        props.insert("fetch.max.wait.ms".into(), "250".into());
        props.insert("max.poll.records".into(), "50".into());
        props.insert("max.partition.fetch.bytes".into(), "524288".into());
        props.insert("session.timeout.ms".into(), "20000".into());
        props.insert("heartbeat.interval.ms".into(), "2000".into());
        props.insert("request.timeout.ms".into(), "15000".into());
        props.insert("metadata.max.age.ms".into(), "120000".into());
        let config = ConsumerConfig::try_from(props).unwrap();
        assert_eq!(config.bootstrap_servers, vec!["h1:9092", "h2:9092"]);
        assert_eq!(config.group_id, Some("test-group".into()));
        assert_eq!(config.group_instance_id, Some("inst-1".into()));
        assert_eq!(config.client_id, "test-client");
        assert_eq!(config.auto_offset_reset, OffsetResetStrategy::None);
        assert!(!config.enable_auto_commit);
        assert_eq!(config.auto_commit_interval_ms, 10_000);
        assert_eq!(config.fetch_min_bytes, 256);
        assert_eq!(config.fetch_max_bytes, 1_000_000);
        assert_eq!(config.fetch_max_wait_ms, 250);
        assert_eq!(config.max_poll_records, 50);
        assert_eq!(config.max_partition_fetch_bytes, 524_288);
        assert_eq!(config.session_timeout_ms, 20_000);
        assert_eq!(config.heartbeat_interval_ms, 2_000);
        assert_eq!(config.request_timeout_ms, 15_000);
        assert_eq!(config.metadata_max_age_ms, 120_000);
    }

    #[test]
    fn test_missing_group_id_is_allowed_when_auto_commit_disabled() {
        let config = ConsumerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".into()])
            .enable_auto_commit(false)
            .build()
            .unwrap();
        assert!(config.group_id.is_none());
    }

    #[test]
    fn test_auto_commit_requires_group_id_validation() {
        // Default enable_auto_commit is true, so missing group_id should fail
        let result = ConsumerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".into()])
            .build();
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("group.id"));

        // Explicitly true with no group_id also fails
        let result2 = ConsumerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".into()])
            .enable_auto_commit(true)
            .build();
        assert!(result2.is_err());
    }

    #[test]
    fn test_session_timeout_and_heartbeat_defaults() {
        let config = ConsumerConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".into()])
            .group_id("g")
            .build()
            .unwrap();
        assert_eq!(config.session_timeout_ms, 45_000);
        assert_eq!(config.heartbeat_interval_ms, 3_000);
    }
}
