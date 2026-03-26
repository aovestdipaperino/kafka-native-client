# kafka-native-client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a native Rust Kafka client (producer, consumer, admin) with high fidelity to the Java client, using proto-kafka for wire protocol.

**Architecture:** Three layers — network (broker connections + metadata), client (producer/consumer/admin with batching, fetching, group coordination), and optional typed serialization wrappers. Each client owns its own ConnectionManager and Metadata. Async-only via tokio with one task per broker.

**Tech Stack:** Rust 2024 edition, tokio, proto-kafka 0.18.1, bytes, dashmap, thiserror

**Spec:** `docs/superpowers/specs/2026-03-26-kafka-native-client-design.md`

---

## Phase 1: Foundation (error, common types, config)

### Task 1: Project setup — dependencies and lib.rs

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/lib.rs`

- [ ] **Step 1: Update Cargo.toml with all dependencies**

```toml
[package]
name = "kafka-native-client"
version = "0.0.1"
edition = "2024"
description = "A native Rust Kafka client (producer, consumer, admin) with high fidelity to the official Java client"
license = "MIT OR Apache-2.0"
repository = "https://github.com/enzolovesbacon/kafka-native-client"
keywords = ["kafka", "streaming", "async", "producer", "consumer"]
categories = ["network-programming"]
exclude = ["java-sources/**", "docs/**"]

[dependencies]
proto-kafka = "0.18.1"
tokio = { version = "1", features = ["full"] }
bytes = "1"
thiserror = "2"
dashmap = "6"
tracing = "0.1"
uuid = { version = "1", features = ["v4"] }
indexmap = "2"
```

- [ ] **Step 2: Set up lib.rs with module declarations and create all stub module files**

`src/lib.rs`:
```rust
//! A native Rust Kafka client (producer, consumer, admin) with high fidelity
//! to the official Apache Kafka Java client.
//!
//! Built on [`proto-kafka`](https://crates.io/crates/proto-kafka) for wire protocol
//! serialization/deserialization.

pub mod common;
pub mod config;
pub mod error;
pub mod metrics;
pub mod network;
pub mod producer;
pub mod consumer;
pub mod admin;
pub mod serialization;

pub use error::{KafkaError, Result};
```

Create stub files so the project compiles:
- `src/error.rs` — empty file
- `src/common/mod.rs` — empty file
- `src/config/mod.rs` — empty file
- `src/metrics/mod.rs` — empty file
- `src/network/mod.rs` — empty file
- `src/producer/mod.rs` — empty file
- `src/consumer/mod.rs` — empty file
- `src/admin/mod.rs` — empty file
- `src/serialization/mod.rs` — empty file

- [ ] **Step 3: Verify it compiles**

Run: `cargo check 2>&1`
Expected: PASS (all modules exist as empty stubs)

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock src/lib.rs
git commit -m "feat: set up project dependencies and module structure"
```

---

### Task 2: Error types

**Files:**
- Create: `src/error.rs`

- [ ] **Step 1: Write tests for error conversions**

```rust
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p kafka-native-client --lib error::tests 2>&1`
Expected: FAIL — module doesn't exist yet

- [ ] **Step 3: Implement KafkaError**

```rust
use std::fmt;

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
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p kafka-native-client --lib error::tests 2>&1`
Expected: All pass

- [ ] **Step 5: Commit**

```bash
git add src/error.rs
git commit -m "feat: add KafkaError type with conversions from proto-kafka errors"
```

---

### Task 3: Common types — TopicPartition, Node, RecordHeader, PartitionInfo

**Files:**
- Create: `src/common/mod.rs`
- Create: `src/common/topic_partition.rs`
- Create: `src/common/node.rs`
- Create: `src/common/header.rs`
- Create: `src/common/partition_info.rs`

- [ ] **Step 1: Write tests**

```rust
// In src/common/topic_partition.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_partition_eq() {
        let tp1 = TopicPartition::new("test", 0);
        let tp2 = TopicPartition::new("test", 0);
        assert_eq!(tp1, tp2);
    }

    #[test]
    fn test_topic_partition_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(TopicPartition::new("test", 0));
        set.insert(TopicPartition::new("test", 0));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_topic_partition_display() {
        let tp = TopicPartition::new("my-topic", 3);
        assert_eq!(format!("{tp}"), "my-topic-3");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p kafka-native-client --lib common 2>&1`
Expected: FAIL

- [ ] **Step 3: Implement common types**

`src/common/mod.rs`:
```rust
pub mod topic_partition;
pub mod node;
pub mod header;
pub mod partition_info;

pub use topic_partition::TopicPartition;
pub use node::Node;
pub use header::RecordHeader;
pub use partition_info::PartitionInfo;
```

`src/common/topic_partition.rs`:
```rust
use std::fmt;

/// A topic name and partition number.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self { topic: topic.into(), partition }
    }
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}
```

`src/common/node.rs`:
```rust
/// A Kafka broker node.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Node {
    pub id: i32,
    pub host: String,
    pub port: u16,
    pub rack: Option<String>,
}

impl Node {
    pub fn new(id: i32, host: impl Into<String>, port: u16) -> Self {
        Self { id, host: host.into(), port, rack: None }
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
```

`src/common/header.rs`:
```rust
use bytes::Bytes;

/// A key-value header attached to a Kafka record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    pub key: String,
    pub value: Option<Bytes>,
}

impl RecordHeader {
    pub fn new(key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self { key: key.into(), value: Some(value.into()) }
    }
}
```

`src/common/partition_info.rs`:
```rust
use super::Node;

/// Metadata about a single partition of a topic.
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub topic: String,
    pub partition: i32,
    pub leader: Option<Node>,
    pub replicas: Vec<Node>,
    pub in_sync_replicas: Vec<Node>,
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p kafka-native-client --lib common 2>&1`
Expected: All pass

- [ ] **Step 5: Commit**

```bash
git add src/common/
git commit -m "feat: add common types — TopicPartition, Node, RecordHeader, PartitionInfo"
```

---

### Task 4: Cluster metadata snapshot

**Files:**
- Create: `src/common/cluster.rs`
- Modify: `src/common/mod.rs`

- [ ] **Step 1: Write tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Node, TopicPartition, PartitionInfo};

    #[test]
    fn test_cluster_leader_for() {
        let node = Node::new(0, "localhost", 9092);
        let pi = PartitionInfo {
            topic: "test".into(),
            partition: 0,
            leader: Some(node.clone()),
            replicas: vec![node.clone()],
            in_sync_replicas: vec![node.clone()],
        };
        let cluster = Cluster::new(
            vec![node.clone()],
            vec![pi],
        );
        let leader = cluster.leader_for(&TopicPartition::new("test", 0));
        assert_eq!(leader, Some(&node));
    }

    #[test]
    fn test_cluster_partitions_for_topic() {
        let node = Node::new(0, "localhost", 9092);
        let partitions: Vec<PartitionInfo> = (0..3).map(|i| PartitionInfo {
            topic: "test".into(),
            partition: i,
            leader: Some(node.clone()),
            replicas: vec![node.clone()],
            in_sync_replicas: vec![node.clone()],
        }).collect();
        let cluster = Cluster::new(vec![node], partitions);
        assert_eq!(cluster.partitions_for_topic("test").len(), 3);
        assert_eq!(cluster.partitions_for_topic("unknown").len(), 0);
    }

    #[test]
    fn test_cluster_nodes() {
        let nodes = vec![
            Node::new(0, "host1", 9092),
            Node::new(1, "host2", 9092),
        ];
        let cluster = Cluster::new(nodes.clone(), vec![]);
        assert_eq!(cluster.nodes().len(), 2);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p kafka-native-client --lib common::cluster 2>&1`

- [ ] **Step 3: Implement Cluster**

```rust
use std::collections::HashMap;
use crate::common::{Node, TopicPartition, PartitionInfo};

/// An immutable snapshot of the Kafka cluster metadata.
#[derive(Debug, Clone)]
pub struct Cluster {
    nodes: Vec<Node>,
    nodes_by_id: HashMap<i32, Node>,
    partitions_by_topic: HashMap<String, Vec<PartitionInfo>>,
    leaders: HashMap<TopicPartition, Node>,
}

impl Cluster {
    pub fn new(nodes: Vec<Node>, partitions: Vec<PartitionInfo>) -> Self {
        let nodes_by_id: HashMap<i32, Node> = nodes.iter().map(|n| (n.id, n.clone())).collect();
        let mut partitions_by_topic: HashMap<String, Vec<PartitionInfo>> = HashMap::new();
        let mut leaders: HashMap<TopicPartition, Node> = HashMap::new();

        for p in &partitions {
            partitions_by_topic.entry(p.topic.clone()).or_default().push(p.clone());
            if let Some(leader) = &p.leader {
                leaders.insert(TopicPartition::new(p.topic.clone(), p.partition), leader.clone());
            }
        }

        Self { nodes, nodes_by_id, partitions_by_topic, leaders }
    }

    pub fn empty() -> Self {
        Self::new(vec![], vec![])
    }

    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    pub fn node_by_id(&self, id: i32) -> Option<&Node> {
        self.nodes_by_id.get(&id)
    }

    pub fn leader_for(&self, tp: &TopicPartition) -> Option<&Node> {
        self.leaders.get(tp)
    }

    pub fn partitions_for_topic(&self, topic: &str) -> &[PartitionInfo] {
        self.partitions_by_topic.get(topic).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn topics(&self) -> impl Iterator<Item = &str> {
        self.partitions_by_topic.keys().map(|s| s.as_str())
    }
}
```

- [ ] **Step 4: Add `pub mod cluster;` and `pub use cluster::Cluster;` to `src/common/mod.rs`**

- [ ] **Step 5: Run tests**

Run: `cargo test -p kafka-native-client --lib common 2>&1`
Expected: All pass

- [ ] **Step 6: Commit**

```bash
git add src/common/
git commit -m "feat: add Cluster metadata snapshot type"
```

---

### Task 5: Config — CommonConfig, ProducerConfig, ConsumerConfig, AdminConfig

**Files:**
- Create: `src/config/mod.rs`
- Create: `src/config/common_config.rs`
- Create: `src/config/producer_config.rs`
- Create: `src/config/consumer_config.rs`
- Create: `src/config/admin_config.rs`

- [ ] **Step 1: Write tests for ProducerConfig**

```rust
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
        assert!(result.is_err()); // delivery_timeout_ms < request_timeout_ms + linger_ms
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p kafka-native-client --lib config 2>&1`

- [ ] **Step 3: Implement Acks enum and CommonConfig in `src/config/common_config.rs`**

```rust
/// Acknowledgement level for produce requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acks {
    None,
    One,
    All,
}

impl Acks {
    pub fn to_i16(self) -> i16 {
        match self {
            Acks::None => 0,
            Acks::One => 1,
            Acks::All => -1,
        }
    }

    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "0" => Ok(Acks::None),
            "1" => Ok(Acks::One),
            "all" | "-1" => Ok(Acks::All),
            other => Err(format!("invalid acks value: {other}")),
        }
    }
}

/// Configuration shared by all client types.
#[derive(Debug, Clone)]
pub struct CommonConfig {
    pub bootstrap_servers: Vec<String>,
    pub client_id: String,
    pub request_timeout_ms: u64,
    pub metadata_max_age_ms: u64,
}

impl CommonConfig {
    pub fn validate(&self) -> crate::Result<()> {
        if self.bootstrap_servers.is_empty() {
            return Err(crate::KafkaError::Config(
                "bootstrap.servers is required".into(),
            ));
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Implement ProducerConfig in `src/config/producer_config.rs`**

```rust
use std::collections::HashMap;
use super::common_config::{Acks, CommonConfig};
use crate::error::{KafkaError, Result};

/// Producer configuration with typed fields.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl CompressionType {
    pub fn parse(s: &str) -> std::result::Result<Self, String> {
        match s {
            "none" => Ok(Self::None),
            "gzip" => Ok(Self::Gzip),
            "snappy" => Ok(Self::Snappy),
            "lz4" => Ok(Self::Lz4),
            "zstd" => Ok(Self::Zstd),
            other => Err(format!("unknown compression type: {other}")),
        }
    }

    pub fn to_proto_kafka(self) -> proto_kafka::records::Compression {
        match self {
            Self::None => proto_kafka::records::Compression::None,
            Self::Gzip => proto_kafka::records::Compression::Gzip,
            Self::Snappy => proto_kafka::records::Compression::Snappy,
            Self::Lz4 => proto_kafka::records::Compression::Lz4,
            Self::Zstd => proto_kafka::records::Compression::Zstd,
        }
    }
}

/// Builder for ProducerConfig.
#[derive(Debug, Default)]
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
    pub fn bootstrap_servers(mut self, v: Vec<String>) -> Self { self.bootstrap_servers = Some(v); self }
    pub fn client_id(mut self, v: impl Into<String>) -> Self { self.client_id = Some(v.into()); self }
    pub fn acks(mut self, v: Acks) -> Self { self.acks = Some(v); self }
    pub fn batch_size(mut self, v: usize) -> Self { self.batch_size = Some(v); self }
    pub fn linger_ms(mut self, v: u64) -> Self { self.linger_ms = Some(v); self }
    pub fn buffer_memory(mut self, v: usize) -> Self { self.buffer_memory = Some(v); self }
    pub fn max_block_ms(mut self, v: u64) -> Self { self.max_block_ms = Some(v); self }
    pub fn max_request_size(mut self, v: usize) -> Self { self.max_request_size = Some(v); self }
    pub fn delivery_timeout_ms(mut self, v: u64) -> Self { self.delivery_timeout_ms = Some(v); self }
    pub fn request_timeout_ms(mut self, v: u64) -> Self { self.request_timeout_ms = Some(v); self }
    pub fn retries(mut self, v: u32) -> Self { self.retries = Some(v); self }
    pub fn retry_backoff_ms(mut self, v: u64) -> Self { self.retry_backoff_ms = Some(v); self }
    pub fn max_in_flight_requests_per_connection(mut self, v: usize) -> Self { self.max_in_flight_requests_per_connection = Some(v); self }
    pub fn compression_type(mut self, v: CompressionType) -> Self { self.compression_type = Some(v); self }
    pub fn enable_idempotence(mut self, v: bool) -> Self { self.enable_idempotence = Some(v); self }
    pub fn metadata_max_age_ms(mut self, v: u64) -> Self { self.metadata_max_age_ms = Some(v); self }

    pub fn build(self) -> Result<ProducerConfig> {
        let config = ProducerConfig {
            bootstrap_servers: self.bootstrap_servers.ok_or_else(|| KafkaError::Config("bootstrap.servers is required".into()))?,
            client_id: self.client_id.unwrap_or_default(),
            acks: self.acks.unwrap_or(Acks::All),
            batch_size: self.batch_size.unwrap_or(16384),
            linger_ms: self.linger_ms.unwrap_or(5),
            buffer_memory: self.buffer_memory.unwrap_or(33_554_432),
            max_block_ms: self.max_block_ms.unwrap_or(60_000),
            max_request_size: self.max_request_size.unwrap_or(1_048_576),
            delivery_timeout_ms: self.delivery_timeout_ms.unwrap_or(120_000),
            request_timeout_ms: self.request_timeout_ms.unwrap_or(30_000),
            retries: self.retries.unwrap_or(i32::MAX as u32),
            retry_backoff_ms: self.retry_backoff_ms.unwrap_or(100),
            max_in_flight_requests_per_connection: self.max_in_flight_requests_per_connection.unwrap_or(5),
            compression_type: self.compression_type.unwrap_or(CompressionType::None),
            enable_idempotence: self.enable_idempotence.unwrap_or(true),
            metadata_max_age_ms: self.metadata_max_age_ms.unwrap_or(300_000),
        };
        config.validate()?;
        Ok(config)
    }
}

impl ProducerConfig {
    pub fn builder() -> ProducerConfigBuilder {
        ProducerConfigBuilder::default()
    }

    pub fn common(&self) -> CommonConfig {
        CommonConfig {
            bootstrap_servers: self.bootstrap_servers.clone(),
            client_id: self.client_id.clone(),
            request_timeout_ms: self.request_timeout_ms,
            metadata_max_age_ms: self.metadata_max_age_ms,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.bootstrap_servers.is_empty() {
            return Err(KafkaError::Config("bootstrap.servers is required".into()));
        }
        if self.delivery_timeout_ms < self.request_timeout_ms + self.linger_ms {
            return Err(KafkaError::Config(format!(
                "delivery.timeout.ms ({}) must be >= request.timeout.ms ({}) + linger.ms ({})",
                self.delivery_timeout_ms, self.request_timeout_ms, self.linger_ms
            )));
        }
        Ok(())
    }
}

impl TryFrom<HashMap<String, String>> for ProducerConfig {
    type Error = KafkaError;

    fn try_from(props: HashMap<String, String>) -> Result<Self> {
        let mut builder = Self::builder();

        if let Some(v) = props.get("bootstrap.servers") {
            builder = builder.bootstrap_servers(v.split(',').map(|s| s.trim().to_string()).collect());
        }
        if let Some(v) = props.get("client.id") {
            builder = builder.client_id(v.clone());
        }
        if let Some(v) = props.get("acks") {
            builder = builder.acks(Acks::parse(v).map_err(KafkaError::Config)?);
        }
        if let Some(v) = props.get("batch.size") {
            builder = builder.batch_size(v.parse().map_err(|e| KafkaError::Config(format!("invalid batch.size: {e}")))?);
        }
        if let Some(v) = props.get("linger.ms") {
            builder = builder.linger_ms(v.parse().map_err(|e| KafkaError::Config(format!("invalid linger.ms: {e}")))?);
        }
        if let Some(v) = props.get("buffer.memory") {
            builder = builder.buffer_memory(v.parse().map_err(|e| KafkaError::Config(format!("invalid buffer.memory: {e}")))?);
        }
        if let Some(v) = props.get("max.block.ms") {
            builder = builder.max_block_ms(v.parse().map_err(|e| KafkaError::Config(format!("invalid max.block.ms: {e}")))?);
        }
        if let Some(v) = props.get("max.request.size") {
            builder = builder.max_request_size(v.parse().map_err(|e| KafkaError::Config(format!("invalid max.request.size: {e}")))?);
        }
        if let Some(v) = props.get("delivery.timeout.ms") {
            builder = builder.delivery_timeout_ms(v.parse().map_err(|e| KafkaError::Config(format!("invalid delivery.timeout.ms: {e}")))?);
        }
        if let Some(v) = props.get("request.timeout.ms") {
            builder = builder.request_timeout_ms(v.parse().map_err(|e| KafkaError::Config(format!("invalid request.timeout.ms: {e}")))?);
        }
        if let Some(v) = props.get("retries") {
            builder = builder.retries(v.parse().map_err(|e| KafkaError::Config(format!("invalid retries: {e}")))?);
        }
        if let Some(v) = props.get("retry.backoff.ms") {
            builder = builder.retry_backoff_ms(v.parse().map_err(|e| KafkaError::Config(format!("invalid retry.backoff.ms: {e}")))?);
        }
        if let Some(v) = props.get("max.in.flight.requests.per.connection") {
            builder = builder.max_in_flight_requests_per_connection(v.parse().map_err(|e| KafkaError::Config(format!("invalid max.in.flight.requests.per.connection: {e}")))?);
        }
        if let Some(v) = props.get("compression.type") {
            builder = builder.compression_type(CompressionType::parse(v).map_err(KafkaError::Config)?);
        }
        if let Some(v) = props.get("enable.idempotence") {
            builder = builder.enable_idempotence(v.parse().map_err(|e| KafkaError::Config(format!("invalid enable.idempotence: {e}")))?);
        }

        builder.build()
    }
}
```

- [ ] **Step 5: Implement ConsumerConfig in `src/config/consumer_config.rs`**

Follow the same builder + TryFrom<HashMap> pattern. `OffsetResetStrategy` is defined here (canonical location — consumer/offset.rs will re-export it):

```rust
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
```

Key fields:
- `bootstrap_servers`, `group_id: Option<String>`, `group_instance_id: Option<String>`, `client_id`
- `auto_offset_reset: OffsetResetStrategy` (default Latest)
- `enable_auto_commit: bool` (default true), `auto_commit_interval_ms: u64` (default 5000)
- `fetch_min_bytes: i32` (1), `fetch_max_bytes: i32` (52428800), `fetch_max_wait_ms: i32` (500)
- `max_poll_records: i32` (500), `max_partition_fetch_bytes: i32` (1048576)
- `session_timeout_ms: i32` (45000), `heartbeat_interval_ms: i32` (3000)
- `request_timeout_ms: u64` (30000), `metadata_max_age_ms: u64` (300000)

Validation: if `enable_auto_commit` is true, `group_id` must be set.

Tests for ConsumerConfig:
```rust
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
}
```

- [ ] **Step 6: Implement AdminConfig in `src/config/admin_config.rs`**

Minimal: `bootstrap_servers`, `client_id`, `request_timeout_ms`, `metadata_max_age_ms`. Same builder + TryFrom<HashMap> pattern.

- [ ] **Step 7: Wire up `src/config/mod.rs`**

```rust
pub mod common_config;
pub mod producer_config;
pub mod consumer_config;
pub mod admin_config;

pub use common_config::{Acks, CommonConfig};
pub use producer_config::{ProducerConfig, ProducerConfigBuilder, CompressionType};
pub use consumer_config::{ConsumerConfig, ConsumerConfigBuilder, OffsetResetStrategy};
pub use admin_config::{AdminConfig, AdminConfigBuilder};
```

- [ ] **Step 8: Run all tests**

Run: `cargo test -p kafka-native-client --lib config 2>&1`
Expected: All pass

- [ ] **Step 9: Commit**

```bash
git add src/config/
git commit -m "feat: add ProducerConfig, ConsumerConfig, AdminConfig with builder and HashMap bridge"
```

---

### Task 6: Metrics system

**Files:**
- Create: `src/metrics/mod.rs`
- Create: `src/metrics/registry.rs`
- Create: `src/metrics/sensor.rs`
- Create: `src/metrics/stat.rs`
- Create: `src/metrics/meter.rs`

- [ ] **Step 1: Write tests**

```rust
// In src/metrics/registry.rs
#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::stat::{Avg, Count, Max};

    #[test]
    fn test_sensor_record_and_measure() {
        let registry = MetricsRegistry::new();
        let sensor = registry.sensor("test-sensor");
        sensor.add("test-group", "test-metric", &[], Box::new(Avg::new()));
        sensor.record(10.0);
        sensor.record(20.0);

        let metrics = registry.metrics();
        let metric = metrics.get("test-group.test-metric").unwrap();
        assert!((metric.value() - 15.0).abs() < 0.001);
    }

    #[test]
    fn test_count_stat() {
        let registry = MetricsRegistry::new();
        let sensor = registry.sensor("counter");
        sensor.add("grp", "count", &[], Box::new(Count::new()));
        sensor.record(1.0);
        sensor.record(1.0);
        sensor.record(1.0);

        let metrics = registry.metrics();
        let m = metrics.get("grp.count").unwrap();
        assert!((m.value() - 3.0).abs() < 0.001);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p kafka-native-client --lib metrics 2>&1`

- [ ] **Step 3: Implement stat.rs**

```rust
/// A statistic that can be recorded and measured.
pub trait Stat: Send + Sync {
    fn record(&mut self, value: f64, time_ms: i64);
    fn measure(&self, time_ms: i64) -> f64;
}

pub struct Avg { sum: f64, count: u64 }
impl Avg {
    pub fn new() -> Self { Self { sum: 0.0, count: 0 } }
}
impl Stat for Avg {
    fn record(&mut self, value: f64, _time_ms: i64) { self.sum += value; self.count += 1; }
    fn measure(&self, _time_ms: i64) -> f64 { if self.count == 0 { 0.0 } else { self.sum / self.count as f64 } }
}

pub struct Count { count: f64 }
impl Count {
    pub fn new() -> Self { Self { count: 0.0 } }
}
impl Stat for Count {
    fn record(&mut self, _value: f64, _time_ms: i64) { self.count += 1.0; }
    fn measure(&self, _time_ms: i64) -> f64 { self.count }
}

pub struct Max { max: f64 }
impl Max {
    pub fn new() -> Self { Self { max: f64::NEG_INFINITY } }
}
impl Stat for Max {
    fn record(&mut self, value: f64, _time_ms: i64) { if value > self.max { self.max = value; } }
    fn measure(&self, _time_ms: i64) -> f64 { self.max }
}

pub struct Min { min: f64 }
impl Min {
    pub fn new() -> Self { Self { min: f64::INFINITY } }
}
impl Stat for Min {
    fn record(&mut self, value: f64, _time_ms: i64) { if value < self.min { self.min = value; } }
    fn measure(&self, _time_ms: i64) -> f64 { self.min }
}

pub struct Rate { count: f64, start_ms: i64, last_ms: i64 }
impl Rate {
    pub fn new() -> Self { Self { count: 0.0, start_ms: 0, last_ms: 0 } }
}
impl Stat for Rate {
    fn record(&mut self, value: f64, time_ms: i64) {
        if self.start_ms == 0 { self.start_ms = time_ms; }
        self.last_ms = time_ms;
        self.count += value;
    }
    fn measure(&self, _time_ms: i64) -> f64 {
        let elapsed = (self.last_ms - self.start_ms) as f64 / 1000.0;
        if elapsed <= 0.0 { 0.0 } else { self.count / elapsed }
    }
}
```

- [ ] **Step 4: Implement meter.rs**

```rust
use std::sync::Mutex;
use crate::metrics::stat::Stat;

/// A named metric backed by a Stat.
pub struct KafkaMetric {
    pub group: String,
    pub name: String,
    pub tags: Vec<(String, String)>,
    stat: Mutex<Box<dyn Stat>>,
}

impl KafkaMetric {
    pub fn new(group: String, name: String, tags: Vec<(String, String)>, stat: Box<dyn Stat>) -> Self {
        Self { group, name, tags, stat: Mutex::new(stat) }
    }

    pub fn record(&self, value: f64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        self.stat.lock().unwrap().record(value, now);
    }

    pub fn value(&self) -> f64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        self.stat.lock().unwrap().measure(now)
    }

    pub fn key(&self) -> String {
        format!("{}.{}", self.group, self.name)
    }
}
```

- [ ] **Step 5: Implement sensor.rs and registry.rs**

`sensor.rs`:
```rust
use std::sync::Arc;
use crate::metrics::meter::KafkaMetric;
use crate::metrics::stat::Stat;

/// A named measurement point that updates one or more metrics.
pub struct Sensor {
    pub name: String,
    metrics: std::sync::Mutex<Vec<Arc<KafkaMetric>>>,
}

impl Sensor {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), metrics: std::sync::Mutex::new(vec![]) }
    }

    pub fn add(&self, group: &str, name: &str, tags: &[(&str, &str)], stat: Box<dyn Stat>) -> Arc<KafkaMetric> {
        let tags = tags.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let metric = Arc::new(KafkaMetric::new(group.to_string(), name.to_string(), tags, stat));
        self.metrics.lock().unwrap().push(metric.clone());
        metric
    }

    pub fn record(&self, value: f64) {
        for metric in self.metrics.lock().unwrap().iter() {
            metric.record(value);
        }
    }
}
```

`registry.rs`:
```rust
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use crate::metrics::sensor::Sensor;
use crate::metrics::meter::KafkaMetric;

/// Central storage for all metrics.
pub struct MetricsRegistry {
    sensors: RwLock<HashMap<String, Arc<Sensor>>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self { sensors: RwLock::new(HashMap::new()) }
    }

    pub fn sensor(&self, name: &str) -> Arc<Sensor> {
        let mut sensors = self.sensors.write().unwrap();
        sensors.entry(name.to_string())
            .or_insert_with(|| Arc::new(Sensor::new(name)))
            .clone()
    }

    pub fn metrics(&self) -> HashMap<String, Arc<KafkaMetric>> {
        let sensors = self.sensors.read().unwrap();
        let mut result = HashMap::new();
        for sensor in sensors.values() {
            for metric in sensor.metrics.lock().unwrap().iter() {
                result.insert(metric.key(), metric.clone());
            }
        }
        result
    }
}
```

Note: `sensor.metrics` field needs to be `pub(crate)` for registry to access it.

- [ ] **Step 6: Wire up `src/metrics/mod.rs`**

```rust
pub mod stat;
pub mod meter;
pub mod sensor;
pub mod registry;

pub use registry::MetricsRegistry;
pub use sensor::Sensor;
pub use meter::KafkaMetric;
```

- [ ] **Step 7: Run tests**

Run: `cargo test -p kafka-native-client --lib metrics 2>&1`
Expected: All pass

- [ ] **Step 8: Commit**

```bash
git add src/metrics/
git commit -m "feat: add metrics system — MetricsRegistry, Sensor, Stat impls (Avg, Count, Max, Min, Rate)"
```

---

## Phase 2: Network Layer

### Task 7: BrokerConnection — per-broker tokio task

**Files:**
- Create: `src/network/mod.rs`
- Create: `src/network/broker_connection.rs`

- [ ] **Step 1: Write test for BrokerConnection request/response framing**

Test that sending bytes through the channel results in them being written as length-prefixed frames, and that responses are routed back by correlation ID. Use a mock TCP listener in the test.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn test_broker_connection_roundtrip() {
        // Start a mock broker that reads a request and echoes back a response
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let mock_broker = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            // Read length-prefixed request
            let len = socket.read_i32().await.unwrap();
            let mut buf = vec![0u8; len as usize];
            socket.read_exact(&mut buf).await.unwrap();
            // Extract correlation_id (first 4 bytes after api_key + api_version in the header)
            // For simplicity, just echo the whole request back as the response
            let response = buf.clone();
            socket.write_i32(response.len() as i32).await.unwrap();
            socket.write_all(&response).await.unwrap();
        });

        let (conn, sender) = BrokerConnection::connect(0, addr.to_string(), 5).await.unwrap();
        let conn_handle = tokio::spawn(conn.run());

        // Send a request with correlation_id = 42
        let mut request_bytes = bytes::BytesMut::new();
        // Write a minimal fake request: correlation_id then some payload
        request_bytes.extend_from_slice(&42i32.to_be_bytes());
        request_bytes.extend_from_slice(b"hello");
        let response = sender.send_raw(42, request_bytes.freeze()).await.unwrap();

        // The mock broker echoed it back
        assert_eq!(&response[..4], &42i32.to_be_bytes());

        drop(sender);
        let _ = conn_handle.await;
        mock_broker.abort();
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p kafka-native-client --lib network::broker_connection 2>&1`

- [ ] **Step 3: Implement BrokerConnection**

Key structure:
```rust
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Semaphore};
use std::collections::HashMap;
use std::sync::Arc;

struct InFlightRequest {
    tx: oneshot::Sender<Bytes>,
    _permit: tokio::sync::OwnedSemaphorePermit, // held until response arrives
}

/// Channel payload: (correlation_id, request_bytes, response_sender, semaphore_permit)
type BrokerRequest = (i32, Bytes, oneshot::Sender<Bytes>, tokio::sync::OwnedSemaphorePermit);

pub struct BrokerConnection {
    broker_id: i32,
    stream: TcpStream,
    rx: mpsc::Receiver<BrokerRequest>,
    in_flight: HashMap<i32, InFlightRequest>,
}

#[derive(Clone)]
pub struct BrokerSender {
    broker_id: i32,
    tx: mpsc::Sender<BrokerRequest>,
    semaphore: Arc<Semaphore>,
}

impl BrokerSender {
    pub async fn send_raw(&self, correlation_id: i32, request: Bytes) -> crate::Result<Bytes> {
        // Acquire permit — held in InFlightRequest until response arrives
        let permit = self.semaphore.clone().acquire_owned().await.map_err(|_| crate::KafkaError::ConnectionFailed {
            broker_id: self.broker_id,
            message: "connection closed".into(),
        })?;
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx.send((correlation_id, request, resp_tx, permit)).await
            .map_err(|_| crate::KafkaError::ConnectionFailed {
                broker_id: self.broker_id,
                message: "connection closed".into(),
            })?;
        resp_rx.await.map_err(|_| crate::KafkaError::ConnectionFailed {
            broker_id: self.broker_id,
            message: "response channel dropped".into(),
        })
    }
}
```

The `run()` loop:
1. `tokio::select!` on receiving from mpsc channel vs reading from socket
2. On channel receive: write length-prefix + request bytes to socket, store oneshot in `in_flight`
3. On socket readable: read 4-byte length, read body, extract correlation_id from first 4 bytes, look up in `in_flight`, send response via oneshot, release semaphore permit

- [ ] **Step 4: Run tests**

Run: `cargo test -p kafka-native-client --lib network::broker_connection 2>&1`
Expected: Pass

- [ ] **Step 5: Commit**

```bash
git add src/network/
git commit -m "feat: add BrokerConnection — per-broker tokio task with length-prefix framing"
```

---

### Task 8: ConnectionManager — broker routing and API version negotiation

**Files:**
- Create: `src/network/connection_manager.rs`
- Modify: `src/network/mod.rs`

- [ ] **Step 1: Write test**

Test that ConnectionManager can send an ApiVersionsRequest to a mock broker and decode the response.

- [ ] **Step 2: Implement ConnectionManager**

```rust
pub struct ConnectionManager {
    connections: RwLock<HashMap<i32, BrokerSender>>,
    api_versions: RwLock<HashMap<i32, HashMap<i16, VersionRange>>>,
    bootstrap_servers: Vec<String>,
    client_id: String,
    max_in_flight: usize,
    correlation_id: AtomicI32,
}
```

Key methods:
- `async fn connect_to_broker(&self, broker_id: i32, addr: &str) -> Result<()>` — creates a BrokerConnection, performs ApiVersions handshake, stores sender and version ranges
- `async fn send<R: proto_kafka::protocol::Request>(&self, broker_id: i32, request: R) -> Result<R::Response>` — allocates correlation_id, encodes header + body via proto-kafka, sends via BrokerSender, decodes response header + body
- `fn next_correlation_id(&self) -> i32` — atomic increment
- `async fn bootstrap(&self) -> Result<()>` — connect to first reachable bootstrap server

- [ ] **Step 3: Run tests, commit**

```bash
git add src/network/
git commit -m "feat: add ConnectionManager with API version negotiation"
```

---

### Task 9: Metadata — cluster metadata fetching and caching

**Files:**
- Create: `src/network/metadata.rs`
- Modify: `src/network/mod.rs`

- [ ] **Step 1: Write test**

Test that Metadata parses a MetadataResponse into a Cluster snapshot with correct leader mappings.

- [ ] **Step 2: Implement Metadata**

```rust
pub struct Metadata {
    cluster: RwLock<Arc<Cluster>>,
    connection_manager: Arc<ConnectionManager>,
    topics: RwLock<HashSet<String>>,
    last_refresh: AtomicI64,
    metadata_max_age_ms: u64,
    notify: Notify,
}
```

Key methods:
- `async fn fetch(&self) -> Result<()>` — sends MetadataRequest for tracked topics, parses MetadataResponse into Cluster
- `async fn request_update(&self)` — signals background refresh
- `fn cluster(&self) -> Arc<Cluster>` — returns current snapshot
- `async fn leader_for(&self, tp: &TopicPartition) -> Result<Node>` — looks up leader, triggers refresh if unknown
- `fn add_topic(&self, topic: &str)` — registers a topic for metadata tracking

Parse MetadataResponse:
```rust
fn parse_metadata_response(response: &MetadataResponse) -> Cluster {
    let nodes: Vec<Node> = response.brokers.iter().map(|b| {
        Node::new(
            b.node_id.0,
            b.host.as_str(),
            u16::try_from(b.port).unwrap_or(9092),
        )
    }).collect();

    let partitions: Vec<PartitionInfo> = response.topics.iter().flat_map(|t| {
        // MetadataResponseTopic.name is Option<TopicName> in proto-kafka
        let topic_name = t.name.as_ref().map(|n| n.0.to_string()).unwrap_or_default();
        t.partitions.iter().map(move |p| {
            let leader = nodes.iter().find(|n| n.id == p.leader_id.0).cloned();
            PartitionInfo {
                topic: topic_name.clone(),
                partition: p.partition_index,
                leader,
                replicas: p.replica_nodes.iter()
                    .filter_map(|id| nodes.iter().find(|n| n.id == id.0).cloned())
                    .collect(),
                in_sync_replicas: p.isr_nodes.iter()
                    .filter_map(|id| nodes.iter().find(|n| n.id == id.0).cloned())
                    .collect(),
            }
        })
    }).collect();

    Cluster::new(nodes, partitions)
}
```

- [ ] **Step 3: Run tests, commit**

```bash
git add src/network/
git commit -m "feat: add Metadata — cluster metadata fetching, caching, background refresh"
```

---

## Phase 3: Producer

### Task 10: ProducerRecord and RecordMetadata types

**Files:**
- Create: `src/producer/mod.rs`
- Create: `src/producer/producer_record.rs`
- Create: `src/producer/record_metadata.rs`
- Create: `src/producer/callback.rs`

- [ ] **Step 1: Implement ProducerRecord, RecordMetadata, Callback, SendFuture**

```rust
// producer_record.rs
use bytes::Bytes;
use crate::common::RecordHeader;

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

    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self { self.key = Some(key.into()); self }
    pub fn with_partition(mut self, partition: i32) -> Self { self.partition = Some(partition); self }
    pub fn with_timestamp(mut self, ts: i64) -> Self { self.timestamp = Some(ts); self }
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push(RecordHeader::new(key, value));
        self
    }

    pub fn estimated_size(&self) -> usize {
        self.key.as_ref().map(|k| k.len()).unwrap_or(0)
            + self.value.as_ref().map(|v| v.len()).unwrap_or(0)
            + self.headers.iter().map(|h| h.key.len() + h.value.as_ref().map(|v| v.len()).unwrap_or(0)).sum::<usize>()
    }
}
```

```rust
// record_metadata.rs
pub struct RecordMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub serialized_key_size: i32,
    pub serialized_value_size: i32,
}
```

```rust
// callback.rs
use crate::error::KafkaError;
use super::record_metadata::RecordMetadata;

pub type Callback = Box<dyn FnOnce(std::result::Result<&RecordMetadata, &KafkaError>) + Send + 'static>;

pub struct SendFuture {
    rx: tokio::sync::oneshot::Receiver<crate::Result<RecordMetadata>>,
}

impl std::future::Future for SendFuture {
    type Output = crate::Result<RecordMetadata>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        match std::pin::Pin::new(&mut self.rx).poll(cx) {
            std::task::Poll::Ready(Ok(result)) => std::task::Poll::Ready(result),
            std::task::Poll::Ready(Err(_)) => std::task::Poll::Ready(Err(crate::KafkaError::ConnectionFailed {
                broker_id: -1,
                message: "producer shut down".into(),
            })),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl SendFuture {
    pub fn new(rx: tokio::sync::oneshot::Receiver<crate::Result<RecordMetadata>>) -> Self {
        Self { rx }
    }
}
```

- [ ] **Step 2: Tests and commit**

```bash
git add src/producer/
git commit -m "feat: add ProducerRecord, RecordMetadata, Callback, SendFuture"
```

---

### Task 11: Partitioner

**Files:**
- Create: `src/producer/partitioner.rs`

- [ ] **Step 1: Write tests**

Test murmur2 hash matches Java's `Utils.murmur2` for known inputs. Test that keyed records always go to the same partition. Test that keyless records distribute.

- [ ] **Step 2: Implement Partitioner trait and DefaultPartitioner**

```rust
pub trait Partitioner: Send + Sync {
    fn partition(&self, topic: &str, key: Option<&[u8]>, value: Option<&[u8]>, cluster: &Cluster) -> i32;
}

pub struct DefaultPartitioner { counter: AtomicI32 }

impl Partitioner for DefaultPartitioner {
    fn partition(&self, topic: &str, key: Option<&[u8]>, _value: Option<&[u8]>, cluster: &Cluster) -> i32 {
        let num_partitions = cluster.partitions_for_topic(topic).len() as i32;
        if num_partitions <= 0 { return 0; }
        match key {
            Some(k) => (murmur2(k) as i32 & 0x7fffffff) % num_partitions,
            None => (self.counter.fetch_add(1, Ordering::Relaxed) & 0x7fffffff) % num_partitions,
        }
    }
}
```

Implement `murmur2()` matching Java's `org.apache.kafka.common.utils.Utils.murmur2`.

- [ ] **Step 3: Run tests, commit**

```bash
git add src/producer/partitioner.rs
git commit -m "feat: add Partitioner trait and DefaultPartitioner with murmur2"
```

---

### Task 12: RecordAccumulator — batching records by partition

**Files:**
- Create: `src/producer/record_accumulator.rs`

- [ ] **Step 1: Write tests**

Test that records for the same partition are batched together. Test that a full batch is marked ready. Test that linger timeout makes a batch ready. Test buffer memory backpressure.

- [ ] **Step 2: Implement RecordAccumulator**

Key structure:
```rust
pub struct RecordAccumulator {
    batches: Mutex<HashMap<TopicPartition, ProducerBatch>>,
    batch_size: usize,
    compression: CompressionType,
    linger_ms: u64,
    buffer_semaphore: Arc<Semaphore>,
    max_block_ms: u64,
    notify: Arc<Notify>,
}

pub struct ProducerBatch {
    pub tp: TopicPartition,
    pub records: Vec<Record>,
    pub callbacks: Vec<(oneshot::Sender<Result<RecordMetadata>>, Option<Callback>)>,
    pub created_ms: i64,
    pub record_count: usize,
    pub size_bytes: usize,
}
```

Key methods:
- `async fn append(&self, tp: TopicPartition, record: ProducerRecord, callback: Option<Callback>) -> Result<SendFuture>` — acquires buffer_semaphore with timeout, appends to batch, returns SendFuture
- `fn drain(&self, ready_nodes: &HashSet<i32>, cluster: &Cluster) -> HashMap<i32, Vec<ProducerBatch>>` — collects ready batches grouped by broker
- `fn ready(&self, cluster: &Cluster) -> HashSet<i32>` — returns broker IDs that have ready batches
- `fn has_unsent(&self) -> bool`

A batch is ready when `size_bytes >= batch_size` OR `now - created_ms >= linger_ms`.

- [ ] **Step 3: Run tests, commit**

```bash
git add src/producer/record_accumulator.rs
git commit -m "feat: add RecordAccumulator — batching by partition with buffer memory management"
```

---

### Task 13: Sender — background drain and produce task

**Files:**
- Create: `src/producer/sender.rs`

- [ ] **Step 1: Write test**

Test that Sender drains ready batches and sends ProduceRequests via a mock ConnectionManager. Verify callbacks are invoked with correct RecordMetadata.

- [ ] **Step 2: Implement Sender**

```rust
pub struct Sender {
    accumulator: Arc<RecordAccumulator>,
    connection_manager: Arc<ConnectionManager>,
    metadata: Arc<Metadata>,
    acks: Acks,
    request_timeout_ms: u64,
    retries: u32,
    retry_backoff_ms: u64,
    delivery_timeout_ms: u64,
    max_request_size: usize,
    idempotent: Option<Arc<ProducerIdManager>>,
    metrics: Arc<MetricsRegistry>,
}
```

The `run()` loop:
1. Wait for accumulator notification or linger timeout
2. Find ready nodes via `accumulator.ready(&cluster)`
3. Drain batches via `accumulator.drain(&ready_nodes, &cluster)`
4. For each broker's batches:
   - Build `ProduceRequest` from batches (encode records via `RecordBatchEncoder`)
   - If idempotent: set producer_id, epoch, sequence on each partition's batch
   - Send via `connection_manager.send(broker_id, produce_request)`
   - Handle response: for each partition in response, check error code
     - Success: complete callbacks with RecordMetadata
     - Retriable + not expired: re-enqueue batch after backoff
     - Fatal or expired: complete callbacks with error
5. Expire batches older than `delivery_timeout_ms`

Building a ProduceRequest from batches:
```rust
use proto_kafka::messages::{ProduceRequest, TopicProduceData, PartitionProduceData};
use proto_kafka::records::{Record, RecordBatchEncoder, RecordEncodeOptions, Compression};
use proto_kafka::protocol::StrBytes;

fn build_produce_request(
    batches: &[ProducerBatch],
    acks: i16,
    timeout_ms: i32,
    compression: Compression,
) -> crate::Result<ProduceRequest> {
    let mut topic_map: HashMap<String, Vec<PartitionProduceData>> = HashMap::new();

    for batch in batches {
        // Convert our records to proto-kafka Record format
        let proto_records: Vec<Record> = batch.records.iter().enumerate().map(|(i, r)| {
            Record {
                transactional: false,
                control: false,
                partition_leader_epoch: proto_kafka::records::NO_PARTITION_LEADER_EPOCH,
                producer_id: batch.producer_id.unwrap_or(proto_kafka::records::NO_PRODUCER_ID),
                producer_epoch: batch.producer_epoch.unwrap_or(proto_kafka::records::NO_PRODUCER_EPOCH),
                timestamp_type: proto_kafka::records::TimestampType::Creation,
                offset: i as i64,
                sequence: batch.base_sequence.map(|s| s + i as i32).unwrap_or(proto_kafka::records::NO_SEQUENCE),
                timestamp: r.timestamp.unwrap_or_else(|| {
                    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default().as_millis() as i64
                }),
                key: r.key.clone(),
                value: r.value.clone(),
                headers: r.headers.iter().map(|h| {
                    (StrBytes::from_string(h.key.clone()), h.value.clone())
                }).collect(),
            }
        }).collect();

        let mut buf = bytes::BytesMut::new();
        RecordBatchEncoder::encode(&mut buf, &proto_records, &RecordEncodeOptions {
            version: 2,
            compression,
        })?;

        topic_map.entry(batch.tp.topic.clone()).or_default().push(
            PartitionProduceData::default()
                .with_index(batch.tp.partition)
                .with_records(Some(buf.freeze()))
        );
    }

    let topic_data: Vec<TopicProduceData> = topic_map.into_iter().map(|(topic, partitions)| {
        TopicProduceData::default()
            .with_name(proto_kafka::messages::TopicName(StrBytes::from_string(topic)))
            .with_partition_data(partitions)
    }).collect();

    Ok(ProduceRequest::default()
        .with_acks(acks)
        .with_timeout_ms(timeout_ms)
        .with_topic_data(topic_data))
}
```

- [ ] **Step 3: Run tests, commit**

```bash
git add src/producer/sender.rs
git commit -m "feat: add Sender — background produce loop with retry and delivery timeout"
```

---

### Task 14: IdempotentProducerManager

**Files:**
- Create: `src/producer/idempotent.rs`

- [ ] **Step 1: Write test**

Test that `ProducerIdManager` tracks per-partition sequence numbers and increments correctly.

- [ ] **Step 2: Implement**

```rust
pub struct ProducerIdManager {
    producer_id: RwLock<i64>,
    producer_epoch: RwLock<i16>,
    sequence_numbers: Mutex<HashMap<TopicPartition, i32>>,
    connection_manager: Arc<ConnectionManager>,
}
```

Key methods:
- `async fn init(&self) -> Result<()>` — sends `InitProducerIdRequest`, stores id+epoch
- `fn next_sequence(&self, tp: &TopicPartition, record_count: i32) -> i32`
- `fn producer_id(&self) -> i64`
- `fn producer_epoch(&self) -> i16`
- `fn reset_sequence(&self, tp: &TopicPartition)`

- [ ] **Step 3: Run tests, commit**

```bash
git add src/producer/idempotent.rs
git commit -m "feat: add ProducerIdManager — idempotent producer with sequence tracking"
```

---

### Task 15: KafkaProducer — public API

**Files:**
- Create: `src/producer/kafka_producer.rs`
- Modify: `src/producer/mod.rs`

- [ ] **Step 1: Implement KafkaProducer**

```rust
pub struct KafkaProducer {
    config: ProducerConfig,
    metadata: Arc<Metadata>,
    accumulator: Arc<RecordAccumulator>,
    partitioner: Box<dyn Partitioner>,
    connection_manager: Arc<ConnectionManager>,
    sender_handle: JoinHandle<()>,
    metrics: Arc<MetricsRegistry>,
}

impl KafkaProducer {
    pub async fn new(config: ProducerConfig) -> Result<Self> {
        let connection_manager = Arc::new(ConnectionManager::new(/* ... */));
        connection_manager.bootstrap().await?;
        let metadata = Arc::new(Metadata::new(/* ... */));
        metadata.fetch().await?;
        let accumulator = Arc::new(RecordAccumulator::new(/* from config */));
        let idempotent = if config.enable_idempotence {
            let mgr = Arc::new(ProducerIdManager::new(connection_manager.clone()));
            mgr.init().await?;
            Some(mgr)
        } else { None };
        let sender = Sender::new(/* ... */);
        let sender_handle = tokio::spawn(sender.run());
        Ok(Self { /* ... */ })
    }

    pub fn send(&self, record: ProducerRecord, callback: Option<Callback>) -> Result<SendFuture> {
        let tp = self.partition_for(&record)?;
        let future = block_on_runtime(self.accumulator.append(tp, record, callback))?;
        Ok(future)
    }

    pub async fn flush(&self) -> Result<()> { /* wait for all batches to be sent */ }
    pub async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>> { /* from metadata */ }
    pub fn metrics(&self) -> &MetricsRegistry { &self.metrics }
    pub async fn close(self, timeout: Duration) -> Result<()> { /* flush + shutdown sender */ }
}
```

- [ ] **Step 2: Wire up `src/producer/mod.rs`**

```rust
pub mod kafka_producer;
pub mod producer_record;
pub mod record_metadata;
pub mod record_accumulator;
pub mod sender;
pub mod partitioner;
pub mod idempotent;
pub mod callback;

pub use kafka_producer::KafkaProducer;
pub use producer_record::ProducerRecord;
pub use record_metadata::RecordMetadata;
pub use callback::{Callback, SendFuture};
pub use partitioner::{Partitioner, DefaultPartitioner};
```

- [ ] **Step 3: Verify it compiles, commit**

```bash
git add src/producer/
git commit -m "feat: add KafkaProducer public API"
```

---

## Phase 4: Consumer

### Task 16: ConsumerRecord, ConsumerRecords, offset types, rebalance listener

**Files:**
- Create: `src/consumer/consumer_record.rs`
- Create: `src/consumer/consumer_records.rs`
- Create: `src/consumer/offset.rs`
- Create: `src/consumer/rebalance_listener.rs`

- [ ] **Step 1: Write tests for ConsumerRecords**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::common::TopicPartition;

    #[test]
    fn test_consumer_records_iter() {
        let tp = TopicPartition::new("test", 0);
        let record = ConsumerRecord {
            topic: "test".into(),
            partition: 0,
            offset: 0,
            timestamp: 1000,
            timestamp_type: TimestampType::Creation,
            key: None,
            value: Some(Bytes::from("hello")),
            headers: vec![],
            leader_epoch: None,
            serialized_key_size: -1,
            serialized_value_size: 5,
        };
        let mut map = std::collections::HashMap::new();
        map.insert(tp.clone(), vec![record]);
        let records = ConsumerRecords::new(map);
        assert_eq!(records.count(), 1);
        assert!(!records.is_empty());
        assert_eq!(records.records_for(&tp).len(), 1);
    }

    #[test]
    fn test_empty_consumer_records() {
        let records = ConsumerRecords::empty();
        assert!(records.is_empty());
        assert_eq!(records.count(), 0);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p kafka-native-client --lib consumer::consumer_records 2>&1`

- [ ] **Step 3: Implement ConsumerRecord**

```rust
// src/consumer/consumer_record.rs
use bytes::Bytes;
use crate::common::RecordHeader;

/// Timestamp type matching proto_kafka::records::TimestampType
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
```

- [ ] **Step 4: Implement ConsumerRecords**

```rust
// src/consumer/consumer_records.rs
use std::collections::{HashMap, HashSet};
use crate::common::TopicPartition;
use super::consumer_record::ConsumerRecord;

pub struct ConsumerRecords {
    records: HashMap<TopicPartition, Vec<ConsumerRecord>>,
}

impl ConsumerRecords {
    pub fn new(records: HashMap<TopicPartition, Vec<ConsumerRecord>>) -> Self {
        Self { records }
    }

    pub fn empty() -> Self {
        Self { records: HashMap::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.records.values().all(|v| v.is_empty())
    }

    pub fn count(&self) -> usize {
        self.records.values().map(|v| v.len()).sum()
    }

    pub fn records_for(&self, tp: &TopicPartition) -> &[ConsumerRecord] {
        self.records.get(tp).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn partitions(&self) -> HashSet<&TopicPartition> {
        self.records.keys().collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = &ConsumerRecord> {
        self.records.values().flat_map(|v| v.iter())
    }
}
```

- [ ] **Step 5: Implement offset types**

```rust
// src/consumer/offset.rs
// Re-export OffsetResetStrategy from config (canonical definition)
pub use crate::config::consumer_config::OffsetResetStrategy;

/// An offset and associated metadata for a committed consumer position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetAndMetadata {
    pub offset: i64,
    pub metadata: Option<String>,
    pub leader_epoch: Option<i32>,
}

impl OffsetAndMetadata {
    pub fn new(offset: i64) -> Self {
        Self { offset, metadata: None, leader_epoch: None }
    }
}

/// An offset and timestamp returned by offsets_for_times.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetAndTimestamp {
    pub offset: i64,
    pub timestamp: i64,
    pub leader_epoch: Option<i32>,
}

/// Consumer group metadata for sendOffsetsToTransaction.
#[derive(Debug, Clone)]
pub struct ConsumerGroupMetadata {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
}
```

- [ ] **Step 6: Implement ConsumerRebalanceListener**

```rust
// src/consumer/rebalance_listener.rs
use crate::common::TopicPartition;

/// Callback interface for consumer group rebalance events.
pub trait ConsumerRebalanceListener: Send + Sync {
    /// Called after partitions are assigned to this consumer.
    fn on_partitions_assigned(&self, partitions: &[TopicPartition]);
    /// Called before partitions are revoked from this consumer (cooperative rebalance).
    fn on_partitions_revoked(&self, partitions: &[TopicPartition]);
    /// Called when partitions are lost (session timeout, no clean revocation).
    fn on_partitions_lost(&self, partitions: &[TopicPartition]) {
        // Default: same as revoked
        self.on_partitions_revoked(partitions);
    }
}
```

- [ ] **Step 7: Run tests**

Run: `cargo test -p kafka-native-client --lib consumer 2>&1`
Expected: All pass

- [ ] **Step 8: Commit**

```bash
git add src/consumer/
git commit -m "feat: add ConsumerRecord, ConsumerRecords, offset types, rebalance listener"
```

---

### Task 17: SubscriptionState

**Files:**
- Create: `src/consumer/subscription_state.rs`

- [ ] **Step 1: Write tests**

Test subscribe/assign/unsubscribe transitions. Test that fetch position tracking works.

- [ ] **Step 2: Implement SubscriptionState**

Tracks: subscription type (topics vs manual), subscribed topics, assigned partitions, per-partition state (position, committed offset, pause state, reset strategy).

- [ ] **Step 3: Tests and commit**

```bash
git add src/consumer/subscription_state.rs
git commit -m "feat: add SubscriptionState — subscription and position tracking"
```

---

### Task 18: Fetcher

**Files:**
- Create: `src/consumer/fetcher.rs`

- [ ] **Step 1: Write test**

Test that Fetcher builds correct FetchRequests and decodes FetchResponses into ConsumerRecords using RecordBatchDecoder.

- [ ] **Step 2: Implement Fetcher**

```rust
pub struct Fetcher {
    connection_manager: Arc<ConnectionManager>,
    subscription_state: Arc<SubscriptionState>,
    metadata: Arc<Metadata>,
    config: FetcherConfig,
    completed_fetches: Mutex<VecDeque<CompletedFetch>>,
}
```

Key methods:
- `async fn send_fetches(&self) -> usize` — builds and sends FetchRequests for all assigned partitions
- `fn collect_fetch(&self, max_records: i32) -> ConsumerRecords` — drains completed fetches up to max_poll_records

Decode FetchResponse partitions using `RecordBatchDecoder::decode()`, convert proto-kafka `Record` to `ConsumerRecord`.

- [ ] **Step 3: Tests and commit**

```bash
git add src/consumer/fetcher.rs
git commit -m "feat: add Fetcher — fetch request lifecycle and response decoding"
```

---

### Task 19: GroupCoordinator

**Files:**
- Create: `src/consumer/group_coordinator.rs`

- [ ] **Step 1: Write test**

Test the JoinGroup/SyncGroup flow with a mock broker. Test heartbeat keeps the session alive. Test rebalance listener is invoked.

- [ ] **Step 2: Implement GroupCoordinator**

```rust
pub struct GroupCoordinator {
    connection_manager: Arc<ConnectionManager>,
    group_id: String,
    member_id: RwLock<String>,
    generation_id: AtomicI32,
    group_instance_id: Option<String>,
    coordinator_id: AtomicI32,
    heartbeat_interval_ms: u64,
    session_timeout_ms: u64,
    rebalance_listener: Option<Box<dyn ConsumerRebalanceListener>>,
    state: RwLock<GroupState>,
    heartbeat_handle: Mutex<Option<JoinHandle<()>>>,
}

enum GroupState { Unjoined, Joining, Stable, PreparingRebalance }
```

Key methods:
- `async fn ensure_coordinator_known(&self) -> Result<()>` — sends FindCoordinatorRequest
- `async fn join_group(&self, subscription: &SubscriptionState) -> Result<Vec<TopicPartition>>` — JoinGroup + SyncGroup, range assignment for leader
- `async fn heartbeat(&self) -> Result<()>` — sends HeartbeatRequest, detects REBALANCE_IN_PROGRESS
- `async fn commit_offsets_sync(&self, offsets: HashMap<TopicPartition, OffsetAndMetadata>) -> Result<()>`
- `async fn commit_offsets_async(&self, offsets: HashMap<TopicPartition, OffsetAndMetadata>)`
- `async fn fetch_committed_offsets(&self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, OffsetAndMetadata>>`
- `async fn leave_group(&self) -> Result<()>`
- `fn start_heartbeat(&self)` — spawns background heartbeat tokio task

Range assignor for leader:
```rust
fn assign_partitions(members: &[JoinGroupResponseMember], topics: &[String], cluster: &Cluster) -> HashMap<String, Vec<TopicPartition>> {
    // Sort partitions, divide evenly among members
}
```

- [ ] **Step 3: Tests and commit**

```bash
git add src/consumer/group_coordinator.rs
git commit -m "feat: add GroupCoordinator — join/sync/heartbeat/leave with range assignor"
```

---

### Task 20: KafkaConsumer — public API

**Files:**
- Create: `src/consumer/kafka_consumer.rs`
- Modify: `src/consumer/mod.rs`

- [ ] **Step 1: Implement KafkaConsumer**

The `poll()` state machine:
1. Check wakeup flag → return `Err(KafkaError::WakeupException)` if set, clear flag
2. Ensure coordinator known (send `FindCoordinator` if needed)
3. If rebalance needed: call `listener.on_partitions_revoked()`, join/sync, call `listener.on_partitions_assigned()`, fetch committed offsets for new partitions
4. Auto-commit if `enable_auto_commit && now - last_commit >= auto_commit_interval_ms`
5. Send fetch requests via `Fetcher::send_fetches()`
6. Await fetch responses up to remaining timeout
7. Return `Fetcher::collect_fetch(max_poll_records)`

**All public methods to implement** (each must be present):
- `async fn new(config: ConsumerConfig) -> Result<Self>`
- `fn subscribe(&self, topics: &[&str], listener: Option<Box<dyn ConsumerRebalanceListener>>) -> Result<()>`
- `fn assign(&self, partitions: &[TopicPartition]) -> Result<()>`
- `fn unsubscribe(&self) -> Result<()>`
- `async fn poll(&self, timeout: Duration) -> Result<ConsumerRecords>`
- `async fn commit_sync(&self) -> Result<()>`
- `async fn commit_sync_offsets(&self, offsets: HashMap<TopicPartition, OffsetAndMetadata>) -> Result<()>`
- `fn commit_async(&self, callback: Option<OffsetCommitCallback>) -> Result<()>`
- `fn seek(&self, partition: &TopicPartition, offset: i64) -> Result<()>`
- `fn seek_to_beginning(&self, partitions: &[TopicPartition]) -> Result<()>`
- `fn seek_to_end(&self, partitions: &[TopicPartition]) -> Result<()>`
- `async fn position(&self, partition: &TopicPartition) -> Result<i64>`
- `async fn committed(&self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, OffsetAndMetadata>>`
- `async fn offsets_for_times(&self, timestamps: HashMap<TopicPartition, i64>) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>>`
- `async fn beginning_offsets(&self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, i64>>`
- `async fn end_offsets(&self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, i64>>`
- `fn current_lag(&self, partition: &TopicPartition) -> Option<i64>`
- `fn assignment(&self) -> HashSet<TopicPartition>`
- `fn subscription(&self) -> HashSet<String>`
- `async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>>`
- `fn pause(&self, partitions: &[TopicPartition]) -> Result<()>`
- `fn resume(&self, partitions: &[TopicPartition]) -> Result<()>`
- `fn paused(&self) -> HashSet<TopicPartition>`
- `fn group_metadata(&self) -> ConsumerGroupMetadata`
- `fn metrics(&self) -> &MetricsRegistry`
- `async fn close(self, timeout: Duration) -> Result<()>`
- `fn wakeup(&self)` — sets `AtomicBool` flag checked at start of `poll()`

- [ ] **Step 2: Wire up consumer/mod.rs**

```rust
pub mod kafka_consumer;
pub mod consumer_record;
pub mod consumer_records;
pub mod fetcher;
pub mod subscription_state;
pub mod offset;
pub mod group_coordinator;
pub mod rebalance_listener;

pub use kafka_consumer::KafkaConsumer;
pub use consumer_record::ConsumerRecord;
pub use consumer_records::ConsumerRecords;
pub use offset::{OffsetAndMetadata, OffsetAndTimestamp, OffsetResetStrategy};
pub use rebalance_listener::ConsumerRebalanceListener;
```

- [ ] **Step 3: Verify it compiles, commit**

```bash
git add src/consumer/
git commit -m "feat: add KafkaConsumer public API with poll() state machine"
```

---

## Phase 5: Admin Client

### Task 21: AdminClient

**Files:**
- Create: `src/admin/mod.rs`
- Create: `src/admin/admin_client.rs`
- Create: `src/admin/types.rs`
- Create: `src/admin/create_topics.rs`
- Create: `src/admin/delete_topics.rs`
- Create: `src/admin/describe_topics.rs`
- Create: `src/admin/list_offsets.rs`
- Create: `src/admin/describe_groups.rs`
- Create: `src/admin/alter_configs.rs`

- [ ] **Step 1: Implement admin types in `types.rs`**

```rust
pub struct NewTopic {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub configs: HashMap<String, String>,
}

pub struct TopicResult { pub name: String, pub error: Option<KafkaError> }
pub struct TopicListing { pub name: String, pub topic_id: uuid::Uuid, pub is_internal: bool }
pub struct TopicDescription { pub name: String, pub topic_id: uuid::Uuid, pub is_internal: bool, pub partitions: Vec<TopicPartitionInfo> }
pub struct TopicPartitionInfo { pub partition: i32, pub leader: Node, pub replicas: Vec<Node>, pub isr: Vec<Node> }
pub struct ClusterDescription { pub cluster_id: String, pub controller: Node, pub nodes: Vec<Node> }
pub struct ConsumerGroupListing { pub group_id: String, pub is_simple: bool, pub state: String }
pub struct ConsumerGroupDescription { pub group_id: String, pub state: String, pub members: Vec<MemberDescription>, pub coordinator: Node }
pub struct MemberDescription { pub member_id: String, pub client_id: String, pub host: String, pub assignment: Vec<TopicPartition> }
pub struct ConfigResource { pub resource_type: ResourceType, pub name: String }
pub enum ResourceType { Topic, Broker }
pub struct ConfigEntry { pub name: String, pub value: Option<String>, pub is_default: bool, pub is_read_only: bool }
pub struct OffsetSpec { pub spec: OffsetSpecType }
pub enum OffsetSpecType { Latest, Earliest, Timestamp(i64) }
pub struct ListOffsetsResult { pub offset: i64, pub timestamp: i64 }
```

- [ ] **Step 2: Implement AdminClient**

Each method: builds the proto-kafka request, sends via ConnectionManager (to controller for create/delete topics, to coordinator for group operations), parses response into result types.

- [ ] **Step 3: Wire up admin/mod.rs, tests, commit**

```bash
git add src/admin/
git commit -m "feat: add AdminClient — create/delete/describe topics, groups, configs, offsets"
```

---

## Phase 6: Serialization Layer

### Task 22: ToBytes/FromBytes traits and typed wrappers

**Files:**
- Create: `src/serialization/mod.rs`
- Create: `src/serialization/traits.rs`
- Create: `src/serialization/builtin.rs`
- Create: `src/serialization/typed_producer.rs`
- Create: `src/serialization/typed_consumer.rs`

- [ ] **Step 1: Write tests**

Test that String, Vec<u8>, Bytes, i32, i64 round-trip through ToBytes/FromBytes. Test that TypedProducer converts typed records to bytes before sending.

- [ ] **Step 2: Implement traits and builtins**

```rust
// traits.rs
pub trait ToBytes {
    fn to_bytes(&self, topic: &str) -> crate::Result<Bytes>;
}
pub trait FromBytes: Sized {
    fn from_bytes(topic: &str, data: &[u8]) -> crate::Result<Self>;
}

// builtin.rs — impls for String, Vec<u8>, Bytes, i32, i64, f64
```

- [ ] **Step 3: Implement TypedProducer and TypedConsumer wrappers**

```rust
// typed_producer.rs
pub struct TypedProducer<K: ToBytes, V: ToBytes> {
    inner: KafkaProducer,
    _phantom: PhantomData<(K, V)>,
}
// Wraps send() to serialize K, V before passing to inner

// typed_consumer.rs
pub struct TypedConsumer<K: FromBytes, V: FromBytes> {
    inner: KafkaConsumer,
    _phantom: PhantomData<(K, V)>,
}
// Wraps poll() to deserialize K, V from ConsumerRecords
```

- [ ] **Step 4: Tests and commit**

```bash
git add src/serialization/
git commit -m "feat: add serialization layer — ToBytes/FromBytes traits, typed producer/consumer wrappers"
```

---

## Phase 7: TODO.md and Final Wiring

### Task 23: Create TODO.md for deferred features

**Files:**
- Create: `TODO.md`

- [ ] **Step 1: Write TODO.md**

```markdown
# TODO — Deferred Features

## Transactions
- [ ] `initTransactions()` — InitProducerIdRequest with transactional_id
- [ ] `beginTransaction()` — local state transition
- [ ] `sendOffsetsToTransaction()` — AddOffsetsToTxnRequest + TxnOffsetCommitRequest
- [ ] `commitTransaction()` — EndTxnRequest with COMMIT
- [ ] `abortTransaction()` — EndTxnRequest with ABORT
- [ ] TransactionManager state machine (Java: `TransactionManager.java`)

## Security
- [ ] SSL/TLS connections (rustls or native-tls)
- [ ] SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI, OAUTHBEARER)

## Interceptors
- [ ] ProducerInterceptor trait (onSend, onAcknowledgement)
- [ ] ConsumerInterceptor trait (onConsume, onCommit)

## Partition Assignors
- [ ] Cooperative sticky assignor
- [ ] Custom assignor trait

## Share Consumer
- [ ] KafkaShareConsumer (share group protocol)

## Compression
- [ ] Custom compressor/decompressor plugin API

## Advanced
- [ ] Consumer regex subscription
- [ ] Log truncation detection and handling
- [ ] Client-side telemetry reporting
```

- [ ] **Step 2: Commit**

```bash
git add TODO.md
git commit -m "docs: add TODO.md tracking deferred features"
```

---

### Task 24: Final compilation check and stub modules

**Files:**
- Modify: `src/lib.rs`
- Various stub `mod.rs` files if needed

- [ ] **Step 1: Ensure all modules compile**

Run: `cargo check 2>&1`

Fill in any missing `mod.rs` files with empty module declarations if needed. The goal is that `cargo check` passes after all tasks are complete.

- [ ] **Step 2: Run all tests**

Run: `cargo test 2>&1`
Expected: All tests pass

- [ ] **Step 3: Run clippy**

Run: `cargo clippy 2>&1`
Fix any warnings.

- [ ] **Step 4: Final commit**

```bash
git add -A
git commit -m "feat: kafka-native-client v0.1.0 — initial implementation"
```

---

## Summary

| Phase | Tasks | What it delivers |
|-------|-------|-----------------|
| 1: Foundation | 1-6 | Error types, common types, config, metrics |
| 2: Network | 7-9 | BrokerConnection, ConnectionManager, Metadata |
| 3: Producer | 10-15 | Full producer with batching, idempotence, partitioning |
| 4: Consumer | 16-20 | Full consumer with fetching, group coordination, auto-commit |
| 5: Admin | 21 | AdminClient for topic/group/config management |
| 6: Serialization | 22 | Typed wrappers with ToBytes/FromBytes |
| 7: Finalization | 23-24 | TODO.md, compilation verification |
