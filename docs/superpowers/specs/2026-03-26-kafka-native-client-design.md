# kafka-native-client Design Spec

## Overview

A Rust reimplementation of the Apache Kafka Java client (producer, consumer, admin), built on `proto-kafka` for wire protocol serialization. Targets high fidelity to the Java client's semantics while providing an idiomatic async Rust API.

**Decisions:**
- Async-only (tokio)
- Typed config with string-map bridge
- Bytes-only core with optional typed wrappers for serialization
- One tokio task per broker connection
- Idempotent producer included (no full transactions yet)
- Metrics system included
- Admin client included

## Architecture

```
┌─────────────────────────────────────────────────┐
│  Typed Layer (optional)                         │
│  TypedProducer<K,V> / TypedConsumer<K,V>        │
│  ToBytes/FromBytes traits                       │
├─────────────────────────────────────────────────┤
│  Client Layer                                   │
│  KafkaProducer / KafkaConsumer / AdminClient     │
│  Config (typed + string-map bridge)             │
│  RecordAccumulator / Fetcher / GroupCoordinator │
├─────────────────────────────────────────────────┤
│  Network Layer                                  │
│  BrokerConnection (one tokio task per broker)   │
│  ConnectionManager / Metadata                   │
│  Uses proto-kafka for all wire protocol         │
└─────────────────────────────────────────────────┘
```

## Module Structure

```
src/
├── lib.rs
├── error.rs                  # KafkaError enum, Result alias
├── common/
│   ├── mod.rs
│   ├── topic_partition.rs    # TopicPartition, TopicIdPartition
│   ├── node.rs               # Node (broker identity)
│   ├── cluster.rs            # Cluster metadata snapshot
│   ├── header.rs             # RecordHeader
│   └── partition_info.rs     # PartitionInfo
├── config/
│   ├── mod.rs
│   ├── producer_config.rs    # ProducerConfig builder + TryFrom<HashMap>
│   ├── consumer_config.rs    # ConsumerConfig builder + TryFrom<HashMap>
│   ├── admin_config.rs       # AdminConfig builder + TryFrom<HashMap>
│   └── common_config.rs      # Shared config keys/defaults
├── metrics/
│   ├── mod.rs
│   ├── registry.rs           # MetricsRegistry, global metric storage
│   ├── sensor.rs             # Sensor (named measurement point)
│   ├── stat.rs               # Stat trait + impls (Avg, Max, Rate, Count, Percentiles)
│   └── meter.rs              # MetricName, KafkaMetric
├── network/
│   ├── mod.rs
│   ├── broker_connection.rs  # Per-broker tokio task, TCP + length-prefix framing
│   ├── connection_manager.rs # Manages BrokerConnections by broker ID
│   └── metadata.rs           # Metadata fetching, caching, refresh on error
├── producer/
│   ├── mod.rs
│   ├── kafka_producer.rs     # KafkaProducer (bytes API)
│   ├── producer_record.rs    # ProducerRecord
│   ├── record_metadata.rs    # RecordMetadata (ack result)
│   ├── record_accumulator.rs # Batching records by partition
│   ├── sender.rs             # Background task: drain batches, send ProduceRequests
│   ├── partitioner.rs        # Default/round-robin/sticky partitioning
│   ├── idempotent.rs         # ProducerIdManager: InitProducerId, epoch management
│   └── callback.rs           # Callback type alias
├── consumer/
│   ├── mod.rs
│   ├── kafka_consumer.rs     # KafkaConsumer (bytes API)
│   ├── consumer_record.rs    # ConsumerRecord
│   ├── consumer_records.rs   # ConsumerRecords (batch from poll)
│   ├── fetcher.rs            # Fetch request lifecycle
│   ├── subscription_state.rs # Topic/partition subscription tracking
│   ├── offset.rs             # OffsetAndMetadata, OffsetResetStrategy
│   └── group_coordinator.rs  # Join/Sync/Heartbeat/LeaveGroup
├── admin/
│   ├── mod.rs
│   ├── admin_client.rs       # AdminClient
│   ├── create_topics.rs      # CreateTopics operation
│   ├── delete_topics.rs      # DeleteTopics operation
│   ├── describe_topics.rs    # DescribeTopics/metadata query
│   ├── list_offsets.rs       # ListOffsets operation
│   ├── describe_groups.rs    # DescribeGroups operation
│   └── alter_configs.rs      # AlterConfigs/DescribeConfigs
├── serialization/
│   ├── mod.rs
│   ├── traits.rs             # ToBytes/FromBytes traits
│   ├── typed_producer.rs     # TypedProducer<K,V> wrapper
│   ├── typed_consumer.rs     # TypedConsumer<K,V> wrapper
│   └── builtin.rs            # String, Vec<u8>, Bytes, i32, i64 impls
└── main.rs                   # To be removed (library crate)
```

## Network Layer

### BrokerConnection

Each broker gets a dedicated tokio task owning a `TcpStream`. Communication pattern:

```
Client code ──mpsc::Sender──▶ BrokerConnection task ──TcpStream──▶ Broker
                                      │
                              matches responses by
                              correlation_id, sends
                              back via oneshot
```

- Receives `(correlation_id, encoded_request_bytes, oneshot::Sender<Bytes>)` via bounded `mpsc`
- Writes 4-byte length prefix + request bytes to socket
- Reads responses: 4-byte length prefix + body, extracts correlation_id from first 4 bytes of body
- Maintains a `HashMap<i32, oneshot::Sender<Bytes>>` for pending requests
- Reconnects on disconnect with exponential backoff
- Enforces `max.in.flight.requests.per.connection` by limiting the pending map size

### ConnectionManager

- `HashMap<i32, mpsc::Sender<...>>` mapping broker ID → channel to its BrokerConnection task
- `async fn send<R: proto_kafka::protocol::Request>(&self, broker_id: i32, request: R) -> Result<R::Response>` — encodes request via proto-kafka, sends through channel, awaits oneshot, decodes response
- Lazily spawns BrokerConnection tasks on first send to a broker
- Bootstrap: connects to initial `bootstrap.servers` addresses to discover broker IDs

### Metadata

- Stores current `Cluster` snapshot: `HashMap<String, Vec<PartitionInfo>>` (topic → partitions with leader/replica info)
- Background refresh on configurable interval (`metadata.max.age.ms`)
- Force-refresh triggered on `NotLeaderOrFollower`, `UnknownTopicOrPartition`, and similar errors
- `async fn leader_for(&self, tp: &TopicPartition) -> Result<Node>` for routing produce/fetch requests

## Producer

### KafkaProducer

Public API:

```rust
impl KafkaProducer {
    pub async fn new(config: ProducerConfig) -> Result<Self>;
    pub async fn send(&self, record: ProducerRecord) -> Result<oneshot::Receiver<Result<RecordMetadata>>>;
    pub async fn flush(&self) -> Result<()>;
    pub async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>>;
    pub fn metrics(&self) -> &MetricsRegistry;
    pub async fn close(self) -> Result<()>;
}
```

### ProducerRecord / RecordMetadata

```rust
pub struct ProducerRecord {
    pub topic: String,
    pub partition: Option<i32>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
    pub timestamp: Option<i64>,  // millis since epoch
}

pub struct RecordMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub serialized_key_size: i32,
    pub serialized_value_size: i32,
}
```

### RecordAccumulator

- Groups records into `ProducerBatch` per `TopicPartition`
- Each batch backed by a `BytesMut` buffer, bounded by `batch.size`
- Records appended as proto-kafka `Record` structs, encoded into batch via `RecordBatchEncoder`
- Tracks `linger.ms` deadline per batch — batch is "ready" when full OR linger timer expires
- `drain(ready_nodes)` returns batches grouped by destination broker
- Uses a `tokio::sync::Notify` or channel to wake the Sender when batches become ready

### Sender

Background tokio task spawned on producer creation:

1. Polls `RecordAccumulator` for ready batches (respecting linger.ms)
2. Groups batches by destination broker (from metadata leader mapping)
3. For each broker, builds a `ProduceRequest` containing all its batches
4. Sends via `ConnectionManager`, handles response:
   - Success → completes oneshot callbacks with `RecordMetadata`
   - Retriable error → re-enqueues batch (respecting `retries` and `retry.backoff.ms`)
   - Fatal error → completes oneshot callbacks with error
5. For idempotent producer: includes producer_id and epoch in ProduceRequest, tracks sequence numbers per partition

### Idempotent Producer

- `ProducerIdManager` sends `InitProducerIdRequest` on startup to obtain `(producer_id, epoch)`
- Maintains per-partition sequence numbers, incremented with each batch
- Sets `ProduceRequest` fields: `producer_id`, `producer_epoch`, `first_sequence` on each batch
- On `OutOfOrderSequenceNumber`: resets sequence and retries
- On `InvalidProducerEpoch` / `ProducerFenced`: propagates fatal error
- Enabled when `enable.idempotence = true` (default true, matching Java 3.0+)

### Partitioner

- `trait Partitioner: Send + Sync { fn partition(&self, topic: &str, key: Option<&[u8]>, value: Option<&[u8]>, cluster: &Cluster) -> i32; }`
- `DefaultPartitioner`: murmur2 hash of key if present, else sticky round-robin (mirrors Java's `BuiltInPartitioner`)
- Configurable via `ProducerConfig`

## Consumer

### KafkaConsumer

Public API:

```rust
impl KafkaConsumer {
    pub async fn new(config: ConsumerConfig) -> Result<Self>;
    pub fn subscribe(&self, topics: &[&str]) -> Result<()>;
    pub fn assign(&self, partitions: &[TopicPartition]) -> Result<()>;
    pub fn unsubscribe(&self) -> Result<()>;
    pub async fn poll(&self, timeout: Duration) -> Result<ConsumerRecords>;
    pub async fn commit_sync(&self) -> Result<()>;
    pub async fn commit_sync_offsets(&self, offsets: HashMap<TopicPartition, OffsetAndMetadata>) -> Result<()>;
    pub fn commit_async(&self, callback: Option<OffsetCommitCallback>) -> Result<()>;
    pub fn seek(&self, partition: &TopicPartition, offset: i64) -> Result<()>;
    pub fn seek_to_beginning(&self, partitions: &[TopicPartition]) -> Result<()>;
    pub fn seek_to_end(&self, partitions: &[TopicPartition]) -> Result<()>;
    pub async fn position(&self, partition: &TopicPartition) -> Result<i64>;
    pub async fn committed(&self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, OffsetAndMetadata>>;
    pub fn assignment(&self) -> HashSet<TopicPartition>;
    pub fn subscription(&self) -> HashSet<String>;
    pub async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>>;
    pub fn pause(&self, partitions: &[TopicPartition]) -> Result<()>;
    pub fn resume(&self, partitions: &[TopicPartition]) -> Result<()>;
    pub fn paused(&self) -> HashSet<TopicPartition>;
    pub fn metrics(&self) -> &MetricsRegistry;
    pub async fn close(self) -> Result<()>;
    pub fn wakeup(&self);
}
```

### ConsumerRecord / ConsumerRecords

```rust
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

pub struct ConsumerRecords {
    records: HashMap<TopicPartition, Vec<ConsumerRecord>>,
}

impl ConsumerRecords {
    pub fn is_empty(&self) -> bool;
    pub fn count(&self) -> usize;
    pub fn records_for(&self, tp: &TopicPartition) -> &[ConsumerRecord];
    pub fn partitions(&self) -> HashSet<&TopicPartition>;
    pub fn iter(&self) -> impl Iterator<Item = &ConsumerRecord>;
}
```

### Fetcher

- Sends `FetchRequest` to each partition leader with current fetch position
- Decodes responses using `proto_kafka::records::RecordBatchDecoder`
- Manages fetch sessions (`fetch.session.id`, `fetch.session.epoch`) for incremental fetches
- Respects `max.poll.records`, `fetch.min.bytes`, `fetch.max.bytes`, `fetch.max.wait.ms`
- Completed fetches queued internally; `poll()` drains them

### GroupCoordinator

Manages consumer group lifecycle:

1. `FindCoordinator` → discovers group coordinator broker
2. `JoinGroup` → sends subscription, receives assignment protocol selection
3. `SyncGroup` → leader assigns partitions (range assignor), all members receive assignment
4. `Heartbeat` → background tokio task at `heartbeat.interval.ms`; detects rebalance triggers
5. `LeaveGroup` → on consumer close
6. `OffsetCommit` / `OffsetFetch` → commit and retrieve committed offsets through coordinator

### SubscriptionState

- Tracks subscription mode: topics (with auto-assign) or manual partition assignment
- Per-partition state: fetch position, committed offset, reset strategy, pause state
- Triggers metadata refresh when subscribed topics change

## Admin Client

### AdminClient

```rust
impl AdminClient {
    pub async fn new(config: AdminConfig) -> Result<Self>;
    pub async fn create_topics(&self, topics: &[NewTopic]) -> Result<Vec<TopicResult>>;
    pub async fn delete_topics(&self, topics: &[&str]) -> Result<Vec<TopicResult>>;
    pub async fn list_topics(&self) -> Result<Vec<TopicListing>>;
    pub async fn describe_topics(&self, topics: &[&str]) -> Result<Vec<TopicDescription>>;
    pub async fn describe_cluster(&self) -> Result<ClusterDescription>;
    pub async fn list_consumer_groups(&self) -> Result<Vec<ConsumerGroupListing>>;
    pub async fn describe_consumer_groups(&self, groups: &[&str]) -> Result<Vec<ConsumerGroupDescription>>;
    pub async fn list_offsets(&self, offsets: HashMap<TopicPartition, OffsetSpec>) -> Result<HashMap<TopicPartition, ListOffsetsResult>>;
    pub async fn describe_configs(&self, resources: &[ConfigResource]) -> Result<Vec<ConfigEntry>>;
    pub async fn alter_configs(&self, configs: HashMap<ConfigResource, Vec<ConfigEntry>>) -> Result<()>;
    pub async fn close(self) -> Result<()>;
}
```

Shares `ConnectionManager` and `Metadata` infrastructure with producer/consumer. Each operation maps to a single Kafka API request/response pair through proto-kafka.

## Metrics System

Mirrors Java's metrics architecture:

- **MetricsRegistry**: Central storage, holds all sensors and metrics. Thread-safe via `DashMap` or `RwLock<HashMap>`.
- **Sensor**: Named measurement point (e.g., `"record-send-rate"`). Has a list of `Stat` instances that get updated on each `record(value)` call.
- **Stat trait**: `fn record(&mut self, value: f64, time_ms: i64)` and `fn measure(&self, time_ms: i64) -> f64`. Built-in impls: `Avg`, `Max`, `Min`, `Count`, `Rate`, `WindowedSum`, `Percentiles`.
- **KafkaMetric**: Named metric (group + name + tags) backed by a Stat. Queryable via `MetricsRegistry::metrics()`.

Producer metrics: record-send-rate, record-send-total, byte-rate, batch-size-avg, record-queue-time-avg, request-latency-avg, etc.
Consumer metrics: records-consumed-rate, bytes-consumed-rate, fetch-latency-avg, poll-rate, commit-latency-avg, etc.
Network metrics: request-rate, response-rate, connection-count, etc.

## Serialization Layer

```rust
/// Convert a typed value to bytes for producing
pub trait ToBytes {
    fn to_bytes(&self, topic: &str) -> Result<Bytes>;
}

/// Convert bytes to a typed value for consuming
pub trait FromBytes: Sized {
    fn from_bytes(topic: &str, data: &[u8]) -> Result<Self>;
}

/// Built-in impls: String, Vec<u8>, Bytes, i32, i64, f64
```

`TypedProducer<K: ToBytes, V: ToBytes>` wraps `KafkaProducer`, serializing key/value before delegating to `send()`.
`TypedConsumer<K: FromBytes, V: FromBytes>` wraps `KafkaConsumer`, deserializing records after `poll()`.

## Config System

Each config struct has:
1. A builder with typed fields and defaults matching Java's defaults
2. `impl TryFrom<HashMap<String, String>>` for string-map bridge with runtime validation
3. Validation on `build()` / `try_from()` (e.g., bootstrap.servers required, acks in {0,1,all})

Common defaults preserved from Java: `batch.size=16384`, `linger.ms=0`, `acks=all`, `retries=i32::MAX`, `fetch.min.bytes=1`, `auto.offset.reset=latest`, `enable.auto.commit=true`, `enable.idempotence=true`.

## Error Handling

```rust
pub enum KafkaError {
    /// Wire-level Kafka error from broker response
    Response(proto_kafka::ResponseError),
    /// Protocol encoding/decoding failure
    Protocol(proto_kafka::ProtoError),
    /// Network I/O error
    Io(std::io::Error),
    /// Invalid configuration
    Config(String),
    /// Serialization/deserialization failure
    Serialization(String),
    /// Operation timed out
    Timeout,
    /// Consumer wakeup signal
    WakeupException,
    /// Record too large for configured batch.size / max.request.size
    RecordTooLarge { size: usize, limit: usize },
    /// Broker connection failure
    ConnectionFailed { broker_id: i32, message: String },
    /// Group coordinator error (rebalance, unknown member, etc.)
    GroupCoordinator(String),
    /// Producer fenced (idempotent producer epoch conflict)
    ProducerFenced,
}

pub type Result<T> = std::result::Result<T, KafkaError>;
```

## Deferred (tracked in TODO.md)

- Transactions (initTransactions, beginTransaction, commitTransaction, abortTransaction, sendOffsetsToTransaction)
- SSL/TLS connections
- SASL authentication
- Producer interceptors / Consumer interceptors
- Cooperative sticky assignor (range assignor only initially)
- Share consumer (KafkaShareConsumer)
- Custom compression plugins
