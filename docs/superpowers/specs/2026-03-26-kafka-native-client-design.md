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

Each client instance (producer, consumer, admin) creates its own `ConnectionManager` and `Metadata`. They reuse the same code but do not share runtime state.

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
│   ├── record_accumulator.rs # Batching records by partition, BufferPool
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
│   ├── offset.rs             # OffsetAndMetadata, OffsetResetStrategy, OffsetAndTimestamp
│   ├── group_coordinator.rs  # Join/Sync/Heartbeat/LeaveGroup
│   └── rebalance_listener.rs # ConsumerRebalanceListener trait
├── admin/
│   ├── mod.rs
│   ├── admin_client.rs       # AdminClient
│   ├── types.rs              # NewTopic, TopicResult, TopicListing, etc.
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
- On connect: sends `ApiVersionsRequest` to negotiate supported protocol versions per API key; stores version ranges for use by `ConnectionManager` when encoding requests

**In-flight request admission control**: A `tokio::sync::Semaphore` with `max.in.flight.requests.per.connection` permits is owned by the `BrokerConnection`. The `ConnectionManager` acquires a permit *before* sending into the mpsc channel, and the `BrokerConnection` releases the permit when the response arrives. This ensures back-pressure propagates to callers and guarantees ordering when `max.in.flight = 1` (required for idempotent producer ordering).

**Throttle handling**: When a response contains `throttle_time_ms > 0`, the BrokerConnection delays reading from the mpsc channel for that duration, honoring broker-side quota enforcement.

### ConnectionManager

- `HashMap<i32, mpsc::Sender<...>>` mapping broker ID → channel to its BrokerConnection task
- `async fn send<R: proto_kafka::protocol::Request>(&self, broker_id: i32, request: R) -> Result<R::Response>` — acquires in-flight semaphore permit, encodes request via proto-kafka (using negotiated API version), sends through channel, awaits oneshot, decodes response
- Lazily spawns BrokerConnection tasks on first send to a broker
- Bootstrap: connects to initial `bootstrap.servers` addresses to discover broker IDs
- Stores per-broker `ApiVersions` (populated after each BrokerConnection handshake)

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
    pub fn send(&self, record: ProducerRecord, callback: Option<Callback>) -> Result<SendFuture>;
    pub async fn flush(&self) -> Result<()>;
    pub async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>>;
    pub fn metrics(&self) -> &MetricsRegistry;
    pub async fn close(self, timeout: Duration) -> Result<()>;
}
```

`send()` is a non-async method that appends to the accumulator and returns a `SendFuture` (wrapping the internal oneshot). The `SendFuture` resolves to `Result<RecordMetadata>` when the broker acks the batch. If a `Callback` is provided, it is invoked when the response arrives (before the future resolves). If accumulator memory is exhausted, `send()` blocks for up to `max.block.ms` (via a tokio semaphore with timeout) before returning `Err(KafkaError::BufferExhausted)`.

`close(timeout)` flushes remaining batches and shuts down the Sender task within the given timeout.

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
- Compression applied per-batch according to `compression.type` config, using proto-kafka's built-in compression support
- Tracks `linger.ms` deadline per batch — batch is "ready" when full OR linger timer expires
- `drain(ready_nodes)` returns batches grouped by destination broker
- Uses a `tokio::sync::Notify` or channel to wake the Sender when batches become ready

**Memory management**: Total memory across all batches is bounded by `buffer.memory` (default 32MB). Implemented as a `tokio::sync::Semaphore` where each permit represents one byte. `send()` acquires `record_size` permits with a timeout of `max.block.ms`. If the semaphore times out, returns `Err(KafkaError::BufferExhausted)`. Permits are released when a batch is completed (acked or failed).

### Sender

Background tokio task spawned on producer creation:

1. Polls `RecordAccumulator` for ready batches (respecting `linger.ms`)
2. Groups batches by destination broker (from metadata leader mapping)
3. For each broker, builds a `ProduceRequest` containing all its batches (respecting `max.request.size`)
4. Sends via `ConnectionManager`, handles response:
   - Success → completes callbacks/futures with `RecordMetadata`
   - Retriable error → re-enqueues batch (respecting `retries`, `retry.backoff.ms`, and `delivery.timeout.ms`)
   - Fatal error → completes callbacks/futures with error
5. **Batch expiration**: batches older than `delivery.timeout.ms` are failed without sending
6. For idempotent producer: includes `producer_id` and `epoch` in ProduceRequest, tracks sequence numbers per partition

### Idempotent Producer

- `ProducerIdManager` sends `InitProducerIdRequest` on startup to obtain `(producer_id, epoch)`
- Maintains per-partition sequence numbers, incremented with each batch
- Sets `ProduceRequest` fields: `producer_id`, `producer_epoch`, `first_sequence` on each batch
- On `OutOfOrderSequenceNumber`: fails all in-flight batches for that partition, re-initializes producer ID if needed. Ordering guarantees depend on `max.in.flight.requests.per.connection` (must be 1 for strict ordering).
- On `InvalidProducerEpoch` / `ProducerFenced`: propagates fatal `KafkaError::ProducerFenced`, no further sends allowed
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
    pub fn subscribe(&self, topics: &[&str], listener: Option<Box<dyn ConsumerRebalanceListener>>) -> Result<()>;
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
    pub async fn offsets_for_times(&self, timestamps: HashMap<TopicPartition, i64>) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>>;
    pub async fn beginning_offsets(&self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, i64>>;
    pub async fn end_offsets(&self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, i64>>;
    pub fn current_lag(&self, partition: &TopicPartition) -> Option<i64>;
    pub fn assignment(&self) -> HashSet<TopicPartition>;
    pub fn subscription(&self) -> HashSet<String>;
    pub async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>>;
    pub fn pause(&self, partitions: &[TopicPartition]) -> Result<()>;
    pub fn resume(&self, partitions: &[TopicPartition]) -> Result<()>;
    pub fn paused(&self) -> HashSet<TopicPartition>;
    pub fn group_metadata(&self) -> ConsumerGroupMetadata;
    pub fn metrics(&self) -> &MetricsRegistry;
    pub async fn close(self, timeout: Duration) -> Result<()>;
    pub fn wakeup(&self);
}
```

`subscribe()` is a local state change only — it records the subscription in `SubscriptionState`. Actual group join and partition assignment happen inside `poll()`.

`close(timeout)` commits offsets (if auto-commit enabled), sends `LeaveGroup`, and shuts down within the given timeout.

### ConsumerRebalanceListener

```rust
pub trait ConsumerRebalanceListener: Send + Sync {
    fn on_partitions_assigned(&self, partitions: &[TopicPartition]);
    fn on_partitions_revoked(&self, partitions: &[TopicPartition]);
    fn on_partitions_lost(&self, partitions: &[TopicPartition]);
}
```

Invoked by the `GroupCoordinator` during rebalance (inside `poll()`). `on_partitions_revoked` is called before partitions are reassigned (opportunity to commit offsets). `on_partitions_assigned` is called after new assignment. `on_partitions_lost` is called when partitions are lost due to session timeout (no opportunity for clean commit).

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
2. `JoinGroup` → sends subscription (including `group.instance.id` for static membership if configured), receives assignment protocol selection
3. `SyncGroup` → leader assigns partitions (range assignor), all members receive assignment
4. Invokes `ConsumerRebalanceListener`: `on_partitions_revoked` before reassignment, `on_partitions_assigned` after
5. `Heartbeat` → background tokio task at `heartbeat.interval.ms`; detects rebalance triggers
6. `LeaveGroup` → on consumer close (immediate for static members unless `group.instance.id` is set, in which case session-based leave)
7. `OffsetCommit` / `OffsetFetch` → commit and retrieve committed offsets through coordinator
8. **Auto-commit**: when `enable.auto.commit=true`, commits current offsets every `auto.commit.interval.ms` during `poll()`. Auto-commit is async (fire-and-forget). On rebalance, a synchronous commit is attempted before revoking partitions.

### poll() Internal State Machine

`poll(timeout)` drives the entire consumer lifecycle:

1. Check for wakeup signal → return `Err(KafkaError::WakeupException)` if set
2. Check subscription state → if subscribed topics changed, trigger metadata refresh
3. **Coordinator**: ensure coordinator is known (send `FindCoordinator` if needed)
4. **Group membership**: if not in a group (or rebalance triggered), execute join/sync protocol, invoke rebalance listener
5. **Fetch positions**: for newly assigned partitions, fetch committed offsets (or apply auto.offset.reset)
6. **Auto-commit**: if interval elapsed, fire async offset commit
7. **Send fetch requests**: for each assigned partition with a valid position, send `FetchRequest` to the leader (if not already in-flight)
8. **Await responses**: wait for fetch responses up to remaining timeout
9. **Decode & return**: decode record batches, respect `max.poll.records`, return `ConsumerRecords`

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

Creates its own `ConnectionManager` and `Metadata` instances (same code as producer/consumer, separate runtime state). Each operation maps to a single Kafka API request/response pair through proto-kafka.

### Admin Result Types

Each admin operation returns per-resource results that can independently succeed or fail:

```rust
pub struct TopicResult {
    pub name: String,
    pub error: Option<KafkaError>,
}

pub struct TopicListing {
    pub name: String,
    pub topic_id: Uuid,
    pub is_internal: bool,
}

pub struct TopicDescription {
    pub name: String,
    pub topic_id: Uuid,
    pub is_internal: bool,
    pub partitions: Vec<TopicPartitionInfo>,
}

pub struct TopicPartitionInfo {
    pub partition: i32,
    pub leader: Node,
    pub replicas: Vec<Node>,
    pub isr: Vec<Node>,
}
```

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
4. Validation constraint: `delivery.timeout.ms >= request.timeout.ms + linger.ms`

### Producer Config Defaults

| Key | Default | Notes |
|-----|---------|-------|
| `bootstrap.servers` | (required) | |
| `acks` | `all` | 0, 1, or all |
| `batch.size` | 16384 | bytes |
| `linger.ms` | 5 | Kafka 4.0+ default |
| `buffer.memory` | 33554432 | 32MB total accumulator memory |
| `max.block.ms` | 60000 | max time send() blocks when buffer full |
| `max.request.size` | 1048576 | max size of a ProduceRequest |
| `delivery.timeout.ms` | 120000 | upper bound on send() to ack time |
| `request.timeout.ms` | 30000 | per-request network timeout |
| `retries` | 2147483647 | bounded by delivery.timeout.ms |
| `retry.backoff.ms` | 100 | |
| `max.in.flight.requests.per.connection` | 5 | set to 1 for strict ordering |
| `compression.type` | `none` | none, gzip, snappy, lz4, zstd |
| `enable.idempotence` | true | |
| `metadata.max.age.ms` | 300000 | |
| `client.id` | `""` | |

### Consumer Config Defaults

| Key | Default | Notes |
|-----|---------|-------|
| `bootstrap.servers` | (required) | |
| `group.id` | (required for subscribe) | |
| `group.instance.id` | `None` | static membership |
| `auto.offset.reset` | `latest` | latest, earliest, none |
| `enable.auto.commit` | true | |
| `auto.commit.interval.ms` | 5000 | |
| `fetch.min.bytes` | 1 | |
| `fetch.max.bytes` | 52428800 | |
| `fetch.max.wait.ms` | 500 | |
| `max.poll.records` | 500 | |
| `max.partition.fetch.bytes` | 1048576 | |
| `session.timeout.ms` | 45000 | |
| `heartbeat.interval.ms` | 3000 | |
| `request.timeout.ms` | 30000 | |
| `metadata.max.age.ms` | 300000 | |
| `client.id` | `""` | |

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
    /// Record too large for configured max.request.size
    RecordTooLarge { size: usize, limit: usize },
    /// Producer accumulator memory exhausted (buffer.memory exceeded for max.block.ms)
    BufferExhausted,
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
