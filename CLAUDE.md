# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Rust reimplementation of the Apache Kafka client (consumer and producer), based on the official Java client source code from Apache Kafka 4.2.0. The goal is a native Rust Kafka client that follows the same architecture and protocol as the Java original.

## Build Commands

```bash
cargo build              # Build the project
cargo test               # Run all tests
cargo test <test_name>   # Run a single test
cargo clippy             # Lint
cargo fmt                # Format code
cargo fmt -- --check     # Check formatting without modifying
```

Uses Rust edition 2024.

## Java Reference Sources

`java-sources/` contains the Apache Kafka 4.2.0 Java client source code (~1,291 files) used as the reference implementation. These are read-only reference files, not build targets.

### Java Client Architecture (what we are porting)

**Producer pipeline** (`java-sources/producer/`):
- `KafkaProducer` -> serializes records -> `RecordAccumulator` (batches by partition) -> `Sender` (background thread) -> `NetworkClient` -> broker
- `RecordAccumulator` manages a `BufferPool` of reusable `ByteBuffer`s and groups records into `ProducerBatch`es per partition
- `Sender` drains ready batches, groups them by destination broker node, builds `ProduceRequest`s, and handles retries
- `TransactionManager` coordinates exactly-once semantics (producer IDs, epochs, transaction lifecycle)
- `BuiltInPartitioner` implements adaptive partitioning (sticky + load-based switching)

**Consumer pipeline** (`java-sources/consumer/`):
- `KafkaConsumer` delegates to `ConsumerDelegate` implementations: `AsyncKafkaConsumer` (new, event-driven) or `ClassicKafkaConsumer` (legacy, blocking)
- `AsyncKafkaConsumer` uses a `ConsumerNetworkThread` with an event queue for non-blocking operation
- Group coordination: `ConsumerCoordinator` / `ConsumerHeartbeatRequestManager` handle group membership, rebalancing, and offset commits
- `SubscriptionState` tracks topic subscriptions, partition assignments, and fetch positions
- `AbstractFetch` / `CompletedFetch` manage the fetch request lifecycle and record deserialization

**Shared networking layer** (`java-sources/` root + `network/` + `requests/`):
- `NetworkClient` implements `KafkaClient` interface: manages connections, in-flight requests, metadata updates, and API version negotiation
- `Selector` wraps Java NIO for non-blocking I/O; `KafkaChannel` abstracts per-connection state (plaintext, SSL, SASL)
- `requests/` contains all Kafka protocol request/response types (one pair per API key)

**Wire protocol** (`protocol/` + `record/`):
- `ApiKeys` enumerates all Kafka API operations with version ranges
- `record/` implements the Kafka record batch format: `DefaultRecordBatch` (v2, current) and `LegacyRecord` (v0/v1)
- Records use varints, zigzag encoding, and delta-encoding for timestamps/offsets within a batch

**Serialization** (`serialization/`): `Serializer`/`Deserializer` interfaces with built-in implementations for primitives, strings, byte arrays, UUIDs, etc.

### Key design patterns in the Java source to preserve in Rust
- **Correlation IDs**: every request gets a monotonic ID; responses are matched by ID
- **In-flight request limiting**: `max.in.flight.requests.per.connection` bounds concurrent requests per broker
- **Metadata-driven routing**: producer and consumer resolve topic partitions to broker nodes via cached `Cluster` metadata, refreshed on errors or timer
- **Completion callbacks**: producer `send()` returns a `Future` and accepts a `Callback`; consumer uses polling
- **Config-driven**: both clients are heavily parameterized via `ProducerConfig`/`ConsumerConfig` with typed defaults and validation
