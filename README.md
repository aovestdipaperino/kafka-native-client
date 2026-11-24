# kafka-native-client

[![GitHub stars](https://img.shields.io/github/stars/aovestdipaperino/kafka-native-client?logo=github&color=brightgreen)](https://github.com/aovestdipaperino/kafka-native-client/stargazers)

A native Rust Kafka client (producer, consumer, admin) with high fidelity to the official Apache Kafka Java client.

It is async-only (tokio), uses [`proto-kafka`](https://crates.io/crates/proto-kafka) for wire protocol serialization, and mirrors the Java client's semantics behind an idiomatic Rust API.

**Status:** early development (version 0.0.1).

## Layout

| Module | Contents |
|---|---|
| `producer` | Producer with an idempotent mode |
| `consumer` | Consumer, fetcher, group coordinator, subscription state, rebalance listener |
| `admin` | Admin client |
| `config` | Typed configuration with a string-map bridge |
| `network` | One tokio task per broker connection, metadata handling |
| `metrics` | Sensors, meters and a registry modelled on the Java client |
| `serialization` | Bytes-only core with optional typed wrappers |

The design is written up in [`docs/superpowers/specs`](docs/superpowers/specs/2026-03-26-kafka-native-client-design.md).

## Not implemented yet

Transactions, SSL/TLS and SASL authentication, producer and consumer interceptors, cooperative sticky and custom partition assignors, share consumer, custom compression plugins and regex subscription. See [`TODO.md`](TODO.md).

## License

Dual licensed under MIT or Apache-2.0, at your option.
