//! A native Rust Kafka client (producer, consumer, admin) with high fidelity
//! to the official Apache Kafka Java client.
//!
//! Built on [`proto-kafka`](https://crates.io/crates/proto-kafka) for wire protocol
//! serialization/deserialization.

pub mod admin;
pub mod common;
pub mod config;
pub mod consumer;
pub mod error;
pub mod metrics;
pub mod network;
pub mod producer;
pub mod serialization;

pub use error::{KafkaError, Result};
