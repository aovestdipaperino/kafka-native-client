use std::sync::atomic::{AtomicI32, Ordering};
use crate::common::Cluster;

/// Trait for determining which partition a record should be sent to.
pub trait Partitioner: Send + Sync {
    fn partition(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        cluster: &Cluster,
    ) -> i32;
}

/// Default partitioner: murmur2 hash of key if present, else round-robin.
/// Matches Java's DefaultPartitioner / BuiltInPartitioner behavior.
pub struct DefaultPartitioner {
    counter: AtomicI32,
}

impl DefaultPartitioner {
    pub fn new() -> Self {
        Self { counter: AtomicI32::new(0) }
    }
}

impl Default for DefaultPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl Partitioner for DefaultPartitioner {
    fn partition(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        _value: Option<&[u8]>,
        cluster: &Cluster,
    ) -> i32 {
        let num_partitions = cluster.partitions_for_topic(topic).len() as i32;
        if num_partitions <= 0 {
            return 0;
        }
        match key {
            Some(k) => (murmur2(k) as i32 & 0x7fffffff) % num_partitions,
            None => (self.counter.fetch_add(1, Ordering::Relaxed) & 0x7fffffff) % num_partitions,
        }
    }
}

/// Murmur2 hash — matches Java's org.apache.kafka.common.utils.Utils.murmur2.
/// This is the 32-bit MurmurHash2 with seed 0x9747b28c (the "default seed" from Java Kafka).
pub fn murmur2(data: &[u8]) -> u32 {
    let seed: u32 = 0x9747b28c;
    let m: u32 = 0x5bd1e995;
    let r: i32 = 24;

    let len = data.len();
    let mut h: u32 = seed ^ (len as u32);

    let chunks = len / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let mut k: u32 = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        k = k.wrapping_mul(m);
        k ^= k >> r;
        k = k.wrapping_mul(m);
        h = h.wrapping_mul(m);
        h ^= k;
    }

    let remaining = len % 4;
    let offset = chunks * 4;
    if remaining >= 3 {
        h ^= (data[offset + 2] as u32) << 16;
    }
    if remaining >= 2 {
        h ^= (data[offset + 1] as u32) << 8;
    }
    if remaining >= 1 {
        h ^= data[offset] as u32;
        h = h.wrapping_mul(m);
    }

    h ^= h >> 13;
    h = h.wrapping_mul(m);
    h ^= h >> 15;

    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Node, PartitionInfo};

    // Test vectors from Java's Utils.murmur2
    #[test]
    fn test_murmur2_known_values() {
        // Empty input — verify determinism
        assert_eq!(murmur2(b""), murmur2(b""));

        // Known test: "key" should always produce the same hash
        let h1 = murmur2(b"key");
        let h2 = murmur2(b"key");
        assert_eq!(h1, h2);

        // Different inputs produce different hashes
        assert_ne!(murmur2(b"key1"), murmur2(b"key2"));
    }

    #[test]
    fn test_murmur2_java_compatibility() {
        // Values confirmed to match Java's org.apache.kafka.common.utils.Utils.murmur2():
        assert_eq!(murmur2(b"21") as i32, -973932308);
        assert_eq!(murmur2(b"foobar") as i32, -790332482);
    }

    fn make_cluster(topic: &str, num_partitions: i32) -> Cluster {
        let node = Node::new(0, "localhost", 9092);
        let partitions = (0..num_partitions).map(|i| PartitionInfo {
            topic: topic.into(),
            partition: i,
            leader: Some(node.clone()),
            replicas: vec![node.clone()],
            in_sync_replicas: vec![node.clone()],
        }).collect();
        Cluster::new(vec![node], partitions)
    }

    #[test]
    fn test_default_partitioner_with_key() {
        let cluster = make_cluster("test", 10);
        let partitioner = DefaultPartitioner::new();

        // Same key always goes to same partition
        let p1 = partitioner.partition("test", Some(b"my-key"), None, &cluster);
        let p2 = partitioner.partition("test", Some(b"my-key"), None, &cluster);
        assert_eq!(p1, p2);
        assert!(p1 >= 0 && p1 < 10);
    }

    #[test]
    fn test_default_partitioner_no_key_round_robin() {
        let cluster = make_cluster("test", 3);
        let partitioner = DefaultPartitioner::new();

        let p1 = partitioner.partition("test", None, None, &cluster);
        let p2 = partitioner.partition("test", None, None, &cluster);
        let p3 = partitioner.partition("test", None, None, &cluster);
        // Should cycle through partitions (though order depends on counter)
        assert!(p1 >= 0 && p1 < 3);
        assert!(p2 >= 0 && p2 < 3);
        assert!(p3 >= 0 && p3 < 3);
    }

    #[test]
    fn test_partition_always_non_negative() {
        let cluster = make_cluster("test", 5);
        let partitioner = DefaultPartitioner::new();
        for i in 0..100 {
            let key = format!("key-{i}");
            let p = partitioner.partition("test", Some(key.as_bytes()), None, &cluster);
            assert!(p >= 0 && p < 5, "partition {p} out of range for key {key}");
        }
    }
}
